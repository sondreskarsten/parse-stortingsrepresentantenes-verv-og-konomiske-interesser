"""Async sync engine for Stortinget register PDFs.

Pipeline:
    1. DISCOVER — Scan all dates in the configured year range, HEAD each
       candidate URL to find which PDFs exist on the Stortinget server.
    2. DIFF — Compare discovered URLs against the manifest to find missing PDFs.
    3. DOWNLOAD — Fetch missing PDFs and write to storage.
    4. MANIFEST — Update manifest with new records.

The engine supports graceful shutdown via max_runtime_minutes.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, date, datetime

import aiohttp
import structlog
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from stortinget_register.checkpoint import CheckpointManager, CheckpointState
from stortinget_register.config import Settings
from stortinget_register.discovery import build_candidate_urls, generate_date_range
from stortinget_register.manifest import ManifestManager, ManifestRecord
from stortinget_register.storage import StorageBackend

logger = structlog.get_logger()

RETRYABLE_ERRORS = (
    aiohttp.ServerDisconnectedError,
    asyncio.TimeoutError,
    ConnectionError,
)

RETRYABLE_HTTP_STATUSES = {429, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, RETRYABLE_ERRORS):
        return True
    if isinstance(exc, aiohttp.ClientResponseError) and exc.status in RETRYABLE_HTTP_STATUSES:
        return True
    return False


def _before_retry_log(retry_state: RetryCallState) -> None:
    logger.warning(
        "retrying_request",
        attempt=retry_state.attempt_number,
        exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
    )


class SyncEngine:
    """Orchestrates discover → diff → download pipeline."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._checkpoint_mgr = CheckpointManager(self._storage, settings.checkpoint_path)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._start_time = time.monotonic()
        self._shutdown_requested = False
        self._stats = {"discovered": 0, "downloaded": 0, "skipped": 0, "failed": 0}

    def _time_remaining(self) -> float | None:
        if self._settings.max_runtime_minutes <= 0:
            return None
        elapsed = time.monotonic() - self._start_time
        limit = self._settings.max_runtime_minutes * 60
        return max(0.0, limit - elapsed)

    def _should_shutdown(self) -> bool:
        remaining = self._time_remaining()
        if remaining is not None and remaining < 60:
            return True
        return self._shutdown_requested

    async def run(self) -> None:
        self._storage.check_credentials()
        state = self._checkpoint_mgr.load()
        state.run_started_at = self._now_iso()

        logger.info(
            "sync_started",
            storage=self._settings.storage_path,
            scan_range=f"{self._settings.scan_start_year}-{self._settings.scan_end_year}",
        )

        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            discovered = await self._discover(session, state)

            if self._should_shutdown():
                self._checkpoint_mgr.save(state)
                logger.info("graceful_shutdown", phase="discover", **self._stats)
                return

            existing_urls = self._manifest.get_downloaded_urls()
            missing = [d for d in discovered if d["url"] not in existing_urls]

            logger.info(
                "diff_complete",
                total_discovered=len(discovered),
                already_downloaded=len(existing_urls),
                to_download=len(missing),
            )

            if missing:
                await self._download_missing(session, missing, state)

        self._checkpoint_mgr.clear()
        logger.info("sync_finished", **self._stats)

    async def _discover(
        self,
        session: aiohttp.ClientSession,
        state: CheckpointState,
    ) -> list[dict]:
        end_year = self._settings.scan_end_year or date.today().year
        start = date(self._settings.scan_start_year, 1, 1)
        end = date(end_year, 12, 31)
        if end > date.today():
            end = date.today()

        all_dates = generate_date_range(start, end)

        if state.last_date_scanned:
            resume_date = date.fromisoformat(state.last_date_scanned)
            all_dates = [d for d in all_dates if d > resume_date]

        logger.info("discover_start", dates_to_scan=len(all_dates))

        discovered: list[dict] = []
        batch_size = 50

        for i in range(0, len(all_dates), batch_size):
            if self._should_shutdown():
                break

            batch = all_dates[i : i + batch_size]
            tasks = [self._check_date(session, d) for d in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for d, result in zip(batch, results):
                state.dates_scanned += 1
                state.last_date_scanned = d.isoformat()

                if isinstance(result, Exception):
                    logger.warning("discover_error", date=d.isoformat(), error=str(result))
                    continue

                if result:
                    for url in result:
                        folder = url.split("/")[-2]
                        discovered.append({
                            "date": d.isoformat(),
                            "url": url,
                            "period_folder": folder,
                        })
                        state.pdfs_found += 1

            if (i // batch_size + 1) % 10 == 0:
                self._checkpoint_mgr.save(state)
                logger.info(
                    "discover_progress",
                    dates_scanned=state.dates_scanned,
                    total=len(all_dates),
                    found=state.pdfs_found,
                )

        self._stats["discovered"] = len(discovered)
        self._checkpoint_mgr.save(state)
        logger.info("discover_complete", found=len(discovered))
        return discovered

    async def _check_date(self, session: aiohttp.ClientSession, d: date) -> list[str]:
        urls = build_candidate_urls(d)
        hits = []
        for url in urls:
            async with self._semaphore:
                try:
                    async with session.head(url, allow_redirects=True) as resp:
                        if resp.status == 200:
                            hits.append(url)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
        return hits

    async def _download_missing(
        self,
        session: aiohttp.ClientSession,
        missing: list[dict],
        state: CheckpointState,
    ) -> None:
        logger.info("download_start", total=len(missing))
        records_buf: list[ManifestRecord] = []

        for idx, item in enumerate(missing):
            if self._should_shutdown():
                logger.info("graceful_shutdown", phase="download", progress=idx, total=len(missing))
                break

            record = await self._download_pdf(session, item)
            records_buf.append(record)

            if record.status == "success":
                self._stats["downloaded"] += 1
                state.pdfs_downloaded += 1
            else:
                self._stats["failed"] += 1
                state.errors += 1

            if (idx + 1) % 20 == 0 or idx + 1 == len(missing):
                self._manifest.upsert(records_buf)
                records_buf = []
                self._checkpoint_mgr.save(state)
                logger.info(
                    "download_progress",
                    progress=idx + 1,
                    total=len(missing),
                    **self._stats,
                )

        if records_buf:
            self._manifest.upsert(records_buf)
            self._checkpoint_mgr.save(state)

    async def _download_pdf(
        self,
        session: aiohttp.ClientSession,
        item: dict,
    ) -> ManifestRecord:
        url = item["url"]
        d = item["date"]
        folder = item.get("period_folder")
        now = self._now_iso()

        try:
            data = await self._fetch_with_retry(session, url)
        except Exception as exc:
            logger.warning("download_failed", url=url, error=str(exc))
            return ManifestRecord(
                date=d,
                url=url,
                period_folder=folder,
                download_timestamp=now,
                status="failed",
                error_detail=str(exc)[:500],
            )

        file_hash = hashlib.sha256(data).hexdigest()
        pdf_path = self._settings.pdf_path(d)
        self._storage.write_bytes(pdf_path, data)

        return ManifestRecord(
            date=d,
            url=url,
            period_folder=folder,
            pdf_path=pdf_path,
            file_hash=file_hash,
            file_size_bytes=len(data),
            download_timestamp=now,
            status="success",
        )

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
        stop=stop_after_attempt(5),
        before_sleep=_before_retry_log,
        reraise=True,
    )
    async def _fetch_with_retry(self, session: aiohttp.ClientSession, url: str) -> bytes:
        async with self._semaphore:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.read()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()
