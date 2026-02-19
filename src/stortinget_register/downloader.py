"""Async sync engine for Stortinget register PDFs.

Tiered discovery pipeline:
    1. SCRAPE  — Fetch landing page, extract latest PDF link directly.
    2. GAPS    — Compare manifest against expected biweekly cadence.
                 For new gaps, check best-guess dates (Mon-Fri of
                 the expected week).
    3. EXHAUST — For gaps already checked once without a hit, escalate
                 to all weekdays in the gap range.
    4. INITIAL — On first run (empty manifest), scan all weekdays in
                 the configured year range.

After discovery, missing PDFs are downloaded with companion population
snapshots from the Stortinget data API.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
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
from stortinget_register.discovery import (
    LANDING_PAGE,
    GapRecord,
    MissedHypotheses,
    best_guess_dates,
    build_candidate_urls,
    estimate_expected_dates,
    exhaustive_dates,
    initial_scan_dates,
    parse_pdf_url,
)
from stortinget_register.manifest import ManifestManager, ManifestRecord
from stortinget_register.storage import StorageBackend
from stortinget_register.stortinget_api import fetch_population, period_for_date

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
    """Orchestrates tiered discover → diff → download pipeline."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._storage = StorageBackend.from_settings(settings)
        self._manifest = ManifestManager(self._storage, settings.manifest_path)
        self._checkpoint_mgr = CheckpointManager(self._storage, settings.checkpoint_path)
        self._semaphore = asyncio.Semaphore(settings.max_concurrent)
        self._start_time = time.monotonic()
        self._shutdown_requested = False
        self._stats = {"discovered": 0, "downloaded": 0, "skipped": 0, "failed": 0}
        self._population_cache: dict[str, list[dict]] = {}

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

    # --- Missed hypotheses persistence ---

    def _load_missed(self) -> MissedHypotheses:
        path = self._settings.missed_hypotheses_path
        if self._storage.exists(path):
            return MissedHypotheses.from_json(self._storage.read_bytes(path))
        return MissedHypotheses()

    def _save_missed(self, missed: MissedHypotheses) -> None:
        self._storage.write_bytes(self._settings.missed_hypotheses_path, missed.to_json())

    # --- Main entry ---

    async def run(self) -> None:
        self._storage.check_credentials()
        state = self._checkpoint_mgr.load()
        state.run_started_at = self._now_iso()

        logger.info("sync_started", storage=self._settings.storage_path)

        connector = aiohttp.TCPConnector(limit=self._settings.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout, trust_env=True
        ) as session:
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

    # --- Tiered discovery ---

    async def _discover(
        self,
        session: aiohttp.ClientSession,
        state: CheckpointState,
    ) -> list[dict]:
        discovered: list[dict] = []

        # Tier 0: scrape landing page for latest
        scraped = await self._scrape_latest(session)
        if scraped:
            discovered.append(scraped)
            logger.info("scrape_hit", date=scraped["date"], url=scraped["url"])

        known_dates = self._manifest.get_downloaded_dates()

        if not known_dates:
            # First run ever — full initial scan (scraped item included after)
            initial = await self._initial_scan(session, state)
            for item in discovered:
                if item["url"] not in {i["url"] for i in initial}:
                    initial.append(item)
            return initial

        all_known = set(known_dates)
        for item in discovered:
            all_known.add(item["date"])

        sorted_known = sorted(all_known)
        last_known = date.fromisoformat(sorted_known[-1])
        today = date.today()

        # Check for internal gaps (between consecutive known dates)
        has_internal_gaps = False
        for i in range(len(sorted_known) - 1):
            d1 = date.fromisoformat(sorted_known[i])
            d2 = date.fromisoformat(sorted_known[i + 1])
            if (d2 - d1).days > 21:
                has_internal_gaps = True
                break

        has_trailing_gap = (today - last_known).days > 21

        if not has_internal_gaps and not has_trailing_gap:
            logger.info("discover_up_to_date", last_known=last_known.isoformat())
            self._stats["discovered"] = len(discovered)
            return discovered

        # Tier 1+2: gap analysis
        missed = self._load_missed()
        gap_dates = await self._fill_gaps(session, state, sorted_known, today, missed)
        discovered.extend(gap_dates)

        self._stats["discovered"] = len(discovered)
        return discovered

    async def _scrape_latest(self, session: aiohttp.ClientSession) -> dict | None:
        """Fetch landing page and extract the current PDF link."""
        try:
            async with session.get(LANDING_PAGE) as resp:
                if resp.status != 200:
                    logger.warning("scrape_failed", status=resp.status)
                    return None
                html = await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("scrape_error", error=str(exc))
            return None

        parsed = parse_pdf_url(html)
        if not parsed:
            logger.warning("scrape_no_link")
            return None

        url, d, folder = parsed
        return {"date": d.isoformat(), "url": url, "period_folder": folder}

    async def _fill_gaps(
        self,
        session: aiohttp.ClientSession,
        state: CheckpointState,
        sorted_known: list[str],
        today: date,
        missed: MissedHypotheses,
    ) -> list[dict]:
        """Analyze gaps between all consecutive manifest dates and from last to today."""
        pairs: list[tuple[date, date]] = []
        for i in range(len(sorted_known) - 1):
            d1 = date.fromisoformat(sorted_known[i])
            d2 = date.fromisoformat(sorted_known[i + 1])
            if (d2 - d1).days > 21:
                pairs.append((d1, d2))

        last_known = date.fromisoformat(sorted_known[-1])
        if (today - last_known).days > 21:
            pairs.append((last_known, today))

        if not pairs:
            return []

        logger.info("gap_analysis", gaps_found=len(pairs))

        all_dates_to_check: list[date] = []
        gap_tracking: dict[str, tuple[date, date, date]] = {}

        for gap_start, gap_end in pairs:
            expected = estimate_expected_dates(gap_start, gap_end)
            for exp_date in expected:
                gap_key = exp_date.isoformat()
                existing = missed.get_gap(gap_key)

                if existing and existing.check_count >= 1:
                    dates = exhaustive_dates(gap_start, gap_end)
                    already_checked = set(existing.dates_checked)
                    dates = [d for d in dates if d.isoformat() not in already_checked]
                    logger.info(
                        "tier_exhaustive",
                        gap_key=gap_key,
                        check_count=existing.check_count,
                        new_dates=len(dates),
                    )
                else:
                    dates = best_guess_dates(exp_date)
                    dates = [d for d in dates if gap_start < d < gap_end and d <= today]
                    logger.info("tier_best_guess", expected=gap_key, dates=len(dates))

                gap_tracking[gap_key] = (gap_start, gap_end, exp_date)
                all_dates_to_check.extend(dates)

        known_set = set(sorted_known)
        all_dates_to_check = sorted(set(all_dates_to_check))
        all_dates_to_check = [d for d in all_dates_to_check if d.isoformat() not in known_set]

        logger.info("gap_check_start", dates_to_check=len(all_dates_to_check))

        discovered: list[dict] = []
        found_dates: set[str] = set()

        batch_size = 50
        for i in range(0, len(all_dates_to_check), batch_size):
            if self._should_shutdown():
                break
            batch = all_dates_to_check[i : i + batch_size]
            tasks = [self._check_date(session, d) for d in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for d, result in zip(batch, results):
                state.dates_scanned += 1
                if isinstance(result, Exception):
                    continue
                if result:
                    for url in result:
                        folder = url.split("/")[-2]
                        discovered.append({
                            "date": d.isoformat(),
                            "url": url,
                            "period_folder": folder,
                        })
                        found_dates.add(d.isoformat())
                        state.pdfs_found += 1

        for gap_key, (gap_start, gap_end, exp_date) in gap_tracking.items():
            existing = missed.get_gap(gap_key)
            checked_dates = [
                d.isoformat()
                for d in all_dates_to_check
                if gap_start < d < gap_end
            ]

            if any(gap_start.isoformat() < fd < gap_end.isoformat() for fd in found_dates):
                missed.remove_gap(gap_key)
                logger.info("gap_resolved", gap_key=gap_key)
            else:
                prev_checked = existing.dates_checked if existing else []
                all_checked = sorted(set(prev_checked + checked_dates))
                count = (existing.check_count if existing else 0) + 1
                missed.upsert_gap(
                    gap_key,
                    GapRecord(
                        gap_start=gap_start.isoformat(),
                        gap_end=gap_end.isoformat(),
                        expected_date=exp_date.isoformat(),
                        check_count=count,
                        dates_checked=all_checked,
                    ),
                )

        self._save_missed(missed)
        logger.info("gap_check_complete", found=len(discovered))
        return discovered

    async def _initial_scan(
        self,
        session: aiohttp.ClientSession,
        state: CheckpointState,
    ) -> list[dict]:
        """First run: scan all weekdays in the configured year range."""
        end_year = self._settings.scan_end_year or date.today().year
        all_dates = initial_scan_dates(self._settings.scan_start_year, end_year)

        logger.info("initial_scan_start", dates_to_scan=len(all_dates))

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

        self._stats["discovered"] = len(discovered)
        logger.info("initial_scan_complete", found=len(discovered))
        return discovered

    # --- URL checking ---

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

    # --- Download ---

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
        pdf_date = date.fromisoformat(d)
        pid = period_for_date(pdf_date)

        try:
            data = await self._fetch_with_retry(session, url)
        except Exception as exc:
            logger.warning("download_failed", url=url, error=str(exc))
            return ManifestRecord(
                date=d,
                url=url,
                period_folder=folder,
                period_id=pid,
                download_timestamp=now,
                status="failed",
                error_detail=str(exc)[:500],
            )

        file_hash = hashlib.sha256(data).hexdigest()
        pdf_path = self._settings.pdf_path(d)
        self._storage.write_bytes(pdf_path, data)

        population_path = None
        population_hash = None
        population_count = None

        try:
            pop_dicts = await self._get_population(session, pdf_date, pid)
            population_snapshot = {
                "date": d,
                "period_id": pid,
                "population": pop_dicts,
            }
            pop_bytes = json.dumps(
                population_snapshot, indent=2, ensure_ascii=False
            ).encode("utf-8")
            population_hash = hashlib.sha256(pop_bytes).hexdigest()
            population_count = len(pop_dicts)
            population_path = self._settings.population_path(d)
            self._storage.write_bytes(population_path, pop_bytes)
        except Exception as exc:
            logger.warning("population_fetch_failed", date=d, error=str(exc))

        return ManifestRecord(
            date=d,
            url=url,
            period_folder=folder,
            pdf_path=pdf_path,
            file_hash=file_hash,
            file_size_bytes=len(data),
            population_path=population_path,
            population_hash=population_hash,
            population_count=population_count,
            period_id=pid,
            download_timestamp=now,
            status="success",
        )

    async def _get_population(
        self,
        session: aiohttp.ClientSession,
        pdf_date: date,
        period_id: str,
    ) -> list[dict]:
        if period_id in self._population_cache:
            return self._population_cache[period_id]

        persons = await fetch_population(session, pdf_date)
        pop_dicts = [p.to_dict() for p in persons]
        self._population_cache[period_id] = pop_dicts
        logger.info("population_fetched", period_id=period_id, count=len(pop_dicts))
        return pop_dicts

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
