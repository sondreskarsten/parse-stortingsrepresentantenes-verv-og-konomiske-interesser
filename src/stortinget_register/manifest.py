"""Parquet manifest for tracking downloaded register PDFs.

Each row represents a single PDF publication with its download status,
file path, hash, and source URL.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from stortinget_register.storage import StorageBackend

MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("date", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("period_folder", pa.string()),
        pa.field("pdf_path", pa.string()),
        pa.field("file_hash", pa.string()),
        pa.field("file_size_bytes", pa.int64()),
        pa.field("download_timestamp", pa.string()),
        pa.field("status", pa.string()),
        pa.field("error_detail", pa.string()),
    ]
)


@dataclass
class ManifestRecord:
    date: str
    url: str
    period_folder: str | None = None
    pdf_path: str | None = None
    file_hash: str | None = None
    file_size_bytes: int | None = None
    download_timestamp: str | None = None
    status: str = "pending"
    error_detail: str | None = None


def _empty_table() -> pa.Table:
    return pa.table(
        {f.name: pa.array([], type=f.type) for f in MANIFEST_SCHEMA},
        schema=MANIFEST_SCHEMA,
    )


def _record_to_dict(r: ManifestRecord) -> dict:
    return {
        "date": r.date,
        "url": r.url,
        "period_folder": r.period_folder,
        "pdf_path": r.pdf_path,
        "file_hash": r.file_hash,
        "file_size_bytes": r.file_size_bytes,
        "download_timestamp": r.download_timestamp,
        "status": r.status,
        "error_detail": r.error_detail,
    }


class ManifestManager:
    """Manages the Parquet manifest tracking all downloaded register PDFs.

    Primary key: (date, url).
    """

    def __init__(self, storage: StorageBackend, manifest_path: str) -> None:
        self._storage = storage
        self._manifest_path = manifest_path

    def load(self) -> pa.Table:
        if not self._storage.exists(self._manifest_path):
            return _empty_table()
        raw = self._storage.read_bytes(self._manifest_path)
        buf = pa.BufferReader(raw)
        table = pq.read_table(buf)
        table = table.select([f.name for f in MANIFEST_SCHEMA])
        return table.cast(MANIFEST_SCHEMA)

    def save(self, table: pa.Table) -> None:
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        self._storage.write_bytes(self._manifest_path, sink.getvalue())

    def upsert(self, records: list[ManifestRecord]) -> None:
        if not records:
            return

        existing = self.load()
        new_keys = {(r.date, r.url) for r in records}

        if existing.num_rows > 0:
            date_col = existing.column("date")
            url_col = existing.column("url")
            keep_mask = pa.array(
                [
                    (date_col[i].as_py(), url_col[i].as_py()) not in new_keys
                    for i in range(existing.num_rows)
                ]
            )
            existing = existing.filter(keep_mask)

        new_rows = {f.name: [] for f in MANIFEST_SCHEMA}
        for r in records:
            d = _record_to_dict(r)
            for col in new_rows:
                new_rows[col].append(d[col])

        new_arrays = {}
        for f in MANIFEST_SCHEMA:
            new_arrays[f.name] = pa.array(new_rows[f.name], type=f.type)
        new_table = pa.table(new_arrays, schema=MANIFEST_SCHEMA)

        merged = pa.concat_tables([existing, new_table], promote_options="none")
        self.save(merged)

    def get_downloaded_urls(self) -> set[str]:
        table = self.load()
        if table.num_rows == 0:
            return set()
        mask = pc.equal(table.column("status"), "success")
        filtered = table.filter(mask)
        return set(filtered.column("url").to_pylist())

    def get_downloaded_dates(self) -> set[str]:
        table = self.load()
        if table.num_rows == 0:
            return set()
        mask = pc.equal(table.column("status"), "success")
        filtered = table.filter(mask)
        return set(filtered.column("date").to_pylist())
