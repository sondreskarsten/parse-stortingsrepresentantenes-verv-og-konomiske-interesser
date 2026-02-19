"""Configuration via environment variables and .env files.

Settings are loaded with this priority: CLI args > env vars > .env file > defaults.
The storage_path prefix determines the backend: s3://, gs://, or local filesystem.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageBackendType(StrEnum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"


class Settings(BaseSettings):
    """All configuration for stortinget-register.

    Required:
        storage_path: Root path for all stored data.
            - Local: ./data or /abs/path
            - S3: s3://bucket-name/prefix
            - GCS: gs://bucket-name/prefix

    Optional:
        max_concurrent: Max simultaneous HTTP connections (default 5).
        max_retries: Max retry attempts per failed request (default 5).
        max_runtime_minutes: Graceful shutdown after N minutes. 0 = unlimited (default 0).
        scan_start_year: Earliest year to scan for PDFs (default 2021).
        scan_end_year: Latest year to scan for PDFs. None = current year (default None).
        log_level: Logging verbosity (default INFO).
    """

    model_config = SettingsConfigDict(
        env_prefix="STORTING_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    storage_path: str = "./data"
    max_concurrent: int = 5
    max_retries: int = 5
    max_runtime_minutes: int = 0
    scan_start_year: int = 2021
    scan_end_year: int | None = None
    log_level: str = "INFO"

    @field_validator("storage_path")
    @classmethod
    def validate_storage_path(cls, v: str) -> str:
        v = v.rstrip("/")
        if not v:
            raise ValueError("storage_path cannot be empty")
        return v

    @property
    def backend_type(self) -> StorageBackendType:
        if self.storage_path.startswith("s3://"):
            return StorageBackendType.S3
        if self.storage_path.startswith("gs://"):
            return StorageBackendType.GCS
        return StorageBackendType.LOCAL

    @property
    def manifest_path(self) -> str:
        return f"{self.storage_path}/manifest.parquet"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.storage_path}/checkpoint.json"

    def pdf_path(self, date_str: str) -> str:
        return f"{self.storage_path}/pdfs/pr-{date_str}.pdf"

    def population_path(self, date_str: str) -> str:
        return f"{self.storage_path}/population/pr-{date_str}.json"
