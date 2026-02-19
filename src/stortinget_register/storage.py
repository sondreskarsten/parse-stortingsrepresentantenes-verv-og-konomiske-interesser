"""Storage abstraction layer wrapping fsspec.

Provides a unified interface for reading/writing files to local filesystem,
S3, or GCS. The backend is determined automatically from the path prefix.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from datetime import UTC, datetime

import fsspec

from stortinget_register.config import Settings, StorageBackendType


class CredentialError(Exception):
    """Raised when storage credentials are missing or invalid."""


class StorageBackend:
    """Unified file storage interface backed by fsspec."""

    def __init__(self, fs: fsspec.AbstractFileSystem, root_path: str) -> None:
        self._fs = fs
        self._root_path = root_path
        self._protocol = self._fs.protocol
        if isinstance(self._protocol, tuple):
            self._protocol = self._protocol[0]

    @classmethod
    def from_settings(cls, settings: Settings) -> StorageBackend:
        fs, _ = fsspec.core.url_to_fs(settings.storage_path)
        return cls(fs=fs, root_path=settings.storage_path)

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        return self._fs

    def _to_fs_path(self, path: str) -> str:
        for prefix in ("s3://", "gs://", "gcs://"):
            if path.startswith(prefix):
                return path[len(prefix):]
        return path

    def check_credentials(self) -> None:
        fs_path = self._to_fs_path(self._root_path)
        if self._protocol in ("file", ""):
            self._fs.mkdirs(fs_path, exist_ok=True)
            return

        backend = StorageBackendType.S3 if self._protocol == "s3" else StorageBackendType.GCS
        try:
            self._fs.ls(fs_path)
        except FileNotFoundError:
            pass
        except Exception as exc:
            raise CredentialError(
                f"{self._credential_help_message(backend)}\n\nOriginal error: {exc}"
            ) from exc

    def write_bytes(self, path: str, data: bytes) -> None:
        fs_path = self._to_fs_path(path)

        if self._protocol in ("file", ""):
            parent = os.path.dirname(fs_path)
            if parent:
                self._fs.mkdirs(parent, exist_ok=True)
            tmp_path = fs_path + f".tmp.{uuid.uuid4().hex[:8]}"
            with self._fs.open(tmp_path, "wb") as f:
                f.write(data)
            self._fs.mv(tmp_path, fs_path)
            return

        with self._fs.open(fs_path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:
        fs_path = self._to_fs_path(path)
        if not self._fs.exists(fs_path):
            raise FileNotFoundError(f"No such file: {path}")
        with self._fs.open(fs_path, "rb") as f:
            return f.read()

    def exists(self, path: str) -> bool:
        fs_path = self._to_fs_path(path)
        return self._fs.exists(fs_path)

    def list_dir(self, prefix: str) -> list[str]:
        fs_path = self._to_fs_path(prefix)
        try:
            entries = self._fs.glob(fs_path.rstrip("/") + "/**")
        except FileNotFoundError:
            return []
        if not entries:
            try:
                entries = self._fs.ls(fs_path, detail=False)
            except FileNotFoundError:
                return []

        proto_prefix = ""
        if self._protocol and self._protocol not in ("file", ""):
            proto_prefix = f"{self._protocol}://"

        return [f"{proto_prefix}{e}" for e in entries]

    def delete(self, path: str) -> None:
        fs_path = self._to_fs_path(path)
        with contextlib.suppress(FileNotFoundError):
            self._fs.rm(fs_path)

    def modified_time(self, path: str) -> datetime | None:
        fs_path = self._to_fs_path(path)
        try:
            info = self._fs.info(fs_path)
        except FileNotFoundError:
            return None
        mtime = info.get("mtime") or info.get("updated") or info.get("LastModified")
        if mtime is None:
            return None
        if isinstance(mtime, (int, float)):
            return datetime.fromtimestamp(mtime, tz=UTC)
        if isinstance(mtime, datetime):
            if mtime.tzinfo is None:
                return mtime.replace(tzinfo=UTC)
            return mtime
        if isinstance(mtime, str):
            return datetime.fromisoformat(mtime.replace("Z", "+00:00"))
        return None

    def _credential_help_message(self, backend: StorageBackendType) -> str:
        if backend == StorageBackendType.S3:
            return (
                "S3 credentials not found. Configure one of:\n"
                "  - AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars\n"
                "  - AWS IAM role (for EC2/ECS/Lambda)\n"
                "  - aws configure (AWS CLI)\n"
                "  - OIDC role-to-assume in GitHub Actions"
            )
        if backend == StorageBackendType.GCS:
            return (
                "GCS credentials not found. Configure one of:\n"
                "  - GOOGLE_APPLICATION_CREDENTIALS env var (path to service account JSON)\n"
                "  - gcloud auth application-default login\n"
                "  - Workload identity in GitHub Actions"
            )
        return "Local filesystem — no credentials needed."
