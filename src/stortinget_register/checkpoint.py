"""Checkpoint persistence for resume-safe sync operations."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stortinget_register.storage import StorageBackend


@dataclass
class CheckpointState:
    last_date_scanned: str | None = None
    run_started_at: str | None = None
    dates_scanned: int = 0
    pdfs_found: int = 0
    pdfs_downloaded: int = 0
    errors: int = 0

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), indent=2).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> CheckpointState:
        return cls(**json.loads(data))


class CheckpointManager:
    def __init__(self, storage: StorageBackend, checkpoint_path: str) -> None:
        self._storage = storage
        self._checkpoint_path = checkpoint_path

    def load(self) -> CheckpointState:
        if not self._storage.exists(self._checkpoint_path):
            return CheckpointState()
        data = self._storage.read_bytes(self._checkpoint_path)
        return CheckpointState.from_json(data)

    def save(self, state: CheckpointState) -> None:
        self._storage.write_bytes(self._checkpoint_path, state.to_json())

    def clear(self) -> None:
        self._storage.delete(self._checkpoint_path)
