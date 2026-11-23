"""Program identity implementation."""
from __future__ import annotations

from pathlib import Path
from demiflow.execution.contracts.artifact import digest


def file_record(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": digest(path.read_bytes())}
