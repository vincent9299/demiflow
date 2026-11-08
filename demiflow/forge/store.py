"""External, atomic Forge lifecycle records."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
import uuid

from .io import atomic_write_json, no_symlinks, read_json
from .schema import ForgeError


def evidence_root(project: Path) -> Path:
    project = project.absolute()
    return no_symlinks(project.parent / ".demiflow" / "forge" / project.name)


@contextmanager
def locked(project: Path):
    root = evidence_root(project)
    root.mkdir(parents=True, exist_ok=True)
    path = no_symlinks(root / ".lock")
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ForgeError(f"Forge project is busy: {project}") from exc
        yield root


def read_record(project: Path, name: str):
    path = evidence_root(project) / f"{name}.json"
    if not path.exists():
        return None
    return read_json(path)


def write_record(project: Path, name: str, value) -> None:
    atomic_write_json(evidence_root(project) / f"{name}.json", value)


def invocation(project: Path) -> Path:
    path = evidence_root(project) / "invocations" / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=False)
    return path


__all__ = ["evidence_root", "invocation", "locked", "read_record", "write_record"]
