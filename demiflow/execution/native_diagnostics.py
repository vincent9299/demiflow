"""Optional, bounded native diagnostics; mandatory result IPC lives in scratch.

Sixteen reusable slots per managed run, including all attempts. Each slot has
four 64 KiB log segments and at most four 64 KiB diagnostic documents. A live
query owns its slot lock; rotation can only evict a stopped query's diagnostics.
"""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path

SLOTS = 16
FILE_BYTES = 64 * 1024


class LimitedWriter:
    def __init__(self, stream, maximum):
        self.stream, self.remaining = stream, maximum

    def write(self, raw):
        if len(raw) > self.remaining:
            raise ValueError("Native result IPC byte budget exceeded")
        self.remaining -= len(raw)
        return self.stream.write(raw)


def mandatory_json(path, value):
    # Unlike optional diagnostics, required IPC may not truncate a fact.
    temporary = path.with_suffix(".tmp")
    try:
        with temporary.open("wb") as stream:
            bounded = LimitedWriter(stream, FILE_BYTES)
            for chunk in json.JSONEncoder(
                ensure_ascii=False, allow_nan=False
            ).iterencode(value):
                bounded.write(chunk.encode())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_document(path, value):
    raw = bytearray()
    for chunk in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(
        value
    ):
        encoded = chunk.encode()
        if len(raw) + len(encoded) > FILE_BYTES:
            value = {"diagnostics_truncated": True, "reason": "document_byte_budget"}
            raw = bytearray(json.dumps(value).encode())
            break
        raw.extend(encoded)
    temporary = path.with_suffix(".tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def slot(root, identity):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    candidates = []
    for number in range(SLOTS):
        path = root / ("slot-" + str(number))
        marker = path / "identity.json"
        candidates.append(
            (marker.stat().st_mtime_ns if marker.exists() else -1, number)
        )
    for _, number in sorted(candidates):
        path = root / ("slot-" + str(number))
        path.mkdir(exist_ok=True)
        fd = os.open(path / "owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            # Closed names only: never recurse into an unknown directory.
            with os.scandir(path) as entries:
                for count, entry in enumerate(entries):
                    if (
                        count >= 16
                        or entry.is_symlink()
                        or not entry.is_file(follow_symlinks=False)
                    ):
                        raise RuntimeError(
                            "Managed diagnostic slot exceeds shape budget"
                        )
                    if entry.name != "owner.lock":
                        Path(entry.path).unlink()
            write_document(path / "identity.json", {"query_id": identity})
            yield path
            return
        finally:
            os.close(fd)
    raise RuntimeError("Managed diagnostic slots exhausted")


class PipeLog:
    """Consume a nonblocking pipe with bounded work and four rotating segments."""

    def __init__(self, pipe, directory):
        self.pipe, self.directory = pipe, Path(directory)
        os.set_blocking(pipe.fileno(), False)
        self.total, self.used, self.segment = 0, 0, 0
        self.stream = None
        self.eof = False
        self.disabled = False

    def drain(self):
        for _ in range(4):
            try:
                raw = os.read(self.pipe.fileno(), FILE_BYTES)
            except BlockingIOError:
                return
            if not raw:
                self.eof = True
                return
            self.total += len(raw)
            if self.disabled:
                continue
            try:
                while raw:
                    if self.stream is None or self.used == FILE_BYTES:
                        if self.stream:
                            self.stream.close()
                        self.stream = (
                            self.directory / f"worker-{self.segment%4}.log"
                        ).open("wb")
                        self.segment += 1
                        self.used = 0
                    chunk, raw = (
                        raw[: FILE_BYTES - self.used],
                        raw[FILE_BYTES - self.used :],
                    )
                    self.stream.write(chunk)
                    self.stream.flush()
                    self.used += len(chunk)
            except OSError:
                self.disabled = True
                if self.stream:
                    try:
                        self.stream.close()
                    except OSError:
                        pass
                    self.stream = None

    def close(self):
        if self.stream:
            try:
                self.stream.close()
            except OSError:
                pass
        self.pipe.close()
