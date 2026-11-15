"""Bounded test-only event probes observable across real worker processes."""
import json
import os
from pathlib import Path


def emit(path, event, **values):
    raw = (json.dumps({'event': event, 'pid': os.getpid(), **values}) + '\n').encode()
    assert len(raw) < 4096
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        assert os.write(fd, raw) == len(raw)
    finally:
        os.close(fd)


def events(path):
    path = Path(path)
    if not path.exists():
        return []
    assert path.stat().st_size < 65536
    return [json.loads(line) for line in path.read_text().splitlines()]
