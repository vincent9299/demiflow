"""Sealed payload loading validates bytes and preserves current program behavior."""
import hashlib
import io
import pickle
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.seal.program_pickle import ProgramPickler, load_payload, load_private_payload

class OriginalStore:
    def __init__(self, raw):
        self.raw = raw

    def read_into(self, ref, buffer):
        buffer[:] = self.raw


def test_private_large_unicode_does_not_duplicate_program_buffer():
    import pickle
    import tracemalloc
    value = 'a' * (4 << 20)
    raw = pickle.dumps(value, protocol=5)
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    tracemalloc.start()
    try:
        loaded = load_private_payload(OriginalStore(raw), ref)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert loaded == value
    assert peak < len(raw) * 2.5


@pytest.mark.parametrize('private', [False, True])
def test_payload_roundtrip_preserves_local_exception_qualified_name(private):
    class LocalFailure(Exception):
        pass
    stream = io.BytesIO()
    ProgramPickler(stream).dump(LocalFailure)
    raw = stream.getvalue()
    ref = SimpleNamespace(size_bytes=len(raw), digest=hashlib.sha256(raw).hexdigest())
    decoded = load_private_payload(OriginalStore(raw), ref) if private else load_payload([raw])
    assert decoded.__qualname__ == LocalFailure.__qualname__
    assert issubclass(decoded, Exception)


def test_private_bytes_are_verified_before_pickle_can_execute(tmp_path):
    marker = tmp_path / 'unpickled'
    class Effect:
        def __reduce__(self):
            return eval, (f"open({str(marker)!r}, 'w').write('executed')",)
    raw = pickle.dumps(Effect())
    ref = SimpleNamespace(size_bytes=len(raw), digest='0' * 64)
    with pytest.raises(DataflowError, match='digest differs'):
        load_private_payload(OriginalStore(raw), ref)
    assert not marker.exists()


def test_chunk_verification_finishes_before_pickle_can_execute(tmp_path):
    marker = tmp_path / 'unpickled'
    class Effect:
        def __reduce__(self):
            return eval, (f"open({str(marker)!r}, 'w').write('executed')",)
    raw = pickle.dumps(Effect())
    def corrupt_chunks():
        yield raw
        raise DataflowError('CORRUPT_ARTIFACT', 'chunk digest differs')
    with pytest.raises(DataflowError, match='chunk digest differs'):
        load_payload(corrupt_chunks())
    assert not marker.exists()
