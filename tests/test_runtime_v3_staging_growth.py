"""Real journal byte ownership, including optional headroom and unknown replies."""
import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_journal import ExecutionBudgetExceeded
from test_runtime_v3_artifact_accounting import store_fixture, consumed


@pytest.mark.parametrize('kind', ['artifact', 'scratch'])
def test_growth_is_bounded_and_amortized_before_every_write(tmp_path, monkeypatch, kind):
    with store_fixture(tmp_path, ceiling=8 << 20) as (run, store, port):
        original = port.artifact
        grows = []
        def observed(command):
            if command.kind == 'grow_staging':
                grows.append(command.size_bytes)
                assert command.size_bytes <= 4 << 20
            return original(command)
        monkeypatch.setattr(port, 'artifact', observed)
        chunks = [b'a' * 8192] * 512
        if kind == 'artifact':
            ref = store.put(iter(chunks), max_bytes=4 << 20, media_type='application/octet-stream')
            assert ref.size_bytes == consumed(run) == 4 << 20
        else:
            with store.scratch(max_bytes=4 << 20, max_io_bytes=8192) as scratch:
                for chunk in chunks:
                    scratch.append(chunk)
                    assert scratch.size <= consumed(run) <= 4 << 20
                assert scratch.read_at(scratch.size - 8192, 8192) == chunks[0]
            assert consumed(run) == 0
        assert 1 <= len(grows) <= 7  # Previously 511 artifact / 63 scratch RPCs.
        assert not list((store.root / store.store_id / 'staging').iterdir())


@pytest.mark.parametrize('kind', ['artifact', 'scratch'])
def test_exact_growth_fits_when_optional_headroom_is_rejected(tmp_path, monkeypatch, kind):
    with store_fixture(tmp_path, ceiling=180 * 1024) as (run, store, port):
        prefix = store.put(iter([b'p' * (70 * 1024)]), max_bytes=70 * 1024,
                           media_type='application/octet-stream')
        original = port.artifact
        rejected = []
        def observed(command):
            try:
                return original(command)
            except ExecutionBudgetExceeded:
                rejected.append(command.size_bytes)
                # Original transaction rolls back the proposed headroom.
                assert consumed(run) == (70 + 64) * 1024
                raise
        monkeypatch.setattr(port, 'artifact', observed)
        if kind == 'artifact':
            ref = store.put(iter([b'a' * (80 * 1024)]), max_bytes=160 * 1024,
                            media_type='application/octet-stream')
            assert consumed(run) == prefix.size_bytes + ref.size_bytes
        else:
            with store.scratch(max_bytes=160 * 1024, max_io_bytes=80 * 1024) as scratch:
                scratch.append(b'a' * (80 * 1024))
                assert scratch.size == scratch.reserved == 80 * 1024
                assert consumed(run) == 150 * 1024
            assert consumed(run) == prefix.size_bytes
        assert rejected == [128 * 1024]


def test_unknown_growth_is_never_retried_as_an_exact_reservation(tmp_path, monkeypatch):
    with store_fixture(tmp_path) as (run, store, port):
        original = port.artifact
        attempted = []
        def lost_reply(command):
            reply = original(command)
            if command.kind == 'grow_staging':
                attempted.append(command.size_bytes)
                raise DataflowError('UNKNOWN_EFFECT', 'growth reply lost after admission')
            return reply
        monkeypatch.setattr(port, 'artifact', lost_reply)
        with store.scratch(max_bytes=1 << 20, max_io_bytes=128 << 10) as scratch:
            with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
                scratch.append(b'x' * (80 << 10))
            assert scratch.size == 0
            assert consumed(run) == 128 << 10
            assert scratch.path.stat().st_size == 0
        assert attempted == [128 << 10]
        assert consumed(run) == 0  # Explicit physical removal precedes release.
