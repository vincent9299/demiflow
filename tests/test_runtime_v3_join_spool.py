"""Join scratch is bounded before write, recoverable, and never copies payloads on partitioning."""
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext
from test_runtime_v3_artifact_recovery import _open, _store
from test_runtime_v3_artifact_accounting import consumed
from test_runtime_v3_real_seams import PreparationOnly


def test_scratch_reserves_before_growth_and_releases_only_after_unlink(tmp_path, monkeypatch):
    with pytest.raises(PreparationOnly), RunContext.start(tmp_path / 'run') as run:
        store, _, _ = _store(run, ceiling=200000)
        pwrite = os.pwrite
        def checked(fd, raw, offset):
            assert consumed(run) >= offset + len(raw)
            return pwrite(fd, raw, offset)
        monkeypatch.setattr(os, 'pwrite', checked)
        with store.scratch(max_bytes=100000, max_io_bytes=80000) as file:
            file.append(b'a' * 70000)
            assert file.read_at(69990, 10) == b'a' * 10
            file.write_at(0, b'head')
            assert file.read_at(0, 4) == b'head'
            before = file.size
            with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
                file.append(b'overflow' * 5000)
            assert file.size == before
            assert file.path.exists() and consumed(run) >= before
        assert consumed(run) == 0
        assert not list((store.root / store.store_id / 'staging').iterdir())
        raise PreparationOnly()


def test_killed_kernel_scratch_is_reclaimed_by_original_journal(tmp_path):
    script = '''
import os, sys
from pathlib import Path
from test_runtime_v3_artifact_recovery import _open, _store
with _open(Path(sys.argv[1])) as run:
    store, _, _ = _store(run, ceiling=200000)
    with store.scratch(max_bytes=100000, max_io_bytes=80000) as scratch:
        scratch.append(b'x' * 70000)
        os._exit(73)
'''
    result = subprocess.run([sys.executable, '-B', '-c', script, str(tmp_path / 'run')],
        capture_output=True, text=True, timeout=45)
    assert result.returncode == 73, result.stdout + result.stderr
    with pytest.raises(PreparationOnly):
        with _open(tmp_path / 'run') as run:
            store, port, _ = _store(run, ceiling=200000)
            assert port.recovery['reconciled'] == 1
            assert consumed(run) == 0
            assert not list((store.root / store.store_id / 'staging').iterdir())
            raise PreparationOnly()


def test_worker_stream_is_lazy_and_refunds_only_with_original_lease_proof(tmp_path):
    from demiflow.execution.dataflow_scratch import ScratchStream
    from demiflow.execution.storage.staging import recover_staging
    with pytest.raises(PreparationOnly), RunContext.start(tmp_path / 'run') as run:
        store, port, bounds = _store(run, ceiling=200000)
        stream = ScratchStream(store, max_bytes=100000, max_io_bytes=80000)
        assert stream._file is None and stream.read(1) == b''
        assert not run.journal.pending_dataflow_staging(run.fence, after='', limit=1)
        stream.write(b'first\nsecond\n')
        stream.seek(0)
        assert stream.readline(100) == b'first\n'
        assert stream.read(2) == b'se'
        stream.seek(0, 2)
        assert stream.tell() == 13
        assert recover_staging(port._action, bounds=bounds)['unknown'] == 1
        charged = consumed(run)
        stream.close()
        assert consumed(run) == charged > 0
        assert recover_staging(port._action, bounds=bounds)['reconciled'] == 1
        assert consumed(run) == 0
        raise PreparationOnly()


def test_repartition_moves_only_references_and_preserves_wide_rows(monkeypatch):
    from test_runtime_v3_k import rec, services, Bounds
    from demiflow.execution.kernels.join_spool import join_pairs, _Spool
    svc = services(bounds=Bounds(max_resident_bytes=65536, max_row_bytes=4096,
        max_spill_bytes=16 * 1024**2))
    partition = _Spool.partition
    hydrations = []
    record = _Spool.record
    def checked(spool, chain, shift):
        before = spool.file.size
        result = partition(spool, chain, shift)
        assert spool.file.size == before  # No payload rewrite or duplicate spill.
        hydrations.append(('partition', before))
        return result
    def decoded(spool, offset, header):
        hydrations.append(('decode', offset))
        return record(spool, offset, header)
    monkeypatch.setattr(_Spool, 'partition', checked)
    monkeypatch.setattr(_Spool, 'record', decoded)
    left = [rec({'k': i, 'l': 'l' * 1000}, sequence=i) for i in range(600)]
    right = [rec({'k': i, 'r': 'r' * 1000}, sequence=i) for i in range(600)]
    pairs = list(join_pairs(iter(left), iter(right), left_keys=('k',), right_keys=('k',),
        how='inner', services=svc))
    assert len(pairs) == 600
    assert sorted((a.inline['k'], b.inline['k']) for a, b in pairs) == [(i, i) for i in range(600)]
    assert all(a.inline['l'] == 'l' * 1000 and b.inline['r'] == 'r' * 1000 for a, b in pairs)
    assert any(kind == 'partition' for kind, _ in hydrations)
    assert sum(kind == 'decode' for kind, _ in hydrations) == 1200


def test_real_wide_join_under_small_memory_grant(tmp_path):
    import lance
    import pyarrow as pa
    left, right = tmp_path / 'left', tmp_path / 'right'
    lance.write_dataset(pa.table({'k': list(range(24)), 'left': ['l' * 60000] * 24}), str(left))
    lance.write_dataset(pa.table({'k': list(range(24)), 'right': ['r' * 60000] * 24}), str(right))
    with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 16 * 1024**2,
        'max_row_bytes': 512 * 1024, 'max_batch_bytes': 512 * 1024,
        'max_state_bytes': 1024 * 1024, 'max_scratch_bytes': 64 * 1024**2}) as run:
        with data.local_execution():
            rows = data.read_lance(str(left), version=1).join(
                data.read_lance(str(right), version=1), on='k').take_all()
        assert len(rows) == 24
        assert all(row['left'] == 'l' * 60000 and row['right'] == 'r' * 60000 for row in rows)
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        assert not list((run.directory / '.dataflow-store').glob('*/staging/.pending-*'))
