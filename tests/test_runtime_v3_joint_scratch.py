"""Coexisting legacy and unified artifacts use one original journal total."""
import pytest
from demiflow.execution.recovery_contract import RecoveryContractError
from test_runtime_v3_artifact_accounting import store_fixture, consumed


def _legacy(run, size):
    path = run.directory/'scratch'/'native.bin'
    path.parent.mkdir(parents=True, exist_ok=True)
    run.allocate_artifact(path)
    run.reserve_artifact_bytes(path, size)
    path.write_bytes(b'x'*size)
    return path


def test_native_then_dataflow_admission_checks_sum_and_rolls_back(tmp_path):
    with store_fixture(tmp_path, ceiling=10000) as (run, store, _):
        _legacy(run, 7000)
        with pytest.raises(RecoveryContractError, match='scratch admission'):
            store.put(iter([b'y'*4000]), max_bytes=4000, media_type='application/octet-stream')
        assert run.journal.storage_usage() == 7000
        assert not list((store.root/store.store_id/'staging').iterdir())
        store.put(iter([b'z'*2000]), max_bytes=2000, media_type='application/octet-stream')
        assert run.journal.storage_usage() == 9000
        assert consumed(run) == 2000


def test_dataflow_then_native_resize_checks_sum_and_rolls_back(tmp_path):
    with store_fixture(tmp_path, ceiling=10000) as (run, store, _):
        store.put(iter([b'z'*7000]), max_bytes=7000, media_type='application/octet-stream')
        path = _legacy(run, 2000)
        assert run.journal.storage_usage() == 9000
        with pytest.raises(RecoveryContractError, match='scratch admission'):
            run.reserve_artifact_bytes(path, 4000)
        assert path.stat().st_size == 2000
        assert run.journal.storage_usage() == 9000


def test_scratch_growth_checks_sum_before_write_and_returns_only_owned_grant(tmp_path):
    with store_fixture(tmp_path, ceiling=160000) as (run, store, _):
        _legacy(run, 80000)
        with store.scratch(max_bytes=150000, max_io_bytes=80000) as scratch:
            scratch.append(b'a'*60000)
            before = run.journal.storage_usage()
            with pytest.raises(RecoveryContractError, match='scratch admission'):
                scratch.append(b'b'*40000)
            assert scratch.size == 60000
            assert run.journal.storage_usage() == before
        assert run.journal.storage_usage() == 80000
        assert consumed(run) == 0
