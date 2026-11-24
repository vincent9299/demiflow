"""Keep original run/store/resource ownership until a held thread really exits."""
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal


@pytest.mark.parametrize('hold_after_error', [0, .05, .15])
def test_held_thread_retains_original_store_services_and_resources_until_exit(tmp_path, monkeypatch, hold_after_error):
    import test_native_delivery_barrier as original
    original._ENTERED, original._RELEASE = threading.Event(), threading.Event()
    executions = []
    prepare = DataflowExecution.prepare
    def captured(execution):
        executions.append(execution)
        return prepare(execution)
    monkeypatch.setattr(DataflowExecution, 'prepare', captured)
    run = RunContext.start(tmp_path/'run', limits={'cancel_grace_s': 1})
    try:
        with pytest.raises(RecoveryContractError, match='isolated'):
            with run:
                data.from_items([{'id': 0}, {'id': 1}]).map(
                    original.HeldThreadMap, execution='thread', concurrency=2).take(1)
        execution = executions[0]
        assert run.active and run._lease is not None
        assert execution._deferred_cleanup.is_alive()
        assert not execution._store._writer_closed
        assert not execution._services.journal_service.physically_stopped()
        assert not execution._scheduler._physical_exit_confirmed
        snapshot = execution._services.resources.get(execution._scheduler.scope_id)
        assert any(a['cleanup'] != 'CONFIRMED' for a in snapshot['allocations'])
        pool = execution._services.resources.provider.pool
        assert any(pool.reservation_view(a['allocation_id'])['state'] != 'RELEASED'
                   for a in snapshot['allocations'])
        assert RunJournal(tmp_path/'run').execution_action('action-1')['result'] is None
        with pytest.raises(BlockingIOError):
            with RunContext.resume(run.ref):
                pytest.fail('a live owner was replaced')
        time.sleep(hold_after_error)
        assert run.active and not execution._store._writer_closed
    finally:
        original._RELEASE.set()
        finalizer = getattr(run, '_drain_finalizer', None)
        if finalizer is not None:
            finalizer.join(5)
            assert not finalizer.is_alive()
        original._ENTERED = original._RELEASE = None
    assert not run.active and run._lease is None
    assert execution._scheduler._physical_exit_confirmed
    assert getattr(execution, '_deferred_cleanup_error', None) is None
    assert execution._store._writer_closed and execution._services.journal_service.physically_stopped()
    assert not execution._services.resources.provider._control.units
    snapshot = execution._services.resources.get(execution._scheduler.scope_id)
    assert snapshot['allocations'] and all(a['cleanup'] == 'CONFIRMED' for a in snapshot['allocations'])
    assert all(pool.reservation_view(a['allocation_id'])['state'] == 'RELEASED'
               for a in snapshot['allocations'])
