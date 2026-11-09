"""A delivery early-close must retain ownership of a late native writer."""
import threading

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError

_ENTERED = None
_RELEASE = None


class HeldThreadMap:
    def __call__(self, row):
        if row['id'] == 1:
            _ENTERED.set()
            assert _RELEASE.wait(15), 'test must release its worker'
        else:
            assert _ENTERED.wait(5)
        return row

    def snapshot_state(self):
        return {}

    def restore_state(self, value):
        assert value == {}


def test_delivery_timeout_quarantines_worker_and_retains_run_lease(tmp_path):
    global _ENTERED, _RELEASE
    _ENTERED, _RELEASE = threading.Event(), threading.Event()
    run = RunContext.start(tmp_path/'run', limits={'cancel_grace_s': 1})
    try:
        with pytest.raises(RecoveryContractError, match='isolated'):
            with run:
                data.from_items([{'id': 0}, {'id': 1}]).map(
                    HeldThreadMap, execution='thread', concurrency=2).take(1)
        assert run.active and run._lease is not None
        with pytest.raises(BlockingIOError):
            with RunContext.resume(run.ref):
                pass
    finally:
        _RELEASE.set()
        finalizer = getattr(run, '_drain_finalizer', None)
        if finalizer is not None:
            finalizer.join(timeout=5)
            assert not finalizer.is_alive()
        else:
            for worker in threading.enumerate():
                if worker.name == 'demiflow-native-delivery':
                    worker.join(timeout=5)
        _ENTERED = _RELEASE = None
    assert not run.active and run._lease is None
