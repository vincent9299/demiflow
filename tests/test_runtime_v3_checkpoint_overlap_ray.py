"""Real Ray Core publication overlap uses the same driver and finite credits."""
import tempfile
import time
from pathlib import Path

from demiflow.data.api import DataAPI
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from test_runtime_v3_checkpoint_overlap import _capture, _lines, _wait


def test_ray_core_publication_keeps_original_bounded_source_credit(tmp_path, monkeypatch):
    import ray
    from runtime_v3_support import _RayDataBlocker
    assert Path(ray.__file__).is_file() and not ray.is_initialized()
    observed = tmp_path/'real-ray-rows'
    def record(row):
        with observed.open('a') as out:
            out.write(str(row['id'])+'\n')
        return row
    executions = _capture(monkeypatch)
    original = CommitCoordinator._finish
    checked = []
    def slow(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1 and fact['intent']['sinks'] == ['materialize']:
            _wait(lambda: len(_lines(observed)) == 6, 'Ray worker did not use bounded publication overlap')
            time.sleep(.1)
            assert len(_lines(observed)) == 6
            assert executions[0]._policy.provider == 'ray_core'
            assert len(executions[0]._checkpoint._pending) == 1
            checked.append(fact['commit_id'])
        return original(coordinator, fact, sinks, acknowledge)
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    executor = RayDatasetExecutor(workers=2)
    api = DataAPI(executor=executor)
    with _RayDataBlocker() as blocked:
        try:
            ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                object_store_memory=128*1024**2, _temp_dir=tempfile.mkdtemp(prefix='demiflow-or-', dir='/tmp'))
            with RunContext.start(tmp_path/'run', limits={'timeout_s': 60, 'checkpoint_rows': 4,
                    'checkpoint_unconfirmed_rows': 6, 'checkpoint_interval_ms': 60000}):
                rows = api.from_items([{'id': i} for i in range(15)]).map(record).materialize().take_all()
            assert rows == [{'id': i} for i in range(15)]
            assert len(checked) == 1
            assert all(f['status'] == 'acked' for f in RunJournal(tmp_path/'run').execution_commits('action-1'))
        finally:
            executor.close()
            ray.shutdown()
        assert not blocked.attempts and not ray.is_initialized()
