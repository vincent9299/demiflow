"""Real Ray Core function ownership under scarce CPU and original ACK recovery."""
import json
from pathlib import Path
import tempfile

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _identity(row):
    return row


def _count(row, *, state, log):
    state[0] += 1
    with open(log, 'a') as stream:
        stream.write(json.dumps({'i': row['i'], 'count': state[0]}) + '\n')
    return {**row, 'count': state[0]}


def _pipeline(log, form):
    source = data.from_items([{'i': i} for i in range(4)]).map_async(_identity, concurrency=1)
    state = [0]
    if form == 'cell':
        def callback(row):
            return _count(row, state=state, log=log)
        return source.map_async(callback, concurrency=1)
    return source.map(_count, fn_kwargs={'state': state, 'log': log})


@pytest.fixture(scope='module', params=[1, 4])
def ray_capacity(request):
    import importlib.util
    if importlib.util.find_spec('ray') is None:
        pytest.skip('requires the existing real Ray validation environment')
    import ray
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import _RayDataBlocker
    assert Path(ray.__file__).is_file() and ray.ObjectRef is ray._raylet.ObjectRef
    assert not ray.is_initialized()
    with _RayDataBlocker() as blocked:
        try:
            ray.init(address='local', num_cpus=request.param, num_gpus=0,
                     include_dashboard=False, object_store_memory=100 * 1024**2,
                     _temp_dir=tempfile.mkdtemp(prefix='demiflow-state-ray-', dir='/tmp'))
            executor = RayDatasetExecutor()
            try:
                with _use_executor(executor):
                    yield request.param
            finally:
                executor.close()
        finally:
            if ray.is_initialized():
                ray.shutdown()
            assert not ray.is_initialized() and not blocked.attempts


@pytest.fixture(autouse=True)
def own_scope(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'capacity'))
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY', raising=False)


@pytest.mark.parametrize('form', ['cell', 'kwargs'])
def test_real_ray_function_state_is_independent_of_host_capacity(tmp_path, ray_capacity, form):
    from demiflow.execution.sealed_plans import load_sealed_plan
    log = str(tmp_path / 'calls.jsonl')
    with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 1,
            'checkpoint_interval_ms': 60000, 'timeout_s': 90}) as run:
        assert _pipeline(log, form).take_all() == [{'i': i, 'count': i + 1} for i in range(4)]
        plan = load_sealed_plan(run.directory, 'action-1')
        assert plan['physical'].policy.provider == 'ray_core'
        assert len(plan['physical'].splits) == 1
        if ray_capacity == 1:
            assert any(edge.mode == 'durable_incremental' for edge in plan['physical'].exchanges)
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert [json.loads(line) for line in Path(log).read_text().splitlines()] == [
        {'i': i, 'count': i + 1} for i in range(4)]


def test_real_ray_bound_state_resumes_the_original_ack(tmp_path, ray_capacity, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    class InterruptedAfterAck(Exception):
        pass
    log = str(tmp_path / 'calls.jsonl')
    advance = DataflowExecution._advance_cut
    saved = {}
    def interrupt(execution):
        result = advance(execution)
        image = execution._restore_image
        if image is not None and not image.cut.final:
            rows = execution._publication(image)['manifest']['total_rows']
            if 0 < rows < 4:
                fact = execution._action.context.journal.execution_commit(image.commit_id)
                assert fact['status'] == 'acked'
                saved.update(rows=rows, commit=image.commit_id)
                raise InterruptedAfterAck()
        return result
    with pytest.raises(InterruptedAfterAck):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 1,
                'checkpoint_interval_ms': 60000, 'timeout_s': 90}) as run:
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_advance_cut', interrupt)
                _pipeline(log, 'kwargs').take_all()
    original = run.journal.execution_commits('action-1')
    prefix = [json.loads(line) for line in Path(log).read_text().splitlines()
              if json.loads(line)['i'] < saved['rows']]
    assert [row['i'] for row in prefix] == list(range(saved['rows']))
    with RunContext.resume(ref) as run:
        assert _pipeline(log, 'kwargs').take_all() == [{'i': i, 'count': i + 1} for i in range(4)]
        commits = run.journal.execution_commits('action-1')
        assert commits[:len(original)] == original and commits[-1]['status'] == 'acked'
        assert saved['commit'] in {entry['commit_id'] for entry in commits}
    assert [json.loads(line) for line in Path(log).read_text().splitlines()
            if json.loads(line)['i'] < saved['rows']] == prefix
