"""Final candidate Ray-only terminal seam checks; same original engine authority."""
import json
import pytest
from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


@pytest.fixture(scope='module')
def private_ray_provider():
    import importlib.util
    import sys
    # An absent optional dependency is the only skip. Installed but broken Ray,
    # an injected module double, or an already-connected cluster must fail.
    if 'ray' not in sys.modules and importlib.util.find_spec('ray') is None:
        pytest.skip('Ray Core is not installed in this interpreter; use the dedicated Ray validation environment')
    import ray
    from pathlib import Path
    import demiflow.execution.dataflow_hosts as hosts
    import demiflow.resources.ray as resource_provider
    assert Path(ray.__file__).is_file(), 'Ray must be a real installed module'
    assert ray.ObjectRef is ray._raylet.ObjectRef
    assert ray.actor.ActorHandle.__module__ == 'ray.actor'
    assert hosts.ray is ray and resource_provider.ray is ray
    assert not ray.is_initialized(), 'test must own a fresh private Ray Core cluster'
    from runtime_v3_recovery_scenarios import _provider
    from runtime_v3_support import _RayDataBlocker
    with _RayDataBlocker() as blocked:
        try:
            # The existing helper pins address='local', num_cpus=6 and a short,
            # unique /tmp directory. It never attaches to an external cluster.
            with _provider('ray_core'):
                yield
        finally:
            # Also cover a partial helper-entry failure; no external cluster was
            # initialized on entry, so only this fixture's runtime is affected.
            if ray.is_initialized():
                ray.shutdown()
            assert not ray.is_initialized()
            assert not blocked.attempts, 'Ray Data must never be imported'


@pytest.fixture(autouse=True)
def actual_ray(monkeypatch, private_ray_provider):
    observed = []
    start = DataflowExecution.start
    def checked(execution):
        assert execution._policy.provider == 'ray_core'
        observed.append(execution)
        return start(execution)
    monkeypatch.setattr(DataflowExecution, 'start', checked)
    yield observed
    assert observed


def test_final_ack_replay_without_worker_or_new_commit(tmp_path, monkeypatch, actual_ray):
    rows = [{'i': i, 'payload': 'x' * 6000} for i in range(20)]
    source = tmp_path / 'rows.jsonl'
    source.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    limits = {'timeout_s': 30, 'memory_bytes': 256 * 1024,
              'max_row_bytes': 16 * 1024, 'max_batch_bytes': 32 * 1024}
    class InterruptedAfterAction(Exception):
        pass
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path / 'run', limits=limits) as run:
            ref = run.ref
            assert data.read_json(str(source)).take_all() == rows
            before = run.journal.execution_commits('action-1')
            assert before[-1]['status'] == 'acked'
            raise InterruptedAfterAction()
    def forbidden(*args, **kwargs):
        raise AssertionError('final replay must not launch any Ray worker')
    monkeypatch.setattr(DataflowExecution, '_launch_phase', forbidden)
    with RunContext.resume(ref) as run:
        assert data.read_json(str(source)).take_all() == rows
        assert run.journal.execution_commits('action-1') == before
    # Replayable sources restore through the ordinary native image. The
    # completed-live special case is not the contract for every source kind.
    image = actual_ray[-1]._restore_image
    assert image is not None and image.cut.final
    assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'


def test_iter_rows_close_keeps_real_nonfinal_prefix(tmp_path, actual_ray):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s':20,
            'checkpoint_interval_ms':60000}) as run:
        rows = data.range(1000).iter_rows()
        assert next(rows) == {'id':0}
        rows.close()
        execution = actual_ray[-1]
        image = execution._restore_image
        assert image is not None and not image.cut.final
        assert execution._stopping and not execution._checkpoint.inputs_complete()
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert 0 < execution._terminal_result.rows < 1000
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        cursors = [cursor for part in image.contributions for cursor in part.sources]
        assert cursors and all(not cursor.end_of_input and cursor.next_sequence < 1000 for cursor in cursors)
        assert not execution._exchange._inflight and not execution._terminal._queue
        assert not execution._endpoints and not execution._active_worker_allocations


@pytest.mark.parametrize('limit', [0,3])
def test_take_all_exact_and_overflow_are_distinct(tmp_path, limit):
    limits = {'timeout_s':30, 'max_output_rows':max(1,limit)}
    with RunContext.start(tmp_path/'exact',limits=limits) as run:
        assert data.range(limit).take_all(limit=limit) == [{'id':i} for i in range(limit)]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    with pytest.raises(ValueError,match='take_all limit'):
        with RunContext.start(tmp_path/'overflow',limits=limits):
            data.range(limit+1).take_all(limit=limit)
