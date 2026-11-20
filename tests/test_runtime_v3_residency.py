"""Real Dataset finite phases: managed exits, native cuts and exact recovery."""
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.state_artifacts import StateArtifacts

LIMITS = {'memory_bytes': 80 * 1024, 'max_batch_bytes': 16 * 1024,
          'max_row_bytes': 8 * 1024, 'max_state_bytes': 128 * 1024,
          'checkpoint_interval_ms': 60000}


def dataset():
    return data.from_items([{'n': n} for n in (4, 1, 3, 2)]).sort('n').sort('n')


def carriers(run):
    facts = run.journal.execution_commits('action-1')
    return facts, [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3']) for f in facts]


def test_dataset_three_phases_release_only_after_real_exit(tmp_path, monkeypatch):
    launches = []
    physical = []
    original = DataflowExecution._launch_phase
    def check_launch(ex):
        snapshot = ex.services.resources.get(ex._scheduler.scope_id)
        if launches:
            assert snapshot['allocations']
            assert all(a['cleanup'] == 'CONFIRMED' for a in snapshot['allocations'])
            assert ex._scheduler._resident_used == 0
        original(ex)
        assert ex._scheduler._resident_used + ex._physical.residency.control_reserved_bytes <= ex._bounds.max_resident_bytes
        launches.append(ex._phases[ex._phase_index])
        physical.append(ex._physical)
    monkeypatch.setattr(DataflowExecution, '_launch_phase', check_launch)
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        assert dataset().take_all() == [{'n': n} for n in range(1, 5)]
        facts, states = carriers(run)
        assert len(launches) >= 3
        assert all(f['status'] == 'acked' for f in facts)
        assert all(not s['cut']['final'] for s in states[:-1]) and states[-1]['cut']['final']
        assert all(len(s['cut']['participants']) < len(states[-1]['contributions']) for s in states)
        origin = states[0]['contributions'][0]
        assert origin in states[-1]['contributions']  # original cut_id and boot_id untouched
        assert states[-1]['dataflow.sources.v3'] == states[0]['dataflow.sources.v3']
        from copy import deepcopy
        from dataclasses import replace
        from demiflow.execution.checkpoint.coordinator import _cut_from_json, _contribution_from_json
        from demiflow.execution.dataflow_residency import validate_recovered_residency
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        last = states[-1]
        cut = _cut_from_json(last['cut'])
        contributions = tuple(_contribution_from_json(c) for c in last['contributions'])
        kwargs = dict(facts=facts, artifacts=StateArtifacts(run), current_commit=facts[-1]['commit_id'])
        validate_recovered_residency(physical[-1], cut, contributions, last['residency'], **kwargs)
        forged = (replace(contributions[0], boot_id='forged-boot'), *contributions[1:])
        with pytest.raises(DataflowError, match='original ACKed'):
            validate_recovered_residency(physical[-1], cut, forged, last['residency'], **kwargs)
        with pytest.raises(DataflowError, match='ACKed completion'):
            validate_recovered_residency(physical[-1], cut, contributions, last['residency'],
                **{**kwargs, 'facts': [facts[-1]]})


def test_resume_after_committed_phase_skips_completed_workers(tmp_path, monkeypatch):
    class Interrupted(Exception): pass
    original = DataflowExecution._retire_phase
    def stop_before_retire(ex, image):
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop_before_retire)
                dataset().take_all()
    launches = []
    launch = DataflowExecution._launch_phase
    def record(ex):
        launches.extend(ex._phases[ex._phase_index])
        return launch(ex)
    with RunContext.resume(ref) as run, data.local_execution():
        before, old = carriers(run)
        completed = old[-1]['residency']['active_stages']
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_launch_phase', record)
            assert dataset().take_all() == [{'n': n} for n in range(1, 5)]
        assert not set(completed) & set(launches)
        after, states = carriers(run)
        assert after[0] == before[0]
        assert states[-1]['cut']['final']
        assert old[0]['contributions'][0] in states[-1]['contributions']


def test_memory_connected_group_rejected_before_launch(tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.planning import dataflow_place
    # A fixed memory contract cannot be reinterpreted; ordinary new plans may
    # now choose durable boundaries automatically before sealing.
    def fixed_memory(graph, snapshots, stages, exchanges, *, hosts, bounds, policy):
        return exchanges, dataflow_place.solve_residency(stages, exchanges, hosts=hosts, bounds=bounds)
    monkeypatch.setattr(dataflow_place, '_select_budgeted_residency', fixed_memory)
    launched = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launched.append(args))
    with pytest.raises(DataflowError, match='resident budget|resident capacity'):
        with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution():
            ds = data.from_items([{'n': 1}])
            for _ in range(4):
                ds = ds.map(lambda row: row)
            ds.take_all()
    assert launched == []


def test_ray_core_finite_phases_use_same_native_cut_protocol(tmp_path, monkeypatch):
    import tempfile
    ray = pytest.importorskip('ray')
    import ray.data
    from demiflow.data.dataset import Dataset
    from demiflow.execution.executors.ray import RayDatasetExecutor
    def forbidden(*args, **kwargs):
        raise AssertionError('Ray Data may not execute the plan')
    for name in ('from_items', 'from_arrow', 'read_datasource'):
        monkeypatch.setattr(ray.data, name, forbidden)
    try:
        ray.init(address="local", num_cpus=4, include_dashboard=False, object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-phase-', dir='/tmp'))
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run:
            ds = dataset()
            hosted = Dataset(ds._source, ds._plan, RayDatasetExecutor())
            assert hosted.take_all() == [{'n': n} for n in range(1, 5)]
            facts, states = carriers(run)
            assert len(states) >= 3 and states[-1]['cut']['final']
            assert all(f['status'] == 'acked' for f in facts)
            assert states[0]['contributions'][0] in states[-1]['contributions']
    finally:
        ray.shutdown()


def test_resume_inside_first_phase_restores_finite_spool_prefix(tmp_path, monkeypatch):
    class Interrupted(Exception): pass
    pump = DataflowExecution.pump
    def interrupt(ex, **kwargs):
        result = pump(ex, **kwargs)
        if ex._checkpoint.residency and not ex._checkpoint.residency['phase_complete']:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits={**LIMITS, 'checkpoint_rows': 2}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                dataset().take_all()
    with RunContext.resume(ref) as run, data.local_execution():
        assert dataset().take_all() == [{'n': n} for n in range(1, 5)]
        facts, states = carriers(run)
        assert not states[0]['residency']['phase_complete']
        assert states[-1]['cut']['final']
        assert len(states) > 3


def limited_rows():
    for n in range(10000):
        if n >= 500:
            raise RuntimeError('logical limit did not stop admission')
        yield {'n': n}


def limited_dataset():
    return data.from_iter(limited_rows).limit(2).sort('n').sort('n')


def test_phase_recovery_keeps_logical_input_completion_distinct_from_eoi(tmp_path, monkeypatch):
    class Interrupted(Exception): pass
    def stop(ex, image):
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                limited_dataset().take_all()
    with RunContext.resume(ref) as run, data.local_execution():
        assert limited_dataset().take_all() == [{'n': 0}, {'n': 1}]
        facts, states = carriers(run)
        assert states[0]['residency']['satisfied_source_splits']
        assert states[-1]['cut']['final']
        assert any(not c['end_of_input'] for c in states[-1]['dataflow.sources.v3'])


def test_finite_phase_write_lance_commits_once_and_recovers(tmp_path, monkeypatch):
    import lance
    class Interrupted(Exception): pass
    def stop(ex, image):
        raise Interrupted()
    target = tmp_path/'output.lance'
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                dataset().write_lance(str(target), mode='create', return_receipt=True)
    assert not target.exists()  # An upstream phase must not publish an empty table.
    with RunContext.resume(ref) as run, data.local_execution():
        receipt = dataset().write_lance(str(target), mode='create', return_receipt=True)
        assert receipt.status == 'committed' and receipt.written_rows == 4
        assert receipt.committed_version == 1
        facts, states = carriers(run)
        assert states[-1]['cut']['final']
    assert lance.dataset(str(target)).to_table().to_pylist() == [{'n': n} for n in range(1, 5)]
