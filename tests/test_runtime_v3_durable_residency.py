"""Live incremental parking with real Dataset workers and native cut receipts."""

from demiflow import data
from demiflow.execution.run_context import RunContext

LIMITS = {'memory_bytes': 40*1024, 'max_batch_bytes': 16*1024,
          'max_row_bytes': 8*1024, 'max_state_bytes': 128*1024,
          'timeout_s': 60, 'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}


def test_two_stage_dataset_progresses_when_only_one_stage_fits(tmp_path, monkeypatch):
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        rows = data.from_items([{'n': n} for n in range(8)]).take_all()
        assert rows == [{'n': n} for n in range(8)]
        facts = run.journal.execution_commits('action-1')
        assert len(facts) > 2 and all(f['status'] == 'acked' for f in facts)


def dataset():
    return data.from_items([{'n': n} for n in range(8)])


def facts(run):
    from demiflow.execution.state_artifacts import StateArtifacts
    commits = run.journal.execution_commits('action-1')
    return commits, [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3']) for f in commits]


import pytest


@pytest.mark.parametrize('boundary', ['before_commit', 'after_park', 'after_consume'])
def test_durable_resume_preserves_original_delivery_and_actual_ack(tmp_path, monkeypatch, boundary):
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    class Interrupted(Exception): pass
    def stop(ex, image):
        entries = ex._checkpoint.residency['deliveries']
        consumed = any(e['consumed'] for e in entries.values())
        if boundary == 'after_park' or consumed:
            raise Interrupted()
        return retire(ex, image)
    retire = DataflowExecution._retire_phase
    commit = DataflowCheckpointCoordinator.commit
    def stop_before(coordinator, *args, **kwargs):
        if coordinator._last_image is not None:
            raise Interrupted()
        return commit(coordinator, *args, **kwargs)
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                if boundary == 'before_commit':
                    patch.setattr(DataflowCheckpointCoordinator, 'commit', stop_before)
                else:
                    patch.setattr(DataflowExecution, '_retire_phase', stop)
                dataset().take_all()
    with RunContext.resume(ref) as run, data.local_execution():
        previous, states_before = facts(run)
        assert dataset().take_all() == [{'n': n} for n in range(8)]
        commits, states = facts(run)
        assert commits[:len(previous)] == previous
        assert states[-1]['cut']['final']
        assert all(f['status'] == 'acked' for f in commits)
        if states_before:
            assert states_before[-1]['residency']['deliveries']
        if boundary == 'after_consume':
            assert any(e['consumed'] and e['consumer_cut'] for e in states_before[-1]['residency']['deliveries'].values())


def test_lost_consumer_ack_reconciles_only_actual_input_frontier(tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    original = DataflowExecution._handle_event
    dropped = []
    def drop(ex, event):
        if event.kind == 'ack':
            dropped.append(event)
            return
        return original(ex, event)
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_handle_event', drop)
            assert dataset().take_all() == [{'n': n} for n in range(8)]
        _, states = facts(run)
        assert dropped
        assert any(e['consumed'] and e['consumer_cut'] for s in states for e in s['residency']['deliveries'].values())


def test_first_uncommitted_park_does_not_invent_recovery_proof(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    class Interrupted(Exception): pass
    def stop(*args, **kwargs):
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                dataset().take_all()
    with pytest.raises(DataflowError, match='No committed cut with journal proof'):
        with RunContext.resume(ref) as run, data.local_execution():
            assert run.journal.execution_commits('action-1') == []
            dataset().take_all()


def test_default_checkpoint_quantum_respects_durable_credit(tmp_path):
    limits = {k: v for k, v in LIMITS.items() if k != 'checkpoint_rows'}
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution():
        assert dataset().take_all() == [{'n': n} for n in range(8)]


def test_shared_source_branches_keep_multiplicity(tmp_path):
    from collections import Counter
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        source = data.from_items([{'n': 0}, {'n': 1}])
        rows = source.map(lambda row: {'n': row['n'], 'side': 'a'}).union(
            source.map(lambda row: {'n': row['n'], 'side': 'b'})).take_all()
        assert Counter((r['n'], r['side']) for r in rows) == Counter((n, side) for n in range(2) for side in ('a', 'b'))
        _, states = facts(run)
        assert states[-1]['cut']['final']


def test_ray_core_durable_parking_uses_same_protocol(tmp_path, monkeypatch):
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
        ray.init(address='local', num_cpus=2, include_dashboard=False, object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-durable-'))
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run:
            ds = data.from_items([{'n': n} for n in range(4)])
            hosted = Dataset(ds._source, ds._plan, RayDatasetExecutor())
            assert hosted.take_all() == [{'n': n} for n in range(4)]
            commits, states = facts(run)
            assert len(commits) > 2 and states[-1]['cut']['final']
            assert all(f['status'] == 'acked' for f in commits)
    finally:
        ray.shutdown()


def _many_rows():
    for n in range(10000):
        if n >= 20:
            raise RuntimeError('logical completion failed to stop source')
        yield {'n': n}


def test_parked_limit_propagates_real_input_satisfaction(tmp_path):
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        assert data.range(10000).limit(2).map(lambda row: row).take_all() == [{'id': 0}, {'id': 1}]
        _, states = facts(run)
        assert states[-1]['cut']['final']
        assert any(not c['end_of_input'] for c in states[-1]['dataflow.sources.v3'])
        assert states[-1]['residency']['satisfied_source_splits']


def test_attempt_only_input_cannot_automatically_park(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='cannot safely park'):
        with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution():
            data.from_iter(_many_rows).take_all()


def test_limit_recovery_preserves_actual_closed_input_proof(tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    class Interrupted(Exception): pass
    retire = DataflowExecution._retire_phase
    def stop(ex, image):
        if ex._checkpoint.residency['satisfied_inputs']:
            raise Interrupted()
        return retire(ex, image)
    def limited():
        return data.range(10000).limit(2).map(lambda row: row)
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                limited().take_all()
    with RunContext.resume(ref) as run, data.local_execution():
        assert limited().take_all() == [{'id': 0}, {'id': 1}]
        _, states = facts(run)
        assert states[-1]['cut']['final']
        assert all(c['next_sequence'] <= 4 and not c['end_of_input'] for c in states[-1]['dataflow.sources.v3'])


def test_durable_write_lance_recovers_without_empty_early_publication(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.dataflow import DataflowExecution
    class Interrupted(Exception): pass
    def stop(ex, image):
        raise Interrupted()
    target = tmp_path/'result.lance'
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                dataset().write_lance(str(target), mode='create', return_receipt=True)
    assert not target.exists()
    with RunContext.resume(ref) as run, data.local_execution():
        receipt = dataset().write_lance(str(target), mode='create', return_receipt=True)
        assert receipt.status == 'committed' and receipt.written_rows == 8
        assert receipt.committed_version == 1
    assert lance.dataset(str(target)).to_table().to_pylist() == [{'n': n} for n in range(8)]


def test_park_metadata_rechecks_bound_after_actual_cut_proofs_grow():
    from types import SimpleNamespace
    from demiflow.execution.dataflow_durable_residency import DurableResidency
    from demiflow.execution.recovery_contract import RecoveryContractError, document
    controller = object.__new__(DurableResidency)
    controller.ex = SimpleNamespace(_bounds=SimpleNamespace(max_state_bytes=65536),
                                    _finite_seals={}, _checkpoint=SimpleNamespace(_operator_complete=set()))
    controller.epoch = 0
    controller.active_index = 0
    controller.groups = [('stage',)]
    controller.visited = controller.completed = controller.acked_live = controller.edge_eoi = controller.satisfied_inputs = set()
    controller.entries = {'delivery': {'payload': {'ref': 'durable-reference'}, 'consumed': False,
                                       'producer_cut': None, 'consumer_cut': None}}
    size_before = len(document(controller.metadata(False)))
    controller.ex._bounds.max_state_bytes = size_before + 32
    controller.entries['delivery']['producer_cut'] = 'a'*64
    with pytest.raises(RecoveryContractError, match='budget'):
        controller.metadata(False)
