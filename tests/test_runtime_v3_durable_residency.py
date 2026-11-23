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
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
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
    from demiflow.execution.action_driver.dataflow import DataflowExecution
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
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
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
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-durable-', dir='/tmp'))
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
    from demiflow.execution.action_driver.dataflow import DataflowExecution
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
    from demiflow.execution.action_driver.dataflow import DataflowExecution
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


@pytest.mark.parametrize('schema', ['demiflow.durable_residency.v1', 'demiflow.durable_residency.v2'])
def test_park_metadata_rechecks_bound_after_actual_cut_proofs_grow(schema):
    from types import SimpleNamespace
    from demiflow.execution.dataflow_durable_residency import DurableResidency
    from demiflow.execution.recovery_contract import RecoveryContractError, document
    controller = object.__new__(DurableResidency)
    from demiflow.execution.exchange.deliveries import DeliveryExchange
    controller._bounds = SimpleNamespace(max_state_bytes=65536)
    controller._finite_seals = {}
    controller._exchange = DeliveryExchange()
    controller._physical = SimpleNamespace(stages=(
        SimpleNamespace(stage_id='stage', partitions=('partition',)),))
    controller.epoch = 0
    controller.active_index = 0
    controller.groups = [('stage',)]
    controller.partition_groups = [('partition',)]
    controller.schema = schema
    controller.visited = controller.completed = controller.edge_eoi = controller.satisfied_inputs = set()
    controller._exchange._parked = {'delivery': {'payload': {'ref': 'durable-reference'}, 'consumed': False,
                                       'producer_cut': None, 'consumer_cut': None}}
    size_before = len(document(controller.metadata(False, ())))
    controller._bounds.max_state_bytes = size_before + 32
    controller._exchange._parked['delivery']['producer_cut'] = 'a'*64
    with pytest.raises(RecoveryContractError, match='budget'):
        controller.metadata(False, ())


def _sort_row(row):
    i = row['id']
    return {'key': i % 3, 'i': i, 'value': b'x' * 12000}


@pytest.mark.parametrize('finish', ['continue', 'restart', 'close'])
def test_last_sealed_sort_keeps_worker_until_a_real_exit(tmp_path, monkeypatch, finish):
    """Keep the last iterator across ACK; a cold restart can rebuild from its cut."""
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.kernels import ordering

    class Interrupted(Exception):
        pass

    run_ref = None
    expected = sorted(range(48), key=lambda i: i % 3)
    runs = []
    retired = []
    acked = []
    seen = []
    sort = ordering.sort_with_body_references
    handle = DataflowExecution._handle_event
    retire = DataflowExecution._retire_phase
    resume = DataflowExecution._continue_after_cut

    def observed(records, **kwargs):
        runs.append(kwargs['rows'])
        return sort(records, **kwargs)

    def event(execution, message):
        value = handle(execution, message)
        if message.kind == 'terminal':
            seen.append(message.payload['record_id'])
            if len(seen) == 2:
                execution.request_checkpoint()
        return value

    def rotation(execution, image):
        retired.append(execution._durable_residency.can_drain_in_place())
        return retire(execution, image)

    def after_ack(execution):
        image = execution._restore_image
        if seen and execution._durable_residency.can_drain_in_place():
            assert execution._action.context.journal.execution_commit(image.commit_id)['status'] == 'acked'
            acked.append(image.commit_id)
            if finish == 'restart':
                raise Interrupted
        return resume(execution)

    def rows():
        return data.range(len(expected)).map(_sort_row).sort('key').iter_rows()

    limits = {'memory_bytes': 2 << 20, 'max_row_bytes': 48000, 'max_batch_bytes': 360000,
              'checkpoint_interval_ms': 60000}
    provider = dict(workers=1, partitions=1, worker_mode='thread')
    with monkeypatch.context() as patch:
        patch.setattr(ordering, 'sort_with_body_references', observed)
        patch.setattr(DataflowExecution, '_handle_event', event)
        patch.setattr(DataflowExecution, '_retire_phase', rotation)
        patch.setattr(DataflowExecution, '_continue_after_cut', after_ack)
        try:
            with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(**provider):
                run_ref = run.ref
                it = rows()
                if finish == 'close':
                    assert next(it)['i'] == expected[0]
                    it.close()
                    assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
                else:
                    assert [r['i'] for r in it] == expected
        except Interrupted:
            assert finish == 'restart'
        assert retired and not any(retired), 'earlier cohorts must still rotate'
        assert runs == [len(expected)], 'parking must not rebuild a live external sort'
        if finish != 'close':
            assert acked, 'exercise a non-final ACK under the live sort worker'
    if finish == 'restart':
        with RunContext.resume(run_ref) as recovered, data.local_execution(**provider):
            assert [r['i'] for r in rows()] == expected
            assert recovered.journal.execution_commit(acked[-1])['status'] == 'acked'
            commits, states = facts(recovered)
            assert states[-1]['cut']['final'] and all(f['status'] == 'acked' for f in commits)
