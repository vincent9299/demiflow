"""Only the memory group the solver cannot place is parked.

When every stage cannot be resident at once, the planner turns live memory edges
into disk-backed boundaries so groups can run in turn. A group that fits keeps its
live edge: parking it as well would make its stages hand a bounded window to each
other through checkpoints, relaunching their workers for every window.
"""
from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy

from test_runtime_v3_codec_profile_groups import physical, request


def _key(row):
    return {'k': row['id'] % 4, 'v': row['id']}


def _count(state, row):
    return {'k': row['k'], 'n': (state or {}).get('n', 0) + 1}


def _relabel(row):
    return {'k': row['n'] % 2, 'n': row['n']}


def _modes_by_kinds(graph, selected):
    kinds = {node.occurrence_id: node.kernel.kind for node in graph.nodes}
    owner = {p: s for s in selected.stages for p in s.partitions}
    def kind(partition):
        return '+'.join(kinds[o] for o in owner[partition].occurrences)
    return sorted((kind(e.producer_partitions[0]), kind(e.consumer_partitions[0]), e.mode) for e in selected.exchanges)


def _select(dataset):
    # Default budget, unfused stages: a reducer's full window cannot share the
    # resident budget with its consumer, while the source and map fit together.
    policy, bounds = resolve_runtime_policy({'fusion': False}, {}, provider='local')
    graph = lower_action(request(dataset), bounds=bounds).graph
    return graph, physical(graph, policy, bounds)


def test_a_group_that_fits_keeps_its_memory_edge_when_another_must_park():
    graph, selected = _select(data.range(40).map(_key).reduce_by_key('k', _count))
    assert _modes_by_kinds(graph, selected) == [
        ('MapOp', 'reduce', 'finite_sealed'),
        ('range', 'MapOp', 'memory_incremental'),
        ('reduce', 'take_all', 'finite_sealed')]
    assert [len(group) for group in selected.residency.simultaneous_groups] == [2, 1, 1]


def test_every_group_that_cannot_fit_is_parked_and_only_those():
    graph, selected = _select(data.range(40).map(_key).reduce_by_key('k', _count)
                              .map(_relabel).reduce_by_key('k', _count))
    modes = _modes_by_kinds(graph, selected)
    # Two groups (each reducer with its consumer) exceed the budget; the source
    # and the first map still run together.
    assert ('range', 'MapOp', 'memory_incremental') in modes
    assert [m for m in modes if m[0] == 'reduce'] == [
        ('reduce', 'MapOp', 'finite_sealed'), ('reduce', 'take_all', 'finite_sealed')]
    assert sum(m[2] == 'memory_incremental' for m in modes) == 1


def test_stages_that_fit_together_run_once_instead_of_in_parked_windows(monkeypatch):
    launches = []
    prepare = DataflowExecution._prepare_launch
    def counted(self, *args, **kwargs):
        launches.append(self._physical.plan_id)
        return prepare(self, *args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', counted)
    with data.local_execution(workers=2, worker_mode='thread'):
        rows = data.range(40).map(_key).reduce_by_key('k', _count).take_all()
    assert sorted((r['k'], r['n']) for r in rows) == [(0, 10), (1, 10), (2, 10), (3, 10)]
    # One launch per stage. Parking the source and the map as well moved at
    # most a few rows per checkpointed window and relaunched both every time.
    assert len(launches) == 4
