"""Native node observations carry measured elapsed time for operators and Lance writes."""
import math
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.journal.dataflow_operator_observations import BusyClock, observe_busy
from demiflow.execution.journal.run_journal import _node_document


def _checked_write(tmp_path, observed):
    from demiflow import data
    from demiflow.execution.pipeline.checked import CheckedExecution
    from demiflow.forge.flow_check import FlowChecker
    from demiflow.forge.platform import inspect_modules, LIMITS
    from demiflow.workbench.runs.manager import DEFAULT_LIMITS
    from test_forge_native_semantics import native
    (tmp_path / 'operators').mkdir()
    (tmp_path / 'operators/__init__.py').write_text('')
    (tmp_path / 'operators/pure.py').write_text(
        'from typing import TypedDict\n'
        'class Row(TypedDict):\n    name: str\n    count: int\n'
        'class Result(TypedDict):\n    name: str\n    double: int\n'
        'def convert(row: Row) -> Result:\n'
        '    return {"name": row["name"], "double": row["count"] * 2}\n')
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'flows/pure.py').write_text(
        'from demiflow import data\nfrom operators.pure import convert\n'
        'def run(*, target):\n'
        '    rows = data.from_items([{"name": "a", "count": 1}, {"name": "b", "count": 2}])\n'
        '    return rows.map(convert).write_lance(target, mode="create", return_receipt=True)\n')
    target = str(tmp_path / 'output.lance')
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(tmp_path, catalog, {}, [target]).inspect('flows/pure.py:run', {'target': target})
    assert report['status'] == 'checked', report['diagnostics']
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None,
        observe=observed.extend, report_digest=report['digest'],
        targets={target: {'mode': 'create', 'expected_version': None}})
    sys.path.insert(0, str(tmp_path))
    try:
        from operators.pure import convert
        with native(tmp_path), checked.bind_context():
            receipt = data.from_items([{'name': 'a', 'count': 1}, {'name': 'b', 'count': 2}]).map(
                convert).write_lance(target, mode='create', return_receipt=True)
        checked.flush_observations(final=True, succeeded=True)
    finally:
        sys.path.remove(str(tmp_path))
        for name in [n for n in sys.modules if n == 'operators' or n.startswith('operators.')]:
            del sys.modules[name]
    return report, receipt


def test_map_and_write_lance_observations_record_elapsed(tmp_path):
    observed = []
    report, receipt = _checked_write(tmp_path, observed)
    assert receipt.written_rows == 2
    latest = {}
    for update in observed:
        latest[update['node_id']] = update['document']
    kinds = {node['id']: node['kind'] for node in report['nodes']}
    by_kind = {kinds[node]: doc for node, doc in latest.items()}
    assert {'from_items', 'map', 'write_table'} <= set(by_kind)
    for kind, doc in by_kind.items():
        _node_document(doc)  # The persisted journal format accepts it unchanged.
        assert doc['timing_scope'] in {'node', 'group'}, (kind, doc)
        assert isinstance(doc['elapsed_s'], float) and math.isfinite(doc['elapsed_s'])
        assert doc['elapsed_s'] >= 0, (kind, doc)
    # The Lance write terminal includes its measured sink publication.
    assert by_kind['write_table']['timing_scope'] == 'node'
    assert by_kind['write_table']['elapsed_s'] > 0
    assert by_kind['map']['timing_scope'] == 'node'


def test_worker_progress_carries_monotonic_busy_time_for_operators(tmp_path, monkeypatch):
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.action_driver.dataflow import DataflowExecution

    seen = {}
    accept = DataflowExecution._handle_event

    def observe(execution, event):
        result = accept(execution, event)
        if event.kind == 'progress':
            node = next(n for n in execution._graph.nodes if n.occurrence_id == event.payload['occurrence_id'])
            value = event.payload['busy_ns']
            previous = seen.get((node.kernel.kind, event.boot_id), 0)
            assert type(value) is int and value >= previous
            seen[(node.kernel.kind, event.boot_id)] = value
        return result

    monkeypatch.setattr(DataflowExecution, '_handle_event', observe)
    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=1, partitions=1):
        result = (data.range(4).map(lambda row: {'x': row['id'] + 1})
                  .filter(lambda row: row['x'] % 2 == 0)
                  .flat_map(lambda row: [row, {'x': row['x'] + 10}]).take_all())
    assert result == [{'x': 2}, {'x': 12}, {'x': 4}, {'x': 14}]
    kinds = {kind for kind, _ in seen}
    assert {'MapOp', 'FilterOp', 'FlatMapOp'} <= kinds


def test_busy_clock_counts_overlapping_causes_once():
    clock = BusyClock()
    assert clock.snapshot() == 0
    clock.begin()
    clock.begin()  # e.g. worker loop and an in-flight call at the same time
    time.sleep(.02)
    clock.end()
    open_value = clock.snapshot()
    assert open_value >= 20_000_000
    clock.end()
    closed = clock.snapshot()
    assert closed >= open_value
    time.sleep(.01)
    assert clock.snapshot() == closed  # idle time is not counted
    with pytest.raises(DataflowError):
        clock.end()


def test_concurrent_calls_report_union_of_inflight_time():
    from demiflow.execution.kernels.concurrent_calls import ConcurrentCalls
    clock = BusyClock()
    calls = ConcurrentCalls(2, check_stop=lambda: None, clock=clock)

    def compute(slot):
        time.sleep(.3)
        yield slot

    outputs = []
    try:
        for _ in range(2):
            outputs.extend(calls.submit(compute, scope=lambda slot: 'a' * 64))
        outputs.extend(calls.finish())
    finally:
        calls.close(timeout=5)
    assert sorted(outputs) == [0, 1]
    busy = clock.snapshot() / 1e9
    # Two overlapping 0.3 s calls occupy the node about 0.3 s, never 0.6 s.
    assert .3 <= busy < .55


class _Checked:
    def __init__(self):
        self.calls = []

    def duration(self, key, elapsed):
        self.calls.append(('node', key, elapsed))

    def group_duration(self, keys, elapsed):
        self.calls.append(('group', tuple(keys), elapsed))


def _execution(report_keys, occurrences=('a', 'b', 'c')):
    stage = SimpleNamespace(occurrences=occurrences)
    endpoint = SimpleNamespace(_assignment=SimpleNamespace(stages=(stage,)))
    return SimpleNamespace(_checked=_Checked(), _report_keys=report_keys,
                           _endpoints={'alloc': endpoint})


def _event(boot):
    return SimpleNamespace(boot_id=boot, allocation_id='alloc')


def test_busy_projection_uses_boot_deltas_and_group_timing():
    execution = _execution({'a': ('act', 'A', 0), 'b': ('act', 'B', 0), 'c': ('act', 'C', 0)})
    observe_busy(execution, _event('boot1'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 0})
    observe_busy(execution, _event('boot1'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 0})
    observe_busy(execution, _event('boot1'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 2_000_000_000})
    observe_busy(execution, _event('boot1'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 3_000_000_000})
    observe_busy(execution, _event('boot1'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 3_000_000_000})
    # A new boot's clock starts at zero; its whole value is new time.
    observe_busy(execution, _event('boot2'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 500_000_000})
    assert execution._checked.calls == [('node', ('act', 'A', 0), 0.0), ('node', ('act', 'A', 0), 2.0),
                                        ('node', ('act', 'A', 0), 1.0), ('node', ('act', 'A', 0), .5)]
    with pytest.raises(DataflowError, match='backwards'):
        observe_busy(execution, _event('boot2'), {'occurrence_id': 'a', 'partition_id': 'p', 'busy_ns': 1})
    execution._checked.calls.clear()
    group = {'partition_id': 'p', 'busy_ns': 4_000_000_000, 'busy_group': ['b', 'c']}
    observe_busy(execution, _event('boot1'), {**group, 'occurrence_id': 'b'})
    observe_busy(execution, _event('boot1'), {**group, 'occurrence_id': 'c'})  # same shared clock
    assert execution._checked.calls == [('group', (('act', 'B', 0), ('act', 'C', 0)), 4.0)]
    for bad in ({'busy_ns': -1}, {'busy_ns': 1.5}, {'busy_ns': 1, 'busy_group': ['b', 'z']},
                {'busy_ns': 1, 'busy_group': ['c']}):
        with pytest.raises(DataflowError):
            observe_busy(execution, _event('boot1'), {'occurrence_id': 'b', 'partition_id': 'p', **bad})
