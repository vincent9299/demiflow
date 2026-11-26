"""Join and reducer kernels run in a measured window only when maximum-size rows cannot be placed.

The planner first gives a join or reducer its full window (every row as large as
``max_row_bytes``). Only when no layout fits that, and the row/batch (and reducer
state) ceilings are engine defaults, does it place them with their sealed floor;
the kernel then counts actual bytes against the grant it received.
"""
import logging

import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.compiler_support.dataflow_compile_inputs import CompileInputs
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy

from test_runtime_v3_codec_profile_groups import physical, request

KB = 1024


def _join(width=0):
    # The small right side is indexed: its rows are the ones the join retains.
    left = data.from_items([{'k': i % 3, 'v': i} for i in range(9)])
    return left.join(data.from_items([{'k': i, 'r': -i, 'pad': 'x' * width} for i in range(3)]), on='k')


def _count(state, row):
    return {'k': row['k'], 'n': (state or {}).get('n', 0) + 1}


def _grow(state, row):
    # One distinct ~1 KiB string per row (a repeated literal would be one shared object).
    return {'k': row['k'], 'values': (state or {}).get('values', []) + ['%06d' % row['v'] * 170]}


def _reduce(reducer=_count, rows=30):
    return data.range(rows).map(lambda r: {'k': r['id'] % 3, 'v': r['id']}).reduce_by_key('k', reducer)


def _layout(selected):
    return ([(len(s.occurrences), len(s.partitions), s.replica_min, s.replica_target, s.replica_max,
              s.profile.requests['memory_bytes'], s.resident_bytes, sorted((s.kernel_memory_bytes or {}).values()))
             for s in selected.stages],
            [e.mode for e in selected.exchanges],
            [len(group) for group in selected.residency.simultaneous_groups])


@pytest.mark.parametrize('build,partitions', [(_join, 2), (_reduce, 1)])
def test_a_plan_that_fits_full_windows_is_laid_out_as_without_a_floor(build, partitions):
    # Both plans fit their full windows at the default budget. Selected with
    # the floors instead, the join and the reducer would each be granted less
    # than their full window. The floor must not reach a selection that full
    # windows already satisfy.
    policy, bounds = resolve_runtime_policy({'max_active_workers': 2, 'default_partitions': partitions}, {},
                                            provider='local')
    graphs = [lower_action(request(build()), bounds=bounds,
                           inputs=CompileInputs(explicit_limits=explicit)).graph
              for explicit in (frozenset(), frozenset({'max_row_bytes', 'max_batch_bytes', 'max_state_bytes'}))]
    relational = [[n.kernel for n in g.nodes if n.kernel.kind in ('join', 'reduce')] for g in graphs]
    assert all('codec_min_bytes' in k.configuration for k in relational[0])
    assert not any('codec_min_bytes' in k.configuration for k in relational[1])
    measured, full = (physical(g, policy, bounds) for g in graphs)
    assert _layout(measured) == _layout(full)
    for stage in measured.stages:
        for kernel in relational[0]:
            occurrence = kernel.configuration['occurrence_id']
            assert occurrence not in (stage.kernel_memory_bytes or {})


def test_small_budget_join_and_reduce_run_measured_and_say_so(caplog):
    caplog.set_level(logging.WARNING, logger='demiflow.planning.memory')
    with data.local_execution(workers=2, worker_mode='thread', memory_bytes=128 * KB) as local:
        rows = _join().take_all()
    assert sorted((r['k'], r['v'], r['r']) for r in rows) == sorted((i % 3, i, -(i % 3)) for i in range(9))
    (step,) = local.stats['measured_memory']
    assert step['kind'] == 'join' and step['granted_bytes'] < step['preferred_bytes'] == 32 * KB + 6 * 32 * KB
    assert 0 < step['row_bytes_hint'] < 32 * KB
    assert any('join' in r.getMessage() and 'measured memory' in r.getMessage() for r in caplog.records)
    caplog.clear()
    with data.local_execution(workers=3, worker_mode='thread', memory_bytes=192 * KB) as local:
        rows = _reduce().take_all()
    assert sorted((r['k'], r['n']) for r in rows) == [(0, 10), (1, 10), (2, 10)]
    (step,) = local.stats['measured_memory']
    assert step['kind'] == 'reduce' and step['granted_bytes'] < step['preferred_bytes']
    assert any('reduce' in r.getMessage() and 'measured memory' in r.getMessage() for r in caplog.records)


def test_a_full_window_plan_reports_no_measured_step():
    with data.local_execution(workers=2, worker_mode='thread') as local:
        assert len(_join().take_all()) == 9
    assert 'measured_memory' not in local.stats


def test_a_row_too_large_for_the_measured_join_fails_during_the_run_naming_its_grant(monkeypatch):
    launches = []
    launch = DataflowExecution._prepare_launch

    def observed(execution, *args, **kwargs):
        launches.append(True)
        return launch(execution, *args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', observed)
    # 20 KiB is a legal row under the 32 KiB default ceiling (and fits the full
    # 224 KiB window), but the measured grant cannot index it with its copy.
    with pytest.raises(DataflowError) as failure:
        with data.local_execution(workers=2, worker_mode='thread', memory_bytes=128 * KB):
            _join(width=20 * KB).take_all()
    assert failure.value.code == 'BUDGET_EXCEEDED' and launches
    message = str(failure.value)
    assert 'join runs with measured memory' in message and 'of its preferred 229376 bytes' in message
    assert 'raise memory_bytes, or declare max_row_bytes' in message


def test_reducer_state_beyond_its_measured_share_is_refused():
    # The state limit stays at its 16 MiB default; the measured grant holds a
    # small share of it. A state growing by 1 KiB a row must stop at that
    # share, not use memory the grant does not have.
    with pytest.raises(DataflowError) as failure:
        with data.local_execution(workers=3, worker_mode='thread', memory_bytes=192 * KB):
            _reduce(_grow, rows=90).take_all()
    assert failure.value.code == 'BUDGET_EXCEEDED'
    assert 'reduce runs with measured memory' in str(failure.value)


@pytest.mark.parametrize('build', [_join, _reduce])
def test_declared_row_ceilings_keep_the_refusal_before_any_work(tmp_path, monkeypatch, build):
    launches = []
    launch = DataflowExecution._prepare_launch

    def observed(execution, *args, **kwargs):
        launches.append(True)
        return launch(execution, *args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', observed)
    memory = 128 * KB if build is _join else 192 * KB
    limits = {'memory_bytes': memory, 'max_row_bytes': memory // 4, 'max_batch_bytes': memory // 4}
    with pytest.raises(DataflowError) as failure:
        with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(workers=2):
            build().take_all()
    assert failure.value.code == 'RESOURCE_INFEASIBLE' and launches == []
