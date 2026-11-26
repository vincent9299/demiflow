"""Concurrent per-record calls share one byte window: each call holds its actual input and one output row."""
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.concurrent_calls import ConcurrentCalls
from demiflow.execution.kernels.operator import OperatorKernel
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.resources.contracts import ResourceProfile
from test_map_prompt_async import prompt, server  # noqa: F401  (server is a pytest fixture)
from test_runtime_v3_codec_profile_groups import physical, request
from test_runtime_v3_k import InputPort, program, rec, rows_of, services as kernel_services, spec


def _same(row):
    return row


async def _same_async(row):
    return row


async def _same_batch(rows):
    return rows


def _compiled(dataset, kind, limits=None):
    policy, bounds = resolve_runtime_policy({}, limits or {}, provider='local')
    graph = lower_action(request(dataset), bounds=bounds).graph
    kernel = next(node.kernel for node in graph.nodes if node.kernel.kind == kind)
    return kernel, graph, policy, bounds


def _thread_map(calls):
    return data.from_items([{'i': 1}]).map(_same, execution='thread', concurrency=calls)


def _stage_of(selected, graph, kind):
    occurrence = next(n.occurrence_id for n in graph.nodes if n.kernel.kind == kind)
    return next(s for s in selected.stages if occurrence in s.occurrences)


# ---- what the planner seals ---------------------------------------------------------------

@pytest.mark.parametrize('build,kind', [(_thread_map, 'MapOp'),
    (lambda calls: data.from_items([{'i': 1}]).map_async(_same_async, concurrency=calls), 'AsyncMapOp')])
@pytest.mark.parametrize('calls', [2, 4, 64])
def test_the_minimum_holds_one_call_whatever_the_declared_concurrency(build, kind, calls):
    one, *_ = _compiled(build(1), kind)
    many, _, _, bounds = _compiled(build(calls), kind)
    row, batch = bounds.max_row_bytes, bounds.max_batch_bytes
    # A single call needs no window: its minimum already holds one full batch and row.
    assert 'call_input_min_bytes' not in one.configuration
    assert 'call_output_min_bytes' not in one.configuration
    assert many.configuration['call_input_min_bytes'] == row
    assert many.configuration['call_output_min_bytes'] == row
    # One input in flight and its reserved output, beside the batch window that
    # waiting input and an emitted output's encoding share, in place of the
    # single call's batch + row. State and fixed costs are the same.
    assert (many.configuration['codec_min_bytes'] - one.configuration['codec_min_bytes']
            == (2 * row + batch) - (batch + row))
    assert many.profile.requests['memory_bytes'] > many.configuration['codec_min_bytes']


def test_batch_calls_keep_a_full_window_per_slot():
    kernel, *_ = _compiled(data.from_items([{'i': 1}]).batch_map(_same_batch, max_batch=2, concurrency=4),
                           'BatchMapOp')
    assert kernel.configuration.get('concurrency') == 4
    assert 'call_input_min_bytes' not in kernel.configuration
    assert 'call_output_min_bytes' not in kernel.configuration


def test_the_planner_and_the_kernel_read_one_definition_of_the_per_record_route():
    from demiflow.execution.contracts.dataflow_kinds import kinds_with, record_call_kinds
    from demiflow.execution.kernels import dataflow, operator
    assert operator._RECORD_CALL_KINDS == record_call_kinds()
    # The route recorded in the compile contract inventory: unary transforms and unary model calls.
    assert record_call_kinds() == dataflow._UNARY_KINDS | (dataflow._MODEL_KINDS - kinds_with('batch_instance'))
    # A batch call takes a tuple of records; its input cannot be charged per record.
    assert not record_call_kinds() & kinds_with('batch_instance')
    assert {'MapOp', 'AsyncMapOp', 'prompt', 'agent', 'fetch', 'document'} <= record_call_kinds()
    assert 'embedding' not in record_call_kinds()


@pytest.mark.parametrize('calls', [8, 64])
def test_default_budget_runs_concurrent_model_calls_in_one_resident_layout(server, calls):
    from demiflow.data.api import DataAPI
    dataset = prompt(DataAPI().from_items([{'item': i} for i in range(8)]), concurrency=calls)
    kernel, graph, policy, bounds = _compiled(dataset, 'prompt')
    row, batch = bounds.max_row_bytes, bounds.max_batch_bytes
    assert kernel.configuration['concurrency'] == calls
    assert kernel.configuration['codec_min_bytes'] == 2 * row + batch
    selected = physical(graph, policy, bounds)
    # Every memory edge stays in memory: no stage is parked and restarted.
    assert {exchange.mode for exchange in selected.exchanges} == {'memory_incremental'}
    assert len(selected.residency.simultaneous_groups) == 1
    stage = _stage_of(selected, graph, 'prompt')
    assert stage.profile.requests['memory_bytes'] >= kernel.configuration['codec_min_bytes']
    assert selected.bounds == bounds                       # the caller's limits are not rewritten


# ---- what the kernel enforces -------------------------------------------------------------

def _kernel(function, configuration, *, memory, kind='MapOp'):
    services = kernel_services(programs={'fn': program(function)})
    kernel = OperatorKernel(spec(kind, {'program': 'fn', **configuration},
                                 inputs=[InputPort('input', 'incremental')],
                                 profile=ResourceProfile(requests={'cpu_millis': 1000, 'memory_bytes': memory})),
                            services=services, partition_id='p0')
    kernel.start()
    return kernel, services


def test_the_window_is_the_sealed_input_and_output_reserve_plus_the_granted_slack():
    config = {'concurrency': 3, 'codec_min_bytes': 1000, 'call_input_min_bytes': 64, 'call_output_min_bytes': 64}
    kernel, _ = _kernel(_same, config, memory=1500)
    try:
        assert kernel._calls.byte_limit == 64 + 64 + 500
        assert kernel._calls.output_reserve == 64
    finally:
        kernel.close()


def test_plans_sealed_before_the_output_reserve_keep_their_original_admission():
    # Sealed by the first step: one row of input credit, one output per slot in the minimum.
    kernel, _ = _kernel(_same, {'concurrency': 3, 'codec_min_bytes': 1000, 'call_input_min_bytes': 64}, memory=1500)
    try:
        assert (kernel._calls.byte_limit, kernel._calls.output_reserve) == (64 + 500, 0)
    finally:
        kernel.close()
    # A map or model plan sealed earlier reserved a full window per slot: no gate.
    kernel, _ = _kernel(_same, {'concurrency': 3, 'codec_min_bytes': 1000}, memory=1500)
    try:
        assert kernel._calls.byte_limit is None
    finally:
        kernel.close()
    # Async map alone already had a one-row credit inside its sealed minimum.
    kernel, services = _kernel(_same_async, {'concurrency': 3, 'codec_min_bytes': 1000}, memory=1500,
                               kind='AsyncMapOp')
    try:
        assert kernel._calls.byte_limit == services.bounds.max_row_bytes + 500
    finally:
        kernel.close()


@pytest.mark.parametrize('config', [{'concurrency': 2, 'call_input_min_bytes': 64},
                                    {'concurrency': 2, 'codec_min_bytes': 1000, 'call_output_min_bytes': 64}])
def test_a_window_field_without_the_rest_of_its_sealed_contract_is_refused(config):
    with pytest.raises(DataflowError) as error:
        _kernel(_same, config, memory=1500)
    assert error.value.code == 'INVALID_CONTRACT'


def _blocking_call(seen, lock, together):
    class Call:
        def __call__(self, row):
            with lock:
                seen['active'] += 1
                seen['peak'] = max(seen['peak'], seen['active'])
                first = together is not None and seen.get('started', 0) < together.parties
                seen['started'] = seen.get('started', 0) + 1
            try:
                if first:
                    together.wait()        # fails unless the first calls are in flight at once
                elif together is None:
                    time.sleep(.02)        # long enough for a second admission to overlap
            finally:
                with lock:
                    seen['active'] -= 1
            return row
    return Call()


@pytest.mark.parametrize('slack_calls,peak', [(0, 1), (1, 2), (2, 3)])
def test_calls_share_the_window_by_actual_input_bytes_and_one_reserved_output(slack_calls, peak):
    lock, seen = threading.Lock(), {'active': 0, 'peak': 0}
    together = threading.Barrier(peak, timeout=5) if peak > 1 else None
    rows = [rec({'v': 'x' * 1000, 'i': i}, sequence=i) for i in range(3)]
    size = rows[0].encoded_bytes
    reserve = 2 * size
    assert all(row.encoded_bytes == size for row in rows)
    # The sealed window holds one call (its input plus the output reserve); each
    # granted call's worth of slack admits one more.
    minimum = 10 * size
    kernel, _ = _kernel(_blocking_call(seen, lock, together),
                        {'concurrency': 3, 'codec_min_bytes': minimum, 'call_input_min_bytes': size,
                         'call_output_min_bytes': reserve},
                        memory=minimum + slack_calls * (size + reserve))
    try:
        out = [record for row in rows for record in kernel.push('input', row)]
        out += list(kernel.finish('input'))
        assert sorted(row['i'] for row in rows_of(out)) == [0, 1, 2]
        assert seen['peak'] == peak
        facts = kernel.call_concurrency
        assert (facts['capacity'], facts['peak']) == (3, peak)
        assert facts['byte_limit'] == size + reserve + slack_calls * (size + reserve)
        assert facts['output_reserve'] == reserve
        # Each of the three rows found a free slot; those that found no room for
        # their bytes waited, which is what limited the concurrency.
        assert facts['byte_waits'] == 3 - peak
    finally:
        kernel.close()


def test_without_a_window_concurrency_is_limited_only_by_slots():
    lock, seen = threading.Lock(), {'active': 0, 'peak': 0}
    rows = [rec({'i': i}, sequence=i) for i in range(4)]
    kernel, _ = _kernel(_blocking_call(seen, lock, threading.Barrier(2, timeout=5)),
                        {'concurrency': 2, 'codec_min_bytes': 1000}, memory=1500)
    try:
        out = [record for row in rows for record in kernel.push('input', row)]
        out += list(kernel.finish('input'))
        assert len(out) == 4
        facts = kernel.call_concurrency
        assert facts == {'capacity': 2, 'peak': 2, 'byte_waits': 0, 'byte_limit': None, 'output_reserve': 0}
    finally:
        kernel.close()


def test_a_lone_call_always_fits_and_an_oversized_charge_is_refused():
    big = rec({'v': 'x' * 2000}, sequence=0)
    calls = ConcurrentCalls(2, check_stop=lambda: None, byte_limit=big.encoded_bytes + 10, output_reserve=10)
    try:
        assert list(calls.submit(lambda slot: iter((big,)), scope=lambda slot: 'a' * 64, input=big)) in ([], [big])
        assert list(calls.finish()) in ([], [big])
        assert calls.bytes_in_flight == 0
        with pytest.raises(DataflowError) as error:
            list(calls.submit(lambda slot: iter(()), scope=lambda slot: 'b' * 64,
                              input=rec({'v': 'x' * 2100}, sequence=1)))
        assert error.value.code == 'BUDGET_EXCEEDED'
    finally:
        calls.close(timeout=5)


def test_restored_calls_are_charged_with_their_output_reserve():
    from demiflow.execution.contracts.recovery_contract import identity
    first = rec({'i': 0}, sequence=0)
    image = [{'slot': 0, 'input': first, 'scope': identity(['s', 0]), 'emitted': 0}]
    calls = ConcurrentCalls(2, check_stop=lambda: None, byte_limit=first.encoded_bytes + 5, output_reserve=10)
    try:
        with pytest.raises(DataflowError) as error:
            calls.restore(image, slots=[1], compute=lambda slot, input: iter((input,)))
        assert error.value.code == 'CORRUPT_ARTIFACT'
        assert not calls.jobs and calls.bytes_in_flight == 0
    finally:
        calls.close(timeout=5)


# ---- what the user sees -------------------------------------------------------------------

async def _slow_async(row):
    import asyncio
    await asyncio.sleep(.05)
    return row


def test_stream_stats_report_declared_and_reached_concurrency(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    with RunContext.start(tmp_path / 'run'):
        stats = data.from_items([{'i': i} for i in range(6)]).map_async(_slow_async, concurrency=4).run_stream()
    assert stats.emitted == 6
    entry, = [item for item in stats.metrics['call_concurrency'] if item['capacity'] == 4]
    assert 1 <= entry['peak'] <= 4 and entry['scope'] == 'attempt'
    # Small rows under the default budget: the declared concurrency, not memory, is the limit.
    assert entry['byte_waits'] == 0 and entry['limited_by'] is None
    assert entry['byte_limit'] >= 2 * entry['output_reserve'] > 0


def test_memory_limited_concurrency_is_visible_in_stats(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    # Each admitted call holds its input and one 16 MiB output row: the default
    # budget holds far fewer than 64 at once, and the stats say memory limited it.
    with RunContext.start(tmp_path / 'run'):
        stats = data.from_items([{'i': i} for i in range(24)]).map_async(_slow_async, concurrency=64).run_stream()
    assert stats.emitted == 24
    entry, = [item for item in stats.metrics['call_concurrency'] if item['capacity'] == 64]
    assert 1 <= entry['peak'] < 64
    assert entry['peak'] <= entry['byte_limit'] // entry['output_reserve']
    assert entry['byte_waits'] > 0 and entry['limited_by'] == 'memory'


def test_local_stats_carry_the_same_concurrency_facts(tmp_path):
    with data.local_execution(workers=2, worker_mode='thread') as local:
        rows = data.from_items([{'i': i} for i in range(6)]).map_async(_slow_async, concurrency=3).take_all()
    assert sorted(row['i'] for row in rows) == list(range(6))
    entry, = [item for item in local.stats['call_concurrency'] if item['capacity'] == 3]
    assert 1 <= entry['peak'] <= 3 and entry['limited_by'] is None


def test_a_concurrency_that_does_not_fit_as_slots_runs_with_fewer_calls(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    # Four slots of 32 KiB rows never fit 128 KiB; one call (an input, its
    # reserved output and one batch window) does. The
    # plan used to be refused before starting; it now runs with fewer calls.
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 32 * 1024, 'max_batch_bytes': 32 * 1024}), \
            data.local_execution(workers=1, memory_bytes=128 * 1024) as local:
        rows = data.from_items([{'n': n} for n in range(5)]).map_async(_slow_async, concurrency=4).take_all()
    assert sorted(row['n'] for row in rows) == list(range(5))
    entry, = [item for item in local.stats['call_concurrency'] if item['capacity'] == 4]
    assert 1 <= entry['peak'] < 4


async def _enrich(row):
    return {'i': row['i'], 'payload': list(range(100))}


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_waiting_input_and_output_encoding_keep_a_batch_window(tmp_path, mode):
    from demiflow.execution.pipeline.run_context import RunContext
    # A batch four times the row. The 600-byte output's typed codec tree is
    # larger than one 8 KiB row; with only a row left beside the call window,
    # its encoding overran the grant by a few dozen bytes (found in review).
    limits = dict(memory_bytes=160 * 1024, max_row_bytes=8192, max_batch_bytes=32768, max_state_bytes=16384)
    with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(workers=2, worker_mode=mode):
        rows = data.from_items([{'i': i} for i in range(6)]).map_async(_enrich, concurrency=2).take_all()
    assert sorted(rows, key=lambda r: r['i']) == [{'i': i, 'payload': list(range(100))} for i in range(6)]
