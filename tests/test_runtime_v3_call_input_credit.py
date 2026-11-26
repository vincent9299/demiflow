"""Concurrent per-record calls reserve one row of in-flight input; the kernel admits the rest by bytes."""
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
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


# ---- what the planner seals ---------------------------------------------------------------

@pytest.mark.parametrize('build,kind', [(_thread_map, 'MapOp'),
    (lambda calls: data.from_items([{'i': 1}]).map_async(_same_async, concurrency=calls), 'AsyncMapOp')])
def test_concurrent_calls_reserve_one_output_per_slot_and_one_row_of_input(build, kind):
    one, *_ = _compiled(build(1), kind)
    many, _, _, bounds = _compiled(build(4), kind)
    row, batch = bounds.max_row_bytes, bounds.max_batch_bytes
    # A single call needs no credit: its minimum already holds one full window.
    assert 'call_input_min_bytes' not in one.configuration
    assert many.configuration['call_input_min_bytes'] == row
    # Four slots keep four outputs, one row in flight and one waiting for credit,
    # in place of the single call's batch + row. State and fixed costs are the same.
    assert (many.configuration['codec_min_bytes'] - one.configuration['codec_min_bytes']
            == (4 + 2) * row - (batch + row))
    assert many.profile.requests['memory_bytes'] > many.configuration['codec_min_bytes']


def test_batch_calls_keep_a_full_window_per_slot():
    kernel, *_ = _compiled(data.from_items([{'i': 1}]).batch_map(_same_batch, max_batch=2, concurrency=4),
                           'BatchMapOp')
    assert kernel.configuration.get('concurrency') == 4
    assert 'call_input_min_bytes' not in kernel.configuration


def test_the_planner_and_the_kernel_read_one_definition_of_the_per_record_route():
    from demiflow.execution.contracts.dataflow_kinds import kinds_with, record_call_kinds
    from demiflow.execution.kernels import operator
    from demiflow.execution.kernels import dataflow
    assert operator._RECORD_CALL_KINDS == record_call_kinds()
    # The route recorded in the compile contract inventory: unary transforms and unary model calls.
    assert record_call_kinds() == dataflow._UNARY_KINDS | (dataflow._MODEL_KINDS - kinds_with('batch_instance'))
    # A batch call takes a tuple of records; its input cannot be charged per record.
    assert not record_call_kinds() & kinds_with('batch_instance')
    assert {'MapOp', 'AsyncMapOp', 'prompt', 'agent', 'fetch', 'document'} <= record_call_kinds()
    assert 'embedding' not in record_call_kinds()


def test_default_budget_admits_eight_concurrent_model_calls(server):
    from demiflow.data.api import DataAPI
    dataset = prompt(DataAPI().from_items([{'item': i} for i in range(8)]), concurrency=8)
    kernel, graph, policy, bounds = _compiled(dataset, 'prompt')
    row = bounds.max_row_bytes
    assert kernel.configuration['concurrency'] == 8
    assert kernel.configuration['call_input_min_bytes'] == row
    assert kernel.configuration['codec_min_bytes'] == (8 + 2) * row
    # Before the credit this node alone needed 8 * (batch + row), more than the default run budget.
    selected = physical(graph, policy, bounds)
    stage = next(s for s in selected.stages if any(
        n.occurrence_id in s.occurrences and n.kernel.kind == 'prompt' for n in graph.nodes))
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


def test_input_window_is_the_sealed_credit_plus_the_granted_slack():
    kernel, _ = _kernel(_same, {'concurrency': 3, 'codec_min_bytes': 1000, 'call_input_min_bytes': 64}, memory=1500)
    try:
        assert kernel._calls.input_limit == 64 + 500
    finally:
        kernel.close()


def test_plans_sealed_before_the_credit_keep_their_original_admission():
    # A map or model plan sealed earlier reserved a full window per slot: no input gate.
    kernel, _ = _kernel(_same, {'concurrency': 3, 'codec_min_bytes': 1000}, memory=1500)
    try:
        assert kernel._calls.input_limit is None
    finally:
        kernel.close()
    # Async map alone already had a one-row credit inside its sealed minimum.
    kernel, services = _kernel(_same_async, {'concurrency': 3, 'codec_min_bytes': 1000}, memory=1500,
                               kind='AsyncMapOp')
    try:
        assert kernel._calls.input_limit == services.bounds.max_row_bytes + 500
    finally:
        kernel.close()


def test_a_credit_without_its_sealed_minimum_is_refused():
    with pytest.raises(DataflowError) as error:
        _kernel(_same, {'concurrency': 2, 'call_input_min_bytes': 64}, memory=1500)
    assert error.value.code == 'INVALID_CONTRACT'


@pytest.mark.parametrize('slack_rows,peak', [(0, 1), (2, 3)])
def test_calls_share_the_input_window_by_actual_row_bytes(slack_rows, peak):
    lock, seen = threading.Lock(), {'active': 0, 'peak': 0}
    together = threading.Barrier(3, timeout=5) if peak == 3 else None

    class Call:
        def __call__(self, row):
            with lock:
                seen['active'] += 1
                seen['peak'] = max(seen['peak'], seen['active'])
            try:
                if together is not None:
                    together.wait()        # fails unless all three rows are in flight at once
                else:
                    time.sleep(.02)
            finally:
                with lock:
                    seen['active'] -= 1
            return row

    rows = [rec({'v': 'x' * 1000, 'i': i}, sequence=i) for i in range(3)]
    size = rows[0].encoded_bytes
    assert all(row.encoded_bytes == size for row in rows)
    kernel, _ = _kernel(Call(), {'concurrency': 3, 'codec_min_bytes': 10 * size, 'call_input_min_bytes': size},
                        memory=(10 + slack_rows) * size)
    try:
        out = [record for row in rows for record in kernel.push('input', row)]
        out += list(kernel.finish('input'))
        assert sorted(row['i'] for row in rows_of(out)) == [0, 1, 2]
        assert seen['peak'] == peak
    finally:
        kernel.close()
