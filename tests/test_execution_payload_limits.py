"""Real payload budgets across native joins, model effects, sinks and recovery."""
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution import RunContext, RecoveryContractError, current_run
from demiflow.planning.dataflow_policy import execution_limits
from demiflow.execution.contracts.dataflow import BATCH_TARGET_BYTES
from demiflow.planning.dataflow_policy import run_row_limit
from demiflow.lance.arrow_batches import arrow_batches
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.outputs.lance_sink import LanceSink
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.contracts.recovery_contract import SourcePosition
from demiflow.lance.model import LanceWriteSpec
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server
from test_forge_runs import base, root


@pytest.mark.parametrize('memory', [64 * 2**20, 256 * 2**20, 1024 * 2**20])
def test_default_payload_ceiling_is_derived_and_explicit_caps_only_tighten(memory):
    limits = execution_limits({'memory_bytes': memory})
    assert limits['max_row_bytes'] == run_row_limit(limits)
    assert limits['max_row_bytes'] <= memory
    assert limits['max_row_bytes'] >= min(BATCH_TARGET_BYTES, memory)
    assert limits['max_batch_bytes'] <= memory
    small = execution_limits({'memory_bytes': memory, 'max_row_bytes': 4096, 'max_batch_bytes': 8192})
    assert small['max_row_bytes'] == 4096 and small['max_batch_bytes'] == 8192
    assert BATCH_TARGET_BYTES == 8 * 2**20


@pytest.mark.parametrize('explicit', [False, True])
def test_multiple_arrow_rows_split_and_indivisible_row_obeys_separate_cap(explicit):
    payload = b'x' * (512 * 1024 if explicit else 3 * 2**20)
    table = pa.table({'payload': pa.array([payload] * 5, type=pa.large_binary())})
    limits = execution_limits({'max_batch_bytes': 1024**2} if explicit else None)
    batches = list(arrow_batches([table], limits))
    assert len(batches) > 1
    assert all(b.nbytes <= min(BATCH_TARGET_BYTES, limits['max_batch_bytes']) for b in batches)
    assert pa.Table.from_batches(batches).equals(table)
    wide = pa.table({'payload': pa.array([b'z' * (9 * 2**20)], type=pa.large_binary())})
    assert list(arrow_batches([wide], execution_limits()))[0].equals(wide.to_batches()[0])
    with pytest.raises(MemoryError, match='cannot split'):
        list(arrow_batches([wide], execution_limits({'max_batch_bytes': 8 * 2**20})))


@pytest.mark.parametrize('reject_memory', [False, True])
def test_borrowed_arrow_offset_counts_parent_against_memory_not_logical_batch(tmp_path, reject_memory):
    parent = pa.table({'payload': pa.array([b'p' * (9 * 2**20), b'kept\x00value'], type=pa.large_binary())})
    borrowed = parent.slice(1, 1)
    assert borrowed.nbytes < 1024 and borrowed.get_total_buffer_size() > BATCH_TARGET_BYTES
    limits = dict(memory_bytes=(2 if reject_memory else 32) * 2**20, max_row_bytes=1024, max_batch_bytes=2048)
    target = tmp_path / 'offset.lance'
    def execute():
        with RunContext.start(tmp_path / 'run', limits=limits) as run:
            with run.action({'kind': 'budget-fixture'}) as action:
                sink = LanceSink(action, LanceWriteSpec(uri=str(target), mode='create', schema=borrowed.schema))
                CommitCoordinator(action).commit(1, outputs={'lance': (sink, [borrowed])},
                    position=SourcePosition('fixed', 1))
                action.complete({'rows': 1})
    if reject_memory:
        with pytest.raises(RecoveryContractError, match='retained buffers.*memory budget'):
            execute()
        assert not target.exists()
    else:
        execute()
        assert lance.dataset(str(target)).to_table().equals(borrowed)
        assert lance.dataset(str(target)).version == 1


def test_small_row_budget_rejects_before_http_and_cannot_be_raised_on_resume(tmp_path, server):
    def graph():
        return (data.from_items([{'payload': 'x' * 4096}])
                .map_prompt_async('enrich', config=parse_prompt_pack(PACK), inputs=['payload'],
                                  output='answer', concurrency=1, queue_depth=1, max_requests=1).materialize())
    small = dict(max_row_bytes=1024, max_batch_bytes=2048)
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='byte budget') as rejected:
        with RunContext.start(tmp_path / 'small', limits=small) as run:
            reference = run.ref
            graph()
    assert rejected.value.code == 'BUDGET_EXCEEDED'
    assert not rejected.value.retryable
    assert not server['requests']
    assert RunJournal(reference.directory).execution_status()['status'] == 'failed'
    with pytest.raises(RecoveryContractError, match='limits changed'):
        with RunContext.resume(reference, limits={'max_row_bytes': 8192, 'max_batch_bytes': 16384}):
            graph()
    assert not server['requests']
    with RunContext.start(tmp_path / 'admitted', limits={'max_row_bytes': 8192, 'max_batch_bytes': 16384}):
        assert graph().take_all()[0]['answer'] == 'ok'
    assert len(server['requests']) == 1


def wide_graph(directory):
    payload = b'wide\x00original' * 750000
    result = (data.from_items([{'id': 1, 'payload': payload}])
              .map_prompt_async('enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'id'},
                                output='answer', concurrency=1, queue_depth=1, max_requests=1).materialize())
    return result.write_lance(str(directory / 'wide.lance'), mode='create', return_receipt=True,
        schema=pa.schema([('id', pa.int64()), ('payload', pa.large_binary()), ('answer', pa.string())]))


@pytest.mark.parametrize('window', ['prepared_failure', 'cancel_after_prepared', 'native_return_loss'])
def test_wide_model_output_recovers_with_identical_budget_without_repeating_effects(tmp_path, server, monkeypatch, window):
    import demiflow.execution.checkpoint.commit as protocol
    original = LanceSink.commit
    def fault(name, commit_id):
        fact = current_run().journal.execution_commit(commit_id)
        if name == 'after_prepared' and fact['intent']['sinks'] == ['write_lance']:
            if window == 'cancel_after_prepared':
                RunContext.cancel(current_run().ref)
            elif window == 'prepared_failure':
                raise OSError('prepared-wide-result')
    def lost(self, prepared, context):
        original(self, prepared, context)
        raise OSError('native-wide-return-lost')
    monkeypatch.setattr(protocol, 'fault_point', fault)
    if window == 'native_return_loss':monkeypatch.setattr(LanceSink, 'commit', lost)
    expected = RecoveryContractError if window == 'cancel_after_prepared' else OSError
    message = {'cancel_after_prepared': 'cancellation', 'prepared_failure': 'prepared-wide-result',
               'native_return_loss': 'native-wide-return-lost'}[window]
    with pytest.raises(expected, match=message):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            original_limits = dict(run.limits)
            wide_graph(tmp_path)
    assert len(server['requests']) == 1
    journal = RunJournal(reference.directory)
    assert journal.execution_status()['status'] == ('cancelled' if window == 'cancel_after_prepared' else 'failed')
    assert journal.execution_commits('action-1')[-1]['status'] == 'acked'  # original materialize
    assert journal.execution_commits('action-2')[-1]['status'] == 'prepared'  # receipt has not returned
    if window == 'native_return_loss':assert lance.dataset(str(tmp_path / 'wide.lance')).version == 1
    else:assert not (tmp_path / 'wide.lance').exists()
    monkeypatch.setattr(protocol, 'fault_point', lambda *_: None)
    monkeypatch.setattr(LanceSink, 'commit', original)
    with RunContext.resume(reference) as resumed:
        assert resumed.limits == original_limits
        receipt = wide_graph(tmp_path)
    assert len(server['requests']) == 1
    table = lance.dataset(str(tmp_path / 'wide.lance'))
    assert table.version == receipt.committed_version == 1
    assert table.to_table().to_pylist() == [{'id': 1, 'payload': b'wide\x00original' * 750000, 'answer': 'ok'}]
    assert RunJournal(reference.directory).execution_status()['status'] == 'succeeded'


@pytest.mark.parametrize('field', ['max_output_bytes', 'max_scratch_bytes'])
def test_explicit_output_and_scratch_caps_prevent_publication(tmp_path, monkeypatch, field):
    target = tmp_path / 'existing.lance'
    lance.write_dataset(pa.table({'text': ['original']}), str(target))
    def no_publish(*args):pytest.fail('budget failure reached native commit')
    monkeypatch.setattr(LanceSink, 'commit', no_publish)
    with pytest.raises(RecoveryContractError, match='budget'):
        with RunContext.start(tmp_path / 'run', limits={field: 128}):
            data.from_items([{'text': 'x' * 2048}]).write_lance(str(target), mode='append', expected_version=1)
    assert lance.dataset(str(target)).version == 1
    assert lance.dataset(str(target)).to_table().to_pylist() == [{'text': 'original'}]


def test_native_small_explicit_budgets_preserve_duplicates_nulls_and_worker_limits(tmp_path, monkeypatch):
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    import json
    batches = []
    prepare = LanceSink.prepare
    def observe(sink, blocks, context):
        def measured():
            for block in blocks:
                batches.append(block.num_rows)
                yield block
        return prepare(sink, measured(), context)
    monkeypatch.setattr(LanceSink, 'prepare', observe)
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    source = DataAPI(executor)
    try:
        with RunContext.start(tmp_path / 'run', limits=dict(memory_bytes=64 * 2**20,
                max_row_bytes=8192, max_batch_bytes=16384, batch_rows=2)) as run:
            left = source.from_items([{'k': 1, 'payload': b'full\x00' * 400}] * 2 + [{'k': 2, 'payload': None}])
            right = source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
            result = left.join(right, on='k', how='left')
            target = tmp_path / 'small.lance'
            result.write_lance(str(target), mode='create', schema=pa.schema([
                ('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())]))
            rows = lance.dataset(str(target)).to_table().to_pylist()
            assert sorted((r['k'], r['tag'] or '') for r in rows) == [(1, 'a'), (1, 'a'), (1, 'b'), (1, 'b'), (2, '')]
            assert all(r['payload'] == b'full\x00' * 400 for r in rows if r['k'] == 1)
            from demiflow.execution.seal.sealed_plans import load_sealed_plan
            sealed = load_sealed_plan(run.ref.directory, 'action-1')
            physical = sealed['physical']
            assert physical.bounds.max_row_bytes == 8192
            assert physical.bounds.max_batch_bytes == 16384
            assert physical.bounds.max_resident_bytes <= 64 * 2**20
            stages = {stage.stage_id: stage for stage in physical.stages}
            assert any(node.kernel.kind == 'join' for node in sealed['graph'].nodes)
            for group in physical.residency.simultaneous_groups:
                assert sum(stages[key].resident_bytes * len(stages[key].partitions) for key in group) + physical.residency.control_reserved_bytes <= 64 * 2**20
            assert run.limits['batch_rows'] == 2
            assert batches and sum(batches) == 5 and max(batches) <= 2
    finally:executor.close()


@pytest.mark.parametrize('admitted', [True, False])
def test_real_forge_wide_join_uses_same_payload_contract(root, admitted):
    from test_forge_runs import custom, execute
    operator = '''from typing import TypedDict
class Input(TypedDict):
    case_id: str
class Output(TypedDict):
    k: int
    payload: bytes
def widen(row: Input) -> Output:
    return {'k': 1, 'payload': b'full-original\\x00' * 750000}
'''
    body = '''from operators.custom import widen
def run(*, source, target):
    left = data.read_lance(source['uri'], version=source['version']).limit(1).map(widen)
    right = data.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
    return left.join(right, on='k', how='left').write_lance(target['uri'], mode='create', return_receipt=True)
'''
    limits = None if admitted else {'max_row_bytes': 1024**2, 'max_batch_bytes': 2 * 1024**2}
    value = custom(root, body, operators=operator, limits=limits)
    result = execute(root, value)
    if admitted:
        assert result['status'] == 'succeeded', result
        output = next(o for o in result['outputs'] if o.get('receipt'))
        table = lance.dataset(output['uri'])
        rows = table.to_table().to_pylist()
        assert sorted(r['tag'] for r in rows) == ['a', 'b']
        assert all(r['payload'] == b'full-original\x00' * 750000 for r in rows)
        assert table.version == 1
    else:
        assert result['status'] == 'failed', result
        assert not any(o.get('receipt') for o in result['outputs'])
        assert not (root / 'data/custom.lance').exists()


async def record_scope(row, audit):
    import json, os
    run = current_run()
    raw = (json.dumps(dict(run.limits)) + '\n').encode()
    fd = os.open(audit, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        assert os.write(fd, raw) == len(raw)
    finally:
        os.close(fd)
    return row


def test_stream_effective_allocation_is_finite_scoped_and_identical_on_resume(tmp_path):
    import json
    audit = str(tmp_path / 'worker-limits.jsonl')
    async def scoped(row):
        return await record_scope(row, audit)
    def observed():
        return [json.loads(line) for line in Path(audit).read_text().splitlines()]
    def graph():
        return data.range(3).map_async(scoped, concurrency=8, queue_depth=16).materialize()
    with pytest.raises(RuntimeError, match='after-materialized'):
        with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 1024**3}) as run:
            reference = run.ref
            ceiling = dict(run.limits)
            result = graph().take_all()
            assert sorted(result, key=lambda row: row['id']) == [{'id': 0}, {'id': 1}, {'id': 2}]
            assert run.limits == ceiling
            raise RuntimeError('after-materialized')
    scopes = observed()
    assert len(scopes) == 3 and all(limits == scopes[0] for limits in scopes)
    admitted = scopes[0]
    assert 0 < admitted['max_row_bytes'] <= ceiling['max_row_bytes']
    assert admitted['max_batch_bytes'] <= ceiling['max_batch_bytes']
    # The v3 physical grant, not retired stream_residency's independent
    # queue estimate, is the finite worker allowance now exposed to callbacks.
    assert 0 < admitted['memory_bytes'] <= ceiling['memory_bytes']
    assert admitted['max_row_bytes'] + admitted['max_batch_bytes'] <= admitted['memory_bytes']
    assert all(0 < value <= ceiling[name] for name, value in admitted.items())
    with RunContext.resume(reference):
        assert graph().take_all() == result
    assert observed() == scopes  # Prepared output is authoritative; no callback replay.


def test_resume_preserves_explicit_limit_semantics_even_when_numbers_match(tmp_path):
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            raise RuntimeError('stop')
    with pytest.raises(RecoveryContractError, match='limits semantics changed'):
        with RunContext.resume(reference, limits={'batch_rows': execution_limits()['batch_rows']}):
            pytest.fail('implicit native batching became an explicit ceiling during resume')
    with RunContext.resume(reference) as run:
        assert not run.explicit_limits


@pytest.mark.parametrize('payload_bytes', [512, 2048])
def test_native_worker_honors_positive_row_cap_below_batch_target(tmp_path, payload_bytes):
    from demiflow.execution.hosts.datafusion import DataFusionSession, DataFusionQueryError
    from test_datafusion_execution import options
    schema = pa.schema([('payload', pa.large_binary())])
    def transform(batch):
        return pa.RecordBatch.from_arrays([pa.array([b'x' * payload_bytes], type=pa.large_binary())], schema=schema)
    with DataFusionSession(resource_directory=tmp_path / 'resources', options=options(max_row_bytes=1024)) as session:
        if payload_bytes > 1024:
            with pytest.raises(DataFusionQueryError) as caught:
                session.query('SELECT 1 AS input', sources={}, schema=schema, batch_transform=transform)
            assert '1024' in caught.value.report['worker']['message']
            assert not caught.value.report['success']
        else:
            result = session.query('SELECT 1 AS input', sources={}, schema=schema, batch_transform=transform)
            assert result.report['worker']['writer_batch_bytes'] == 1024
            assert result.dataset().take_all() == [{'payload': b'x' * payload_bytes}]


def test_explicit_native_row_allowance_is_not_replaced_by_a_default_estimate(tmp_path):
    payload = b'full-original\x00' * 750000
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 16 * 2**20, 'max_batch_bytes': 32 * 2**20}):
        with data.local_execution(workers=1, worker_mode='thread', native_resources={
                'memory_bytes': 128 * 2**20, 'max_rss_bytes': 2 * 2**30,
                'max_row_bytes': 16 * 2**20}) as session:
            session._executor.resource_root = tmp_path
            left = data.from_items([{'k': 1, 'payload': payload}, {'k': 2, 'payload': None}])
            right = data.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
            target = tmp_path / 'explicit.lance'
            left.join(right, on='k', how='left').write_lance(str(target), mode='create', schema=pa.schema([
                ('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())]))
    rows = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted((r['k'], r['tag'] or '') for r in rows) == [(1, 'a'), (1, 'b'), (2, '')]
    assert all(r['payload'] == payload for r in rows if r['k'] == 1)


def test_explicit_row_allowance_can_exceed_default_estimate_without_raising_other_budgets():
    defaults = execution_limits({'memory_bytes': 128 * 2**20})
    explicit = execution_limits({'memory_bytes': 128 * 2**20, 'max_row_bytes': 16 * 2**20})
    assert defaults['max_row_bytes'] == 8 * 2**20
    assert explicit['max_row_bytes'] == explicit['max_batch_bytes'] == 16 * 2**20
    assert explicit['memory_bytes'] == defaults['memory_bytes']
    assert explicit['max_rss_bytes'] == defaults['max_rss_bytes']
    from demiflow.workbench.pipeline_policy import limits_with
    managed = limits_with(policy={'memory_bytes': 128 * 2**20, 'max_row_bytes': 16 * 2**20})
    assert managed['max_row_bytes'] == 16 * 2**20


def test_native_default_row_floor_remains_compatible_when_pool_supports_it(tmp_path):
    from demiflow.planning.dataflow_policy import native_row_limit
    assert native_row_limit(64 * 2**20, 2 * 2**30) == 8 * 2**20
    assert native_row_limit(2 * 2**20, 2 * 2**30) <= 2 * 2**20
    payload = b'full-original\x00' * 450000
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 16 * 2**20, 'max_batch_bytes': 32 * 2**20}):
        with data.local_execution(workers=1, worker_mode='thread', native_resources={
                'memory_bytes': 64 * 2**20, 'max_rss_bytes': 2 * 2**30}) as session:
            session._executor.resource_root = tmp_path
            rows = data.from_items([{'k': 1, 'payload': payload}]).join(data.from_items([{'k': 1}]), on='k')
            target = tmp_path / 'native-default.lance'
            rows.write_lance(str(target), mode='create')
            assert lance.dataset(str(target)).to_table().to_pylist() == [{'k': 1, 'payload': payload}]


def test_peak_scope_keeps_durable_cumulative_ceiling_across_resume(tmp_path):
    from demiflow.execution.pipeline.checked import CheckedExecution
    from demiflow.execution.pipeline.run_context import payload_scope
    with pytest.raises(RuntimeError, match='interrupt'):
        with RunContext.start(tmp_path / 'run', limits={'max_output_bytes': 10}) as run:
            reference = run.ref
            assert run.reserve('output_bytes', 6, reservation_id='first', ceiling=run.limits['max_output_bytes']) == 6
            raise RuntimeError('interrupt')
    with RunContext.resume(reference) as run:
        remaining = {**run.limits, 'max_output_bytes': 4, 'max_row_bytes': 1024, 'max_batch_bytes': 2048}
        checked = CheckedExecution(nodes=[], edges=[], actions=[], operators=[], source_root=tmp_path,
            limits=remaining, record=lambda *args: None, check_stop=lambda: None)
        with payload_scope(run, None, owner_limits=remaining):
            assert run.reserve('output_bytes', 4, reservation_id='second', ceiling=run.limits['max_output_bytes']) == 10
            assert run.limits['max_output_bytes'] == 10
            assert checked.limits['max_output_bytes'] == 4
            assert run.limits['max_row_bytes'] == checked.limits['max_row_bytes'] == 1024
            with pytest.raises(RecoveryContractError, match='exhausted'):
                run.reserve('output_bytes', 1, reservation_id='excess', ceiling=run.limits['max_output_bytes'])


def test_real_forge_resume_delivers_new_output_under_original_cumulative_ceiling(root, monkeypatch):
    import time
    from demiflow.workbench.runs import manager as runs
    from test_forge_runs import custom, wait
    from test_forge_run_faults import _first_committed
    from test_forge_resume import stopped
    value = custom(root, '''from operators.custom import finite_slow
def run(*, source, target, other):
    rows = data.read_lance(source['uri'], version=source['version'])
    first = rows.write_lance(target['uri'], mode='create', return_receipt=True)
    second = rows.map(finite_slow, inputs={'text': 'question'}, output='waiting').write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''', operators='''def finite_slow(text: str | None) -> str | None:
    n = 0
    for i in range(50000000):
        n = n + i
    return text
''', arguments={'source': {'kind': 'table', 'alias': 'candidates'},
                  'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'first.lance'},
                  'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'second.lance'}},
        limits={'timeout_s': 180, 'cancel_grace_s': 1, 'max_output_bytes': 128 * 1024})
    from test_managed_image_joint import start_fault
    start_fault(root, value, "final_after_acked", monkeypatch)
    try:
        runs.cancel(root, value['run_id'])
        stopped(root, value['run_id'])
        runs.reconcile(root, value['run_id'])
        previous = runs.show(root, value['run_id'])
        assert previous['status'] == 'cancelled', previous
        committed = next(o for o in previous['outputs'] if o.get('result'))
        version = committed['receipt']['committed_version']
        assert 0 < previous['state']['usage']['output_bytes'] < value['limits']['max_output_bytes']
        runs.resume(root, value['run_id'], expected=value['digest'], expected_attempt=1)
        result = wait(root, value['run_id'], timeout=90)
        assert result['status'] == 'succeeded', (result, runs.logs(root, value['run_id']))
        assert sum(o.get('result') is not None for o in result['outputs']) == 2
        assert lance.dataset(committed['uri']).version == version
        assert runs.show(root, value['run_id'], attempt=1) == previous
        import sqlite3
        with sqlite3.connect(runs.directory(root, value['run_id']) / 'run.sqlite') as db:
            consumed, ceiling = db.execute("SELECT consumed,ceiling FROM runtime_budgets WHERE name='output_bytes'").fetchone()
        assert 0 < consumed <= ceiling == value['limits']['max_output_bytes']
    finally:
        current = runs.show(root, value['run_id'])
        if current['status'] not in runs.TERMINAL:
            runs.cancel(root, value['run_id'])
            stopped(root, value['run_id'])
