import asyncio

import lance
import pyarrow as pa
import pytest

from demiflow import data
from _worker_event_fixtures import emit, events
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError, UnresolvedCommit
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_stream_checkpoint_contract import native_scopes, interrupt_after_success


SCHEMA = pa.schema([('id', pa.string()), ('value', pa.string())])


def test_append_checkpoint_preserves_head_and_verifies_repeated_keys(tmp_path):
    uri = tmp_path/'out.lance'
    def save(rows):
        return data.from_items(rows).save_lance(uri, schema=SCHEMA, key='id',
            stage='out', mode='append', max_batch=1, output_ref='ref').run_stream()
    first = save([{'id': 'a', 'value': 'one'}]).outputs['out']
    replay = save([{'id': 'a', 'value': 'one'}]).outputs['out']
    assert replay == first
    second = save([{'id': 'b', 'value': 'two'}]).outputs['out']
    assert lance.dataset(**first).count_rows() == 1
    assert lance.dataset(**second).count_rows() == 2
    with pytest.raises(ValueError, match='conflicting'):
        save([{'id': 'a', 'value': 'changed'}])
    assert lance.dataset(str(uri)).version == second['version']


def test_prepend_injects_pending_at_boundary_even_when_upstream_empty(tmp_path):
    seen = str(tmp_path / 'pending-seen.jsonl')
    upstream = str(tmp_path / 'upstream-seen.jsonl')
    (data.from_items([]).map_async(lambda r: emit(upstream, 'row', row=r) or r)
        .prepend(data.from_items([{'id': 'pending', 'value': 'saved'}]), max_rows=1)
        .map(lambda r: emit(seen, 'seen', value=r['id']) or r)
        .save_lance(tmp_path/'out.lance', schema=SCHEMA, key='id', stage='out').run_stream())
    assert events(upstream) == [] and [e['value'] for e in events(seen)] == ['pending']


def test_prepend_streams_without_waiting_for_upstream(tmp_path):
    released = tmp_path / 'prefix-consumed'
    seen = str(tmp_path / 'prepend-order.jsonl')
    async def wait(row):
        async def prefix_consumed():
            while not released.exists():
                await asyncio.sleep(.001)
        await asyncio.wait_for(prefix_consumed(), 3)
        return row
    def consume(row):
        emit(seen, 'seen', value=row['id'])
        released.touch()
        return row
    (data.from_items([{'id': 'new', 'value': 'x'}]).map_async(wait)
        .prepend(data.from_items([{'id': 'old', 'value': 'y'}]), max_rows=1)
        .map(consume).save_lance(tmp_path/'out.lance', schema=SCHEMA, key='id', stage='out')
        .run_stream())
    assert [e['value'] for e in events(seen)] == ['old', 'new']


@pytest.mark.parametrize('rows,bounds,error', [
    ([{'id': 'a'}], {'max_rows': 0}, 'max_rows'),
    ([{'id': 'a', 'payload': 'x'*1000}], {'max_rows': 1, 'max_row_bytes': 512}, 'chunk_bytes'),
])
def test_prepend_bounds(rows, bounds, error):
    reason = 'max_row_bytes' if error == 'chunk_bytes' else error
    with pytest.raises(DataflowError, match=reason) as failure:
        data.from_items([]).prepend(data.from_items(rows), **bounds).run_stream()
    assert failure.value.code == 'BUDGET_EXCEEDED'


def test_save_when_passes_unsaved_rows_and_append_bound(tmp_path):
    seen = str(tmp_path / 'save-when.jsonl')
    uri = tmp_path/'out.lance'
    (data.from_items([{'id': 'a', 'value': 'one'}, {'id': 'skip'}])
        .save_lance(uri, schema=SCHEMA, key='id', stage='out', mode='append',
            when=lambda r: r['id'] != 'skip', max_rows=1)
        .map(lambda r: emit(seen, 'seen', value=r['id']) or r).run_stream())
    assert [e['value'] for e in events(seen)] == ['a', 'skip']
    assert lance.dataset(str(uri)).to_table().to_pylist() == [{'id': 'a', 'value': 'one'}]
    with pytest.raises(ValueError, match='max_rows'):
        (data.from_items([{'id': 'b', 'value': 'two'}])
            .save_lance(uri, schema=SCHEMA, key='id', stage='out', mode='append', max_rows=1).run_stream())


def test_global_checkpoint_recovers_commit_before_manifest_publication(tmp_path, monkeypatch):
    from demiflow.execution import commit
    uri = tmp_path / 'out.lance'
    calls = []
    def execute():
        return data.from_items([{'id': 'a', 'value': 'one'}]).save_lance(
            uri, schema=SCHEMA, key='id', stage='out', mode='append').run_stream()
    def interrupt(name, commit_id):
        if name == 'after_sink_commit_before_receipt':
            fact = run.journal.execution_commit(commit_id)
            if fact['action_id'] in {key for key, _ in native_scopes(run.journal)}:
                calls.append(commit_id)
                raise OSError('crash after table commit before original receipt')
    with monkeypatch.context() as patch:
        patch.setattr(commit, 'fault_point', interrupt)
        with pytest.raises(OSError, match='crash after table commit'):
            with RunContext.start(tmp_path / 'run', entry_identity={'input': 1}) as run:
                reference = run.ref
                execute()
    assert len(calls) == 1
    assert run.journal.execution_commit(calls[0])['status'] == 'prepared'
    version = lance.dataset(str(uri)).version
    with pytest.raises(RecoveryContractError, match='identity'):
        with RunContext.resume(reference, entry_identity={'input': 2}):
            execute()
    with RunContext.resume(reference) as recovered:
        stats = execute()
    assert stats.outputs['out'] == {'uri': str(uri), 'version': version}
    assert lance.dataset(**stats.outputs['out']).to_table().to_pylist() == [{'id': 'a', 'value': 'one'}]
    assert recovered.journal.execution_commit(calls[0])['status'] == 'acked'
    assert not (tmp_path / 'checkpoint.json').exists()


def test_global_checkpoint_never_publishes_downstream_without_upstream(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_stream_lance import NativeStreamLanceService
    vectors = []
    original = NativeStreamLanceService.request
    def observe(self, command, record):
        reply = original(self, command, record)
        vector = self.outputs
        if 'down' in vector:
            assert 'up' in vector
            assert lance.dataset(**vector['down']).count_rows() <= lance.dataset(**vector['up']).count_rows()
            vectors.append(vector)
        return reply
    monkeypatch.setattr(NativeStreamLanceService, 'request', observe)
    with RunContext.start(tmp_path / 'run') as run:
        stats = (data.from_items([{'id': 'a', 'value': 'one'}])
            .save_lance(tmp_path/'up.lance', schema=SCHEMA, key='id', stage='up', mode='append')
            .save_lance(tmp_path/'down.lance', schema=SCHEMA, key='id', stage='down', mode='append')
            .run_stream())
    assert vectors and stats.outputs == vectors[-1]
    assert lance.dataset(**stats.outputs['up']).to_table().to_pylist() == [{'id': 'a', 'value': 'one'}]
    assert lance.dataset(**stats.outputs['down']).to_table().to_pylist() == [{'id': 'a', 'value': 'one'}]
    facts = [f for key, _ in native_scopes(run.journal) for f in run.journal.execution_commits(key)]
    assert facts and all(f['status'] == 'acked' for f in facts)


def test_checkpoint_rejects_missing_stage_and_foreign_table_head(tmp_path):
    uri = tmp_path / 'out.lance'
    def execute():
        return data.from_items([{'id': 'a', 'value': 'one'}]).save_lance(
            uri, schema=SCHEMA, key='id', stage='out', mode='append').run_stream()
    reference = interrupt_after_success(tmp_path / 'run', execute, entry_identity='graph')
    original_version = lance.dataset(str(uri)).version
    with pytest.raises(RecoveryContractError, match='identity|changed|declaration'):
        with RunContext.resume(reference):
            data.from_items([]).map_async(lambda r: r).run_stream()
    foreign = [{'id': 'foreign', 'value': 'unrelated'}]
    data.from_items(foreign).write_lance(str(uri), schema=SCHEMA, mode='overwrite')
    foreign_version = lance.dataset(str(uri)).version
    with pytest.raises(UnresolvedCommit, match='head differs|outside|original native receipts'):
        with RunContext.resume(reference):
            execute()
    assert lance.dataset(str(uri)).version == foreign_version
    assert lance.dataset(str(uri)).to_table().to_pylist() == foreign
    assert lance.dataset(str(uri), version=original_version).to_table().to_pylist() == [{'id': 'a', 'value': 'one'}]
