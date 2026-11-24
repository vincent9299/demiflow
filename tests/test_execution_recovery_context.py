"""Native callbacks retain one run and release it in its original context."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow.data.api import DataAPI
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.pipeline.run_context import RunContext, current_run, execution_scope, bind_iteration
from demiflow.execution.journal.run_journal import RunJournal


def on_thread(fn):
    def invoke():
        try:
            return fn()
        finally:
            assert current_run() is None  # No scope leaked into the native caller.
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(invoke).result(timeout=30)


class Reader:
    def __init__(self):
        self.closed_under = None

    def rows(self, fail=False):
        def consume():
            with execution_scope():
                try:
                    yield current_run().ref
                    if fail:
                        raise RuntimeError('source-stopped')
                    yield current_run().ref
                finally:
                    self.closed_under = current_run().ref
        return bind_iteration(consume())


def test_existing_run_is_retained_through_native_pull_and_close(tmp_path):
    reader = Reader()
    with RunContext.start(tmp_path / 'run') as run:
        stream = reader.rows()
        assert on_thread(lambda: next(stream)) == run.ref
        on_thread(stream.close)
        assert reader.closed_under == run.ref
        assert current_run() is run and run.active
    assert RunJournal(tmp_path / 'run').execution_status()['status'] == 'succeeded'


def test_unscoped_pull_owns_one_context_across_thread_switches():
    reader = Reader()
    stream = reader.rows()
    reference = on_thread(lambda: next(stream))
    assert on_thread(lambda: next(stream)) == reference
    with pytest.raises(StopIteration):
        on_thread(lambda: next(stream))
    assert reader.closed_under == reference and current_run() is None
    assert RunJournal(reference.directory).execution_status()['status'] == 'succeeded'


def test_failed_pull_releases_owner_without_masking_original_error():
    reader = Reader()
    stream = reader.rows(fail=True)
    reference = on_thread(lambda: next(stream))
    with pytest.raises(RuntimeError, match='source-stopped'):
        on_thread(lambda: next(stream))
    assert reader.closed_under == reference and current_run() is None
    assert RunJournal(reference.directory).execution_status()['status'] == 'failed'
    with RunContext.resume(reference):
        pass  # Ownership lease was released by the failed iterator.


def test_native_arrow_pack_uses_parent_run_for_every_callback(tmp_path):
    import json
    seen = tmp_path / 'source-callbacks.jsonl'
    def records():
        for key in range(6):
            reference = current_run().ref
            with seen.open('a') as evidence:
                evidence.write(json.dumps([reference.directory, reference.run_id]) + '\n')
            yield {'k': key, 'payload': b'x' * (2 * 1024**2)}
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    source = DataAPI(executor)
    try:
        with RunContext.start(tmp_path / 'run') as run:
            rows = (source.from_iter(records)
                    .join(source.from_items([{'k': i} for i in range(6)]), on='k')
                    .map(lambda row: {'k': row['k']}).take_all())
            assert sorted(rows, key=lambda r: r['k']) == [{'k': i} for i in range(6)]
            assert [json.loads(line) for line in seen.read_text().splitlines()] == [
                [run.ref.directory, run.ref.run_id]] * 6
            assert executor._run_ref == run.ref
            assert current_run() is run
    finally:
        executor.close()


def test_wide_native_row_rejects_without_commit_and_accepts_explicit_budget(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    payload = b'full-original\x00' * 750000
    output = tmp_path / 'wide.lance'
    schema = pa.schema([('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())])
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    source = DataAPI(executor)
    result = source.from_items([{'k': 1, 'payload': payload}, {'k': 2, 'payload': None}]).join(
        source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}]), on='k', how='left')
    try:
        with pytest.raises((DataflowError, RuntimeContractError)) as error:
            with RunContext.start(tmp_path / 'rejected-run', limits={
                    'max_row_bytes': 8 * 1024**2, 'max_batch_bytes': 8 * 1024**2}):
                result.write_lance(str(output), mode='create', schema=schema)
        if isinstance(error.value, DataflowError):
            assert error.value.code == 'BUDGET_EXCEEDED'
        else:
            assert 'byte budget' in str(error.value)
        assert not (output / '_versions').exists()
        failed = RunJournal(tmp_path / 'rejected-run')
        assert failed.execution_status()['status'] == 'failed'
        assert failed.execution_action('action-1')['result'] is None
        assert not failed.execution_commits('action-1')
        with RunContext.start(tmp_path / 'admitted-run', limits={
                'memory_bytes': 256 * 1024**2,
                'max_row_bytes': 16 * 1024**2, 'max_batch_bytes': 32 * 1024**2}):
            result.write_lance(str(output), mode='create', schema=schema)
        table = lance.dataset(str(output))
        rows = table.to_table().to_pylist()
        assert sorted((row['k'], row['tag'] or '') for row in rows) == [(1, 'a'), (1, 'b'), (2, '')]
        assert all(row['payload'] == payload for row in rows if row['k'] == 1)
        assert [row for row in rows if row['k'] == 2] == [{'k': 2, 'payload': None, 'tag': None}]
        assert table.schema == schema and table.version == 1
        from test_dataset_native import sealed_plan
        _, physical, _ = sealed_plan(result)
        assert physical.bounds.max_row_bytes == 16 * 1024**2
        assert physical.bounds.max_batch_bytes == 32 * 1024**2
    finally:
        executor.close()


def test_source_scope_shadows_outer_run_and_restores_action_on_error():
    from demiflow.execution.pipeline.run_context import run_scope, action_scope, source_run_scope, current_action, persisting_effect_scope, is_persisting_effect
    outer, source, action = object(), object(), object()
    with run_scope(outer), action_scope(action):
        with pytest.raises(ValueError, match='source failed'):
            with source_run_scope(source):
                assert current_run() is source and current_action() is None
                with persisting_effect_scope(source):
                    assert is_persisting_effect(source) and not is_persisting_effect(outer)
                assert not is_persisting_effect(source)
                raise ValueError('source failed')
        assert current_run() is outer and current_action() is action
    assert current_run() is None and current_action() is None
