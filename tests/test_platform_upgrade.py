import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from demiflow.operator_llm.journal import request_key, UncertainPromptCall
from demiflow.operator_llm.errors import PromptBudgetExceededError


def test_uncertain_recovery_excludes_live_writer_and_keeps_budget(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 1}
    owner = SQLitePromptJournal(path, max_requests=1)
    other = SQLitePromptJournal(path, max_requests=1)
    owner.reserve(request)
    with pytest.raises(UncertainPromptCall, match='active writer'):
        other.requeue_uncertain([request_key(request)], actor='tester', reason='restart')
    owner.close()
    receipt = other.requeue_uncertain([request_key(request)], actor='tester', reason='restart', operation_id='recovery-1')
    assert other.lookup(request) is None
    with pytest.raises(PromptBudgetExceededError):
        other.reserve(request)
    assert other.recovery_history()[0]['request_json']
    assert other.requeue_uncertain([request_key(request)], actor='tester', reason='restart', operation_id='recovery-1')['reused']
    other.limit = 2
    assert other.reserve(request)
    other.response(request, {'body': 'complete'})
    with pytest.raises(ValueError, match='unanswered'):
        other.requeue_uncertain([request_key(request)], actor='tester', reason='invalid')
    assert other.lookup(request)['body'] == 'complete'
    other.close()


def test_recovery_is_atomic_for_mixed_keys(tmp_path):
    j = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    for n in range(2):
        j.reserve({'n': n})
        if n:
            j.response({'n': n}, {'ok': True})
    j.close()
    with pytest.raises(ValueError):
        j.requeue_uncertain([request_key({'n': n}) for n in range(2)], actor='tester', reason='mixed')
    assert j.stats()['requests'] == 2
    assert not j.recovery_history()
    j.close()


def test_offline_sqlite_images_are_external_and_roundtrip(tmp_path):
    from demiflow.operator_llm.sqlite_offline import materialize, submit_response, SQLiteOfflinePromptClient
    from demiflow.operator_llm.model import OperatorLLMRequest
    from demiflow.operator_llm.offline import PromptResponsePending
    request = OperatorLLMRequest('test', 'v1', 'test-model', 'hello', response_schema={'type':'object'})
    options = {'path': str(tmp_path / 'calls.sqlite')}
    record, ref = materialize(request, options)
    assert ref.read() == record
    client = SQLiteOfflinePromptClient(None, {'offline_store': options})
    with pytest.raises(PromptResponsePending):
        client.lookup(request)
    with pytest.raises(ValueError, match='model'):
        submit_response(None, ref, {'result': 3}, model='wrong')
    response = submit_response(None, ref, {'result': 3}, model='test-model')
    assert client.lookup(request).content == {'result': 3}
    assert response.read()['content'] == {'result': 3}
    assert not list(tmp_path.glob('*.lance'))
    client.journal.close()


def test_externalized_input_and_content_hash(tmp_path):
    from demiflow.operator_llm.sqlite_offline import externalize, restore
    import json
    value = {'image': 'data:image/png;base64,YWJj'}
    stored = externalize(value, tmp_path)
    assert 'base64,YWJj' not in json.dumps(stored)
    assert restore(stored) == value
    next(tmp_path.iterdir()).write_bytes(b'wrong')
    with pytest.raises(ValueError, match='digest'):
        restore(stored)


def test_empty_run_reset_and_activity_guard(tmp_path):
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(tmp_path / 'run')
    journal.initialize({'code': 'draft'})
    journal.reset_empty(actor='tester', reason='finalize source')
    journal.initialize({'code': 'final'})
    assert len(journal.inspect()['resets']) == 1
    journal.activity('model', 'request-1')
    with pytest.raises(ValueError, match='not empty'):
        journal.reset_empty(actor='tester', reason='cannot')


def test_empty_reset_excludes_writer(tmp_path):
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.artifacts import run_lock
    journal = RunJournal(tmp_path)
    journal.initialize({'code': 'x'})
    with run_lock(tmp_path), pytest.raises(RuntimeError, match='Another writer'):
        journal.reset_empty(actor='tester', reason='cannot')


def test_materialized_release_invalidates_aliases_and_keeps_other_cache(tmp_path):
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    executor = LocalDatasetExecutor(materialize_memory_limit=1)
    api = DataAPI(executor=executor)
    first = api.from_items([{'value': 'a' * 1000}]).materialize()
    alias = first.map(lambda row: row)
    second = api.from_items([{'value': 2}]).materialize()
    first.release()
    first.release()
    with pytest.raises(RuntimeError, match='released'):
        alias.take_all()
    with pytest.raises(RuntimeError, match='released'):
        first.count()
    assert second.take_all() == [{'value': 2}]
    executor.close()


def test_sort_pool_failure_does_not_repeat_input(tmp_path, monkeypatch):
    import demiflow.data.local_relational as rel
    from concurrent.futures import Future
    from concurrent.futures.process import BrokenProcessPool
    class Broken:
        def __init__(self, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *args): self.shutdown()
        def shutdown(self, **kw): pass
        def submit(self, fn, *args):
            future = Future()
            future.set_exception(BrokenProcessPool())
            return future
    monkeypatch.setattr(rel, 'ProcessPoolExecutor', Broken)
    seen = []
    def rows():
        for i in range(50):
            seen.append(i)
            yield {'k': str(50 - i), 'text': 'x' * 250000}
    actual = list(rel.sorted_rows(rows(), ['k'], tmp_path, 8*1024*1024, workers=2))
    assert len(seen) == len(actual) == 50
    assert [k for k, _ in actual] == sorted(k for k, _ in actual)


def test_sort_workers_override(monkeypatch):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    monkeypatch.setenv('DEMIFLOW_LOCAL_SORT_WORKERS', '1')
    assert LocalDatasetExecutor(workers=8)._sort_workers == 1
    assert LocalDatasetExecutor(workers=8, sort_workers=2)._sort_workers == 2


def test_isolated_stage_returns_or_propagates():
    from demiflow.execution.isolation import run_isolated
    assert run_isolated(os.getpid) != os.getpid()
    with pytest.raises(ZeroDivisionError):
        run_isolated(lambda: 1/0)


def test_stale_call_owner_cannot_complete_recovered_attempt(tmp_path):
    request = {'task': 'late'}
    first = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    first.reserve(request)
    first.close()
    second = SQLitePromptJournal(first.path)
    second.requeue_uncertain([request_key(request)], actor='tester', reason='old process stopped')
    second.reserve(request)
    with pytest.raises(UncertainPromptCall, match='no longer owns'):
        first.response(request, {'body':'late response'})
    second.response(request, {'body':'current response'})
    assert second.lookup(request) == {'body':'current response'}
    first.close()
    second.close()


def test_append_commit_receipt_gap_and_uncertainty(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution.dataset_commit import DatasetCommit, UncertainDatasetCommit
    uri = str(tmp_path / 'data.lance')
    args = (tmp_path / 'control', 'batch-1', uri)
    source = data.from_items([{'v':1}])
    schema = pa.schema([('v',pa.int64())])
    with pytest.raises(OSError):
        with DatasetCommit(*args) as commit:
            receipt = source.write_lance(uri, schema=schema, return_receipt=True)
            commit.confirm(receipt)
            raise OSError('business receipt lost')
    with DatasetCommit(*args) as commit:
        assert commit.output == {'uri':uri,'version':1}
    assert lance.dataset(uri).count_rows() == 1
    # Data committed but writer result was lost: no automatic append retry.
    args = (tmp_path / 'control', 'batch-2', uri)
    with pytest.raises(OSError):
        with DatasetCommit(*args):
            source.write_lance(uri, schema=schema)
            raise OSError('writer result lost')
    with pytest.raises(UncertainDatasetCommit):
        with DatasetCommit(*args):
            pytest.fail('uncertain append was retried')
    with pytest.raises(UncertainDatasetCommit, match='Target changed'):
        DatasetCommit(*args).abort_uncommitted(actor='tester', reason='cannot reset committed bytes')
    assert lance.dataset(uri).count_rows() == 2


def test_explicit_append_recovery_checks_unchanged_target(tmp_path):
    from demiflow.execution.dataset_commit import DatasetCommit
    args = (tmp_path / 'control', 'batch', str(tmp_path / 'absent.lance'))
    with pytest.raises(OSError):
        with DatasetCommit(*args):
            raise OSError('before writer')
    receipt = DatasetCommit(*args).abort_uncommitted(actor='tester', reason='confirmed writer exit before commit')
    assert receipt['intent']['base_version'] is None
    with DatasetCommit(*args) as commit:
        assert commit.output is None


def test_file_uri_and_path_share_append_intent_and_detect_changed_target(tmp_path):
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution.dataset_commit import DatasetCommit, UncertainDatasetCommit
    path = tmp_path / 'data.lance'
    source = data.from_items([{'v': 1}])
    schema = pa.schema([('v', pa.int64())])
    source.write_lance(str(path), schema=schema)
    control = tmp_path / 'control'
    with pytest.raises(OSError):
        with DatasetCommit(control, 'batch', path.as_uri()):
            source.write_lance(str(path), schema=schema, mode='append')
            raise OSError('writer result lost')
    for uri in (str(path), path.as_uri()):
        with pytest.raises(UncertainDatasetCommit):
            with DatasetCommit(control, 'batch', uri):
                pytest.fail('URI alias bypassed the uncertain append')
        with pytest.raises(UncertainDatasetCommit, match='Target changed'):
            DatasetCommit(control, 'batch', uri).abort_uncommitted(actor='test', reason='must refuse')


def test_readonly_snapshot_keeps_imported_unanswered_keys(tmp_path):
    import sqlite3
    from demiflow.operator_llm.call_ref import call_snapshot
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    journal._connect()
    with sqlite3.connect(journal.path) as db:
        db.execute('INSERT INTO calls(request_key) VALUES (?)', ('a'*64,))
    journal.close()
    before = journal.path.read_bytes()
    assert call_snapshot(path=journal.path)['requests'] == {'a'*64:{}}
    assert journal.path.read_bytes() == before
