"""Exact-key recovery retains failed HTTP responses, old refs, and paid budgets."""
import json
import sqlite3

import pytest

from demiflow.operator_llm.call_ref import read_call, journal_totals, journal_observation
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def test_timeout_then_quota_then_success_preserves_every_attempt(tmp_path):
    path = tmp_path/'calls.sqlite'
    journal = SQLitePromptJournal(path, max_requests=3)
    request = {'concept': 'unchanged payload'}; key = request_key(request)
    journal.reserve(request)
    original = journal.references(request)
    journal.failed(request, TimeoutError('slow stream'), 600)
    old_error = {**original['request_ref'], 'kind': 'error'}
    journal.requeue_uncertain([key], actor='test', reason='longer timeout')
    journal.reserve(request)
    rejected = {'status_code': 429, 'body': {'error': {'message': 'MONTHLY_LIMIT_EXCEEDED'}}, 'elapsed_s': .5}
    refs = journal.response(request, rejected)
    before = journal_totals(path)
    options = dict(expected_statuses=[429], actor='test', reason='quota restored', operation_id='quota-once')
    assert not journal.requeue_http_errors([key], **options)['reused']
    assert journal.requeue_http_errors([key], **options)['reused']
    assert journal_totals(path) == before
    assert journal_observation(path)['pending_reservations'] == 0
    assert read_call(refs['response_ref']) == rejected
    assert read_call(old_error)['detail'] == 'slow stream'
    journal.reserve(request)
    successful = {'status_code': 200, 'body': {'usage': {'prompt_tokens': 11, 'completion_tokens': 4}}, 'elapsed_s': 3}
    final = journal.response(request, successful)
    assert final['response_ref']['attempt'] == 3
    assert read_call(final['response_ref']) == successful
    assert read_call(refs['response_ref']) == rejected
    assert read_call(original['request_ref']) == request
    with pytest.raises(KeyError):
        read_call(original['response_ref'])
    assert journal.requeue_http_errors([key], **options)['reused']
    assert journal.lookup(request) == successful  # Idempotence cannot release the new success.
    with pytest.raises(PromptBudgetExceededError):
        journal.reserve({'another': 'request'})
    totals = journal_totals(path)
    assert (totals['requests'], totals['responses'], totals['transport_errors']) == (3, 2, 1)
    assert (totals['http_successes'], totals['http_errors'], totals['input_tokens']) == (1, 1, 11)
    journal.close()


def test_batch_recovery_is_atomic_and_cannot_requeue_a_success(tmp_path):
    path = tmp_path/'calls.sqlite'; journal = SQLitePromptJournal(path)
    requests = sorted([{'n': 1}, {'n': 2}], key=request_key)
    for request, status in zip(requests, [429, 200]):
        journal.reserve(request); journal.response(request, {'status_code': status, 'body': {}})
    before = journal_totals(path)
    with pytest.raises(ValueError, match='Only matching saved HTTP errors'):
        journal.requeue_http_errors([request_key(r) for r in requests], expected_statuses=[429], actor='test', reason='quota restored')
    assert journal_totals(path) == before
    assert journal.recovery_history() == []
    assert journal.lookup(requests[0])['status_code'] == 429
    for status in [200, True, '429', 600]:
        with pytest.raises(ValueError, match='HTTP error statuses'):
            journal.requeue_http_errors([request_key(requests[0])], expected_statuses=[status], actor='test', reason='invalid')
    journal.close()


def test_active_writers_and_readonly_recovery_are_rejected(tmp_path):
    path = tmp_path/'calls.sqlite'; request = {'n': 1}; key = request_key(request)
    owner = SQLitePromptJournal(path); owner.reserve(request)
    other = SQLitePromptJournal(path)
    with pytest.raises(UncertainPromptCall, match='active writer'):
        other.requeue_http_errors([key], expected_statuses=[429], actor='test', reason='busy')
    reader = SQLitePromptJournal(path, read_only=True)
    with pytest.raises(PermissionError):
        reader.requeue_http_errors([key], expected_statuses=[429], actor='test', reason='readonly')
    reader.close(); other.close(); owner.close()


def test_old_archive_schema_is_readable_before_additive_migration(tmp_path):
    path = tmp_path/'old.sqlite'; request = {'n': 1}; key = request_key(request)
    with sqlite3.connect(path) as db:
        db.executescript('''
            CREATE TABLE calls(request_key TEXT PRIMARY KEY,request_json TEXT,response_json TEXT,error_json TEXT,legacy_refs_json TEXT);
            CREATE TABLE recovery_events(operation_id TEXT,request_key TEXT,actor TEXT,reason TEXT,recovered_at REAL,request_json TEXT,error_json TEXT,attempt INTEGER,PRIMARY KEY(operation_id,request_key));
            CREATE TABLE journal_state(name TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO journal_state VALUES('request_count','1');
        ''')
        db.execute('INSERT INTO recovery_events VALUES(?,?,?,?,?,?,?,?)',
                   ('old', key, 'test', 'timeout', 1., json.dumps(request), json.dumps({'type': 'TimeoutError'}), 1))
    old_ref = {'journal_path': str(path), 'request_id': key, 'kind': 'error'}
    assert read_call(old_ref)['type'] == 'TimeoutError'
    assert journal_totals(path)['requests'] == 1
    assert journal_observation(path)['pending_reservations'] == 0
    journal = SQLitePromptJournal(path)
    journal.reserve(request)
    response = {'status_code': 429, 'body': {}}
    refs = journal.response(request, response)
    journal.requeue_http_errors([key], expected_statuses=[429], actor='test', reason='fixed')
    assert read_call(old_ref)['type'] == 'TimeoutError'
    assert read_call(refs['response_ref']) == response
    assert journal_totals(path)['requests'] == 2
    journal.close()
