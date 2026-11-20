"""Real Dataset workers, native operator tools and the run-owned journal."""
import json
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from test_agentmap_async import agent, api_call, resource, server
from test_codex_agent import app_server, stream, events, assert_stopped


def test_agent_reads_native_document_across_turns(tmp_path, server, resource):
    def respond(body, index):
        if index == 0:
            return {'api_calls': [api_call(resource)], 'response': None}
        assert 'Full qualifying context.' in json.dumps(body['messages'])
        return {'api_calls': [], 'response': {'result': 'read-ok'}}
    server['respond'] = respond
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        rows = agent([{'item': 'question', 'resources': {'D1': resource}}], max_requests=2).take_all()
        row, = rows
        assert 'error' not in row, row
        assert row['answer'] == 'read-ok'
        assert len(row['call']['environment']['observations']) == 1
        assert run.journal.budget_consumed('requests') == 2
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert len(server['requests']) == 2


def test_codex_native_callback_uses_driver_request_journal(tmp_path, app_server, resource):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        rows = stream(app_server, resource, tmp_path, journal=False, limit=1).take_all()
        row, = rows
        assert 'error' not in row, row
        assert row['answer'] == 'done'
        assert row['call']['budget_unit'] == 'codex_session'
        assert len(row['call']['environment']['observations']) == 1
        assert run.journal.budget_consumed('requests') == 1
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
    assert len([e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start']) == 1
    assert_stopped(app_server)


@pytest.mark.parametrize('unknown', [False, True])
def test_codex_native_response_resume_before_first_cut(tmp_path, app_server, resource, monkeypatch, unknown):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.run_journal import RunJournal
    from demiflow.operator_llm.journal import UncertainPromptCall
    if unknown:
        monkeypatch.setenv('DF_TEST_CODEX_MODE', 'sleep')
    timeout = 2 if unknown else 20
    def interrupted(*args, **kwargs):
        raise RuntimeError('stop before first Dataset commit')
    with monkeypatch.context() as fault:
        fault.setattr(DataflowCheckpointCoordinator, 'commit', interrupted)
        with pytest.raises(RuntimeError, match='stop before first Dataset commit'):
            with RunContext.start(tmp_path / 'run') as run, data.local_execution():
                reference = run.ref
                stream(app_server, resource, tmp_path, journal=False, limit=1, timeout=timeout).take_all()
    journal = RunJournal(tmp_path / 'run')
    assert journal.execution_commits('action-1') == []
    assert journal.budget_consumed('requests') == 1
    turns_before = len([e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start'])
    assert turns_before == 1
    if unknown:
        with pytest.raises(UncertainPromptCall):
            with RunContext.resume(reference), data.local_execution():
                stream(app_server, resource, tmp_path, journal=False, limit=1, timeout=timeout).take_all()
    else:
        with RunContext.resume(reference), data.local_execution():
            rows = stream(app_server, resource, tmp_path, journal=False, limit=1, timeout=timeout).take_all()
        assert len(rows) == 1 and rows[0]['answer'] == 'done'
        assert journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert len([e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start']) == turns_before
    assert journal.budget_consumed('requests') == 1
    assert_stopped(app_server)


def test_codex_native_node_quota_is_not_replaced_by_global_quota(tmp_path, app_server, resource):
    inputs = [{'item': str(i), 'resources': {'D1': resource}} for i in range(2)]
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 9}) as run, data.local_execution():
        rows = stream(app_server, resource, tmp_path, rows=inputs, journal=False, limit=1).take_all()
        assert len(rows) == 2
        assert sum('error' not in row for row in rows) == 1
        error = next(row['error'] for row in rows if 'error' in row)
        assert 'budget' in str(error).lower(), error
        assert run.journal.budget_consumed('requests') == 1
    assert len([e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start']) == 1
    assert_stopped(app_server)
