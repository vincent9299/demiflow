"""Real worker callbacks retain run identity without gaining driver authority."""
import json
import os

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext, current_run
from demiflow.execution.run_journal import RunJournal


@pytest.mark.parametrize('mode', ['process', 'thread'])
@pytest.mark.parametrize('failure', [False, True])
def test_source_open_pull_close_has_only_original_run_view(tmp_path, mode, failure):
    evidence = tmp_path / 'callbacks.jsonl'

    def factory():
        from demiflow.execution.pipeline.run_context import current_run, current_action
        from demiflow.execution.recovery_contract import RecoveryContractError

        def record(phase):
            run = current_run()
            assert current_action() is None
            assert not hasattr(run, 'journal')
            assert not hasattr(run, 'reserve')
            try:
                run.action({})
            except RecoveryContractError as exc:
                assert 'cannot create driver actions' in str(exc)
            else:
                raise AssertionError('source gained driver action authority')
            with evidence.open('a') as stream:
                stream.write(json.dumps({'phase': phase, 'directory': run.ref.directory,
                    'run_id': run.ref.run_id, 'attempt': run.attempt, 'pid': os.getpid()}) + '\n')

        record('open')

        def rows():
            try:
                for key in range(3):
                    record('pull')
                    if failure and key == 1:
                        raise ValueError('source-context-original-failure')
                    yield {'key': key}
            finally:
                record('close')
        return rows()

    def execute():
        with RunContext.start(tmp_path / 'run') as run, data.local_execution(worker_mode=mode):
            try:
                result = data.from_iter(factory).take_all()
                assert result == [{'key': key} for key in range(3)]
                assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            finally:
                assert current_run() is run

    if failure:
        with pytest.raises(ValueError, match='source-context-original-failure'):
            execute()
    else:
        execute()
    assert current_run() is None
    observed = [json.loads(line) for line in evidence.read_text().splitlines()]
    journal = RunJournal(tmp_path / 'run')
    identity = journal.recovery_identity()
    assert [item['phase'] for item in observed] == [
        'open', *(['pull'] * (2 if failure else 3)), 'close']
    for item in observed:
        assert item['directory'] == str(tmp_path / 'run')
        assert item['run_id'] == identity['run_id'] and item['attempt'] == 1
        assert (item['pid'] == os.getpid()) == (mode == 'thread')
    if failure:
        assert journal.execution_action('action-1')['result'] is None
        assert not any(commit['prepared'].get('final') for commit in journal.execution_commits('action-1'))
