"""The Dataset execution port owns iteration, bounded take and early close."""
from contextlib import closing

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext, current_action


def test_public_take_records_one_action_and_rejects_negative_limit_before_work(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(worker_mode='thread'):
        ds = data.from_items([{'id':i} for i in range(6)])
        with pytest.raises(ValueError, match='non-negative'):
            ds.take(-1)
        assert run.position == 0
        assert ds.take(3) == [{'id': 0}, {'id': 1}, {'id': 2}]
        assert run.position == 1 and current_action() is None


@pytest.mark.parametrize('format', ['rows', 'pyarrow'])
def test_public_delivery_closes_original_action_and_releases_workers(tmp_path, format):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(worker_mode='thread'):
        ds = data.range(32)
        results = ds.iter_rows() if format == 'rows' else ds.iter_batches(batch_format=format, batch_size=2)
        assert run.position == 0
        with closing(results) as iterator:
            first = next(iterator)
            if format == 'rows':
                assert first == {'id': 0}
            else:
                assert first.to_pylist() == [{'id': 0}, {'id': 1}]
            assert current_action().record['result'] is None
        assert run.journal.execution_action('action-1')['result']['delivery'] == 'closed'
        assert current_action() is None and run.position == 1
        commits = run.journal.execution_commits('action-1')
        assert commits and all(commit['status'] == 'acked' for commit in commits)
        assert ds._executor._last_metadata.diagnostics['state'] in {'succeeded', 'closed'}
