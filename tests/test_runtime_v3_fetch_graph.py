"""Real multi-input fetch graph execution with original native receipts."""
import pytest

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.execution.run_context import RunContext
from test_raw_fetch import BODY, origin_server


def _fetch(tmp_path, url, key, *, output='result', source=None):
    source = source if source is not None else data.from_items([{'key': key, 'request': {'uri': url+'/file'}}])
    return source.fetch(store=LocalObjectStore(tmp_path/'raw'), output=output,
        policy=FetchPolicy(max_object_bytes=128*1024, http=HttpSource(allow_private=True)), concurrency=1)


@pytest.mark.parametrize('shared', [False, True])
def test_nested_fetch_union_keeps_occurrences_and_receipts(tmp_path, shared):
    with origin_server() as (url, calls), RunContext.start(tmp_path/'run') as run:
        left = _fetch(tmp_path, url, 1)
        right = left if shared else _fetch(tmp_path, url, 2)
        rows = left.union(right).take_all()
        assert sorted(row['key'] for row in rows) == ([1, 1] if shared else [1, 2])
        commits = {row['result']['acquisition_ref']['commit_id'] for row in rows}
        assert len(commits) == (1 if shared else 2)
        assert all(run.journal.execution_commit(cid)['status'] == 'acked' for cid in commits)
        assert calls['/file'] == (1 if shared else 2)
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        for row in rows:
            assert ObjectRef(**row['result']['raw_ref']).read(max_bytes=len(BODY)) == BODY


def test_sequential_fetches_keep_distinct_native_namespaces(tmp_path):
    with origin_server() as (url, calls), RunContext.start(tmp_path/'run') as run:
        first = _fetch(tmp_path, url, 1, output='first')
        rows = _fetch(tmp_path, url, 1, output='second', source=first).take_all()
        assert len(rows) == 1 and calls['/file'] == 2
        commits = {rows[0][name]['acquisition_ref']['commit_id'] for name in ('first', 'second')}
        assert len(commits) == 2
        assert all(run.journal.execution_commit(cid)['status'] == 'acked' for cid in commits)


def test_fetch_graph_read_does_not_mutate_declarations(tmp_path):
    with origin_server() as (url, calls), RunContext.start(tmp_path/'run'):
        ds = _fetch(tmp_path, url, 1)
        actor = ds._plan.operations[-1].callable.target
        actor = getattr(actor, '__self__', actor)
        before = actor.recovery_identity()
        assert ds.take_all()[0]['result']['status'] == 'ok'
        assert actor.io is None and actor.run is None and actor.node_id is None
        assert actor.recovery_identity() == before
        assert calls['/file'] == 1


def test_nested_fetch_recovers_original_native_receipt_before_first_cut(tmp_path, monkeypatch):
    import demiflow.execution.commit as commits
    fired = []
    def fault(name, commit_id):
        if name != 'after_acked' or fired:
            return
        fact = run.journal.execution_commit(commit_id)
        if fact and fact['intent']['sinks'] == ['raw']:
            fired.append(commit_id)
            raise RuntimeError('native-fetch-ack-interruption')
    with origin_server() as (url, calls):
        def execute():
            return _fetch(tmp_path, url, 1).union(_fetch(tmp_path, url, 2)).take_all()
        monkeypatch.setattr(commits, 'fault_point', fault)
        with pytest.raises(RuntimeError, match='native-fetch-ack-interruption'):
            with RunContext.start(tmp_path/'run') as run:
                reference = run.ref
                execute()
        assert fired and not run.journal.execution_commits('action-1')
        monkeypatch.setattr(commits, 'fault_point', lambda *_: None)
        with RunContext.resume(reference) as resumed:
            rows = execute()
            assert sorted(row['key'] for row in rows) == [1, 2]
            receipts = {row['result']['acquisition_ref']['commit_id'] for row in rows}
            assert fired[0] in receipts and len(receipts) == 2
            assert all(resumed.journal.execution_commit(cid)['status'] == 'acked' for cid in receipts)
        assert calls['/file'] == 2
