"""Real multi-input fetch graph execution with original native receipts."""
import pytest

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.execution.pipeline.run_context import RunContext
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


@pytest.mark.parametrize('nested', [False, True],
                         ids=['before_first_cut', 'after_empty_residency_cut'])
def test_fetch_recovers_original_native_receipt_across_dataset_cut(tmp_path, monkeypatch, nested):
    import json
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
            if nested:
                # Preserve the original two distinct native occurrences and
                # union. Its six stages rotate under the default four workers.
                return _fetch(tmp_path, url, 1).union(_fetch(tmp_path, url, 2)).take_all()
            source = data.from_items([{'key': key, 'request': {'uri': url+'/file'}}
                                      for key in (1, 2)])
            return _fetch(tmp_path, url, None, source=source).take_all()

        monkeypatch.setattr(commits, 'fault_point', fault)
        # The small graph can coexist within the original resource envelope;
        # only its timer is explicit to isolate the no-Dataset-cut boundary.
        limits = {} if nested else {'checkpoint_interval_ms': 60000}
        with pytest.raises(RuntimeError, match='native-fetch-ack-interruption'):
            with RunContext.start(tmp_path/'run', limits=limits) as run:
                reference = run.ref
                assert run.limits['checkpoint_interval_ms'] == (1000 if nested else 60000)
                execute()
        assert fired
        assert run.journal.execution_commit(fired[0])['status'] == 'acked'
        dataset_cuts = run.journal.execution_commits('action-1')
        if not nested:
            assert not dataset_cuts
        else:
            # The original zero-cut assumption contradicted the sealed durable
            # handoff. Prove only empty non-final ACKs exist before raw failure,
            # including a real source-to-consumer parked delivery.
            assert dataset_cuts
            parked_sources = []
            for fact in dataset_cuts:
                assert fact['status'] == 'acked'
                assert set(fact['prepared']['sinks']) == {'take_all'}
                manifest = fact['prepared']['sinks']['take_all']['manifest']
                assert manifest['total_rows'] == manifest['total_bytes'] == 0
                assert manifest['final'] is False
                ref = fact['prepared']['state']['dataflow-cut-v3']
                state = json.loads((run.directory/ref['path']).read_text())
                assert state['cut']['final'] is False
                residency = state['residency']
                assert residency['schema'] == 'demiflow.durable_residency.v1'
                parked_sources.extend(delivery for delivery in residency['deliveries'].values()
                    if delivery['producer_cut'] == state['cut']['cut_id']
                    and not delivery['consumed']
                    and any(part['sources'] for part in state['contributions']))
            assert parked_sources
        monkeypatch.setattr(commits, 'fault_point', lambda *_: None)
        with RunContext.resume(reference) as resumed:
            rows = execute()
            assert sorted(row['key'] for row in rows) == [1, 2]
            receipts = {row['result']['acquisition_ref']['commit_id'] for row in rows}
            assert fired[0] in receipts and len(receipts) == 2
            assert all(resumed.journal.execution_commit(cid)['status'] == 'acked' for cid in receipts)
            assert all(ObjectRef(**row['result']['raw_ref']).read(max_bytes=len(BODY)) == BODY for row in rows)
            with resumed.journal._connection() as db:
                budgets = dict(db.execute('SELECT name,consumed FROM runtime_budgets'))
            assert budgets['requests'] == 2
            assert budgets['raw_wire_bytes'] == 2 * len(BODY)
        assert calls['/file'] == 2
