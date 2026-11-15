"""Real Dataset search, native response receipts and worker cut recovery."""
import pytest

from demiflow import data
from demiflow.collect import SearchConfig
from demiflow.collect.session import WebSession
from demiflow.execution.recovery_contract import UnresolvedCommit
from demiflow.execution.run_context import RunContext
from test_native_search_runtime import engines, server


def _session(tmp_path, engines):
    return WebSession(cache_path=tmp_path/'cache.sqlite', object_directory=tmp_path/'objects',
        search=SearchConfig(engines=(engines[0],), language='en', host_interval_s=0,
                           workers=1, max_bytes=65536, max_results=8))


def _dataset(session, query='oak'):
    return data.from_items([{'queries': [{'query': query}]}]).search_web(
        requests='queries', output='found', session=session, request_concurrency=1,
        concurrency=1, queue_depth=1)


def test_standalone_search_uses_native_journal_and_reports_metrics(tmp_path, engines, server):
    session = _session(tmp_path, engines)
    with RunContext.start(tmp_path/'run') as run:
        ds = _dataset(session)
        rows = ds.take_all()
        assert len(rows) == 1 and rows[0]['found'][0]['status'] == 'ok'
        assert rows[0]['found'][0]['candidates']
        assert len(server[1]) == 1
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        with run.journal._connection() as db:
            assert db.execute("SELECT count(*) FROM runtime_receipts WHERE sink_id='search'").fetchone()[0] == 1
        metrics = ds.execution_metadata().diagnostics['resources']['WebSession:0']
        assert metrics['search_http_requests'] == metrics['search_queries'] == 1
    assert session.native is None and session._operator_task is None
    assert session.connections.closed


@pytest.mark.parametrize('outcome', ['fixed', 'unknown'])
def test_search_resume_before_first_dataset_cut_never_repeats_http(tmp_path, engines, server, monkeypatch, outcome):
    from demiflow.collect.search_contract import NativeSearchResultSink
    import demiflow.execution.commit as commits
    fired = []
    original = NativeSearchResultSink.prepare
    def interrupt(self, *args, **kwargs):
        fired.append(True)
        raise RuntimeError('unfixed-search')
    def fault(name, commit_id):
        if name != 'after_acked' or fired:
            return
        fact = run.journal.execution_commit(commit_id)
        if fact and 'search' in fact['receipts']:
            fired.append(True)
            raise RuntimeError('fixed-search')
    if outcome == 'unknown':
        monkeypatch.setattr(NativeSearchResultSink, 'prepare', interrupt)
    else:
        monkeypatch.setattr(commits, 'fault_point', fault)
    with pytest.raises(RuntimeError, match='fixed-search'):
        with RunContext.start(tmp_path/'run') as run:
            ref = run.ref
            _dataset(_session(tmp_path, engines)).take_all()
    assert fired and len(server[1]) == 1
    (tmp_path/'cache.sqlite').unlink()
    monkeypatch.setattr(NativeSearchResultSink, 'prepare', original)
    monkeypatch.setattr(commits, 'fault_point', lambda *_: None)
    if outcome == 'unknown':
        with pytest.raises(UnresolvedCommit):
            with RunContext.resume(ref):
                _dataset(_session(tmp_path, engines)).take_all()
    else:
        with RunContext.resume(ref):
            rows = _dataset(_session(tmp_path, engines)).take_all()
        assert len(rows) == 1 and rows[0]['found'][0]['status'] == 'ok'
    assert len(server[1]) == 1


def test_independent_union_searches_share_session_without_sharing_receipts(tmp_path, engines, server):
    session = _session(tmp_path, engines)
    with RunContext.start(tmp_path/'run') as run:
        rows = _dataset(session, 'oak').union(_dataset(session, 'pine')).take_all()
        assert sorted(row['found'][0]['query'] for row in rows) == ['oak', 'pine']
        assert len(server[1]) == 2
        with run.journal._connection() as db:
            assert db.execute("SELECT count(*) FROM runtime_receipts WHERE sink_id='search'").fetchone()[0] == 2
    assert session.native is None and session.connections.closed


def test_fetch_then_search_uses_one_native_io_authority(tmp_path, engines, server):
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.objects import LocalObjectStore
    from test_raw_fetch import origin_server
    session = _session(tmp_path, engines)
    with origin_server() as (url, calls), RunContext.start(tmp_path/'run'):
        ds = (data.from_items([{'request': {'uri': url+'/file'}, 'queries': [{'query': 'oak'}]}])
            .fetch(store=LocalObjectStore(tmp_path/'raw'),
                   policy=FetchPolicy(max_object_bytes=128*1024, http=HttpSource(allow_private=True)), concurrency=1)
            .search_web(requests='queries', output='found', session=session, concurrency=1, queue_depth=1))
        rows = ds.take_all()
    assert len(rows) == 1 and rows[0]['result']['raw_ref'] and rows[0]['found'][0]['status'] == 'ok'
    from demiflow.objects import ObjectRef
    from test_raw_fetch import BODY
    assert ObjectRef(**rows[0]['result']['raw_ref']).read(max_bytes=len(BODY)) == BODY
    assert calls['/file'] == 1 and len(server[1]) == 1
    assert session.native is None and session.connections.closed


def test_standalone_search_run_stream_retains_native_metrics(tmp_path, engines, server):
    session = _session(tmp_path, engines)
    with RunContext.start(tmp_path/'run'):
        stats = _dataset(session).run_stream()
    assert stats.emitted == 1
    assert stats.metrics['resources']['WebSession:0']['search_http_requests'] == 1
    assert len(server[1]) == 1 and session.native is None
