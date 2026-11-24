"""Same native commit transitions on real SQLite and task-private PostgreSQL."""
from dataclasses import asdict
from contextlib import contextmanager
import json
import os
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.native_queue.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message
from demiflow.execution.native_queue.queue_commit import producer_action, task_action
from demiflow.execution.native_queue.queue_sinks import QueuePublishSink, QueueResultSink
from demiflow.execution.contracts.recovery_contract import SourcePosition, RecoveryContractError, identity
from demiflow.execution.contracts.queue_contract import StaleQueueOwner
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.secrets import Secret


@pytest.fixture(params=['sqlite','postgresql'])
def channel(tmp_path,request):
    if request.param=='postgresql':
        dsn=os.environ.get('DEMIFLOW_TEST_PG_DSN')
        if not dsn:pytest.skip('task-private PostgreSQL DSN not provided')
        assert 'host=/tmp/demiflow-fetch-pg-' in dsn
        config=QueueConfig(backend='postgresql',queue_id='fixture',dsn=Secret('DEMIFLOW_TEST_PG_DSN'),namespace='test_'+uuid.uuid4().hex)
    else:
        config=QueueConfig(backend='sqlite',queue_id='fixture',path=str(tmp_path/'q.sqlite'))
    yield QueueChannel(config)


@contextmanager
def native_parent(tmp_path):
    with RunContext.start(tmp_path/'run') as run,run.action({'kind':'producer-fixture'}) as parent:
        yield parent
        parent.complete({'kind':'producer-fixture','finished':True})


def source():
    return ProducerSourceSpec(kind='manifest',source_id='fixture-source',uri='file:///fixture',sha256='1'*64)


def publish(parent,channel,producer,index,messages=(),end=True,eos=False):
    occurrence=identity(['fixture',index,'eos' if eos else 'tasks'])
    action=producer_action(parent,channel,producer,occurrence,'fixture-contract')
    sink=QueuePublishSink(action,producer,'fixture-contract')
    prepared=dict(format=2,occurrence=occurrence,source_position=index,output_ordinal=0,
                  messages=list(messages),unit_end=end,eos=eos)
    return CommitCoordinator(action).commit(1,outputs={'publish':(sink,prepared)},position=SourcePosition(producer.source.source_id,index))


def domain(name='one'):
    return dict(domain_id='domain-'+name,store_id='raw',read_domain='host',physical_root='/tmp/domain-'+name,
                host_identity='host-'+name,marker_digest='marker-'+name)


def worker(channel,name='one'):
    return channel.register_worker('p',domain(name),{'contract':'fixture'}, {'max_commits':10})


def test_unknown_task_count_incremental_publication_and_eos(channel,tmp_path):
    producer=channel.open('p',producer_id='producer',source=source())
    with pytest.raises(RecoveryContractError,match='EOS'):channel.seal(producer)
    with native_parent(tmp_path) as parent:
        first=publish(parent,channel,producer,0,[task_message('t0',{'request':{'uri':'https://example.org/a'}},pool='p')])
        assert first['status']=='acked'
        assert channel.state('p')['tasks']==1 and not channel.state('p')['sealed']
        handle=channel.claim(worker(channel))
        assert handle.task_id=='t0'
        second=publish(parent,channel,producer,1,[],eos=True)
        assert second['status']=='acked'
    sealed=channel.seal(producer)
    assert sealed['actual_tasks']==sealed['cursor']==1
    assert not channel.state('p')['queue_drained']
    with pytest.raises(RecoveryContractError,match='Sealed'):
        channel.open('p',producer_id='producer',source=source())


def test_same_native_chunk_replay_has_no_new_counts(channel,tmp_path):
    p=channel.open('p',producer_id='producer',source=source())
    with native_parent(tmp_path) as parent:
        messages=[task_message('x',{'value':1},pool='p')]
        a=publish(parent,channel,p,0,messages)
        b=publish(parent,channel,p,0,messages)
        assert a==b and channel.state('p')['tasks']==1
    assert channel.snapshot('p',kind='tasks').cursor==1


def test_task_identity_conflict_and_old_producer_fence(channel,tmp_path):
    p=channel.open('p',producer_id='producer',source=source())
    with native_parent(tmp_path) as parent:
        publish(parent,channel,p,0,[task_message('x',{'value':1},pool='p')])
        with pytest.raises(RecoveryContractError,match='Conflicting'):
            publish(parent,channel,p,1,[task_message('x',{'value':2},pool='p')])
        newer=channel.open('p',producer_id='producer',source=source())
        assert newer.epoch==p.epoch+1
        with pytest.raises(StaleQueueOwner):publish(parent,channel,p,1,[])


def test_pool_accepts_different_host_domains_and_fences_old_owner(channel,tmp_path):
    p=channel.open('p',producer_id='producer',source=source())
    with native_parent(tmp_path) as parent:
        publish(parent,channel,p,0,[task_message('x',{'value':1},pool='p'),task_message('y',{'value':2},pool='p')])
    one=channel.claim(worker(channel,'one'));two=channel.claim(worker(channel,'two'))
    assert one.task_id!=two.task_id and one.domain!=two.domain
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=? AND task_id=?',(one.generation,one.task_id))
    replacement=channel.claim(worker(channel,'three'))
    assert replacement.task_id==one.task_id and replacement.epoch==one.epoch+1
    with pytest.raises(StaleQueueOwner):channel.renew(one)
    with pytest.raises(StaleQueueOwner):channel.release(one)
    channel.renew(replacement)


def test_publication_counter_rolls_back_with_record(channel):
    p=channel.open('p',producer_id='producer',source=source())
    with pytest.raises(RuntimeError):
        with channel.database.connection(write=True) as db:
            assert channel.database.publication(db,p.generation,'tasks')==1
            raise RuntimeError('rollback')
    assert channel.snapshot('p',kind='tasks').cursor==0


def test_completion_lookup_does_not_write_or_claim(channel):
    p=channel.open('p',producer_id='producer',source=source())
    assert channel.completion(p.generation,'missing') is None
    assert channel.state('p')['tasks']==0


def test_public_queue_fetch_result_terminal(channel,tmp_path):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    from test_raw_fetch import origin_server,BODY
    from demiflow.execution.native_queue.queue import QueueResultSink
    with origin_server() as (url,calls):
        p=channel.open('p',producer_id='producer',source=source())
        with native_parent(tmp_path) as parent:
            publish(parent,channel,p,0,[task_message('t0',{'id':1,'request':{'uri':url+'/file'}},pool='p'),
                                       task_message('t1',{'id':2,'request':{'uri':url+'/missing'}},pool='p')])
            publish(parent,channel,p,1,[],eos=True)
        channel.seal(p)
        with RunContext.start(tmp_path/'worker',limits={'max_row_bytes':256*1024,'max_batch_bytes':1024*1024}):
            result=(data.read_queue(channel.config,pool='p',idle_timeout_s=2)
                .fetch(store=LocalObjectStore(tmp_path/'objects'),
                       policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=2)
                .write_datasink(QueueResultSink(channel.config,pool='p')))
            assert result is None
        state=channel.state('p')
        assert state['queue_drained'] and state['counts']=={'done_failed':1,'done_ok':1}
        assert calls['/file']==calls['/missing']==1
        proof=channel.completion(p.generation,'t0')
        assert proof['settled'] is True and proof['status']=='ok'
        with channel.database.connection() as db:
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',(proof['final_commit'],)).fetchone()[0]=='acked'
            assert db.execute('SELECT token,lease_until FROM queue_tasks WHERE generation=? AND task_id=?',(p.generation,'t0')).fetchone()==(None,None)


def caught_task(value):
    raise ValueError('caught-task-must-fail-preflight')


@pytest.mark.parametrize('bound',[False,True])
def test_task_catch_rejected_before_claim_or_get(channel,tmp_path,bound):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    from test_raw_fetch import origin_server
    with origin_server() as (url,calls):
        p=channel.open('p',producer_id='producer',source=source())
        with native_parent(tmp_path) as parent:
            publish(parent,channel,p,0,[task_message('t0',{'request':{'uri':url+'/file'}},pool='p')])
            publish(parent,channel,p,1,[],eos=True)
        channel.seal(p)
        ds=data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(http=HttpSource(allow_private=True)),concurrency=1)
        ds=ds.map(caught_task,inputs={'value':'request'},output='bad',catch=(ValueError,)) if bound else ds.map(caught_task,catch=(ValueError,))
        with pytest.raises(RecoveryContractError,match='cannot drop'):
            ds.write_datasink(QueueResultSink(channel.config,pool='p'))
        with channel.database.connection() as db:
            assert db.execute('SELECT status,epoch FROM queue_tasks WHERE generation=?',(p.generation,)).fetchone()==('pending',0)
        assert not calls and channel.completion(p.generation,'t0') is None


def expand_input(row):
    for index in range(row['n']):
        yield {'messages':[task_message(str(row['id'])+'-'+str(index),{'value':index},pool='p')]}


def test_public_incremental_producer_zero_and_many_outputs(channel,tmp_path):
    from demiflow import data
    spec=ProducerSourceSpec(kind='records',source_id='literal-records',uri='memory:records',version=1)
    p=channel.open('p',producer_id='public-producer',source=spec)
    with RunContext.start(tmp_path/'producer-run'):
        stats=(data.from_items([{'id':0,'n':0},{'id':1,'n':65}])
               .flat_map(expand_input).enqueue(channel.config,producer=p).run_stream())
    assert stats.emitted==65
    state=channel.state('p')
    assert state['tasks']==65 and state['eos'] and not state['sealed']
    assert channel.seal(p)['actual_tasks']==65
    with channel.database.connection() as db:
        assert db.execute('SELECT source_position,output_offset FROM queue_pools WHERE generation=?',(p.generation,)).fetchone()==(2,0)
        assert db.execute("SELECT count(*) FROM runtime_commits WHERE status!='acked'").fetchone()[0]==0


def test_terminal_refuses_lost_source_confirmation(channel,tmp_path,monkeypatch):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    from demiflow.execution.native_queue.queue_source import QueueDatasource
    from demiflow.execution.contracts.queue_contract import QueueIncomplete
    from test_raw_fetch import origin_server
    with origin_server() as (url,calls):
        p=channel.open('p',producer_id='producer',source=source())
        with native_parent(tmp_path) as parent:
            publish(parent,channel,p,0,[task_message('t0',{'request':{'uri':url+'/file'}},pool='p')])
            publish(parent,channel,p,1,[],eos=True)
        channel.seal(p)
        monkeypatch.setattr(QueueDatasource,'confirmed',lambda *a,**kw:None)
        with pytest.raises(QueueIncomplete,match='unconfirmed claimed tasks'):
            (data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1)
             .fetch(store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(http=HttpSource(allow_private=True)),concurrency=1)
             .write_datasink(QueueResultSink(channel.config,pool='p')))
        assert calls['/file']==1
        assert channel.completion(p.generation,'t0')['settled']


from test_native_search_runtime import server,engines


def search_messages(row):
    for result in row['searched']:
        for index,candidate in enumerate(result['candidates']):
            yield {'messages':[task_message(row['id']+'-'+str(index),{'request':{'uri':candidate['url']}},pool='p')]}


def search_producer(channel,p,tmp_path,engine_specs,name):
    from demiflow import data
    from demiflow.collect import SearchConfig
    from demiflow.collect.session import WebSession
    session=WebSession(search=SearchConfig(engines=(engine_specs[0],),language='en',workers=1,
        request_concurrency=1,max_bytes=65536,max_results=8,host_interval_s=0),
        cache_path=tmp_path/'search-cache.sqlite',object_directory=tmp_path/'unused')
    with RunContext.start(tmp_path/name):
        return (data.from_items([{'id':'a','queries':[{'query':'oak','request_id':'oak'}]},
                                 {'id':'b','queries':[{'query':'pine','request_id':'pine'}]}])
            .search_web(requests='queries',output='searched',session=session,max_candidates=2,
                        request_concurrency=1,concurrency=1,queue_depth=1)
            .flat_map(search_messages).enqueue(channel.config,producer=p).run_stream())


def test_public_search_fixed_before_partial_publication_resume(channel,tmp_path,engines,server,monkeypatch):
    import demiflow.execution.checkpoint.commit as commits
    p=channel.open('p',producer_id='search-producer',source=ProducerSourceSpec(
        kind='keywords',source_id='fixed-keywords',uri='memory:keywords',version=1))
    fired=[]
    def fault(name,commit_id):
        if name!='after_sink_receipt' or fired:return
        with channel.database.connection() as db:
            row=db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
        value=json.loads(row[0]) if row and row[0] else {}
        if value.get('sinks',{}).get('publish',{}).get('messages'):
            fired.append(commit_id)
            raise RuntimeError('stop-after-first-publish')
    monkeypatch.setattr(commits,'fault_point',fault)
    with pytest.raises(Exception,match='stop-after-first-publish'):
        search_producer(channel,p,tmp_path,engines,'search-first')
    assert len(server[1])==1 and channel.state('p')['tasks']==1
    assert not channel.state('p')['eos']
    (tmp_path/'search-cache.sqlite').unlink()
    monkeypatch.setattr(commits,'fault_point',lambda *_:None)
    newer=channel.open('p',producer_id='search-producer',source=p.source)
    search_producer(channel,newer,tmp_path,engines,'search-second')
    assert len(server[1])==2  # Only the next keyword performed HTTP.
    assert channel.state('p')['tasks']==4
    assert channel.seal(newer)['actual_tasks']==4
    with channel.database.connection() as db:
        fixed=db.execute("SELECT count(*) FROM runtime_receipts WHERE sink_id='search'").fetchone()[0]
        assert fixed==2


def publish_and_consume(channel,tmp_path,url,*,explicit=False):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    p=channel.open('p',producer_id='all-public',source=ProducerSourceSpec(
        kind='records',source_id='requests',uri='memory:requests',version=1))
    with RunContext.start(tmp_path/'producer'):
        data.from_items([{'messages':[task_message('t'+str(i),{'id':i,'request':{'uri':url+'/file'}},pool='p')]} for i in range(3)]).enqueue(channel.config,producer=p).run_stream()
    channel.seal(p)
    with RunContext.start(tmp_path/'worker'):
        ds=data.read_queue(channel.config,pool='p',settlement='explicit' if explicit else 'sink',max_tasks=3,idle_timeout_s=1).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=2)
        if explicit:ds.ack_queue(channel.config,keep_row=False).run_stream()
        else:ds.write_datasink(QueueResultSink(channel.config,pool='p'))
    return p


def test_public_producer_consumer_history_and_explicit_ack(channel,tmp_path):
    from demiflow import data
    from demiflow.execution.contracts.queue_contract import HistoryReadLimits, HistoryReadLimit
    from test_raw_fetch import origin_server
    with origin_server() as (url,calls):
        p=publish_and_consume(channel,tmp_path,url,explicit=True)
        assert channel.state('p')['queue_drained'] and calls['/file']==3
        tasks=channel.snapshot('p',kind='tasks')
        assert len(data.read_queue_records(channel.config,pool='p',through=tasks).take_all())==3
        snapshot=channel.snapshot('p')
        with pytest.raises(HistoryReadLimit) as caught:
            data.read_queue_records(channel.config,pool='p',through=snapshot,results=True,limits=HistoryReadLimits(max_rows=2)).take_all()
        cursor=caught.value.next_cursor
        assert cursor['next_ordinal']==3
        rows=data.read_queue_records(channel.config,pool='p',through=snapshot,results=True,after=cursor).take_all()
        assert len(rows)==1 and rows[0]['verification']['status']=='verified'
        with pytest.raises(HistoryReadLimit,match='hash budget'):
            data.read_queue_records(channel.config,pool='p',through=snapshot,results=True,limits=HistoryReadLimits(max_hash_bytes=0)).take_all()
        from urllib.parse import urlsplit,unquote
        obj=Path(unquote(urlsplit(rows[0]['result']['raw_ref']['uri']).path))
        before=obj.read_bytes();obj.write_bytes(b'x'*len(before))
        with pytest.raises(Exception,match='checksum'):
            data.read_queue_records(channel.config,pool='p',through=snapshot,results=True).take_all()
        metadata=data.read_queue_records(channel.config,pool='p',through=snapshot,results=True,verify='metadata').take_all()
        assert len(metadata)==3 and all(r['verification']['status']=='not_checked' for r in metadata)
        assert calls['/file']==3


def consume_one(channel,tmp_path,*,explicit=False):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    ds=data.read_queue(channel.config,pool='p',settlement='explicit' if explicit else 'sink',max_tasks=1,max_inflight=1,idle_timeout_s=1).fetch(
        store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
    if explicit:return ds.ack_queue(channel.config).run_stream()
    return ds.write_datasink(QueueResultSink(channel.config,pool='p'))


@pytest.mark.parametrize('seam',['after_checkpoint_committed','after_acked'])
@pytest.mark.parametrize('explicit',[False,True])
def test_same_run_recovers_own_unconfirmed_final_without_get(channel,tmp_path,monkeypatch,seam,explicit):
    import demiflow.execution.checkpoint.commit as commits
    from test_raw_fetch import origin_server
    with origin_server() as (url,calls):
        p=channel.open('p',producer_id='p',source=source())
        with native_parent(tmp_path) as parent:
            publish(parent,channel,p,0,[task_message('x',{'request':{'uri':url+'/file'}},pool='p')])
            publish(parent,channel,p,1,[],eos=True)
        channel.seal(p);fired=[]
        def fault(name,commit_id):
            if name!=seam or fired:return
            with channel.database.connection() as db:
                row=db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
            if row and json.loads(row[0])['sinks']==['result']:
                fired.append(commit_id);raise RuntimeError('interrupt-own-final')
        monkeypatch.setattr(commits,'fault_point',fault)
        with pytest.raises(Exception,match='interrupt-own-final'):
            with RunContext.start(tmp_path/'same-run') as run:
                reference=run.ref
                consume_one(channel,tmp_path,explicit=explicit)
        assert calls['/file']==1
        if seam=='after_checkpoint_committed':
            with channel.database.connection(write=True) as db:
                db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?',(p.generation,))
        monkeypatch.setattr(commits,'fault_point',lambda *_:None)
        with RunContext.resume(reference):consume_one(channel,tmp_path,explicit=explicit)
        assert channel.state('p')['queue_drained'] and calls['/file']==1
        with channel.database.connection() as db:
            assert db.execute('SELECT claimed,settled FROM queue_workers WHERE generation=?',(p.generation,)).fetchall()==[(1,1)]


def test_new_worker_does_not_inspect_completed_history(channel,tmp_path,monkeypatch):
    from demiflow import data
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    from demiflow.execution.native_queue.raw_objects import RawInspection
    from test_raw_fetch import origin_server
    with origin_server() as (url,calls):
        publish_and_consume(channel,tmp_path,url)
        def forbidden(*a,**kw):raise AssertionError('fresh worker inspected old artifact')
        monkeypatch.setattr(RawInspection,'__init__',forbidden)
        with RunContext.start(tmp_path/'fresh-empty'):
            ds=data.read_queue(channel.config,pool='p',max_tasks=1,idle_timeout_s=1).fetch(
                store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
            ds.write_datasink(QueueResultSink(channel.config,pool='p'))
        assert calls['/file']==3
        assert ds.execution_metadata().diagnostics['queue']['claimed']==0
