"""Public topology, shared-root recovery and fixed-source cursor boundaries."""
import hashlib
import json
from pathlib import Path
import threading
import time

import pytest
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.queue import ProducerSourceSpec,task_message
from demiflow.execution.queue_sinks import QueueResultSink
from demiflow.execution.queue_contract import QueueIncomplete,HistoryReadLimits
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from test_queue_v2_protocol import channel,native_parent,publish,source
from test_raw_fetch import origin_server


def seed(channel,tmp_path,url):
    p=channel.open('p',producer_id='seed',source=source())
    with native_parent(tmp_path) as parent:
        publish(parent,channel,p,0,[task_message('x',{'request':{'uri':url+'/file'}},pool='p')])
        publish(parent,channel,p,1,[],eos=True)
    channel.seal(p)
    return p


def worker(channel,root,*,transform=None,stop=None,shared=False):
    ds=data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=.2,stop=stop).fetch(
        store=LocalObjectStore(root,read_domain='shared' if shared else 'host',domain_id='shared-fixture' if shared else None),
        policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
    if transform is not None:ds=transform(ds)
    return ds.write_datasink(QueueResultSink(channel.config,pool='p'))


def yes(row):return True

def expand(row):yield row


def drop(row):return None


def expand_list(row):return [row,row]


def alter_receipt(row):
    return {**row,'result':{**row['result'],'size_bytes':1}}


def reject_business(row):return {**row,'accepted':False}


@pytest.mark.parametrize('kind',['filter','flat_map','wrong_sink','double_ack'])
def test_invalid_task_topology_rejected_before_claim(channel,tmp_path,kind):
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url)
        ds=data.read_queue(channel.config,pool='p',max_tasks=1).fetch(store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(http=HttpSource(allow_private=True)),concurrency=1)
        if kind=='filter':ds=ds.filter(yes)
        elif kind=='flat_map':ds=ds.flat_map(expand)
        elif kind=='double_ack':ds=ds.ack_queue(channel.config)
        sink=object() if kind=='wrong_sink' else QueueResultSink(channel.config,pool='p')
        with pytest.raises((RecoveryContractError,TypeError,ValueError)):
            ds.write_datasink(sink)
        assert not calls and channel.state('p')['counts']=={'pending':1}


@pytest.mark.parametrize('callback',[drop,expand_list,alter_receipt])
def test_runtime_drop_expansion_or_receipt_mutation_cannot_ack(channel,tmp_path,callback):
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url)
        with pytest.raises((RecoveryContractError,TypeError)):
            worker(channel,tmp_path/'objects',transform=lambda ds:ds.map(callback))
        assert calls['/file']==1 and channel.completion(p.generation,'x') is None
        assert channel.state('p')['counts']=={'leased':1}


def test_business_rejection_retains_raw_settlement(channel,tmp_path):
    with origin_server() as (url,calls):
        seed(channel,tmp_path,url)
        worker(channel,tmp_path/'objects',transform=lambda ds:ds.map(reject_business))
        snapshot=channel.snapshot('p')
        row,=data.read_queue_records(channel.config,pool='p',through=snapshot,results=True).take_all()
        assert row['accepted'] is False and row['result']['status']=='ok' and channel.state('p')['queue_drained']


def test_shared_prepared_recovery_at_another_mount_root(channel,tmp_path,monkeypatch):
    import demiflow.execution.commit as commits
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url);fired=[]
        def fault(name,commit_id):
            if name!='after_prepared' or fired:return
            with channel.database.connection() as db:
                fact=db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
            if fact and json.loads(fact[0])['sinks']==['raw']:
                fired.append(commit_id);raise RuntimeError('prepared-shared')
        monkeypatch.setattr(commits,'fault_point',fault)
        old=tmp_path/'old-mount';new=tmp_path/'new-mount'
        with pytest.raises(Exception,match='prepared-shared'):worker(channel,old,shared=True)
        old.rename(new)
        with channel.database.connection(write=True) as db:db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?',(p.generation,))
        monkeypatch.setattr(commits,'fault_point',lambda *_:None)
        worker(channel,new,shared=True)
        assert calls['/file']==1 and channel.state('p')['queue_drained']
        assert len(data.read_queue_records(channel.config,pool='p',through=channel.snapshot('p'),results=True).take_all())==1


def test_source_stop_during_final_artifact_inspection_does_not_ack(channel,tmp_path,monkeypatch):
    from demiflow.execution.queue_sinks import QueueResultSink
    original=QueueResultSink.verify_acquisition
    stop=tmp_path/'stop';fired=[]
    def inspection(self,prepared):
        original(self,prepared)
        if not fired:fired.append(True);stop.touch()
    monkeypatch.setattr(QueueResultSink,'verify_acquisition',inspection)
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url)
        with pytest.raises(QueueIncomplete):worker(channel,tmp_path/'objects',stop=str(stop))
        assert channel.completion(p.generation,'x') is None and channel.state('p')['counts']=={'leased':1}


def test_jsonl_producer_resumes_from_native_byte_cursor(channel,tmp_path,monkeypatch):
    import demiflow.execution.commit as commits
    path=tmp_path/'metadata.jsonl'
    path.write_text(''.join(json.dumps({'messages':[task_message('x'+str(i),{'value':i},pool='p')]})+'\n' for i in range(5)))
    p=channel.open('p',producer_id='jsonl',source=ProducerSourceSpec(kind='records',source_id='records',uri=path.as_uri(),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    fired=[]
    def fault(name,commit_id):
        if name!='after_sink_receipt' or fired:return
        with channel.database.connection() as db:
            row=db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
        chunk=json.loads(row[0]).get('sinks',{}).get('publish',{}) if row and row[0] else {}
        if chunk.get('unit_end') and not chunk.get('eos'):
            fired.append(True);raise RuntimeError('after-source-unit')
    monkeypatch.setattr(commits,'fault_point',fault)
    with pytest.raises(Exception,match='after-source-unit'):
        with RunContext.start(tmp_path/'first'):data.read_json(str(path)).enqueue(channel.config,producer=p).run_stream()
    with channel.database.connection() as db:
        position,cursor=db.execute('SELECT source_position,source_cursor FROM queue_pools WHERE generation=?',(p.generation,)).fetchone()
    assert position==1 and json.loads(cursor)['offset']==len(path.read_bytes().splitlines(keepends=True)[0])
    monkeypatch.setattr(commits,'fault_point',lambda *_:None)
    newer=channel.open('p',producer_id='jsonl',source=p.source)
    with RunContext.start(tmp_path/'second'):data.read_json(str(path)).enqueue(channel.config,producer=newer).run_stream()
    assert channel.seal(newer)['actual_tasks']==5


def test_lance_producer_uses_exact_version_and_native_offset(channel,tmp_path):
    path=str(tmp_path/'source.lance')
    data.from_items([{'messages':[task_message('x'+str(i),{'value':i},pool='p')]} for i in range(3)]).write_lance(path,mode='create')
    p=channel.open('p',producer_id='lance',source=ProducerSourceSpec(kind='lance',source_id='lance-records',uri=path,version=1))
    with RunContext.start(tmp_path/'producer'):
        data.read_lance(path,version=1).enqueue(channel.config,producer=p).run_stream()
    assert channel.seal(p)['actual_tasks']==3
