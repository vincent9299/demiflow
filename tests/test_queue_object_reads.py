"""Explicit domain reads preserve native receipts across a shared remount."""
import hashlib
import json
from pathlib import Path

import pytest
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore,ObjectRef,read_local_object
from demiflow.execution.native_queue.raw_objects import RawNamespace
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from test_queue_v2_protocol import channel
from test_queue_v2_boundaries import seed
from test_raw_fetch import origin_server,BODY


class ReadQuality:
    def __call__(self,row,store):
        receipt=row['result']
        current=store.resolve(receipt['raw_ref'],domain=receipt['storage_domain'])
        raw=read_local_object(current.uri,current.sha256,root=store.directory,max_bytes=131072)
        return {**row,'consumed_bytes':len(raw),'content_sha256':hashlib.sha256(raw).hexdigest()}


def _worker(channel,root,run_root):
    store=LocalObjectStore(root,read_domain='shared',domain_id='shared-fixture')
    with RunContext.start(run_root):
        (data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=.2)
            .fetch(store=store,concurrency=1,policy=FetchPolicy(max_object_bytes=131072,http=HttpSource(allow_private=True)))
            .map(ReadQuality,fn_kwargs={'store':store})
            .write_datasink(QueueResultSink(channel.config,pool='p')))
    return store


def _facts(channel):
    with channel.database.connection() as db:
        return {name:db.execute(sql).fetchall() for name,sql in {
            'commits':'SELECT commit_id,prepared,status FROM runtime_commits ORDER BY commit_id LIMIT 128',
            'receipts':'SELECT commit_id,sink_id,document FROM runtime_receipts ORDER BY commit_id,sink_id LIMIT 128',
            'completions':'SELECT proof FROM queue_completions ORDER BY ordinal LIMIT 8',
            'results':'SELECT row_document FROM queue_results ORDER BY task_id LIMIT 8',
            'tasks':'SELECT task_id,status FROM queue_tasks ORDER BY task_id LIMIT 8',
        }.items()}


def test_shared_remount_ordinary_actor_and_readonly_history_consume_original_receipt(channel,tmp_path,monkeypatch):
    import demiflow.execution.checkpoint.commit as commits
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url);fired=[]
        def fault(name,commit_id):
            if name!='after_prepared' or fired:return
            with channel.database.connection() as db:
                fact=db.execute('SELECT intent,prepared FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
            if fact and json.loads(fact[0])['sinks']==['raw']:
                fired.append((commit_id,fact[1]));raise RuntimeError('prepared-shared')
        monkeypatch.setattr(commits,'fault_point',fault)
        old,new=tmp_path/'old-mount',tmp_path/'new-mount'
        with pytest.raises(Exception,match='prepared-shared'):_worker(channel,old,tmp_path/'first')
        original=json.loads(fired[0][1])['sinks']['raw']['result']
        old.rename(new)
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?',(p.generation,))
        monkeypatch.setattr(commits,'fault_point',lambda *_:None)
        store=_worker(channel,new,tmp_path/'second')
        assert calls['/file']==1 and channel.state('p')['queue_drained']
        with channel.database.connection() as db:
            raw=db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?',(fired[0][0],)).fetchone()[0]
        assert raw==fired[0][1]
        before=_facts(channel)
        for mode in ('artifacts','metadata'):
            row,=(data.read_queue_records(channel.config,pool='p',through=channel.snapshot('p'),results=True,verify=mode)
                  .map(ReadQuality,fn_kwargs={'store':store}).take_all())
            assert row['result']==original and row['consumed_bytes']==len(BODY)
            assert row['content_sha256']==hashlib.sha256(BODY).hexdigest()
            assert row['verification']['status']==('verified' if mode=='artifacts' else 'not_checked')
            current=store.resolve(ObjectRef(**original['raw_ref']),domain=original['storage_domain'])
            assert current.uri!=original['raw_ref']['uri'] and current.sha256==original['raw_ref']['sha256']
            # Ordinary content verification works, retaining its legacy alias
            # semantics. ReadQuality above is the descriptor-guarded read path.
            assert current.read(max_bytes=131072)==BODY
        assert _facts(channel)==before and calls['/file']==1
        assert not old.exists()


def _object(tmp_path,*,shared=True):
    store=LocalObjectStore(tmp_path/'store',read_domain='shared' if shared else 'host',domain_id='team' if shared else None)
    namespace=RawNamespace(store,'fixture');domain=namespace.open()
    value=b'content';digest=hashlib.sha256(value).hexdigest();path=namespace.target(digest)
    path.parent.mkdir();path.write_bytes(value)
    return store,namespace,domain,ObjectRef(path.as_uri(),digest)


@pytest.mark.parametrize('kind',['domain','marker','store','declared','extra','uri','traversal','oversize','missing_root'])
def test_bad_domain_binding_rejected_without_creating_files(tmp_path,kind):
    store,namespace,domain,ref=_object(tmp_path)
    bad=dict(domain);candidate=ref
    if kind=='domain':bad['domain_id']='0'*64
    elif kind=='marker':bad['marker_digest']='0'*64
    elif kind=='store':bad['store_id']='another'
    elif kind=='declared':bad['declared_domain']='other'
    elif kind=='extra':bad['surprise']=True
    elif kind=='uri':candidate=ObjectRef((tmp_path/ref.sha256).as_uri(),ref.sha256)
    elif kind=='traversal':bad['physical_root']=str(namespace.root/'..')
    elif kind=='oversize':bad['physical_root']='/'+'x'*16384
    elif kind=='missing_root':
        store=LocalObjectStore(tmp_path/'unavailable',read_domain='shared',domain_id='team')
    before={str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*')}
    with pytest.raises((ValueError,RecoveryContractError,OSError)):
        store.resolve(candidate,domain=bad)
    assert {str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*')}==before


def test_same_declared_name_without_original_marker_cannot_rebind(tmp_path):
    store,namespace,domain,ref=_object(tmp_path)
    other=LocalObjectStore(tmp_path/'other',read_domain='shared',domain_id='team')
    RawNamespace(other,'fixture').open()
    with pytest.raises(ValueError,match='original artifact domain'):other.resolve(ref,domain=domain)


@pytest.mark.parametrize('field',['uri','physical_root','store_id','declared_domain'])
def test_oversize_declarations_rejected_before_parsing_or_serialization(tmp_path,monkeypatch,field):
    store,namespace,domain,ref=_object(tmp_path)
    def forbidden(*args,**kwargs):raise AssertionError('Oversize input reached allocation/parsing')
    if field=='uri':
        monkeypatch.setattr('demiflow.objects._local_path',forbidden)
        reference={'uri':'file:///'+'x'*8192,'sha256':ref.sha256}
    else:
        monkeypatch.setattr('demiflow.execution.raw_objects.document',forbidden)
        domain={**domain,field:'/'+'x'*8192};reference=ref
        if field=='store_id':store.store_id=domain[field]
        elif field=='declared_domain':store.domain_id=domain[field]
    with pytest.raises(ValueError):store.resolve(reference,domain=domain)


@pytest.mark.parametrize('kind',['root','host'])
def test_host_domain_does_not_relocate_or_change_host(tmp_path,kind):
    store,namespace,domain,ref=_object(tmp_path,shared=False)
    if kind=='host':domain={**domain,'host_identity':'0'*64}
    else:
        moved=tmp_path/'new';store.directory.rename(moved);store=LocalObjectStore(moved)
    with pytest.raises(ValueError,match='cannot change host or physical root'):store.resolve(ref,domain=domain)


@pytest.mark.parametrize('kind',['corrupt','missing','target_symlink','parent_symlink','declared_symlink','marker_symlink'])
def test_actual_bounded_reader_guards_content_and_paths(tmp_path,kind):
    store,namespace,domain,ref=_object(tmp_path)
    current=store.resolve(ref,domain=domain)
    if kind=='corrupt':namespace.target(ref.sha256).write_bytes(b'broken')
    elif kind=='missing':namespace.target(ref.sha256).unlink()
    elif kind=='target_symlink':
        target=namespace.target(ref.sha256);outside=tmp_path/'outside';target.rename(outside);target.symlink_to(outside)
    elif kind=='parent_symlink':
        moved=tmp_path/'moved';store.directory.rename(moved);store.directory.symlink_to(moved,target_is_directory=True)
    elif kind=='declared_symlink':
        alias=tmp_path/'alias';alias.symlink_to(store.directory,target_is_directory=True)
        other=LocalObjectStore(alias,read_domain='shared',domain_id='team')
        with pytest.raises(ValueError,match='without aliases or symlinks'):other.resolve(ref,domain=domain)
        return
    elif kind=='marker_symlink':
        marker=namespace.base/'.domain.json';outside=tmp_path/'outside-marker';marker.rename(outside);marker.symlink_to(outside)
        with pytest.raises(OSError):store.resolve(ref,domain=domain)
        return
    with pytest.raises((ValueError,OSError)):
        read_local_object(current.uri,current.sha256,root=store.directory,max_bytes=1024)
