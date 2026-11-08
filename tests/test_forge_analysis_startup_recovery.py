"""Real SQLite recovery coordination with an explicit delayed runtime double.

This covers bounded startup selection, later cleanup and no replay. It does
not certify a process exit, OCI watch, installed package or real helper restart.
"""
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import controller as module
from demiflow.workbench.analysis.controller import Controller
from demiflow.workbench.analysis.metadata import Metadata
from demiflow.workbench.analysis.sources import Sources
from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.operations import Operations
from demiflow.workbench.analysis.output_reads import OutputReads
from demiflow.workbench.analysis.media_captures import MediaCaptures
from demiflow.workbench.analysis.execution_backend import cleanup_receipt
from demiflow.forge.schema import ForgeError
from test_forge_analysis_presentations import store


def recovery_controller(records,access,root,count=1):
    control=Controller.__new__(Controller)
    control.database=records.database
    # The in-memory fixture has no files/rlimit guard. Its actual schema,
    # transactions, original documents and cleanup records remain real SQLite.
    control.database.guard=SimpleNamespace(check=lambda:None)
    control.records=records;control.sources=Sources(records,root)
    control.artifacts=Artifacts(records);control.operations=Operations(records)
    control.deployment=SimpleNamespace(paths=SimpleNamespace(state=root))
    control.metadata=Metadata(records,control.sources)
    control.metadata.execution_available=lambda:control._recovery is None
    control.sessions={};control._recovery=None
    class Runtime:
        mode='oci'
        def __init__(self):
            self.root=root;self.exited=False;self.cleaned=[];self.stops=[]
            self.leases={'j'+f'{index:032x}':dict(job_id='j'+f'{index:032x}',role='kernel',project=access.project,
                execution_id='session',state='started' if index==0 else 'cleanup_complete',owner_digest='a'*64,origin_digest='b'*64)
                for index in range(count)}
        def stop(self,identifier,**kwargs):self.stops.append(identifier)
        def cleanup(self,identifier,**kwargs):
            self.cleaned.append(identifier)
            if self.exited:self.leases[identifier]['state']='cleanup_complete'
            receipt=cleanup_receipt(job_id=identifier,mode=self.mode,state='complete' if self.exited else 'pending',
                processes=self.exited,descendants=self.exited,channels=self.exited,temporary=self.exited,reason='explicit_runtime_double')
            return dict(cleanup_proven=self.exited,cleanup_complete=self.exited,cleanup_receipt=receipt)
        def create(self,**kwargs):raise AssertionError('Startup must never create a job')
        def start(self,*args,**kwargs):raise AssertionError('Startup must never start a job')
    control.runtime=Runtime();control.output_reads=OutputReads(control);control.media_captures=MediaCaptures(control)
    return control


def test_delayed_original_cleanup_is_revisited_without_replay_or_history_rewrite(tmp_path,monkeypatch):
    with store() as (records,access,witness,raw,source,descriptor):
        control=recovery_controller(records,access,tmp_path)
        db=records.database.connection
        ready=db.execute("SELECT manifest FROM analysis_objects WHERE state='ready'").fetchone()[0]
        clock=[100.];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
        control._recover()
        assert control._recovery is not None and control.sessions=={}
        assert db.execute('SELECT state FROM analysis_sessions').fetchone()[0]=='cleanup_pending'
        authority=SimpleNamespace(access=lambda:access)
        with pytest.raises(ForgeError) as failure:
            control.metadata.route(authority,'session-start',dict(request_id='not-replayed',notebook_id='book',revision=1,sources={}))
        assert failure.value.code=='busy'
        count=len(control.runtime.cleaned);control._tick_recovery()
        assert len(control.runtime.cleaned)==count # bounded backoff, no busy loop
        control.runtime.exited=True;clock[0]+=1
        control._tick_recovery()
        assert control._recovery is None and control.sessions=={}
        assert db.execute('SELECT state FROM analysis_sessions').fetchone()[0]=='stopped'
        assert db.execute('SELECT count(*) FROM analysis_executions').fetchone()[0]==1
        assert db.execute("SELECT manifest FROM analysis_objects WHERE state='ready'").fetchone()[0]==ready


def test_startup_cleanup_is_fair_bounded_and_does_not_rescan_capture_history(tmp_path,monkeypatch):
    with store() as (records,access,*_) :
        control=recovery_controller(records,access,tmp_path,count=10);control.runtime.exited=True
        calls=[];original=control.media_captures.pending_ids
        control.media_captures.pending_ids=lambda:(calls.append('startup') or original())
        clock=[100.];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
        control._recover();assert len(control.runtime.cleaned)==4
        for _ in range(3):clock[0]+=1;control._tick_recovery()
        assert control._recovery is None and len(control.runtime.cleaned)==10 and len(set(control.runtime.cleaned))==10
        assert calls==['startup']


def test_changed_original_lease_is_not_cleaned_or_freed(tmp_path,monkeypatch):
    with store() as (records,access,*_):
        control=recovery_controller(records,access,tmp_path)
        clock=[100.];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
        control._recover();identifier=next(iter(control.runtime.leases));count=len(control.runtime.cleaned)
        control.runtime.leases[identifier]['origin_digest']='c'*64
        control.runtime.exited=True;clock[0]+=1;control._tick_recovery()
        assert control._recovery is not None and len(control.runtime.cleaned)==count
        assert records.database.connection.execute('SELECT state FROM analysis_sessions').fetchone()[0]=='cleanup_pending'


def test_priority_owner_unlink_then_fsync_failure_retries_only_original_directory(tmp_path,monkeypatch):
    control=Controller.__new__(Controller);calls=[]
    control.runtime=SimpleNamespace(root=tmp_path,owner=lambda *args:(calls.append('clear') or dict(bound=False)))
    path=tmp_path/module.PRIORITY_OWNER;path.write_text('{}')
    monkeypatch.setattr(module,'load_priority_owner',lambda root:{})
    real_sync=module.os.fsync;syncs=[]
    def sync(descriptor):
        real_sync(descriptor);syncs.append(descriptor)
        if len(syncs)==1:raise OSError('injected original directory fsync failure')
    monkeypatch.setattr(module.os,'fsync',sync)
    original=dict(priority_unlinked=False);digest=module.c.digest({})
    with pytest.raises(OSError):control._clear_priority_owner(digest,original)
    assert original['priority_unlinked'] and not path.exists()
    control._clear_priority_owner(digest,original)
    assert len(syncs)==2 and calls==['clear']
