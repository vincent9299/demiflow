"""Shared read orchestration over real Channel/SQLite, explicit runtime doubles.

Only queued-frame, cancellation and cleanup/return ordering are exercised.
Runtime/Watchdog peer/cgroup/OCI and external B1 have separate acceptance.
"""
from contextlib import contextmanager
import socket
import time
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis import output_reads
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.framing import Frames
from demiflow.forge.schema import ForgeError
from test_forge_analysis_presentations import store


@contextmanager
def reading(monkeypatch):
    with store() as (records,access,witness,raw,source,descriptor):
        local,remote=socket.socketpair();channel=Channel(local);log=[]
        connection=dict(channel=channel,phase='header',close_after_write=False)
        authority=SimpleNamespace(project=access.project,principal=access.principal,peer=SimpleNamespace(live=lambda:True),
            deployment=None,access=lambda:access)
        identifier='job-owned-read'
        class Runtime:
            mode='oci'
            def __init__(self):self.leases={identifier:dict(state='running',cgroup=dict(cgroup_identity={}),execution_id='read')}
            def group(self,lease):pass
            groups=SimpleNamespace(populated=lambda *args:False)
            def observe(self,key):return dict(state='exited')
            def read_owner(self,name,digest,**kwargs):log.append(name);return dict(bound=True,stop_requested=False)
            def stop(self,key,**kwargs):log.append('stop-job')
            def retire_read(self,key):
                assert self.leases[key]['state']=='cleanup_complete'
                log.append('retire');del self.leases[key]
        class Watchdog:
            def read_owner(self,name,digest,**kwargs):log.append(name);return dict(bound=True,stop_requested=False)
        class Controller:
            def __init__(self):self.records=records;self.runtime=Runtime();self.watchdog=Watchdog()
            def clean_job(self,key):log.append('cleanup');self.runtime.leases[key]['state']='cleanup_complete'
            def reply(self,connection,value):log.append('reply');connection['channel'].queue('json',value);connection['close_after_write']=True
        controller=Controller();reads=output_reads.OutputReads(controller)
        reads.admit(connection,authority,'output',dict(offset=0,limit=8),witness)
        owned=reads.current;owned.update(job_id=identifier,digest='a'*64,owner=dict(deadline=time.monotonic()+5))
        worker_local,worker_remote=socket.socketpair();worker_channel=Channel(worker_local)
        worker_remote.settimeout(1)
        worker=dict(owner=owned,job_id=identifier,kind='output-read',channel=worker_channel,reply_reader=Frames(worker_remote))
        page=output_reader.project_document(raw,witness)
        class Task:
            def __init__(self,deployment,peer,project,metadata,name,payload,**kwargs):self.metadata=metadata;self.value=None
            def step(self):log.append('current-metadata');self.value=self.metadata.route(authority,'output-return',{});return True
            def close(self):pass
        monkeypatch.setattr(output_reads,'Task',Task)
        def clear_owner(digest,value=None):log.append('clear-owner')
        monkeypatch.setattr(reads,'_clear_owner',clear_owner)
        try:yield reads,controller,connection,worker,page,witness,log
        finally:channel.close();remote.close();worker_channel.close();worker_remote.close()


def test_exited_worker_does_not_lose_queued_end_before_cleanup_or_readopt_generation(monkeypatch):
    with reading(monkeypatch) as (reads,controller,connection,worker,page,witness,log):
        # cgroup population is already false, but the complete socket end has
        # not yet been consumed. No bridge cleanup can race ahead of it.
        reads.frame(worker,'json',dict(operation='output-read',artifact=witness['artifact'],value=page))
        reads.tick();assert 'cleanup' not in log and reads.current is not None
        reads.frame(worker,'end',None)
        for _ in range(2):worker['channel'].flush()
        assert worker['reply_reader'].json()==dict(format='demiflow_analysis_read_end_v1',accepted=True)
        reads.tick()
        assert reads.current is None and not controller.runtime.leases
        assert log.index('cleanup')<log.index('retire')<log.index('clear-owner')<log.index('current-metadata')<log.index('reply')
        assert controller.records.database.connection.execute('SELECT generation FROM analysis_sessions').fetchone()[0]=='new-live-generation'


def test_owner_clear_failure_after_retirement_holds_return_without_accessing_retired_lease(monkeypatch):
    with reading(monkeypatch) as (reads,controller,connection,worker,page,witness,log):
        reads.frame(worker,'json',dict(operation='output-read',artifact=witness['artifact'],value=page));reads.frame(worker,'end',None)
        attempts=[]
        def clear(digest,value=None):
            attempts.append(digest)
            if len(attempts)==1:raise OSError('injected parent fsync failure')
            log.append('clear-owner')
        monkeypatch.setattr(reads,'_clear_owner',clear)
        reads.tick()
        assert reads.current is not None and reads.current['retired'] and 'reply' not in log
        assert not controller.runtime.leases and 'current-metadata' not in log
        reads.tick();assert reads.current is None and attempts==['a'*64,'a'*64]
        assert log.count('retire')==1 and 'current-metadata' not in log
        assert connection['close_after_write'] # failure returned, never the page


def test_incomplete_read_does_not_acknowledge_end_or_release_owner(monkeypatch):
    with reading(monkeypatch) as (reads,controller,connection,worker,page,witness,log):
        with pytest.raises(ForgeError,match='without its confirmed'):
            reads.frame(worker,'end',None)
        assert not reads.current['ended'] and not worker['channel'].outgoing
        assert 'cleanup' not in log and 'reply' not in log


def test_consumer_cancel_fences_owned_job_and_cleans_before_releasing_slot(monkeypatch):
    with reading(monkeypatch) as (reads,controller,connection,worker,page,witness,log):
        reads.disconnected(connection)
        assert reads.current['stopped'] and log.index('stop-read-owner')<log.index('stop-job')
        with pytest.raises(ForgeError):reads.admit(connection,reads.current['authority'],'output',{},witness)
        reads.tick();assert reads.current is None
        assert log.index('stop-job')<log.index('cleanup')<log.index('retire')<log.index('reply')
        assert 'current-metadata' not in log


def test_original_output_range_accepts_integer_http_status_and_retires_before_bytes(monkeypatch):
    with reading(monkeypatch) as (reads,controller,connection,worker,page,witness,log):
        owned=reads.current
        owned.update(operation='output-original',config=dict(version=1,operation='output-original',witness=witness,range='bytes=0-0'))
        metadata=dict(status=206,mime='application/json',start=0,end=0,total=witness['artifact']['bytes'],attachment=True,sha256=witness['artifact']['sha256'])
        reads.frame(worker,'json',dict(operation='output-read',artifact=witness['artifact'],metadata=metadata))
        reads.frame(worker,'blob',b'{');reads.frame(worker,'end',None);reads.tick()
        assert reads.current is None and not controller.runtime.leases
        assert log.index('cleanup')<log.index('retire')<log.index('current-metadata')
        assert connection['close_after_write']
