"""Actual native reader processes, fixed Lance versions and failure cleanup.

Malformed-wire cases use explicitly identified real subprocess fixtures; those
are transport/cleanup evidence, not native-data or product permission proof.
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time

import lance
import pyarrow as pa
import pytest

from demiflow.execution.process_observation import _signal_fd
from demiflow.workbench.process import process_identity
from demiflow.forge.schema import ForgeError
from demiflow.workbench.reader import Reader
from demiflow.workbench.reader import using
from demiflow.workbench.reader import current
from demiflow.workbench.reader import RESPONSE_BYTES
from demiflow.workbench.worker import operation

@pytest.fixture
def table(tmp_path):
    path=tmp_path/'fixed.lance'
    lance.write_dataset(pa.table({'value':[1,2,3,4]}),str(path))
    return path

def request(path,version=1):return dict(operation='describe',root=str(path),arguments=dict(version=version))
def rows(value):
    assert value['ok'],value
    return value['value']['row_count']

def test_same_process_rechecks_versions_roots_and_conflict_without_cached_data(table,tmp_path):
    reader=Reader()
    try:
        assert rows(reader.call(request(table)))==4
        original=reader.identity
        lance.write_dataset(pa.table({'value':[5]}),str(table),mode='append')
        assert rows(reader.call(request(table,2)))==5
        assert rows(reader.call(request(table,1)))==4
        other=tmp_path/'other.lance';lance.write_dataset(pa.table({'value':[9,10]}),str(other))
        assert rows(reader.call(request(other)))==2 and reader.identity==original
        failed=reader.call(request(table,999))
        assert not failed['ok'] and reader.process is None
        assert process_identity(original['pid'])!=original
        assert rows(reader.call(request(table,2)))==5 and reader.identity!=original
    finally:reader.close()


def test_resident_reader_does_not_inherit_a_control_calls_expired_deadline(table,monkeypatch):
    from demiflow.forge import operation_budget as budget
    monkeypatch.setenv(budget.SCOPE_ENV,'oneshot-v1')
    monkeypatch.setenv(budget.DEADLINE_ENV,'1')
    reader=Reader()
    try:
        with budget.scope(deadline=time.monotonic()+.1):
            assert rows(reader.call(request(table)))==4
            original=reader.identity
            with pytest.raises(ForgeError,match='deadline exceeded'):
                budget.check()
        assert budget.current_deadline() is None
        assert rows(reader.call(request(table)))==4
        assert reader.identity==original
        assert os.environ[budget.DEADLINE_ENV]=='1'
    finally:reader.close()


def test_unknown_retirement_cannot_reuse_old_child_before_proved_exit(table,monkeypatch):
    reader=Reader()
    try:
        assert rows(reader.call(request(table)))==4;original=reader.identity
        actual=reader._retire
        def unknown():
            reader.retirement_pending=True
            raise ForgeError('Injected original retirement still unknown')
        with monkeypatch.context() as patch:
            patch.setattr(reader,'_retire',unknown)
            with pytest.raises(ForgeError):reader.call(request(table),consumer_closed=lambda:True)
            assert reader.identity==original and process_identity(original['pid'])==original
            with pytest.raises(ForgeError):reader.call(request(table))
            assert reader.identity==original and reader.requests==1
        assert rows(reader.call(request(table)))==4
        assert process_identity(original['pid'])!=original and reader.identity!=original
        assert not reader.retirement_pending
    finally:reader.close()

def test_64_requests_retire_original_and_next_explicit_read_starts_new_process_without_fd_growth(table):
    before=len(os.listdir('/proc/self/fd'));reader=Reader()
    try:
        assert rows(reader.call(request(table)))==4;original=reader.identity
        child_fds=len(os.listdir('/proc/'+str(original['pid'])+'/fd'))
        for index in range(63):
            assert rows(reader.call(request(table)))==4
            if index<62:
                assert reader.identity==original
                assert len(os.listdir('/proc/'+str(original['pid'])+'/fd'))<=child_fds+2
        assert reader.process is None and process_identity(original['pid'])!=original
        assert rows(reader.call(request(table)))==4 and reader.identity!=original
    finally:reader.close()
    assert len(os.listdir('/proc/self/fd'))<=before+1

def test_idle_timeout_and_next_explicit_read_recovery(table):
    reader=Reader(idle_seconds=.01)
    try:
        with pytest.raises(ForgeError) as error:reader.call(request(table),timeout=.01)
        assert error.value.code=='resource_limit' and reader.process is None
        assert rows(reader.call(request(table)))==4;original=reader.identity
        reader.maintain(now=reader.last_used+.02)
        assert reader.process is None and process_identity(original['pid'])!=original
        assert rows(reader.call(request(table)))==4
    finally:reader.close()


@pytest.mark.parametrize('kind',['bytes','entries','link'])
def test_owned_scratch_bounds_refuse_without_following_links_and_next_read_recovers(table,kind):
    reader=Reader()
    try:
        assert rows(reader.call(request(table)))==4;original=reader.identity
        scratch=Path(reader.scratch.name)
        if kind=='bytes':
            with (scratch/'too-wide').open('wb') as stream:stream.truncate(16*1024**2+1)
        elif kind=='entries':
            for index in range(257):(scratch/str(index)).touch()
        else:(scratch/'foreign-data').symlink_to(table,target_is_directory=True)
        with pytest.raises(ForgeError) as error:reader.call(request(table))
        assert error.value.code=='resource_limit' and reader.process is None
        assert process_identity(original['pid'])!=original and table.is_dir()
        assert rows(reader.call(request(table)))==4
    finally:reader.close()

def test_native_import_exceeding_the_explicit_rss_reservation_is_retired(table):
    reader=Reader(rss_bytes=32*1024**2)
    try:
        with pytest.raises(ForgeError) as error:reader.call(request(table))
        assert error.value.code=='resource_limit' and reader.process is None
    finally:reader.close()

@pytest.mark.parametrize('stop_service',[False,True])
def test_real_stopped_child_cancelled_by_socket_consumer_or_concurrent_close(table,stop_service):
    from demiflow.workbench.api import Handler
    reader=Reader();local,remote=socket.socketpair();local.settimeout(65)
    target=type('Consumer',(),dict(connection=local))();errors=[]
    try:
        assert rows(reader.call(request(table)))==4;original=reader.identity
        _signal_fd(reader.pidfd,signal.SIGSTOP)
        def run():
            try:reader.call(request(table),consumer_closed=lambda:Handler._consumer_closed(target))
            except ForgeError as error:errors.append(error.code)
        thread=threading.Thread(target=run);thread.start()
        deadline=time.monotonic()+2
        while not reader.lock.locked() and time.monotonic()<deadline:time.sleep(.005)
        assert reader.lock.locked()
        if stop_service:reader.close()
        else:remote.close()
        thread.join(5);assert not thread.is_alive() and errors==['operation_rejected']
        assert reader.process is None and process_identity(original['pid'])!=original
        if not stop_service:assert rows(reader.call(request(table)))==4
    finally:reader.close();local.close();remote.close()

def test_context_restores_and_inherited_output_fd_retires_reader_before_original_single_call(table,tmp_path,monkeypatch):
    from demiflow.forge import platform_worker
    reader=Reader();original_capture=platform_worker.capture;calls=[]
    try:
        with using(reader):
            assert operation('describe',table,version=1)['row_count']==4
            original=reader.identity
            def capture(*args,**kwargs):
                assert reader.process is None and process_identity(original['pid'])!=original
                calls.append(kwargs['pass_fds'])
                return original_capture(*args,**kwargs)
            monkeypatch.setattr(platform_worker,'_capture',capture)
            with (tmp_path/'owned-output').open('wb') as output:
                assert operation('describe',table,version=1,_pass_fds=(output.fileno(),))['row_count']==4
            assert len(calls)==1
            assert current()[0] is reader
            with using(None):assert current() is None
            assert current()[0] is reader
        assert current() is None
    finally:reader.close()

@pytest.mark.parametrize('mode',['oversize','json','sequence','stderr','exit'])
def test_bad_real_process_wire_retires_then_next_explicit_native_request_recovers(table,tmp_path,monkeypatch,mode):
    fixture=tmp_path/'wire-fixture.py'
    fixture.write_text('import os,struct,json,time\n'
        'size=struct.unpack("!I",os.read(0,4))[0]\n'
        'raw=b""\nwhile len(raw)<size:raw+=os.read(0,size-len(raw))\n'
        'request=json.loads(raw)\n'
        +(f'os.write(1,struct.pack("!I",{RESPONSE_BYTES+1}))\ntime.sleep(5)\n' if mode=='oversize' else
          'os.write(1,struct.pack("!I",1)+b"{")\ntime.sleep(5)\n' if mode=='json' else
          'raw=json.dumps({"sequence":99,"response":{}}).encode()\nos.write(1,struct.pack("!I",len(raw))+raw)\ntime.sleep(5)\n' if mode=='sequence' else
          'for i in range(18):os.write(2,b"x"*65536)\ntime.sleep(5)\n' if mode=='stderr' else ''))
    actual_popen=subprocess.Popen;started=[];reader=Reader()
    def launch(argv,**kwargs):
        if argv[-1]=='--reader':argv=[argv[0],'-I','-B',str(fixture)]
        process=actual_popen(argv,**kwargs);started.append(process);return process
    try:
        with monkeypatch.context() as patch:
            patch.setattr(subprocess,'Popen',launch)
            with pytest.raises(ForgeError):reader.call(request(table))
        assert reader.process is None and all(process.poll() is not None for process in started)
        assert rows(reader.call(request(table)))==4
    finally:reader.close()
