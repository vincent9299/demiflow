"""Real Controller.serve + WatchdogService + current Unix authority composition.

Injected controller blocking IO and synthetic cgroups/OCI; this is not B1.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

import pytest

from test_forge_analysis_controller_loop import Web, ROOT
from demiflow.workbench.analysis.ipc import Channel


def line(process, seconds=5):
    assert select.select([process.stdout], [], [], seconds)[0], 'fixture did not produce bounded readiness'
    value=process.stdout.readline().decode().strip()
    assert value, process.stderr.read().decode() if process.poll() is not None else 'fixture pipe closed'
    return value


@contextmanager
def composition(tmp_path,mode='controller'):
    web=Web(tmp_path)
    script=Path(__file__).with_name('_forge_analysis_combined_fault.py')
    env=dict(os.environ,PYTHONPATH=str(ROOT)+os.pathsep+str(ROOT/'tests'),PYTHONDONTWRITEBYTECODE='1')
    controller=subprocess.Popen([sys.executable,'-B','-u',str(script),mode,str(tmp_path),str(web.path),str(os.getpid())],
        cwd=ROOT,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    read,write=os.pipe2(os.O_NONBLOCK|os.O_CLOEXEC)
    watchdog=subprocess.Popen([sys.executable,'-B','-u',str(script),'watchdog',str(tmp_path),str(web.path),str(controller.pid),str(write)],
        cwd=ROOT,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,pass_fds=(write,))
    os.close(write)
    try:
        assert line(watchdog)=='ready'
        controller.stdin.write((str(watchdog.pid)+'\n').encode());controller.stdin.flush()
        assert json.loads(line(controller))==dict(ready=True)
        assert web.call('status')['active']
        yield web,controller,watchdog,read
    finally:
        # Unblock only this fixture's pipe; gracefully close the real loops.
        if controller.poll() is None:
            controller.stdin.write(b'x');controller.stdin.flush()
            controller.send_signal(signal.SIGTERM)
        if watchdog.poll() is None:watchdog.send_signal(signal.SIGTERM)
        for process in (controller,watchdog):
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
        web.close();os.close(read)
        for process in (controller,watchdog):
            error=process.stderr.read().decode()
            for stream in (process.stdin,process.stdout,process.stderr):
                if stream is not None:stream.close()
            assert process.returncode==0,error


def stop(web,path,payload, *, operation='stop-generation', project='w'+'1'*32):
    connection=__import__('socket').socket(__import__('socket').AF_UNIX,__import__('socket').SOCK_STREAM)
    connection.connect(str(path));channel=Channel(connection)
    channel.queue('json',dict(version=1,project=project,operation=operation,payload=payload))
    deadline=time.monotonic()+5
    try:
        while time.monotonic()<deadline:
            channel.flush();web.step()
            reply=channel.receive()
            if reply is not None:return reply[1]
            time.sleep(.005)
        raise AssertionError('independent stop timed out')
    finally:channel.close()


def test_priority_stop_progresses_while_real_controller_loop_is_blocked(tmp_path,record_property):
    with composition(tmp_path) as (web,controller,watchdog,events):
        controller.send_signal(signal.SIGUSR1)
        assert line(controller)=='persistent-io-blocked'
        before=(tmp_path/'state/runtime/priority-owner.json').read_bytes()
        started=time.monotonic()
        value=stop(web,tmp_path/'p.sock',dict(request_id='stop',session_id='session',generation='generation'))
        elapsed=time.monotonic()-started
        assert value['accepted'] and value['force'] and not value['cleanup_proven']
        assert select.select([events],[],[],1)[0] and os.read(events,1)==b'K'
        assert controller.poll() is None and watchdog.poll() is None
        assert (tmp_path/'state/runtime/priority-owner.json').read_bytes()==before
        lease=json.loads((tmp_path/'state/runtime'/('j'+'a'*32)/'lease.json').read_bytes())
        assert lease['state']=='started'  # no cleanup or capacity release invented
        assert elapsed < 3
        # A late stop for another generation cannot affect the original owner.
        stale=stop(web,tmp_path/'p.sock',dict(request_id='late',session_id='session',generation='old-generation'))
        assert stale['code']=='conflict'
        record_property('independent_stop_ack_seconds',elapsed)
        record_property('layers','real Controller/Watchdog/SQLite/Unix authority; blocked pipe injection; fake cgroups/OCI')


def test_controller_parent_exit_is_observed_without_waiting_for_disk(tmp_path):
    with composition(tmp_path) as (web,controller,watchdog,events):
        controller.send_signal(signal.SIGTERM)
        controller.wait(timeout=5)
        assert select.select([events],[],[],2)[0] and os.read(events,1)==b'K'
        assert watchdog.poll() is None
        assert (tmp_path/'state/runtime'/('j'+'a'*32)/'lease.json').exists()


def test_current_revocation_over_real_authority_socket_refuses_stop(tmp_path,monkeypatch):
    with composition(tmp_path) as (web,controller,watchdog,events):
        controller.send_signal(signal.SIGUSR1)
        assert line(controller)=='persistent-io-blocked'
        original_queue=Channel.queue
        def revoked(channel,kind,value=None):
            # Fault at the trusted Web permission decision, transported by the
            # real challenge-bound Unix exchange to the separate watchdog.
            if kind=='json' and isinstance(value,dict) and value.get('value')==dict(allowed=True):
                value={**value,'value':dict(allowed=False)}
            return original_queue(channel,kind,value)
        monkeypatch.setattr(Channel,'queue',revoked)
        value=stop(web,tmp_path/'p.sock',dict(request_id='revoked-stop',session_id='session',generation='generation'))
        assert value['code']=='operation_rejected'
        assert not select.select([events],[],[],.1)[0]
        assert controller.poll() is None and watchdog.poll() is None


@pytest.mark.parametrize('mode',['controller-media','controller-both'])
def test_independent_media_stop_without_kernel_or_alongside_other_project(tmp_path,record_property,mode):
    with composition(tmp_path,mode) as (web,controller,watchdog,events):
        controller.send_signal(signal.SIGUSR1)
        assert line(controller)=='persistent-io-blocked'
        started=time.monotonic()
        value=stop(web,tmp_path/'p.sock',dict(request_id='stop-read',read_request_id='read-one'),
                   operation='stop-read',project='w'+'2'*32)
        elapsed=time.monotonic()-started
        assert value['format']=='demiflow_analysis_priority_read_stop_v1' and value['accepted'] and not value['cleanup_proven']
        assert select.select([events],[],[],1)[0] and os.read(events,64)==b'M'
        assert not select.select([events],[],[],.1)[0]  # concurrent generation remains live
        lease=json.loads((tmp_path/'state/runtime'/('j'+'b'*32)/'lease.json').read_bytes())
        assert lease['state']=='started' and lease['owner_kind']=='media-read'
        record_property('read_stop_ack_seconds',elapsed)
        assert elapsed<3


def test_watchdog_current_read_revocation_kills_only_media_while_controller_blocked(tmp_path,monkeypatch,record_property):
    with composition(tmp_path,'controller-both') as (web,controller,watchdog,events):
        controller.send_signal(signal.SIGUSR1);assert line(controller)=='persistent-io-blocked'
        original_queue=Channel.queue
        def revoked(channel,kind,value=None):
            if kind=='json' and isinstance(value,dict) and value.get('value')==dict(allowed=True):
                value={**value,'value':dict(allowed=False)}
            return original_queue(channel,kind,value)
        monkeypatch.setattr(Channel,'queue',revoked)
        start=time.monotonic();seen=b''
        while time.monotonic()-start<2.5:
            web.step()
            if select.select([events],[],[],.01)[0]:seen+=os.read(events,64);break
        assert seen==b'M'
        assert not select.select([events],[],[],.1)[0]
        record_property('read_revocation_to_synthetic_kill_seconds',time.monotonic()-start)
        assert controller.poll() is None and watchdog.poll() is None
