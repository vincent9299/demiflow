"""Explicit persistent HTTP service supervisor: start / status / stop.

A separate supervisor retains GPU/port locks across pipeline runs. Nodes using it
keep service=None. Only this supervisor's recorded process identity is stopped.
"""
import argparse
import hashlib
import fcntl
import asyncio
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
import uuid
from .http import ManagedHTTPService
from ..execution.artifacts import read, run_lock
from contextlib import suppress
from ..collect.sqlite_queue import process_identity


def _directory(root, name):
    if not re.fullmatch('[A-Za-z0-9_-]{1,100}',name):
        raise ValueError('service name must be a safe identifier')
    return Path(root).resolve()/'_demiflow/managed_services'/name


def _write(path,record):
    temporary = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('x') as stream:
            json.dump(record,stream,ensure_ascii=False,sort_keys=True)
            stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)


def _alive(record):
    return bool(record and record.get('host')==socket.gethostname() and record.get('manager_start')
                and process_identity(record['manager_pid'])==record['manager_start'])


def status_service(root,name,*,probe=False):
    path=_directory(root,name)/'state.json'
    # Read directly rather than an exists/read TOCTOU. Some shared filesystems
    # briefly expose ENOENT while replacing a published state record.
    for attempt in range(3):
        try:
            record=read(path)
            break
        except FileNotFoundError:
            if attempt==2:return {'name':name,'state':'absent','alive':False}
            time.sleep(.01)
    result={**record,'alive':_alive(record)}
    if probe:
        import urllib.request
        result['healthy']=False
        if result['alive'] and record['state']=='ready':
            try:
                opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(record['ready_url'],timeout=2) as response:
                    result['healthy']=response.status==200
            except (OSError,ValueError):
                pass
    return result


def start_service(root,name,configuration,*,reuse=False):
    directory=_directory(root,name)
    configuration={**configuration,'root':str(Path(root).resolve())}
    declaration=ManagedHTTPService(**configuration)  # Validate before any launch.
    fingerprint=hashlib.sha256(json.dumps(configuration,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    with run_lock(directory/'launcher'):
        previous=status_service(root,name)
        if previous['alive']:
            if not reuse or previous.get('configuration_sha256')!=fingerprint:
                raise RuntimeError('Service supervisor is already alive; configuration differs or reuse was not requested')
            if previous['state']=='ready': return previous
            # The ensure lock in a shared owner serializes startup/reuse.
            raise RuntimeError('Service supervisor is not ready; wait for its owner')
        launch_id=uuid.uuid4().hex
        config_path=directory/('launch-'+launch_id+'.json')
        descriptor=os.open(config_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'w') as stream:
            json.dump(configuration,stream)
            stream.flush();os.fsync(stream.fileno())
        log_path=directory/'supervisor.log'
        with log_path.open('ab') as output:
            process=subprocess.Popen([sys.executable,'-m','demiflow.services.manage','_serve','--root',str(Path(root).resolve()),
                '--name',name,'--config',str(config_path),'--launch-id',launch_id],
                stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        from urllib.parse import urlsplit
        url=urlsplit(declaration.base_url)
        record={'configuration_sha256':fingerprint,'name':name,'state':'starting','launch_id':launch_id,'host':socket.gethostname(),
                'manager_pid':process.pid,'manager_start':process_identity(process.pid),
                'base_url':declaration.base_url,'ready_url':f'{url.scheme}://{url.netloc}'+declaration.ready_path,
                'started_at':time.time()}
        _write(directory/'state.json',record)
    deadline=time.monotonic()+declaration.startup_timeout_s+5
    while time.monotonic()<deadline:
        record=status_service(root,name)
        if record.get('state')=='absent':
            time.sleep(.05); continue
        if record.get('launch_id')!=launch_id:
            raise RuntimeError('Service supervisor identity changed during startup')
        if record['state']=='ready' and record['alive']:
            return record
        if record['state']=='failed' or process.poll() is not None:
            raise RuntimeError('Managed service startup failed; see '+str(log_path))
        time.sleep(.05)
    _stop_service(root,name,timeout_s=declaration.shutdown_timeout_s+5)
    raise TimeoutError('Managed service supervisor startup timed out')


def stop_service(root,name,*,timeout_s=40):
    directory=_directory(root,name)
    directory.mkdir(parents=True,exist_ok=True)
    with (directory/'users.lock').open('a') as lease:
        try: fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Service is in use by a Dataset action; stop refused') from exc
        return _stop_service(root,name,timeout_s=timeout_s)


def _stop_service(root,name,*,timeout_s):
    with run_lock(_directory(root,name)/'launcher'):
        record=status_service(root,name)
        if not record['alive']:
            return record
        os.kill(record['manager_pid'],signal.SIGTERM)
        deadline=time.monotonic()+timeout_s
        while _alive(record) and time.monotonic()<deadline:
            time.sleep(.05)
        if _alive(record):
            raise TimeoutError('Supervisor did not finish cleanup; inspect its recorded child and log')
        return status_service(root,name)


async def _serve(root,name,config_path,launch_id):
    directory=_directory(root,name)
    stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        loop.add_signal_handler(sig,stop.set)
    # Parent atomically publishes the PID after Popen; never contend for the
    # launcher lock while stop_service holds it waiting for this process to exit.
    deadline=time.monotonic()+5
    while True:
        path=directory/'state.json'
        record=read(path) if path.exists() else {}
        if record.get('launch_id')==launch_id and record.get('manager_pid')==os.getpid():
            break
        if time.monotonic()>=deadline:
            raise RuntimeError('Supervisor launch identity differs')
        await asyncio.sleep(.01)
    owner=ManagedHTTPService(**read(config_path)).bind()
    try:
        with run_lock(directory/'supervisor'):
            ready=asyncio.create_task(owner.ensure_ready())
            stopped=asyncio.create_task(stop.wait())
            try:
                done,_=await asyncio.wait([ready,stopped],return_when=asyncio.FIRST_COMPLETED)
                if stopped in done:
                    return
                await ready
                record.update(state='ready',child_pid=owner._process.pid,ready_at=time.time())
                _write(directory/'state.json',record)
                while not stop.is_set():
                    if owner._process.poll() is not None:
                        raise RuntimeError('Managed HTTP child exited')
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop.wait(),.5)
            finally:
                stopped.cancel()
                if not ready.done():ready.cancel()
                await owner.aclose()
                await asyncio.gather(ready,stopped,return_exceptions=True)
    except BaseException as error:
        record.update(state='failed',error=type(error).__name__,finished_at=time.time())
        _write(directory/'state.json',record)
        raise
    finally:
        await owner.aclose()
        if record['state']!='failed':
            record.update(state='stopped',finished_at=time.time())
            _write(directory/'state.json',record)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['start','status','stop','_serve'])
    parser.add_argument('--root',required=True)
    parser.add_argument('--name',required=True)
    parser.add_argument('--config')
    parser.add_argument('--launch-id')
    args=parser.parse_args()
    if args.action in {'start','_serve'} and not args.config:
        parser.error('start requires --config JSON')
    if args.action=='_serve':
        asyncio.run(_serve(args.root,args.name,args.config,args.launch_id))
        return
    result=(start_service(args.root,args.name,read(args.config)) if args.action=='start' else
            stop_service(args.root,args.name) if args.action=='stop' else status_service(args.root,args.name,probe=True))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
