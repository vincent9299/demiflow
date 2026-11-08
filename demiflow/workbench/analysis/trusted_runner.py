"""One bounded local subreaper per job; not a malicious-code containment unit."""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import resource
import select
import signal
import subprocess
import time

from demiflow.workbench.process import process_identity
from demiflow.execution.process_observation import descendants
from demiflow.execution.process_observation import signal_owned
from demiflow.execution.process_observation import drain_owned
from demiflow.forge.io import atomic_write_json
from demiflow.workbench.analysis import contracts as c


def scratch_sample(context):
    total=entries=0
    pending=[(Path(context.path('/scratch')),0),(Path(context.path('/work')),0)]
    while pending:
        directory,depth=pending.pop()
        if depth>10:raise RuntimeError('Sampled scratch depth exceeded')
        with os.scandir(directory) as scan:
            for item in scan:
                entries+=1
                if entries>8192 or item.is_symlink():raise RuntimeError('Sampled scratch entry/link policy exceeded')
                info=item.stat(follow_symlinks=False)
                if item.is_dir(follow_symlinks=False):pending.append((Path(item.path),depth+1))
                else:total+=max(info.st_size,info.st_blocks*512)
                if total>context.value['limits']['scratch_bytes']:return total
    return total


def run(context,path,digest,control_fd):
    value=context.value;owned=Path(path).parent.parent;unit=value['unit_fd']
    if unit<3:raise ValueError('Original local startup unit descriptor required')
    def empty_exit(reason):
        atomic_write_json(owned/'status.json',dict(job_id=value['job_id'],context_sha256=digest,
            wrapper=process_identity(os.getpid()),worker=None,state='exited',reason=reason,
            peak_sampled_rss_bytes=0,descendants_exited=True,returncode=None))
        os.close(control_fd);os.close(unit)
        return 0
    libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(36,1,0,0,0)!=0:return empty_exit('subreaper_unavailable')
    original=value['controller'];parent=process_identity(os.getppid())
    if parent!=original:return empty_exit('controller_lost_before_start')
    # Do not SIGKILL this wrapper on parent loss: it must drain its children.
    stopping=[False]
    signal.signal(signal.SIGTERM,lambda *_:stopping.__setitem__(0,True))
    signal.signal(signal.SIGINT,lambda *_:stopping.__setitem__(0,True))
    atomic_write_json(owned/'status.json',dict(job_id=value['job_id'],context_sha256=digest,
        wrapper=process_identity(os.getpid()),worker=None,state='running',reason='wrapper_ready',
        peak_sampled_rss_bytes=0,descendants_exited=False))
    startup_deadline=min(value['deadline'],time.monotonic()+10)
    while not select.select([control_fd],[],[],.05)[0]:
        if stopping[0] or time.monotonic()>=startup_deadline:return empty_exit('startup_not_released')
    if os.read(control_fd,1)!=b'S':return empty_exit('startup_not_released')
    if stopping[0] or select.select([control_fd],[],[],0)[0]:return empty_exit('stop_requested_before_worker')
    soft,hard=value['original_fsize']
    current_soft,current_hard=resource.getrlimit(resource.RLIMIT_FSIZE)
    if current_hard!=hard:return empty_exit('original_account_file_limit_not_preserved')
    resource.setrlimit(resource.RLIMIT_FSIZE,(soft,hard))
    entry=Path(value['code_root'])/'demiflow/workbench/analysis/trusted_entry.py'
    argv=[value['python'],'-I','-B',str(entry),'--code-root',value['code_root'],'--operation','worker',
        '--context',str(path),'--context-sha256',digest]
    env=dict(os.environ,TMPDIR=context.path('/scratch'),HOME=context.path('/work'),
        DEMIFLOW_EXECUTION_UNIT_FD=str(unit),PYTHONDONTWRITEBYTECODE='1')
    process=None;reason=None;complete=False;worker=None;peak=0;diagnostic=bytearray()
    release_reader=release_writer=None
    wrapper=process_identity(os.getpid())
    def status(state,**facts):
        atomic_write_json(owned/'status.json',dict(job_id=value['job_id'],context_sha256=digest,
            wrapper=wrapper,worker=worker,state=state,reason=reason,peak_sampled_rss_bytes=peak,**facts))
    def drain_diagnostic():
        if process is not None and process.stderr is not None:
            try:data=os.read(process.stderr.fileno(),4096)
            except BlockingIOError:return
            diagnostic.extend(data[:max(0,4096-len(diagnostic))])
    try:
        release_reader,release_writer=os.pipe2(os.O_CLOEXEC)
        argv+=['--release-fd',str(release_reader)]
        process=subprocess.Popen(argv,cwd=context.path('/work'),env=env,stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,close_fds=True,pass_fds=(unit,release_reader))
        os.close(release_reader);release_reader=None;os.set_blocking(process.stderr.fileno(),False)
        worker=process_identity(process.pid)
        status('running',descendants_exited=False)
        if stopping[0] or select.select([control_fd],[],[],0)[0]:
            reason='stop_requested_before_worker'
        else:os.write(release_writer,b'S')
        os.close(release_writer);release_writer=None
        last_sample=0
        while reason is None and process.poll() is None:
            drain_diagnostic()
            if stopping[0]:reason='stop_requested';break
            if time.monotonic()>=value['deadline']:reason='deadline';break
            if process_identity(original['pid'])!=original:reason='controller_lost';break
            if select.select([control_fd],[],[],.05)[0]:
                if os.read(control_fd,1)!=b'':reason='stop_requested'
                else:reason='controller_pipe_closed'
                break
            if time.monotonic()-last_sample>=.5:
                sample=descendants(os.getpid());last_sample=time.monotonic()
                observed=sum(row['rss_bytes'] for row in sample.values());peak=max(peak,observed)
                if observed>value['limits']['sample_rss_bytes']:reason='sampled_rss_limit';break
                disk=scratch_sample(context)
                if disk>value['limits']['scratch_bytes']:reason='sampled_scratch_limit';break
                status('running',descendants_exited=False,sampled_processes=len(sample),sampled_scratch_bytes=disk)
        if reason is None:reason='worker_exited'
        signal_owned(os.getpid(),signal.SIGTERM)
        deadline=time.monotonic()+.5
        while process.poll() is None and time.monotonic()<deadline:time.sleep(.02)
        complete=drain_owned(process,timeout_s=5)
        drain_diagnostic()
        status('exited' if complete else 'unknown',descendants_exited=complete,returncode=process.returncode,
            diagnostic=diagnostic.decode('utf-8',errors='replace'))
        return 0 if complete else 125
    except BaseException as exc:
        reason='wrapper_failure'
        if process is not None:
            try:complete=drain_owned(process,timeout_s=5)
            except (RuntimeError,OSError):complete=False
        else:
            # No returned child handle is not itself proof. Confirm that this
            # dedicated subreaper has no children, including zombies.
            try:
                os.waitpid(-1,os.WNOHANG)
            except ChildProcessError:complete=True
        status('exited' if complete else 'unknown',descendants_exited=complete,
            returncode=None if process is None else process.returncode,diagnostic=str(exc)[:1024])
        raise
    finally:
        for descriptor in (release_reader,release_writer):
            if descriptor is not None:os.close(descriptor)
        if process is not None and process.stderr is not None:process.stderr.close()
        os.close(control_fd);os.close(unit)
