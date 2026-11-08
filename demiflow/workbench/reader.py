"""One bounded native reader in the Web's existing single read lane.

Only trusted imports persist. No results, authority or source checks are cached.
The standalone workspace worker remains the sole operation dispatcher.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from collections import deque
from pathlib import Path
import gc
import json
import os
import selectors
import signal
import struct
import stat
import subprocess
import sys
import tempfile
import threading
import time

from demiflow.forge.schema import ForgeError

ALLOWED=frozenset({'describe','metadata','catalog','project_table','table','sample',
    'table_profile','analysis_source_read','analysis_media_sources'})
REQUEST_BYTES=1024**2
RESPONSE_BYTES=9*1024**2
STDERR_BYTES=1024**2
RSS_BYTES=768*1024**2
_context=ContextVar('demiflow_workspace_read_lane',default=None)

@contextmanager
def using(reader,consumer_closed=None):
    token=_context.set(None if reader is None else (reader,consumer_closed))
    try:yield
    finally:_context.reset(token)

def current():return _context.get()

class Reader:
    def __init__(self,*,max_requests=64,idle_seconds=60,timeout_seconds=20,rss_bytes=RSS_BYTES,owner_loop=False):
        if not (type(max_requests) is int and 1<=max_requests<=64 and 0<idle_seconds<=60
                and 0<timeout_seconds<=20 and type(rss_bytes) is int and 0<rss_bytes<=RSS_BYTES):
            raise ValueError('Reader policy may only tighten the fixed bounds')
        if type(owner_loop) is not bool:raise ValueError("Explicit reader owner mode required")
        self.owner_loop=owner_loop;self.owner_ident=None
        self.launch_lock=threading.Lock();self.launch=None;self.retirement_pending=False
        self.max_requests,self.idle_seconds=max_requests,idle_seconds
        self.timeout_seconds,self.rss_bytes=timeout_seconds,rss_bytes
        self.lock=threading.Lock();self.cancelled=threading.Event();self.closed=False
        self.process=None;self.identity=None;self.pidfd=None;self.scratch=None
        self.requests=0;self.last_used=0;self.stderr_bytes=0;self.peak_rss_bytes=0
        self.peak_scratch_bytes=0;self.children={};self.stderr_tail=bytearray()
        self.observations=deque(maxlen=16)

    def _spawn(self):
        from demiflow.forge.platform_worker import inspection_environment
        from demiflow.workbench.process import process_identity
        from demiflow.execution.process_observation import _pidfd
        env,fds=inspection_environment(allow_preparation=False)
        for name in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS','LANCE_CPU_THREADS'):
            env[name]='1'
        self.creator_thread=threading.get_ident()
        self.scratch=tempfile.TemporaryDirectory(prefix='demiflow-reader-')
        env['TMPDIR']=self.scratch.name
        env['XDG_CACHE_HOME']=str(Path(self.scratch.name)/'cache')
        try:
            self.process=subprocess.Popen([sys.executable,'-I','-B',str(Path(__file__).with_name('worker.py')),'--reader'],
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=self.scratch.name,
                env=env,start_new_session=True,pass_fds=fds)
            self.identity=process_identity(self.process.pid)
            if self.identity is None:raise ForgeError('Original native reader identity unavailable')
            self.pidfd=_pidfd(self.process.pid)
            for pipe in (self.process.stdin,self.process.stdout,self.process.stderr):os.set_blocking(pipe.fileno(),False)
            self.requests=0;self.stderr_bytes=0;self.peak_rss_bytes=0
            self.peak_scratch_bytes=0;self.children={};self.stderr_tail=bytearray()
        except BaseException:
            raise

    def begin_owner(self):
        if self.owner_ident is not None:raise ForgeError('Native reader already has a live owner loop')
        self.owner_ident=threading.get_ident()

    def end_owner(self):
        if self.owner_ident!=threading.get_ident():raise ForgeError('Native reader owner thread differs')
        self.owner_ident=None
        with self.launch_lock:
            ticket=self.launch
            if ticket is not None and not ticket['started']:
                ticket['cancelled']=True;ticket['ready'].set()

    def launch_pending(self):
        # PR_SET_PDEATHSIG binds the creating thread. Only the original
        # long-lived HTTP server loop can create a persistent Web reader.
        if not self.owner_loop or threading.get_ident()!=self.owner_ident:return
        with self.launch_lock:
            ticket=self.launch
            if ticket is None or ticket['started'] or ticket['ready'].is_set():return
            if self.closed or ticket['cancelled']:
                ticket['cancelled']=True;ticket['ready'].set();return
            ticket['started']=True
        try:self._spawn()
        except BaseException as error:
            ticket['error']=(str(error)[:512],getattr(error,'code','operation_failed'))
        finally:ticket['ready'].set()

    def _start(self,deadline,consumer_closed):
        if not self.owner_loop:
            self._spawn();return
        ticket=dict(ready=threading.Event(),started=False,cancelled=False,error=None)
        with self.launch_lock:
            if self.owner_ident is None:raise ForgeError('Original native reader owner loop is unavailable',code='operation_rejected')
            if self.launch is not None:raise ForgeError('Original native reader startup remains pending',code='operation_rejected')
            self.launch=ticket
        cancelled=None
        while not ticket['ready'].wait(.05):
            if self.closed or self.cancelled.is_set() or consumer_closed is not None and consumer_closed():
                cancelled=('Readonly consumer closed before reader startup','operation_rejected')
            elif time.monotonic()>=deadline:cancelled=('Native reader startup deadline exceeded','resource_limit')
            if cancelled is not None:
                with self.launch_lock:
                    ticket['cancelled']=True
                    if not ticket['started']:ticket['ready'].set()
                if not ticket['ready'].wait(4):
                    raise ForgeError('Original reader startup outcome unknown; ownership held',code='operation_rejected')
                break
        with self.launch_lock:
            if self.launch is ticket:self.launch=None
        if cancelled is not None:raise ForgeError(cancelled[0],code=cancelled[1])
        if ticket['cancelled']:raise ForgeError('Original reader owner stopped before startup',code='operation_rejected')
        if ticket['error'] is not None:raise ForgeError(ticket['error'][0],code=ticket['error'][1])

    def _sample(self):
        from demiflow.execution.process_observation import descendants
        from demiflow.workbench.process import process_identity
        if process_identity(self.process.pid)!=self.identity:
            raise ForgeError('Original native reader exited or changed identity')
        values=descendants(self.process.pid,include_owner=True)
        rss=sum(item['rss_bytes'] for item in values.values())
        self.peak_rss_bytes=max(self.peak_rss_bytes,rss)
        for pid,item in values.items():
            if pid!=self.process.pid:self.children[pid]=dict(pid=pid,start=item['start'],boot=self.identity['boot'])
        if rss>self.rss_bytes:raise ForgeError('Native reader tree exceeds its RSS reservation',code='resource_limit')
        pending=[Path(self.scratch.name)];entries=size=0
        while pending:
            with os.scandir(pending.pop()) as names:
                for item in names:
                    entries+=1
                    if entries>256:raise ForgeError('Native reader scratch entry budget exceeded',code='resource_limit')
                    try:info=item.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        # Native libraries may unlink a temporary leaf after
                        # scandir yielded it. Charge the listed entry above,
                        # but do not retire a valid reader for a vanished file.
                        # Missing directories and every other I/O error still
                        # fail; no request or source proof is replayed.
                        continue
                    if stat.S_ISDIR(info.st_mode):pending.append(Path(item.path))
                    elif stat.S_ISREG(info.st_mode):size+=info.st_size
                    else:raise ForgeError('Native reader scratch contains an unsupported entry',code='resource_limit')
                    self.peak_scratch_bytes=max(self.peak_scratch_bytes,size)
                    if size>16*1024**2:raise ForgeError('Native reader scratch byte budget exceeded',code='resource_limit')
        self.peak_scratch_bytes=max(self.peak_scratch_bytes,size)
        # This closed read dispatcher has no child-process operation. Retire an
        # unexpected native child instead of admitting another execution lane.
        if len(values)>1:raise ForgeError('Native reader created an unsupported child process',code='resource_limit')
        return rss

    def _retire(self):
        self.retirement_pending=True
        with self.launch_lock:
            if self.launch is not None:
                if not self.launch['ready'].is_set():raise ForgeError('Original reader startup is still pending; ownership held',code='operation_rejected')
                self.launch=None
        process=self.process
        if process is None:
            if self.scratch is not None:self.scratch.cleanup();self.scratch=None
            self.retirement_pending=False
            return
        from demiflow.workbench.process import process_identity
        from demiflow.execution.process_observation import _signal_fd
        from demiflow.execution.process_observation import signal_owned
        from demiflow.execution.process_observation import _pidfd
        if process.poll() is None:
            if self.identity is None or process_identity(process.pid)!=self.identity:
                raise ForgeError('Original reader exit is unknown; ownership remains held',code='operation_rejected')
            descriptor=self.pidfd
            if descriptor is None:descriptor=_pidfd(process.pid);self.pidfd=descriptor
            signal_owned(process.pid,signal.SIGKILL)
            try:_signal_fd(descriptor,signal.SIGTERM)
            except ProcessLookupError:pass
            try:process.wait(1)
            except subprocess.TimeoutExpired:
                if process_identity(process.pid)!=self.identity:
                    raise ForgeError('Original reader identity became unknown during stop',code='operation_rejected')
                signal_owned(process.pid,signal.SIGKILL)
                try:_signal_fd(descriptor,signal.SIGKILL)
                except ProcessLookupError:pass
                try:process.wait(2)
                except subprocess.TimeoutExpired as error:
                    raise ForgeError('Original reader has not exited; ownership remains held',code='operation_rejected') from error
        process.wait()
        for child in self.children.values():
            if process_identity(child['pid'])==child:
                descriptor=_pidfd(child['pid'])
                try:
                    if process_identity(child['pid'])==child:_signal_fd(descriptor,signal.SIGKILL)
                except ProcessLookupError:pass
                finally:os.close(descriptor)
        deadline=time.monotonic()+1
        while any(process_identity(child['pid'])==child for child in self.children.values()):
            if time.monotonic()>=deadline:raise ForgeError('Observed native child exit remains unknown; ownership held',code='operation_rejected')
            time.sleep(.01)
        self.children={}
        for pipe in (process.stdin,process.stdout,process.stderr):
            if pipe is not None:pipe.close()
        if self.pidfd is not None:os.close(self.pidfd)
        self.process=None;self.identity=None;self.pidfd=None
        if self.scratch is not None:self.scratch.cleanup();self.scratch=None
        self.retirement_pending=False

    def retire_idle(self):
        if not self.lock.acquire(blocking=False):raise ForgeError('Native reader is busy',code='busy')
        try:self._retire()
        finally:self.lock.release()

    def close(self):
        self.closed=True;self.cancelled.set()
        if not self.lock.acquire(timeout=7):
            raise ForgeError('Original reader close still pending',code='operation_rejected')
        try:self._retire()
        finally:self.lock.release()

    def maintain(self,now=None):
        if not self.lock.acquire(blocking=False):return
        try:
            if self.retirement_pending or self.launch is not None and self.launch['ready'].is_set():self._retire()
            if self.process is None:return
            if self.process.poll() is not None or (time.monotonic() if now is None else now)-self.last_used>=self.idle_seconds:
                self._retire()
            else:self._sample()
        except (ForgeError,OSError,RuntimeError) as error:
            self.observations.append(dict(state='idle_failed',reason=str(error)[:256]))
            self._retire()
        finally:self.lock.release()

    def call(self,request,*,timeout=20,consumer_closed=None):
        from demiflow.forge.platform import bounded_json
        if self.closed:raise ForgeError('Native reader service is closed',code='operation_rejected')
        if request.get('operation') not in ALLOWED:raise ForgeError('Operation is outside the readonly reader',code='invalid_request')
        bounded_json(request,REQUEST_BYTES)
        if not self.lock.acquire(blocking=False):raise ForgeError('Native reader is busy',code='busy')
        started=time.monotonic();cold=self.process is None;state='failed';reason=None
        try:
            if self.closed:raise ForgeError('Native reader service is closed',code='operation_rejected')
            if self.retirement_pending or self.launch is not None:self._retire()
            if self.process is not None and self.process.poll() is not None:self._retire();cold=True
            if self.process is None:self._start(started+min(timeout,self.timeout_seconds),consumer_closed)
            sequence=self.requests+1
            raw=bounded_json(dict(sequence=sequence,request=request),REQUEST_BYTES)
            wire=self._exchange(raw,started+min(timeout,self.timeout_seconds),consumer_closed)
            if type(wire) is not dict or set(wire)!={"sequence","response"} or type(wire["sequence"]) is not int or wire["sequence"]!=sequence:
                raise ForgeError("Readonly reader reply belongs to another request")
            value=wire["response"]
            if type(value) is not dict or value.get('format')!='demiflow_forge_workspace_response_v1' or type(value.get('ok')) is not bool:
                raise ForgeError('Invalid readonly reader response')
            self.requests+=1;self.last_used=time.monotonic()
            if value['ok']:self._sample()
            if not value['ok']:
                reason=value.get('code','operation_failed');self._retire()
            else:
                state='complete'
                if self.requests>=self.max_requests:self._retire()
            return value
        except BaseException as error:
            reason=getattr(error,'code',type(error).__name__)
            self._drain_stderr()
            self._retire()
            raise
        finally:
            self.observations.append(dict(state=state,reason=reason,cold=cold,seconds=time.monotonic()-started,
                peak_sampled_rss_bytes=self.peak_rss_bytes,peak_sampled_scratch_bytes=self.peak_scratch_bytes,requests=self.requests,
                started_monotonic=started,ended_monotonic=time.monotonic(),stderr_tail=bytes(self.stderr_tail).decode('utf-8','replace')[-1024:]))
            self.lock.release()

    def _stderr(self,part):
        self.stderr_bytes+=len(part);self.stderr_tail.extend(part)
        if len(self.stderr_tail)>4096:del self.stderr_tail[:-4096]
        if self.stderr_bytes>STDERR_BYTES:raise ForgeError('Native reader stderr budget exceeded',code='resource_limit')

    def _drain_stderr(self):
        if self.launch is not None and not self.launch['ready'].is_set():return
        if self.process is None or self.process.stderr is None:return
        try:
            for _ in range(17):
                part=os.read(self.process.stderr.fileno(),65536)
                if not part:return
                self._stderr(part)
        except (BlockingIOError,OSError,ForgeError):pass

    def _exchange(self,raw,deadline,consumer_closed):
        pending=memoryview(struct.pack('!I',len(raw))+raw)
        output=bytearray();expected=None;next_sample=0
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdin,selectors.EVENT_WRITE,'input')
            selector.register(self.process.stdout,selectors.EVENT_READ,'output')
            selector.register(self.process.stderr,selectors.EVENT_READ,'error')
            while True:
                now=time.monotonic()
                if self.cancelled.is_set() or consumer_closed is not None and consumer_closed():
                    raise ForgeError('Readonly consumer closed; no request was replayed',code='operation_rejected')
                if now>=deadline:raise ForgeError('Native reader request timed out',code='resource_limit')
                if now>=next_sample:self._sample();next_sample=now+.1
                for key,_ in selector.select(min(.1,deadline-now)):
                    if key.data=='input':
                        try:count=os.write(key.fileobj.fileno(),pending[:65536])
                        except BlockingIOError:continue
                        if count<=0:raise ForgeError('Native reader input made no progress')
                        pending=pending[count:]
                        if not pending:selector.unregister(key.fileobj)
                        continue
                    try:part=os.read(key.fileobj.fileno(),65536)
                    except BlockingIOError:continue
                    if key.data=='error':
                        self._stderr(part)
                        if not part:selector.unregister(key.fileobj)
                        continue
                    if not part:raise ForgeError('Original native reader exited before a complete reply')
                    output.extend(part)
                    if expected is None and len(output)>=4:
                        expected=struct.unpack('!I',output[:4])[0]
                        if not 1<=expected<=RESPONSE_BYTES:raise ForgeError('Native reader response exceeds byte budget',code='resource_limit')
                    if expected is not None and len(output)>expected+4:raise ForgeError('Native reader returned extra response bytes')
                    if expected is not None and len(output)==expected+4:
                        if pending:raise ForgeError('Native reader replied before receiving the admitted request')
                        try:return json.loads(output[4:])
                        except (ValueError,UnicodeError,RecursionError) as error:raise ForgeError('Invalid native reader JSON') from error


def _read_exact(stream,size,*,eof=False):
    output=bytearray()
    while len(output)<size:
        part=stream.read(size-len(output))
        if not part:
            if eof and not output:return None
            raise ValueError('Incomplete readonly reader request')
        output.extend(part)
    return bytes(output)

def serve(prepare,dispatch):
    """Child-only framed loop; shared dispatch never retains request objects."""
    import resource
    from demiflow.forge.platform import bounded_json
    for which,maximum in ((resource.RLIMIT_CPU,45),(resource.RLIMIT_FSIZE,16*1024**2)):
        _,hard=resource.getrlimit(which)
        maximum=maximum if hard==resource.RLIM_INFINITY else min(maximum,hard)
        resource.setrlimit(which,(maximum,maximum))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    prepare(analysis_read=True)
    initial_cwd=os.getcwd();initial_environment=dict(os.environ);initial_path=list(sys.path)
    incoming,outgoing=sys.stdin.buffer,sys.stdout.buffer
    count=0
    while True:
        header=_read_exact(incoming,4,eof=True)
        if header is None:return
        if count>=64:raise ValueError('Readonly reader request lifetime exhausted')
        size=struct.unpack('!I',header)[0]
        if not 1<=size<=REQUEST_BYTES:raise ValueError('Readonly reader request budget exceeded')
        request=result=None
        try:
            message=json.loads(_read_exact(incoming,size))
            if type(message) is not dict or set(message)!={"sequence","request"} or type(message["sequence"]) is not int or message["sequence"]!=count+1:
                raise ValueError("Readonly request sequence differs")
            request=message["request"];message=None
            if (type(request) is not dict or set(request)!={'operation','root','arguments'}
                    or request['operation'] not in ALLOWED or type(request['arguments']) is not dict
                    or type(request['root']) is not str or not Path(request['root']).is_absolute()):
                raise ValueError('Invalid closed readonly operation')
            result=dispatch(request)
            raw=bounded_json(dict(sequence=count+1,response=result),RESPONSE_BYTES)
            outgoing.write(struct.pack('!I',len(raw)));outgoing.write(raw);outgoing.flush()
            count+=1
        finally:
            request=result=raw=None
            os.chdir(initial_cwd)
            if dict(os.environ)!=initial_environment:os.environ.clear();os.environ.update(initial_environment)
            sys.path[:]=initial_path
            gc.collect()
