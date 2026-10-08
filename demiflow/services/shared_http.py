"""Dataset-owned leases on demiflow's existing persistent HTTP supervisor."""
import asyncio
from pathlib import Path
import fcntl
from contextlib import asynccontextmanager
from .http import ManagedHTTPService
from .manage import _directory, start_service, status_service
from ..execution.artifacts import run_lock


class SharedHTTPService:
    def __init__(self, *, root, name, configuration, request_concurrency=8, stop_when_idle=False):
        ManagedHTTPService(**{**configuration,'root':root})
        if type(request_concurrency) is not int or request_concurrency<1: raise ValueError('Invalid service concurrency')
        self.root,self.name,self.configuration = str(root),name,dict(configuration)
        self.request_concurrency=request_concurrency
        if type(stop_when_idle) is not bool: raise ValueError('stop_when_idle must be boolean')
        self.stop_when_idle=stop_when_idle
        _directory(root,name)  # validate name, no filesystem access

    def bind(self): return _SharedOwner(self)


class _SharedOwner:
    def __init__(self,spec): self.spec=spec;self.lease=None;self.startup=None
    async def ensure_ready(self):
        if self.startup is None: self.startup=asyncio.create_task(asyncio.to_thread(self._start))
        await asyncio.shield(self.startup)
    def _start(self):
        spec=self.spec; directory=_directory(spec.root,spec.name)
        directory.mkdir(parents=True,exist_ok=True)
        with (directory/'ensure.lock').open('a') as startup_lock:
            fcntl.flock(startup_lock,fcntl.LOCK_EX)
            previous=status_service(spec.root,spec.name)
            import socket
            if previous.get('host') not in {None,socket.gethostname()}:
                raise RuntimeError('Shared service name belongs to another host; declare a host-specific name')
            capacity=directory/'capacity'
            if previous['alive'] and capacity.exists() and int(capacity.read_text())!=spec.request_concurrency:
                raise ValueError('Shared service request concurrency differs; use another service name')
            self.lease=(directory/'users.lock').open('a')
            fcntl.flock(self.lease,fcntl.LOCK_SH)
            try:
                start_service(spec.root,spec.name,spec.configuration,reuse=True)
                capacity.write_text(str(spec.request_concurrency))
            except BaseException:
                self.lease.close();self.lease=None
                raise
    @asynccontextmanager
    async def request_slot(self):
        # File locks bound requests across local processes. OS releases a slot
        # after a crash; no expiring distributed lease / extra component needed.
        directory=_directory(self.spec.root,self.spec.name)
        slot=None
        try:
            while slot is None:
                for i in range(self.spec.request_concurrency):
                    candidate=(directory/f'request-{i}.lock').open('a')
                    try: fcntl.flock(candidate,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError: candidate.close()
                    else: slot=candidate;break
                if slot is None: await asyncio.sleep(.02)
            yield
        finally:
            if slot is not None: slot.close()
    async def aclose(self):
        if self.startup is not None:
            await asyncio.gather(self.startup,return_exceptions=True)
        await asyncio.to_thread(self._close)

    def _close(self):
        if self.lease is None: return
        if not self.spec.stop_when_idle:
            self.lease.close();self.lease=None
            return  # Persistent by default; services.manage owns explicit stop.
        from .manage import _stop_service
        directory=_directory(self.spec.root,self.spec.name)
        # Serialize last-user shutdown with acquiring the next lease. Finishing
        # one arm must never stop a service still used by another arm.
        with (directory/'ensure.lock').open('a') as gate:
            fcntl.flock(gate,fcntl.LOCK_EX)
            self.lease.close();self.lease=None
            with (directory/'users.lock').open('a') as users:
                try: fcntl.flock(users,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError: return
                _stop_service(self.spec.root,self.spec.name,
                              timeout_s=self.spec.configuration.get('shutdown_timeout_s',30)+5)
