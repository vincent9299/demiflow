"""Dataset-owned leases on demiflow's existing persistent HTTP supervisor."""
import asyncio
from pathlib import Path
import fcntl
import time
from contextlib import asynccontextmanager
from .http import ManagedHTTPService
from .contracts import ModelServiceError, ModelServiceConfigurationError
from .manage import _directory, start_service, status_service
from .preparation_io import locked


class SharedHTTPService:
    def __init__(self, *, root, name, configuration, request_concurrency=8, stop_when_idle=False,
                 request_wait_timeout_s=120):
        ManagedHTTPService(**{**configuration,'root':root})
        if type(request_concurrency) is not int or not 1<=request_concurrency<=512: raise ValueError('Invalid service concurrency')
        self.root,self.name,self.configuration = str(root),name,dict(configuration)
        self.request_concurrency=request_concurrency
        if type(stop_when_idle) is not bool: raise ValueError('stop_when_idle must be boolean')
        self.stop_when_idle=stop_when_idle
        import math
        if (type(request_wait_timeout_s) not in (int, float)
                or not math.isfinite(request_wait_timeout_s) or request_wait_timeout_s <= 0):
            raise ValueError('request_wait_timeout_s must be positive and finite')
        self.request_wait_timeout_s = request_wait_timeout_s
        _directory(root,name)  # validate name, no filesystem access

    def bind(self, model=None):
        ManagedHTTPService(**{**self.configuration, 'root': self.root}).bind(model)
        return _SharedOwner(self)


class _SharedOwner:
    def __init__(self,spec):
        self.spec=spec;self.lease=None;self.startup=None;self._closed=False
        self._created=False
    @property
    def base_url(self): return self.spec.configuration['base_url'].rstrip('/')
    async def ensure_ready(self):
        if self._closed: raise ModelServiceError('Shared service lease is closed')
        if self.startup is None: self.startup=asyncio.create_task(asyncio.to_thread(self._start))
        try:
            await asyncio.shield(self.startup)
            state = await asyncio.to_thread(status_service,self.spec.root,self.spec.name)
            if not state['alive'] or state.get('state')!='ready':
                raise ModelServiceError('Shared service exited or is no longer ready')
        except asyncio.CancelledError:
            raise
        except ValueError as exc:
            raise ModelServiceConfigurationError(str(exc)) from exc
        except Exception as exc:
            raise ModelServiceError(f'Shared service startup failed: {exc}') from exc
    def _start(self):
        spec=self.spec; directory=_directory(spec.root,spec.name)
        directory.mkdir(parents=True,exist_ok=True)
        with locked(directory/'ensure.lock',spec.configuration.get('startup_timeout_s',600)+30):
            previous=status_service(spec.root,spec.name)
            import socket
            if previous.get('host') not in {None,socket.gethostname()}:
                raise RuntimeError('Shared service name belongs to another host; declare a host-specific name')
            capacity=directory/'capacity'
            if previous['alive'] and capacity.exists() and int(capacity.read_text())!=spec.request_concurrency:
                raise ValueError('Shared service request concurrency differs; use another service name')
            self.lease=(directory/'users.lock').open('a')
            try: fcntl.flock(self.lease,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BaseException:
                self.lease.close(); self.lease=None
                raise
            try:
                start_service(spec.root,spec.name,spec.configuration,reuse=True)
                self._created=not previous['alive']
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
        deadline = time.monotonic() + self.spec.request_wait_timeout_s
        try:
            while slot is None:
                if self._closed: raise ModelServiceError('Shared service lease is closed')
                for i in range(self.spec.request_concurrency):
                    candidate=(directory/f'request-{i}.lock').open('a')
                    try: fcntl.flock(candidate,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError: candidate.close()
                    else: slot=candidate;break
                if slot is None:
                    remaining = deadline-time.monotonic()
                    if remaining <= 0: raise TimeoutError('Shared service request admission timed out')
                    await asyncio.sleep(min(.02, remaining))
            yield
        finally:
            if slot is not None: slot.close()
    async def aclose(self):
        if self._closed: return
        self._closed=True
        if self.startup is not None:
            await asyncio.gather(self.startup,return_exceptions=True)
        await asyncio.to_thread(self._close)

    async def rollback(self):
        """A failed composite launch stops newly acquired idle components only."""
        if self._closed: return
        self._closed=True
        if self.startup is not None:
            await asyncio.gather(self.startup,return_exceptions=True)
        await asyncio.to_thread(self._close, self._created)

    def _close(self, rollback=False):
        if self.lease is None: return
        if not self.spec.stop_when_idle and not rollback:
            self.lease.close();self.lease=None
            return  # Persistent by default; services.manage owns explicit stop.
        from .manage import _stop_service
        directory=_directory(self.spec.root,self.spec.name)
        # Serialize last-user shutdown with acquiring the next lease. Finishing
        # one arm must never stop a service still used by another arm.
        with locked(directory/'ensure.lock',self.spec.configuration.get('startup_timeout_s',600)+30):
            self.lease.close();self.lease=None
            with (directory/'users.lock').open('a') as users:
                try: fcntl.flock(users,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError: return
                _stop_service(self.spec.root,self.spec.name,
                              timeout_s=self.spec.configuration.get('shutdown_timeout_s',30)+5)
