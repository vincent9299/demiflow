"""A service process tree retains its admitted worker's original unit lock.

This guardian is an owned child, not a scheduler or resource allocator. It is a
subreaper so services that start another session still have an observable owner.
The service inherits the same unit FD: killing the guardian cannot manufacture
an exit proof while a cooperating service process remains alive.
"""
import ctypes
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

if __package__ in (None, ''):
    # The guardian runs by its pinned platform file, independent of a model's
    # declared PYTHONPATH/PYTHONHOME. The model command still receives its env.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    __package__ = 'demiflow.execution'

from ..resources.process_observation import (
    UNIT_ENV, descendants, drain_owned, inherited_unit, signal_owned,
)
from ..resources.process_unit import birth_start
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document


async def close_owned_service(owner):
    """Bound control wait; unknown tree ownership retains its original unit."""
    import asyncio
    from ..services.contracts import ModelServiceError
    process = owner._process
    if process is not None:
        try:
            os.kill(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + owner.declaration.shutdown_timeout_s
        while process.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(min(.01, max(0, deadline - time.monotonic())))
        if process.poll() is None:
            # Never turn failed observation into a synthetic exit by killing
            # its guardian. The guardian owns the same FD while it retries.
            raise ModelServiceError('Managed service cleanup is unconfirmed; original unit and port ownership retained')
        process.wait(timeout=0)
        owner._process = None
    if owner._log_pump is not None:
        pump, owner._log_pump = owner._log_pump, None
        await asyncio.to_thread(pump.close)
    owner._stack.close()


def spawn_owned_service(spec, *, env, resource_fds=()):
    binding = None
    if getattr(spec, '_worker_gpu_requested', False):
        check = getattr(spec, '_worker_device_check', None)
        if check is None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service has no original worker device grant')
        binding = check()
        env = {**env, 'CUDA_VISIBLE_DEVICES': ','.join(binding['device_ids']),
               'CUDA_DEVICE_ORDER': 'PCI_BUS_ID'}
    unit = inherited_unit()
    if not unit:
        raise DataflowError('UNSUPPORTED_CAPABILITY',
                            'Managed worker services require an owned process unit')
    declaration = {'parent': os.getpid(), 'birth': birth_start(os.getpid()),
                   'command': list(spec.command), 'shutdown_timeout_s': spec.shutdown_timeout_s}
    if binding is not None:
        declaration['max_rss_bytes'] = binding['max_rss_bytes']
    raw = document(declaration, 65536).decode('utf-8')
    # Keep the original port flock open through worker death and unknown
    # cleanup. The guardian receives the same open files, never another lock.
    inherited = tuple(dict.fromkeys((*unit, *resource_fds)))
    return subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()), raw],
        cwd=spec.root, env={**env, UNIT_ENV: str(unit[0])}, pass_fds=inherited,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        start_new_session=True)


def main(raw):
    if len(raw.encode('utf-8')) > 65536:
        raise ValueError('Managed service launch exceeds its byte budget')
    declaration = json.loads(raw)
    document(declaration, 65536)
    unit = inherited_unit()
    if not unit:
        raise RuntimeError('Managed service has no inherited unit ownership')
    parent, expected = declaration['parent'], declaration['birth']
    stopping = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.__setitem__(0, True))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise RuntimeError('Managed service subreaper unavailable')
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise RuntimeError('Managed service parent-death supervision unavailable')

    def parent_alive():
        return os.getppid() == parent and birth_start(parent) == expected

    if stopping[0] or not parent_alive():
        return 1  # Original unit FD was already held throughout startup.
    child = None
    try:
        child = subprocess.Popen(declaration['command'], stdin=subprocess.DEVNULL,
                                 pass_fds=unit)
        if not parent_alive():
            stopping[0] = True
        while not stopping[0] and child.poll() is None:
            if not parent_alive():
                break
            # Bounded original descendant traversal also rejects runaway trees.
            descendants(os.getpid())
            if 'max_rss_bytes' in declaration:
                # One admitted worker + guardian + service tree, no second
                # reservation. This sampled ceiling is not an OS hard cap.
                tree = descendants(parent, include_owner=True)
                if sum(row['rss_bytes'] for row in tree.values()) > declaration['max_rss_bytes']:
                    raise RuntimeError('Managed model worker/service tree exceeds its original max_rss_bytes grant')
            time.sleep(.02)
    finally:
        if child is not None:
            grace = min(1., declaration['shutdown_timeout_s'] / 2)
            while True:
                try:
                    signal_owned(os.getpid(), signal.SIGTERM)
                    deadline = time.monotonic() + grace
                    while child.poll() is None and time.monotonic() < deadline:
                        time.sleep(min(.01, max(0, deadline - time.monotonic())))
                    # Never return merely because the direct command exited.
                    if drain_owned(child, timeout_s=max(.02, grace)):
                        break
                except Exception:
                    # /proc limits and transient observation failures are
                    # unknown ownership. Keep the same FD and retry rather
                    # than letting a finally error manufacture an exit proof.
                    pass
                time.sleep(.1)
    return child.returncode if child is not None and child.returncode is not None else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1]))
