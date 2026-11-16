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


def spawn_owned_service(spec, *, env):
    unit = inherited_unit()
    if not unit:
        raise DataflowError('UNSUPPORTED_CAPABILITY',
                            'Managed worker services require an owned process unit')
    declaration = {'parent': os.getpid(), 'birth': birth_start(os.getpid()),
                   'command': list(spec.command), 'shutdown_timeout_s': spec.shutdown_timeout_s}
    raw = document(declaration, 65536).decode('utf-8')
    return subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()), raw],
        cwd=spec.root, env={**env, UNIT_ENV: str(unit[0])}, pass_fds=unit,
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
            time.sleep(.02)
    finally:
        if child is not None:
            signal_owned(os.getpid(), signal.SIGTERM)
            grace = min(1., declaration['shutdown_timeout_s'] / 2)
            deadline = time.monotonic() + grace
            while child.poll() is None and time.monotonic() < deadline:
                time.sleep(min(.01, max(0, deadline - time.monotonic())))
            # Never return merely because the direct command exited. Adopt and
            # reap children from every session before releasing the unit FD.
            while not drain_owned(child, timeout_s=max(.02, grace)):
                # Unknown/stuck descendants keep the original lock charged.
                # The outer owner may terminate this guardian; descendants
                # still inherit the same lock rather than falsely refunding it.
                time.sleep(.02)
    return child.returncode if child is not None and child.returncode is not None else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1]))
