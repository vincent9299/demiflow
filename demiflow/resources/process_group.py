"""Run a command tree within an existing process unit and retain its exit evidence."""
import ctypes
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    __package__ = 'demiflow.resources'

from .contracts import document
from .process_observation import UNIT_ENV, descendants, drain_owned, inherited_unit, signal_owned
from .process_unit import birth_start


def spawn_command(command, *, cwd, env, shutdown_timeout_s, max_rss_bytes=None,
                  resource_fds=(), stdout=None):
    unit = inherited_unit()
    if not unit:
        raise RuntimeError('A guarded command requires an existing process unit')
    declaration = {'parent': os.getpid(), 'birth': birth_start(os.getpid()),
                   'command': list(command), 'shutdown_timeout_s': shutdown_timeout_s}
    if max_rss_bytes is not None:
        declaration['max_rss_bytes'] = max_rss_bytes
    raw = document(declaration, 65536).decode('utf-8')
    # Keep the original port flock open through worker death and unknown
    # cleanup. The guardian receives the same open files, never another lock.
    inherited = tuple(dict.fromkeys((*unit, *resource_fds)))
    return subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()), raw],
        cwd=cwd, env={**env, UNIT_ENV: str(unit[0])}, pass_fds=inherited,
        stdin=subprocess.DEVNULL, stdout=stdout, stderr=subprocess.STDOUT,
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
    return _supervise(declaration, unit, owner=(parent, expected))


def supervise_unit(command, *, deadline_unix, max_rss_bytes, shutdown_timeout_s):
    """Own a complete job group independently of its submitting driver."""
    unit = inherited_unit()
    if not unit:
        raise RuntimeError('Job supervision requires an inherited process unit')
    return _supervise(dict(command=command, deadline_unix=deadline_unix,
                      max_rss_bytes=max_rss_bytes, shutdown_timeout_s=shutdown_timeout_s), unit)


def _supervise(declaration, unit, *, owner=None):
    stopping = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.__setitem__(0, True))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError('Managed process subreaper unavailable')
    if owner is not None and libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
        raise RuntimeError('Managed service parent-death supervision unavailable')

    def parent_alive():
        return owner is None or (os.getppid() == owner[0] and birth_start(owner[0]) == owner[1])

    if stopping[0] or not parent_alive():
        return 1
    deadline = None
    if 'deadline_unix' in declaration:
        deadline = time.monotonic() + max(0, declaration['deadline_unix'] - time.time())
    child = None
    try:
        child = subprocess.Popen(declaration['command'], stdin=subprocess.DEVNULL,
                                 pass_fds=unit)
        if not parent_alive():
            stopping[0] = True
        while not stopping[0] and child.poll() is None:
            if not parent_alive() or (deadline is not None and time.monotonic() >= deadline):
                break
            # Bounded original descendant traversal also rejects runaway trees.
            descendants(os.getpid())
            if 'max_rss_bytes' in declaration:
                # One admitted worker + guardian + service tree, no second
                # reservation. This sampled ceiling is not an OS hard cap.
                tree = descendants(owner[0] if owner else os.getpid(), include_owner=True)
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
