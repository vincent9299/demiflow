"""Bounded, explicit preparation subprocesses; no model imports."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time


@contextmanager
def locked(path, timeout_s=120):
    with Path(path).open('a') as stream:
        deadline = time.monotonic()+timeout_s
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Timed out waiting for '+str(path))
                time.sleep(.05)
        yield stream


def tree_bytes(root, *, max_entries=200000):
    """No symlink traversal or unbounded directory lists."""
    total = count = 0
    pending = [Path(root)]
    while pending:
        directory=pending.pop()
        try: entries=os.scandir(directory)
        except FileNotFoundError: continue
        with entries:
            for entry in entries:
                count += 1
                if count > max_entries:
                    raise ValueError('Prepared environment exceeds file-count budget')
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
                elif not entry.is_symlink():
                    try: total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError: pass  # Installer replaced a temporary file.
    return total


def run_bounded(command, *, timeout_s, output_bytes=1024*1024, log_path=None,
                disk_root=None, disk_bytes=16*1024**3):
    """Cap captured output; kill the owned process group on timeout/overflow.

    Installation disk use is monitored, not a filesystem quota: there can be
    growth between checks. Runtime libraries remain outside demiflow's process.
    """
    environment={**os.environ, 'CUDA_VISIBLE_DEVICES':'', 'PIP_NO_CACHE_DIR':'1'}
    if disk_root is not None:
        temporary=Path(disk_root)/'tmp'; temporary.mkdir(exist_ok=True)
        environment.update(TMPDIR=str(temporary),UV_CACHE_DIR=str(Path(disk_root)/'uv-cache'),
                           UV_PYTHON_INSTALL_DIR=str(Path(disk_root)/'python'))
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, start_new_session=True,
        env=environment)
    data = bytearray()
    deadline = time.monotonic()+timeout_s
    next_disk_check = time.monotonic()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while selector.get_map():
                if time.monotonic() >= deadline:
                    raise TimeoutError('Preparation timed out')
                if disk_root is not None and time.monotonic() >= next_disk_check:
                    if tree_bytes(disk_root) > disk_bytes:
                        raise ValueError('Preparation exceeds 16 GiB disk budget')
                    next_disk_check = time.monotonic()+1
                for key, _ in selector.select(min(.1, max(0, deadline-time.monotonic()))):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    elif len(data)+len(chunk)>output_bytes:
                        raise ValueError('Preparation output exceeds byte budget')
                    else: data.extend(chunk)
            code = process.wait(timeout=max(.01, deadline-time.monotonic()))
            if code:
                raise RuntimeError(f'Preparation exited {code}: '+bytes(data[-4096:]).decode(errors='replace'))
            if disk_root is not None and tree_bytes(disk_root)>disk_bytes:
                raise ValueError('Preparation exceeds disk budget')
            return bytes(data)
    finally:
        try: os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        process.wait()
        process.stdout.close()
        if log_path is not None:
            Path(log_path).write_bytes(data)
