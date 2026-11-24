"""Process identity inspection and lifecycle helpers for caller-owned services."""
from pathlib import Path

def command(pid):
    try:return Path(f'/proc/{pid}/cmdline').read_bytes().decode().strip('\0').split('\0')
    except FileNotFoundError:return []

def matching(fragment):
    return [int(p.name) for p in Path('/proc').iterdir() if p.name.isdigit() and fragment in ' '.join(command(int(p.name)))]


import os
import signal
import subprocess
import sys
import time

def wait_until(check, seconds, label, process=None):
    deadline = time.monotonic() + seconds
    while not check():
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'{label}: service exited {process.returncode}')
        if time.monotonic() >= deadline:
            raise TimeoutError(label)
        time.sleep(5)

def spawn(command_line, log, *, cwd=None):
    env = os.environ.copy()
    env['PATH'] = str(Path(sys.executable).parent) + os.pathsep + env.get('PATH', '')
    with Path(log).open('ab') as stream:
        return subprocess.Popen(command_line, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                stdout=stream, stderr=stream, start_new_session=True)

def stop_owned(process):
    if process is not None and process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try: process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=30)
