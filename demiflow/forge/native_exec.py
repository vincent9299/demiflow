"""Task-owned exec limits without preexec_fn in a threaded HTTP server."""
import ctypes
import os
import resource
import signal
import sys
from pathlib import Path


def launch_command(command):
    """Bind a native child to this parent before exec, without threaded preexec_fn."""
    return [sys.executable, '-I', '-B', str(Path(__file__).resolve()), str(os.getpid()), *command]


def main():
    expected_parent = int(sys.argv[1])
    if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0):
        raise OSError('Cannot bind provider parent-death signal')
    if os.getppid() != expected_parent:
        return 1
    for kind, cap in ((resource.RLIMIT_NOFILE, 256), (resource.RLIMIT_FSIZE, 64 * 1024**2), (resource.RLIMIT_CPU, 120)):
        _, hard = resource.getrlimit(kind)
        cap = cap if hard == resource.RLIM_INFINITY else min(cap, hard)
        resource.setrlimit(kind, (cap, cap))
    os.execv(sys.argv[2], sys.argv[2:])


if __name__ == '__main__':
    raise SystemExit(main())
