"""Small exec launcher: task-owned process limits, no imports from the platform."""
import ctypes
import math
import os
import resource
import signal
import sys


def main():
    max_file, seconds = int(sys.argv[1]), int(sys.argv[2])
    # Avoid preexec_fn in a multithreaded parent. Set Linux parent-death signal
    # in this separate process before replacing it with the app-server binary.
    parent = os.getppid()
    if sys.platform == 'linux':
        if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL) != 0:
            raise OSError(ctypes.get_errno(), 'Could not set parent-death signal')
        if os.getppid() != parent or parent == 1:
            return
    for kind, cap in ((resource.RLIMIT_NOFILE, 256), (resource.RLIMIT_FSIZE, max_file),
                      (resource.RLIMIT_CPU, max(1, math.ceil(seconds * 2)))):
        _, hard = resource.getrlimit(kind)
        cap = cap if hard == resource.RLIM_INFINITY else min(cap, hard)
        resource.setrlimit(kind, (cap, cap))
    os.execv(sys.argv[3], sys.argv[3:])


if __name__ == '__main__':
    main()
