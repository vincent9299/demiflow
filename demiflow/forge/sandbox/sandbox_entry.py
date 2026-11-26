"""Fixed host-side parent-death binding, then util-linux namespace creation."""
import ctypes
import os
from pathlib import Path
import signal
import sys


def main():
    args = sys.argv[1:]
    allow_net = False
    if args and args[0] == '--allow-net':
        allow_net = True
        args = args[1:]
    parent = int(args[0])
    if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0):
        raise RuntimeError('Cannot bind namespace launcher to parent')
    if os.getppid() != parent:
        return 1
    command = ['/usr/bin/unshare', '--user', '--map-root-user', '--pid', '--fork', '--kill-child=SIGKILL']
    if not allow_net:
        command.append('--net')
    command.extend([sys.executable, '-I', '-B', str(Path(__file__).with_name('sandbox_child.py')), *args[1:]])
    os.execv(command[0], command)


if __name__ == '__main__':
    raise SystemExit(main())
