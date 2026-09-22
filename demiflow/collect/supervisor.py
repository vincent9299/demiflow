"""demiflow supervisor：子进程常驻守护（run_*.sh 的 python 化）。

机制：拉起 → 等待退出 → 退避重拉（外部可 SIGTERM 整组收尾）。
比 shell 版多两件事：退出码语义透传（看门狗击穿=3 立即重拉不退避加长）、
子进程 stdout/stderr 原样转发（无需 nohup 重定向约定）。
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

__all__ = ["run_forever"]


def run_forever(argv: list, backoff_s: float = 30.0,
                log_prefix: str = "supervisor") -> None:
    child: subprocess.Popen = None
    stopping = {"v": False}

    def _term(signum, frame):
        stopping["v"] = True
        if child and child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    while not stopping["v"]:
        ts = time.strftime("%m-%d %H:%M:%S")
        print(f"[{ts}] 拉起 {' '.join(argv)}", flush=True)
        child = subprocess.Popen(argv)
        rc = child.wait()
        ts = time.strftime("%m-%d %H:%M:%S")
        print(f"[{ts}] 退出码 {rc}", flush=True)
        if stopping["v"]:
            break
        wait = 0.0 if rc == 3 else backoff_s   # 看门狗击穿: 零退避
        for _ in range(int(wait)):
            if stopping["v"]:
                return
            time.sleep(1.0)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("用法: python -m demiflow.collect.supervisor <cmd> [args...]")
    run_forever(sys.argv[1:])
