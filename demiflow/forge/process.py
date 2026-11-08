"""Bounded trusted test processes; this is not an operating-system sandbox."""
from __future__ import annotations

import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time


def run_pytest(project: Path, scratch: Path, *, timeout: int, tail_limit: int, hard_limit: int) -> dict:
    scratch.mkdir(parents=True, exist_ok=False)
    package_root = Path(__file__).resolve().parents[2]
    argv = [sys.executable, "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"]
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "HOME": str(scratch),
        "TMPDIR": str(scratch),
        "PYTHONPATH": os.pathsep.join((str(package_root), str(project.absolute()))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
    }
    process = subprocess.Popen(
        argv, cwd=project, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    streams = {"stdout": process.stdout, "stderr": process.stderr}
    tails = {name: bytearray() for name in streams}
    observed = {name: 0 for name in streams}
    selector = selectors.DefaultSelector()
    started = time.monotonic()
    reason = None
    try:
        for stream in streams.values():
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
        while selector.get_map():
            if time.monotonic() - started > timeout:
                reason = "timeout"
                break
            for key, _ in selector.select(timeout=0.1):
                name = next(name for name, stream in streams.items() if stream is key.fileobj)
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                observed[name] += len(chunk)
                tails[name].extend(chunk)
                if len(tails[name]) > tail_limit:
                    del tails[name][:-tail_limit]
                if observed[name] > hard_limit:
                    reason = "output_limit"
                    break
            if reason:
                break
            if process.poll() is not None and not selector.get_map():
                break
        if reason is None and process.poll() is None:
            try:
                process.wait(timeout=max(0.01, timeout - (time.monotonic() - started)))
            except subprocess.TimeoutExpired:
                reason = "timeout"
    finally:
        if process.poll() is None:
            _terminate(process)
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait()
        selector.close()
        for stream in streams.values():
            stream.close()
    return {
        "argv": argv,
        "returncode": process.returncode,
        "failure": reason,
        "seconds": round(time.monotonic() - started, 3),
        "stdout": tails["stdout"].decode("utf-8", errors="replace"),
        "stderr": tails["stderr"].decode("utf-8", errors="replace"),
        "observed_bytes": observed,
        "truncated": {name: observed[name] > len(tails[name]) for name in tails},
        "passed": reason is None and process.returncode == 0,
    }


def _terminate(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


__all__ = ["run_pytest"]
