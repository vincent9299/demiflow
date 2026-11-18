"""Bounded trusted-command runner. This is not an OS sandbox."""
from __future__ import annotations

import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

OUTPUT_LIMIT = 1024 * 1024


def run(command, project, scratch, *, launch=None):
    scratch.mkdir(mode=0o700, parents=True, exist_ok=False)

    def expand(value):
        for key, path in {"project": project.root, "tool": project.tool_root,
                          "scratch": scratch}.items():
            value = value.replace("{" + key + "}", str(path))
        return value

    argv = [expand(arg) for arg in command["argv"]]
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8",
           **{k: expand(v) for k, v in command["env"].items()},
           "HOME": str(scratch), "TMPDIR": str(scratch), "PYTHONDONTWRITEBYTECODE": "1",
           "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}
    started = time.monotonic()
    output = bytearray()
    reason = None
    process = (subprocess.Popen(argv, cwd=project.root, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True) if launch is None else
               launch(argv, cwd=project.root, env=env, scratch=scratch, timeout=command['timeout']))
    selector = selectors.DefaultSelector()
    try:
        if launch is not None and process.stdin is not None:
            process.stdin.close()
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ)
        while selector.get_map():
            if time.monotonic() - started > command["timeout"]:
                reason = "timeout"
                break
            for key, _ in selector.select(timeout=0.1):
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                room = OUTPUT_LIMIT - len(output)
                output.extend(chunk[:room])
                if len(chunk) > room:
                    reason = "output_limit"
                    break
            if reason:
                break
        if not reason:
            try:
                process.wait(timeout=max(0.01, command["timeout"] - (time.monotonic() - started)))
            except subprocess.TimeoutExpired:
                reason = "timeout"
    finally:
        # Clean the whole command process group, including orphaned children.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        selector.close()
        process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()
    result = {"id": command["id"], "argv": argv, "returncode": process.returncode,
            "failure": reason, "seconds": round(time.monotonic() - started, 3),
            "output": output.decode("utf-8", errors="replace"),
            "passed": reason is None and process.returncode == 0}
    if launch is not None:
        evidence = getattr(process, 'execution_evidence', {})
        result['execution'] = dict(assurance=getattr(process, 'execution_assurance', 'gate_only'),
            backend=evidence.get('backend'), implementation=evidence.get('implementation'),
            id=evidence.get('execution_id'))
        # The trusted PID-namespace supervisor reserves these exit statuses for
        # deadline/resource/admission failures or signalled children. They must
        # not become a determined fixture failure which triggers another run.
        if (result['execution']['assurance'] == 'enforced' and result['failure'] is None and
                (process.returncode in {124, 125, 126} or 128 <= process.returncode <= 192)):
            result['failure'] = 'sandbox_execution_incomplete'
    return result
