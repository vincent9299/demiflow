"""Managed Local fault windows through actual DataFusion sessions and HTTP."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

import pytest

from demiflow.workbench.runs import manager as runs

from demiflow.workbench.process import process_identity
from demiflow.workbench.runs import view as run_view
from demiflow.workbench.projects import registry as console
from demiflow.workbench.api import WorkspaceServer
from demiflow.resources.process_observation import _pidfd, _signal_fd, unit_stopped
from test_forge_runs import base, root, prepared, wait, custom
from test_forge_run_faults import _slow, _first_committed


def _native_slow(root):
    return custom(
        root,
        """
from operators.custom import slow
def run(*,source,target,other):
    d=data.read_lance(source['uri'],version=source['version'])
    first=d.write_lance(target['uri'],mode='create',return_receipt=True)
    joined=d.join(d,on='case_id')
    second=joined.map(slow,inputs={'text':'question'},output='waiting').write_lance(other['uri'],mode='create',return_receipt=True)
    return first,second
""",
        operators="""def slow(text: str | None) -> str | None:
    n=0
    for i in range(2000000000):
        n=n+i
    return text
""",
        arguments={
            "source": {"kind": "table", "alias": "candidates"},
            "target": {
                "kind": "output",
                "location_id": "local",
                "relative_uri": "first.lance",
            },
            "other": {
                "kind": "output",
                "location_id": "local",
                "relative_uri": "native.lance",
            },
        },
        limits={"timeout_s": 90, "cancel_grace_s": 1},
    )


def _kill(identity):
    assert runs.process_matches(identity)
    fd = _pidfd(identity["pid"])
    try:
        _signal_fd(fd, signal.SIGKILL)
    finally:
        os.close(fd)


def _native(root, value):
    target = runs.directory(root, value["run_id"])
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        directory = target / "diagnostics"
        if directory.exists():
            for entry in directory.iterdir():
                path = entry / "result.json"
                if not path.exists():
                    continue
                try:
                    report = json.loads(path.read_text())
                except (OSError, ValueError):
                    continue
                pid = report.get("pid")
                identity = process_identity(pid) if pid else None
                if identity and not report.get("complete"):
                    fd = _pidfd(pid)
                    try:
                        _signal_fd(fd, signal.SIGSTOP)
                    finally:
                        os.close(fd)
                    return identity
        time.sleep(0.05)
    raise AssertionError("No actual native query reached its independent session")


def _request(server, path, body=None):
    headers = {"X-Forge-Token": server.token}
    if body is not None:
        headers.update(Origin=server.origin, **{"Content-Type": "application/json"})
    req = urllib.request.Request(
        server.origin + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


@contextmanager
def _server(*args, **kwargs):
    server = WorkspaceServer(*args, **kwargs)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("lost", ["worker", "supervisor"])
def test_rm08_real_native_session_parent_loss_and_canary(root, lost):
    value = _native_slow(root)
    current = _first_committed(root, value)
    canary = subprocess.Popen(
        [sys.executable, "-B", "-c", "import time;time.sleep(30)"],
        start_new_session=True,
    )
    native = None
    try:
        native = _native(root, value)
        assert os.getsid(native["pid"]) != os.getsid(current["state"]["worker"]["pid"])
        _kill(current["state"][lost])
        if runs.process_matches(native):
            fd = _pidfd(native["pid"])
            try:
                _signal_fd(fd, signal.SIGCONT)
            finally:
                os.close(fd)
        deadline = time.monotonic() + 15
        while runs.process_matches(native) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not runs.process_matches(native)
        assert canary.poll() is None
        if lost == "supervisor":
            # Supervisor loss must not turn open accounting into complete usage.
            assert runs.show(root, value["run_id"])["status"] in runs.ACTIVE
            result = runs.reconcile(root, value["run_id"])
            assert (
                result["status"] == "interrupted"
                and not result["state"]["usage_complete"]
            )
        else:
            result = wait(root, value["run_id"], timeout=15)
            assert result["state"]["execution_stopped"] is True
        deadline = time.monotonic() + 5
        unit = runs.directory(root, value["run_id"]) / "unit-1.lock"
        while not unit_stopped(unit) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert unit_stopped(unit)
        assert sum(o["result"] is not None for o in result["outputs"]) == 1
        assert result["state"]["cleanup"] == "complete"
    finally:
        if native and runs.process_matches(native):
            fd = _pidfd(native["pid"])
            try:
                _signal_fd(fd, signal.SIGCONT)
            finally:
                os.close(fd)
        canary.kill()
        canary.wait(timeout=5)
        state = runs.show(root, value["run_id"])
        if state["status"] in runs.ACTIVE and state.get("supervisor_alive"):
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)


def test_rm17_control_only_and_reserved_http_control_lane(root, tmp_path):
    value = _slow(root)
    _first_committed(root, value)
    registry = tmp_path / "registry.json"
    registered = console.add(registry, root, allow_runs=True)
    before = registry.read_bytes()
    inventory = console.load_registry(registry)
    console.projects(inventory)
    assert registry.read_bytes() == before
    console.set_permissions(
        registry,
        registered["workspace_id"],
        expected=console.projects(inventory)["digest"],
        run_submit=False,
        run_control=True,
        attach=False,
    )
    try:
        with _server(registry=registry, enable_runs=True) as server:
            prefix = "/api/workspaces/" + registered["workspace_id"] + "/"
            status, context = _request(server, prefix + "context")
            assert status == 200
            assert context["run_permissions"] == dict(
                run_submit=False, run_control=True, attach=False
            )
            assert context["runs_enabled"] is False
            status, _ = _request(
                server,
                prefix + "run-prepare",
                dict(entry="flows/questions.py:select", params="flows/select_a.json"),
            )
            assert status == 403
            # A long inspection owns the ordinary worker lane. Cancel still
            # commits independently and returns its durable stopping receipt.
            server.workers.acquire()
            try:
                started = time.monotonic()
                status, result = _request(
                    server,
                    prefix + "run-cancel",
                    dict(run_id=value["run_id"], expected_attempt=1),
                )
                assert status == 200 and time.monotonic() - started < 3
                assert result["state"].get("cancel_requested_at")
            finally:
                server.workers.release()
        result = wait(root, value["run_id"], timeout=20)
        assert result["status"] == "cancelled"
    finally:
        if runs.show(root, value["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)


def _flood(batch):
    chunk = "x" * 65536
    for _ in range(160):
        print(chunk, end="")
    return batch


def test_rm16_real_native_stdout_and_query_rotation(tmp_path):
    from demiflow.execution.hosts.datafusion import DataFusionSession, DataFusionOptions

    diagnostic = tmp_path / "diagnostics"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    options = DataFusionOptions(
        memory_bytes=128 * 1024**2,
        max_rss_bytes=2 * 1024**3,
        threads=1,
        partitions=1,
        timeout_s=30,
        admission_timeout_s=10,
    )
    peak = [0]
    stopped = threading.Event()

    def measure():
        while not stopped.wait(0.01):
            size = 0
            if diagnostic.exists():
                for parent in diagnostic.iterdir():
                    for path in parent.iterdir():
                        try:
                            size += path.stat().st_size
                        except FileNotFoundError:
                            pass
            peak[0] = max(peak[0], size)

    sampler = threading.Thread(target=measure, daemon=True)
    sampler.start()
    try:
        with DataFusionSession(
            resource_directory=tmp_path / "pool",
            diagnostics_directory=diagnostic,
            temp_directory=scratch,
            managed_diagnostics=True,
            options=options,
        ) as session:
            result = session.query("SELECT 1 AS x", sources={}, batch_transform=_flood)
            assert result.row_count == 1
            assert result.report["log_total_bytes"] == 10 * 1024**2
            assert result.report["diagnostics_truncated"] is True
            assert result.report["log_retained_bytes"] == 256 * 1024
            assert not (diagnostic / "worker_result.json").exists()
        # More than one retention window, using the same public slot primitive
        # across attempts; an active slot cannot be reclaimed.
        from demiflow.execution.hosts.datafusion import slot
        from demiflow.execution.journal.native_diagnostics import write_document

        for index in range(40):
            with slot(diagnostic, "query-" + str(index)) as path:
                write_document(
                    path / "result.json", dict(attempt=index // 20 + 1, complete=True)
                )
        assert len(list(diagnostic.iterdir())) == 16
    finally:
        stopped.set()
        sampler.join(timeout=2)
    assert peak[0] < 16 * 1024**2
    (tmp_path / "peak.json").write_text(
        json.dumps(dict(peak_bytes=peak[0], log_produced_bytes=10 * 1024**2))
    )


def test_rm05_two_projects_cli_http_and_explicit_aggregate_capacity(
    base, tmp_path, monkeypatch
):
    import shutil

    first = tmp_path / "first"
    second = tmp_path / "second"
    shutil.copytree(base, first)
    shutil.copytree(base, second)
    a = _slow(first)
    b = _slow(second)
    domain = tmp_path / "capacity"
    domain.mkdir()
    (domain / "policy.json").write_text(
        json.dumps(
            dict(
                max_active=2,
                cpu_threads=12,
                max_rss_bytes=16 * 1024**3,
                device_bytes=32 * 1024**3,
            )
        )
    )
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(domain))
    try:
        command = [
            sys.executable,
            "-B",
            "-m",
            "demiflow.cli",
            "forge",
            "workspace",
            "run",
            "start",
            str(first),
            a["run_id"],
            "--expected",
            a["digest"],
            "--expected-attempt",
            "1",
            "--json",
        ]
        started = subprocess.run(command, text=True, capture_output=True, timeout=45)
        assert started.returncode == 0, started.stdout + started.stderr
        with _server(second, enable_runs=True) as server:
            status, value = _request(
                server,
                "/api/run-start",
                dict(run_id=b["run_id"], expected=b["digest"], expected_attempt=1),
            )
            assert status == 200, value
            assert runs.show(first, a["run_id"])["status"] in runs.ACTIVE
            assert runs.show(second, b["run_id"])["status"] in runs.ACTIVE
            owners = [json.loads(p.read_text()) for p in domain.glob("slot-*.json")]
            assert len(owners) == 2 and len({o["root"] for o in owners}) == 2
            assert sum(o["rss_bytes"] for o in owners) <= 16 * 1024**3
            assert (
                runs.directory(first, a["run_id"]) / "work"
                != runs.directory(second, b["run_id"]) / "work"
            )
            # Disabling additional capacity cannot silently revoke active work.
            policy = json.loads((domain / "policy.json").read_text())
            policy["max_active"] = 1
            (domain / "policy.json").write_text(json.dumps(policy))
            assert runs.show(first, a["run_id"])["status"] in runs.ACTIVE
            status, _ = _request(
                server, "/api/run-cancel", dict(run_id=b["run_id"], expected_attempt=1)
            )
            assert status == 200
    finally:
        for root, value in [(first, a), (second, b)]:
            current = runs.show(root, value["run_id"])
            if current["status"] in runs.ACTIVE:
                runs.cancel(root, value["run_id"])
                wait(root, value["run_id"], timeout=25)


def test_rm14_optional_sampler_loss_and_corruption_leave_control_intact(root):
    value = _slow(root)
    current = _first_committed(root, value)
    target = runs.directory(root, value["run_id"])
    try:
        deadline = time.monotonic() + 10
        snapshot = run_view.snapshot(root, value["run_id"])
        while (
            snapshot["telemetry"]["availability"] != "available"
            and time.monotonic() < deadline
        ):
            time.sleep(0.1)
            snapshot = run_view.snapshot(root, value["run_id"])
        assert snapshot["telemetry"]["availability"] == "available"
        from demiflow.resources.process_observation import descendants

        helpers = []
        for pid in descendants(current["state"]["supervisor"]["pid"]):
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as stream:
                    argv = stream.read(4096).split(b"\0")
            except FileNotFoundError:
                continue
            if b"observe" in argv and value["run_id"].encode() in argv:
                helpers.append(process_identity(pid))
        assert len(helpers) == 1
        _kill(helpers[0])
        (target / "telemetry.sqlite").write_bytes(b"owned optional-store fault")
        snapshot = run_view.snapshot(root, value["run_id"])
        assert snapshot["telemetry"]["availability"] == "unavailable"
        assert snapshot["state"]["status"] in runs.ACTIVE
        assert sum(o["result"] is not None for o in snapshot["outputs"]) == 1
        cancelled = runs.cancel(root, value["run_id"], compact=True)
        assert cancelled["state"].get("cancel_requested_at")
        result = wait(root, value["run_id"], timeout=20)
        assert result["status"] == "cancelled" and result["state"]["usage_complete"]
    finally:
        if runs.show(root, value["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)


def test_rm14_explicit_no_metrics_keeps_native_protection_and_results(root):
    value = prepared(root, telemetry=False)
    runs.start(root, value["run_id"], expected=value["digest"])
    result = wait(root, value["run_id"])
    assert result["status"] == "succeeded", result
    assert result["outputs"][0]["result"]["ref"]["row_count"] == 2
    assert result["state"]["sampled_peaks"]["rss_bytes"] > 0
    assert result["state"]["usage_complete"] is True
    assert not (runs.directory(root, value["run_id"]) / "telemetry.sqlite").exists()


def test_rm15_required_scratch_probe_failure_stops_without_complete_usage(
    root, tmp_path
):
    value = _slow(root)
    target = runs.directory(root, value["run_id"])
    link = None
    try:
        _first_committed(root, value)
        work = runs.attempt_directory(target, "work", 1)
        canary = tmp_path / "owned-canary"
        canary.write_text("preserve")
        link = work / "unexpected-link"
        link.symlink_to(canary)
        result = wait(root, value["run_id"], timeout=20)
        assert result["status"] == "failed", result
        assert result["state"]["execution_stopped"] is True
        assert result["state"]["usage_complete"] is False
        assert result["state"]["cleanup"] == "pending"
        assert canary.read_text() == "preserve"
        assert sum(o["result"] is not None for o in result["outputs"]) == 1
        link.unlink()
        link = None
        recovered = runs.reconcile(root, value["run_id"])
        assert recovered["state"]["cleanup"] == "complete"
        assert recovered["state"]["usage_complete"] is False
    finally:
        if link is not None:
            link.unlink(missing_ok=True)
        if runs.show(root, value["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)
