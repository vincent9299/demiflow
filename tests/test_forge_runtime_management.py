"""Runtime control invariants: owned fixtures, bounded probes, no model/network."""

from demiflow.workbench import host_partition
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

import pytest

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.runs import domain as run_domain
from demiflow.workbench.runs import requests as run_requests
from demiflow.workbench.runs import view as run_view
from demiflow.forge.schema import ForgeError
from demiflow.resources.process_observation import unit_stopped, require_pidfds
from demiflow.execution.journal.run_telemetry import Telemetry, series, latest
from demiflow.execution.native_diagnostics import PipeLog, slot
from demiflow.execution.run_journal import RunJournal
from test_forge_runs import base, root, prepared, execute


def test_rm03_receipt_is_directory_durable_before_work(tmp_path, monkeypatch):
    (tmp_path / ".forge").mkdir()
    seen = []
    original = os.fsync

    def fsync(fd):
        seen.append(os.readlink(f"/proc/self/fd/{fd}"))
        original(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    identity = run_requests.request_id()
    calls = []

    def operation(_, root, **args):
        receipt = run_requests.read(root, identity)
        assert (
            receipt["status"] == "pending"
            and receipt["run_id"] == args["reserved_run_id"]
        )
        assert str(tmp_path / ".forge/requests") in seen
        calls.append(receipt["run_id"])
        raise RuntimeError("owned injected preparation failure")

    with pytest.raises(RuntimeError, match="injected"):
        run_requests.prepare(
            tmp_path, "flows/a.py:run", {"params": {}}, operation, identity
        )
    assert run_requests.read(tmp_path, identity)["status"] == "failed"
    with pytest.raises(ForgeError, match="failed"):
        run_requests.prepare(
            tmp_path, "flows/a.py:run", {"params": {}}, operation, identity
        )
    with pytest.raises(ForgeError, match="different"):
        run_requests.prepare(
            tmp_path,
            "flows/a.py:run",
            {"params": {"changed": True}},
            operation,
            identity,
        )
    assert len(calls) == 1
    monkeypatch.setattr(
        run_requests.time, "time", lambda: int(identity[1:14]) / 1000 + 8 * 86400
    )
    with pytest.raises(ForgeError, match="expired"):
        run_requests.read(tmp_path, identity)
    with pytest.raises(ForgeError, match="expired"):
        run_requests.prepare(
            tmp_path, "flows/a.py:run", {"params": {}}, operation, identity
        )


def test_rm03_lost_ready_receipt_reconciles_same_sealed_run(root, monkeypatch):
    identity = run_requests.request_id()
    writer = run_requests.atomic_write_json

    def lose_ready(path, value):
        if value["status"] == "ready":
            raise OSError("injected after prepare before ready receipt")
        writer(path, value)

    monkeypatch.setattr(run_requests, "atomic_write_json", lose_ready)
    with pytest.raises(OSError, match="injected"):
        prepared(root, request_id=identity)
    receipt = run_requests.read(root, identity)
    value = runs.show(root, receipt["run_id"])
    assert value["status"] == "prepared"
    with pytest.raises(ForgeError, match="not durably ready"):
        runs.start(root, value["run_id"], expected=value["digest"])
    monkeypatch.setattr(run_requests, "atomic_write_json", writer)
    ready = run_requests.reconcile(root, identity)
    replay = prepared(root, request_id=identity)
    assert ready["run_id"] == replay["run_id"] == receipt["run_id"]
    assert len(list((root / ".forge/runs").iterdir())) == 1


def test_rm04_repeated_cancel_preserves_first_request(tmp_path):
    manifest = {
        "format": runs.FORMAT,
        "run_id": "r" + "1" * 24,
        "root": str(tmp_path),
        "entry": "flows/a.py:run",
        "outputs": {},
        "limits": {},
        "parameter_path": None,
        "created_at": time.time(),
        "report_digest": "test",
        "runtime_contract": "demiflow_forge_runtime_v1",
    }
    from demiflow.forge.platform import content_digest

    manifest["digest"] = content_digest(manifest)
    target = runs.directory(tmp_path, manifest["run_id"])
    target.mkdir(parents=True)
    journal = RunJournal(target)
    journal.initialize(manifest, attempts=True)
    journal.transition({None}, "prepared")
    journal.transition({"prepared"}, "starting")
    first = runs.cancel(tmp_path, manifest["run_id"])
    second = runs.cancel(tmp_path, manifest["run_id"])
    assert (
        first["state"]["cancel_requested_at"] == second["state"]["cancel_requested_at"]
    )
    assert first["state"]["revision"] == second["state"]["revision"]
    assert (
        run_view.events(tmp_path, manifest["run_id"])["events"][-1]["status"]
        == "stopping"
    )


def _manifest(identity, output):
    return dict(
        run_id=identity,
        outputs={"out": {"uri": str(output)}},
        limits=dict(runs.DEFAULT_LIMITS),
    )


def test_rm05_default_capacity_and_per_device_totals(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    config = {
        **host_partition.DEFAULTS,
        "max_active": 2,
        "cpu_threads": 12,
        "max_rss_bytes": 16 * 1024**3,
        "device_bytes": 32 * 1024**3,
    }
    with run_domain.lock(tmp_path / "domain") as (domain, _):
        first = run_domain.reserve(
            domain, config, a, _manifest("r" + "1" * 24, a / "out"), 1
        )
        with pytest.raises(ForgeError, match="overlapping"):
            run_domain.reserve(
                domain, config, b, _manifest("r" + "2" * 24, a / "out/child"), 1
            )
        second = run_domain.reserve(
            domain, config, b, _manifest("r" + "2" * 24, b / "out"), 1
        )
        assert first["slot"] != second["slot"]
        owner = json.loads((domain / "slot-0.json").read_text())
        assert (
            owner["rss_bytes"] == runs.DEFAULT_LIMITS["max_rss_bytes"] + 384 * 1024**2
        )
        assert (
            sum(owner["devices"].values())
            == runs.DEFAULT_LIMITS["max_scratch_bytes"]
            + runs.DEFAULT_LIMITS["max_output_bytes"]
            + runs.DEFAULT_LIMITS["max_evidence_bytes"]
            + run_domain.MANAGEMENT_RESERVE
        )
        with pytest.raises(ForgeError, match="capacity"):
            run_domain.reserve(
                domain, config, b, _manifest("r" + "3" * 24, b / "other"), 1
            )


def test_rm05_domain_does_not_follow_process_tmpdir(tmp_path):
    (tmp_path / "root").mkdir()
    code = """from pathlib import Path
from demiflow.workbench.runs.domain import lock,reserve
from demiflow.workbench.host_partition import location
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
import sys,json
print(location(),flush=True)
with lock() as (domain,capacity):
 reserve(domain,capacity,Path(sys.argv[1]),dict(run_id='r'+sys.argv[2]*24,outputs={},limits=DEFAULT_LIMITS),1)
"""
    for index in (1, 2):
        temporary = tmp_path / str(index)
        temporary.mkdir()
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, str(tmp_path / "root"), str(index)],
            env={
                **os.environ,
                "DEMIFLOW_FORGE_DOMAIN": str(tmp_path / "domain"),
                "TMPDIR": str(temporary),
            },
            text=True,
            capture_output=True,
            timeout=10,
        )
        assert result.stdout.strip() == str(tmp_path / "domain")
        assert (result.returncode == 0) == (index == 1)
        if index == 2:
            assert "capacity" in result.stderr


def test_rm08_inherited_unit_fences_unbound_child(tmp_path):
    path = tmp_path / "unit.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", "import time;time.sleep(10)"],
        pass_fds=(fd,),
        start_new_session=True,
    )
    os.close(fd)
    try:
        assert unit_stopped(path) is False
    finally:
        child.kill()
        child.wait(timeout=5)
    assert unit_stopped(path) is True


def test_rm08_cross_session_descendants_and_unrelated_canary(tmp_path):
    require_pidfds()
    canary = subprocess.Popen(
        [sys.executable, "-B", "-c", "import time;time.sleep(15)"],
        start_new_session=True,
    )
    code = """import os,sys,subprocess,time,json
from demiflow.workbench.runs.worker import _adopt_descendants
from demiflow.resources.process_observation import drain_owned,descendants
_adopt_descendants()
# The leader exits before its separate-session child; subreaper owns the latter.
child=subprocess.Popen([sys.executable,'-c',"import subprocess,sys;subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)'],start_new_session=True)"],start_new_session=True)
child.wait(timeout=5)
print(json.dumps({'children':len(descendants(os.getpid())),'drained':drain_owned(child)}))
"""
    try:
        result = subprocess.run(
            [sys.executable, "-B", "-c", code],
            text=True,
            capture_output=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
        value = json.loads(result.stdout)
        assert value["children"] >= 1 and value["drained"]
        assert canary.poll() is None
    finally:
        canary.kill()
        canary.wait(timeout=5)


def test_rm12_sampled_cpu_rss_and_missing_first_rate():
    from demiflow.resources.process_observation import ProcessSampler

    sampler = ProcessSampler()
    first = sampler.sample(os.getpid())
    assert first["cpu_cores"] is None and first["coverage"] == "partial"
    deadline = time.monotonic() + 0.12
    while time.monotonic() < deadline:
        sum(range(1000))
    second = sampler.sample(os.getpid())
    assert second["cpu_cores"] > 0 and second["rss_bytes"] > 0


def test_rm14_telemetry_capacity_retention_and_missing(tmp_path):
    assert latest(tmp_path)["sample"] is None
    store = Telemetry(tmp_path, 1)
    try:
        for number in range(10100):
            store.write(
                dict(
                    time=number * 60.0,
                    cpu_cores=None if number % 2 else 1.0,
                    rss_bytes=12000,
                    output_bytes=number,
                    coverage="partial",
                    reasons=["fixture"],
                )
            )
    finally:
        store.close()
    with closing(sqlite3.connect(tmp_path / "telemetry.sqlite")) as db:
        assert db.execute("select count(*) from minutes").fetchone()[0] == 10080
        assert db.execute("select count(*) from samples").fetchone()[0] <= 31
    assert (tmp_path / "telemetry.sqlite").stat().st_size <= 16 * 1024**2
    assert len(series(tmp_path, points=600, resolution="minute")["points"]) == 600
    with pytest.raises(ValueError):
        series(tmp_path, points=601)
    (tmp_path / "telemetry.sqlite").write_bytes(b"not a database")
    assert latest(tmp_path)["availability"] == "unavailable"


def test_rm16_native_pipe_log_is_bounded_during_flood(tmp_path):
    code = "import os;chunk=b'x'*65536\nfor n in range(160):os.write(1,chunk)"
    peak = 0
    with slot(tmp_path / "diagnostics", "owned-flood") as directory:
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", code], stdout=subprocess.PIPE
        )
        log = PipeLog(child.stdout, directory)
        try:
            deadline = time.monotonic() + 15
            while (child.poll() is None or not log.eof) and time.monotonic() < deadline:
                log.drain()
                peak = max(peak, sum(p.stat().st_size for p in directory.iterdir()))
                time.sleep(0.001)
            assert child.wait(timeout=1) == 0
            assert log.total == 10 * 1024**2
        finally:
            child.kill() if child.poll() is None else None
            child.wait()
            log.close()
    assert peak <= 4 * 65536 + 1024


def test_rm16_history_excludes_active_scratch_and_recovery(tmp_path):
    parent = tmp_path / ".forge/runs" / ("r" + "1" * 24)
    for kind in ("work", "evidence", "checkpoints", "source"):
        (parent / kind).mkdir(parents=True)
    for kind in ("work", "evidence", "checkpoints"):
        (parent / kind / "owned").write_bytes(b"x" * 4096)
    (parent / "source/code.py").write_bytes(b"x" * 100)
    assert run_domain.history_admission(tmp_path, reserve=100, ceiling=201) == 100
    with pytest.raises(ForgeError, match="history capacity"):
        run_domain.history_admission(tmp_path, reserve=100, ceiling=199)


def test_rm18_snapshot_revision_includes_usage_and_attempt_cursor(root):
    value = prepared(root)
    first = run_view.snapshot(root, value["run_id"])
    assert len(json.dumps(first).encode()) < 128 * 1024
    assert run_view.snapshot(root, value["run_id"], revision=first["revision"])[
        "unchanged"
    ]
    journal = RunJournal(runs.directory(root, value["run_id"]), attempt=1)
    journal.record_usage(elapsed_ms=1, output_bytes=0, complete=True)
    second = run_view.snapshot(root, value["run_id"], revision=first["revision"])
    assert not second.get("unchanged") and second["revision"] != first["revision"]
    events = run_view.events(root, value["run_id"])
    with pytest.raises(ForgeError, match="different"):
        run_view.events(
            root, value["run_id"], cursor=events["cursor"].replace(":1:", ":2:")
        )


def test_rm03_fsync_failure_never_calls_preparation(tmp_path, monkeypatch):
    (tmp_path / ".forge").mkdir()
    called = []

    def fail(_):
        raise OSError("injected directory durability failure")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="durability"):
        run_requests.prepare(
            tmp_path,
            "flows/a.py:run",
            {"params": {}},
            lambda *a, **k: called.append(True),
            run_requests.request_id(),
        )
    assert called == []
    assert not (tmp_path / ".forge/runs").exists()


def test_rm16_reliable_action_reservation_keeps_terminal_capacity(tmp_path):
    from demiflow.forge.platform import content_digest

    manifest = dict(
        format=runs.FORMAT,
        run_id="r" + "2" * 24,
        root=str(tmp_path),
        entry="flows/a.py:run",
        outputs={},
        limits={},
        created_at=time.time(),
        report_digest="test",
        parameter_path=None,
        runtime_contract="demiflow_forge_runtime_v1",
    )
    manifest["digest"] = content_digest(manifest)
    journal = RunJournal(tmp_path)
    journal.initialize(manifest, attempts=True)
    journal.transition({None}, "prepared")
    with pytest.raises(ValueError, match="reservation"):
        journal.reserve_actions(["action-" + str(i) for i in range(9)])
    with closing(sqlite3.connect(tmp_path / "run.sqlite")) as db:
        assert (
            db.execute("select count(*) from forge_action_reservations").fetchone()[0] == 0
        )
    assert journal.transition({"prepared"}, "starting")
    assert journal.transition({"starting"}, "stopping", cancel_requested_at=time.time())
    assert journal.transition({"stopping"}, "interrupted", execution_stopped=True)
    assert (tmp_path / "run.sqlite").stat().st_size < 64 * 1024**2


def test_rm16_allocated_blocks_are_not_charged_twice(tmp_path, monkeypatch):
    import collections

    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    config = {
        **host_partition.DEFAULTS,
        "max_active": 2,
        "cpu_threads": 12,
        "max_rss_bytes": 16 * 1024**3,
        "device_bytes": 32 * 1024**3,
    }
    ma = _manifest("r" + "a" * 24, a / "output")
    mb = _manifest("r" + "b" * 24, b / "output")
    with run_domain.lock(tmp_path / "domain") as (domain, _):
        run_domain.reserve(domain, config, a, ma, 1)
        before = json.loads((domain / "slot-0.json").read_text())
        target = runs.directory(a, ma["run_id"])
        (target / "work").mkdir(parents=True)
        (target / "work/allocated").write_bytes(b"x" * 65536)
        current = run_domain._allocated(before, deadline=time.monotonic() + 5)
        device = str(a.stat().st_dev)
        growth = current[device] - before["allocated_baseline"].get(device, 0)
        assert growth >= 65536
        reservation = sum(before["devices"].values())
        free = reservation * 2 - growth
        Usage = collections.namedtuple("Usage", "total used free")
        monkeypatch.setattr(
            run_domain.shutil, "disk_usage", lambda _: Usage(free, 0, free)
        )
        # Exact remaining-reservation boundary. Charging the first full reserve
        # again against already reduced free space would incorrectly reject it.
        run_domain.reserve(domain, config, b, mb, 1)


def test_rm03_preparation_lease_is_nonblocking_and_process_inherited(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    child = None
    with run_domain.preparation_lock() as fd:
        with pytest.raises(ForgeError, match="preparation capacity"):
            with run_domain.preparation_lock():
                pass
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", "import time;time.sleep(10)"], pass_fds=(fd,)
        )
    try:
        with pytest.raises(ForgeError, match="preparation capacity"):
            with run_domain.preparation_lock():
                pass
    finally:
        child.kill()
        child.wait(timeout=5)
    with run_domain.preparation_lock():
        pass


def test_rm18_large_action_values_never_enter_runtime_snapshot(tmp_path):
    from demiflow.forge.platform import content_digest

    identity = "r" + "4" * 24
    target = runs.directory(tmp_path, identity)
    manifest = dict(
        format=runs.FORMAT,
        run_id=identity,
        root=str(tmp_path),
        entry="flows/a.py:run",
        outputs={},
        limits={},
        created_at=time.time(),
        report_digest="test",
        parameter_path=None,
        runtime_contract="demiflow_forge_runtime_v1",
    )
    manifest["digest"] = content_digest(manifest)
    journal = RunJournal(target)
    journal.initialize(manifest, attempts=True)
    journal.reserve_actions(["a"])
    journal.transition({None}, "prepared")
    journal.action_event("a", "value", {"value": "x" * 200000})
    result = run_view.snapshot(tmp_path, identity)
    assert result["action_sequence"] > 0 and len(json.dumps(result)) < 4096
    events = run_view.events(tmp_path, identity)
    assert events["scope"] == "control_events_only"
    assert len(json.dumps(events)) < 4096


def test_rm16_history_reserves_active_growth_and_admitted_run(tmp_path):
    identity = "r" + "5" * 24
    target = runs.directory(tmp_path, identity)
    journal = RunJournal(target)
    journal.initialize({"run_id": identity}, attempts=True)
    journal.transition({None}, "prepared")
    actual = run_domain.history_admission(tmp_path, reserve=0)
    assert actual < run_domain.MANAGEMENT_RESERVE
    cap = run_domain.MANAGEMENT_RESERVE
    assert (
        run_domain.history_admission(
            tmp_path, reserve=0, admitting=identity, ceiling=cap
        )
        == cap
    )
    journal.transition({"prepared"}, "starting")
    with pytest.raises(ForgeError, match="history capacity"):
        run_domain.history_admission(tmp_path, reserve=1, ceiling=cap)
    journal.transition({"starting"}, "failed")
    assert run_domain.history_admission(tmp_path, reserve=1, ceiling=cap) == actual


def test_rm14_full_optional_sidecar_preserves_control(tmp_path):
    store = Telemetry(tmp_path, 1)
    store.db.execute("PRAGMA max_page_count=8")
    sizes = []

    def trace(_):
        sizes.append(sum(p.stat().st_size for p in tmp_path.iterdir() if p.is_file()))

    store.db.set_trace_callback(trace)
    try:
        with pytest.raises(sqlite3.OperationalError, match="full"):
            for number in range(500):
                store.write(
                    {
                        "time": number * 2.0,
                        "cpu_cores": 1.0,
                        "reasons": ["x" * 1500],
                        "coverage": "partial",
                    }
                )
    finally:
        store.close()
    assert (tmp_path / "telemetry.sqlite").stat().st_size <= 8 * 4096
    assert max(sizes) < 2 * 8 * 4096 + 8192
    assert latest(tmp_path)["sample"] is not None


def test_rm05_ancestor_inode_alias_and_allocation_dedup(tmp_path):
    # Model the device/inode facts a bind alias supplies without requiring mount privileges.
    a = {
        "path": "/first/new/child",
        "ancestor": [1, 1],
        "suffix": ["new", "child"],
        "anchors": [[1, 1, "new/child"], [1, 9, "first/new/child"]],
    }
    b = {
        "path": "/alias/new",
        "ancestor": [1, 2],
        "suffix": [],
        "anchors": [[1, 2, ""], [1, 1, "new"]],
    }
    assert run_domain._overlap(a, b)
    identity = "r" + "6" * 24
    target = runs.directory(tmp_path, identity)
    target.mkdir(parents=True)
    original = target / "data"
    original.write_bytes(b"x" * 8192)
    (target / "alias").hardlink_to(original)
    owner = {
        "root": str(tmp_path),
        "run_id": identity,
        "outputs": [{"path": str(target)}],
    }
    allocation = run_domain._allocated(owner, deadline=time.monotonic() + 5)
    assert (
        allocation[str(target.stat().st_dev)]
        == target.stat().st_blocks * 512 + original.stat().st_blocks * 512
    )


def test_rm18_minute_metrics_never_mix_attempts(tmp_path):
    first = Telemetry(tmp_path, 1)
    first.write(
        {"time": 60.0, "cpu_cores": 1.0, "rss_bytes": 100, "coverage": "sampled"}
    )
    first.close()
    second = Telemetry(tmp_path, 2)
    second.write(
        {"time": 62.0, "cpu_cores": 2.0, "rss_bytes": 200, "coverage": "sampled"}
    )
    second.close()
    a = series(tmp_path, attempt=1, resolution="minute")["points"]
    b = series(tmp_path, attempt=2, resolution="minute")["points"]
    assert len(a) == len(b) == 1
    assert a[0]["cpu_sum"] == 1.0 and a[0]["attempts"] == [1] and a[0]["rss_max"] == 100
    assert b[0]["cpu_sum"] == 2.0 and b[0]["attempts"] == [2] and b[0]["rss_max"] == 200
