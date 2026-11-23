"""Root-review regressions: optional bytes, launch fences and retained capacity."""

from demiflow.workbench import host_partition
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from contextlib import closing

import pytest

from demiflow.execution.run_journal import RunJournal
from demiflow.execution.journal.run_telemetry import Telemetry, latest, series
from demiflow.workbench.projects import index
from demiflow.workbench.runs import domain as run_domain
from demiflow.workbench.runs import requests as run_requests
from demiflow.workbench.runs import view as run_view
from demiflow.workbench.runs import manager as runs
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_runs import base, root, wait
from test_forge_run_faults import _slow, _first_committed
from test_forge_runtime_integration import _server, _request


def control_fixture(root, digit="1", request=None, prepared=True, objects=()):
    root = Path(root)
    manifest = dict(
        format=runs.FORMAT,
        run_id="r" + digit * 24,
        root=str(root),
        entry="flows/a.py:run",
        outputs={},
        object_locations=list(objects),
        limits=dict(runs.DEFAULT_LIMITS),
        parameter_path=None,
        created_at=time.time(),
        report_digest="fixture",
        runtime_contract="demiflow_forge_runtime_v1",
        platform={"files": [{"path": "workbench/runs/worker.py"}]},
        prepare_request_id=request,
    )
    manifest["digest"] = content_digest(manifest)
    target = runs.directory(root, manifest["run_id"])
    RunJournal(target).initialize(manifest, attempts=True)
    journal = RunJournal(target, attempt=1)
    if prepared:
        journal.transition({None}, "prepared")
        journal.record_usage(elapsed_ms=0, output_bytes=0, complete=True)
    return manifest, target, journal


@pytest.mark.parametrize(
    "corruption",
    [
        None,
        [],
        {},
        {"time": "bad"},
        {"time": True},
        {"time": float("nan")},
        {"cpu_cores": float("inf")},
        {"rss_bytes": -1},
        {"attempt": True},
        {"sequence": "1"},
        {"epoch": []},
        {"coverage": {}},
        {"io": []},
        {"throughput": {"rows_per_s": "bad"}},
        {"reasons": 1},
        {"usage_complete": 1},
    ],
)
def test_r1_malformed_optional_samples_are_unavailable(tmp_path, corruption):
    store = Telemetry(tmp_path, 1)
    good = store.write(
        dict(time=time.time(), cpu_cores=1.0, rss_bytes=100, coverage="sampled")
    )
    store.close()
    bad = (
        {**good, **corruption}
        if isinstance(corruption, dict) and corruption
        else corruption
    )
    with closing(sqlite3.connect(tmp_path / "telemetry.sqlite")) as db:
        db.execute("UPDATE samples SET document=?", (json.dumps(bad),))
        db.commit()
    assert latest(tmp_path)["availability"] == "unavailable"
    assert series(tmp_path, attempt=1)["availability"] == "unavailable"


@pytest.mark.parametrize(
    "corruption",
    [
        None,
        [],
        {},
        {"attempts": 1},
        {"time": "bad"},
        {"cpu_sum": float("nan")},
        {"count": False},
        {"cpu_count": -1},
    ],
)
def test_r1_malformed_minute_aggregates_are_unavailable(tmp_path, corruption):
    store = Telemetry(tmp_path, 1)
    store.write(
        dict(time=time.time(), cpu_cores=1.0, rss_bytes=100, coverage="sampled")
    )
    store.close()
    with closing(sqlite3.connect(tmp_path / "telemetry.sqlite")) as db:
        good = json.loads(db.execute("SELECT document FROM minutes").fetchone()[0])
        bad = (
            {**good, **corruption}
            if isinstance(corruption, dict) and corruption
            else corruption
        )
        db.execute("UPDATE minutes SET document=?", (json.dumps(bad),))
        db.commit()
    assert (
        series(tmp_path, attempt=1, resolution="minute")["availability"]
        == "unavailable"
    )


def test_r1_snapshot_and_compact_cancel_survive_bad_optional_json(tmp_path):
    manifest, target, journal = control_fixture(tmp_path)
    journal.transition({"prepared"}, "starting")
    store = Telemetry(target, 1)
    store.write(dict(time=time.time(), coverage="sampled"))
    store.close()
    with closing(sqlite3.connect(target / "telemetry.sqlite")) as db:
        db.execute("UPDATE samples SET document='{}'")
        db.commit()
    assert (
        run_view.snapshot(tmp_path, manifest["run_id"])["telemetry"]["availability"]
        == "unavailable"
    )
    cancelled = runs.cancel(tmp_path, manifest["run_id"], compact=True)
    assert (
        cancelled["status"] == "stopping" and cancelled["state"]["cancel_requested_at"]
    )
    # Corruption of the necessary journal must still fail explicitly.
    (target / "run.sqlite").write_bytes(b"owned required-store fault")
    with pytest.raises(sqlite3.Error):
        run_view.snapshot(tmp_path, manifest["run_id"])


def test_r1_supervisor_and_http_stop_ignore_malformed_optional_samples(root):
    value = _slow(root)
    try:
        _first_committed(root, value)
        target = runs.directory(root, value["run_id"])
        deadline = time.monotonic() + 10
        while (
            latest(target)["availability"] != "available"
            and time.monotonic() < deadline
        ):
            time.sleep(0.1)
        assert latest(target)["availability"] == "available"
        with closing(sqlite3.connect(target / "telemetry.sqlite", timeout=1)) as db:
            db.execute(
                "CREATE TRIGGER corrupt_optional AFTER INSERT ON samples BEGIN UPDATE samples SET document='{}' WHERE sequence=NEW.sequence; END"
            )
            db.execute("UPDATE samples SET document='{}'")
            db.commit()
        time.sleep(6)
        active = runs.show(root, value["run_id"])
        assert active["status"] == "running" and active["supervisor_alive"]
        with _server(root, control_only=True) as server:
            status, view = _request(
                server, "/api/run-snapshot?run_id=" + value["run_id"]
            )
            assert status == 200 and view["telemetry"]["availability"] == "unavailable"
            status, stopped = _request(
                server,
                "/api/run-cancel",
                dict(run_id=value["run_id"], expected_attempt=1, compact=True),
            )
            assert status == 200 and stopped["state"]["cancel_requested_at"]
        final = wait(root, value["run_id"], timeout=20)
        assert final["status"] == "cancelled" and final["state"]["usage_complete"]
        assert sum(o["result"] is not None for o in final["outputs"]) == 1
    finally:
        if runs.show(root, value["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)


def test_r2_actual_launch_failure_before_spawn_is_terminal_and_releases_capacity(
    tmp_path, monkeypatch
):
    root = tmp_path / "project"
    manifest, target, journal = control_fixture(root)
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    monkeypatch.setattr(
        "demiflow.workbench.worker.operation",
        lambda *a, **k: {"status": "checked"},
    )
    monkeypatch.setattr(
        runs,
        "worker_environment",
        lambda *a, **k: (_ for _ in ()).throw(OSError("owned launch failure")),
    )
    monkeypatch.setattr(
        subprocess, "Popen", lambda *a, **k: pytest.fail("must not spawn")
    )
    with pytest.raises(OSError, match="owned launch failure"):
        runs.start(root, manifest["run_id"], expected=manifest["digest"])
    state = journal.execution_status()
    assert state["status"] == "failed" and state["reason"] == "launch_failed"
    assert state["usage_complete"] and runs.execution_stopped(target, state)
    assert (target / "unit-1.lock").is_file()
    with run_domain.lock() as (domain, config):
        other = tmp_path / "other"
        other.mkdir()
        assert (
            run_domain.reserve(
                domain, config, other, {**manifest, "run_id": "r" + "2" * 24}, 1
            )["slot"]
            == 0
        )


@pytest.mark.parametrize("window", ["reserved", "admitted", "spawning"])
def test_r2_launcher_hard_exit_has_durable_reconciliation_fence(
    tmp_path, monkeypatch, window
):
    root = tmp_path / "project"
    manifest, target, journal = control_fixture(root)
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    code = """import json,os,sys
from unittest.mock import patch
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import index
root,identity,digest,window=sys.argv[1:]
hook={'reserved':'demiflow.workbench.runs.manager._claim_lease','admitted':'demiflow.workbench.projects.index.publish','spawning':'demiflow.workbench.runs.manager.subprocess.Popen'}[window]
claim=runs._claim_lease
def disappear(*a,**k):
 if window=='reserved' and a[1] is None:return claim(*a,**k)
 os._exit(31)
with patch('demiflow.workbench.worker.operation',return_value={'status':'checked'}),patch(hook,side_effect=disappear):
 runs.start(root,identity,expected=digest)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            code,
            str(root),
            manifest["run_id"],
            manifest["digest"],
            window,
        ],
        timeout=20,
    )
    assert result.returncode == 31
    assert (target / "unit-1.lock").is_file() and runs.execution_stopped(
        target, journal.execution_status()
    )
    recovered = runs._reconcile(root, manifest["run_id"], expected_attempt=1)
    assert recovered["status"] == ("interrupted" if window == "spawning" else "failed")
    assert recovered["state"]["usage_complete"] is (window != "spawning")
    assert recovered["state"]["cleanup"] == "complete"
    with run_domain.lock() as (domain, config):
        other = tmp_path / "other"
        other.mkdir()
        assert (
            run_domain.reserve(
                domain, config, other, {**manifest, "run_id": "r" + "2" * 24}, 1
            )["slot"]
            == 0
        )


def test_r3_pending_cleanup_retains_device_reservation_until_reconcile(
    tmp_path, monkeypatch
):
    root = tmp_path / "project"
    manifest, target, journal = control_fixture(root)
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    (target / "unit-1.lock").touch()
    scratch = runs.attempt_directory(target, "work", 1)
    scratch.mkdir(parents=True)
    (scratch / "owned-residue").write_bytes(b"x" * 8192)
    other = tmp_path / "other"
    other.mkdir()
    config = {
        **host_partition.DEFAULTS,
        "max_active": 2,
        "cpu_threads": 12,
        "max_rss_bytes": 16 * 1024**3,
        "device_bytes": 5 * 1024**3,
    }
    with run_domain.lock() as (domain, _):
        run_domain.reserve(domain, config, root, manifest, 1)
        journal.transition(
            {"prepared"},
            "failed",
            execution_stopped=True,
            cleanup="pending",
            recoverable=True,
        )
        owner = json.loads((domain / "slot-0.json").read_text())
        assert not run_domain._owner_finished(owner)
        with pytest.raises(ForgeError, match="device reservation"):
            run_domain.reserve(
                domain, config, other, {**manifest, "run_id": "r" + "2" * 24}, 1
            )
        assert (scratch / "owned-residue").exists()
    # This control-only fixture needs a valid native protocol to reach the
    # cleanup gate. Claim/release is fixture setup, not native execution evidence.
    from demiflow.planning.dataflow_policy import execution_limits
    configuration = dict(format=1, stream_protocol="aligned-barrier-v2", run_id="4"*32,
                         entry_identity=None, started_at=time.time(), limits_explicit=[],
                         platform_revision="original-platform", limits=execution_limits(None))
    journal.claim_execution(configuration, "f"+"1"*32)
    journal.release_execution("f"+"1"*32)
    before = journal.execution_status()
    with pytest.raises(ForgeError, match="reconcile pending cleanup"):
        runs._resume(
            root, manifest["run_id"], expected=manifest["digest"], expected_attempt=1
        )
    assert journal.execution_status() == before
    assert journal.recovery_identity() == configuration
    assert journal.execution_status()["attempt"] == 1
    assert len(RunJournal(target).list_attempts()) == 1
    runs._reconcile(root, manifest["run_id"], expected_attempt=1)
    assert not scratch.exists() and run_domain._owner_finished(owner)
    with run_domain.lock() as (domain, _):
        assert (
            run_domain.reserve(
                domain, config, other, {**manifest, "run_id": "r" + "2" * 24}, 1
            )["slot"]
            == 0
        )


def test_r2_lost_popen_return_retains_live_unit_and_open_accounting(
    tmp_path, monkeypatch
):
    root = tmp_path / "project"
    manifest, target, journal = control_fixture(root)
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    monkeypatch.setattr(
        "demiflow.workbench.worker.operation",
        lambda *a, **k: {"status": "checked"},
    )
    monkeypatch.setattr(index, "publish", lambda *a: None)
    launch = subprocess.Popen
    children = []

    def lose_return(*args, **kwargs):
        # Actual independently-sessioned descendant holds the execution fence,
        # including the interval before its process identity could be saved.
        child = launch(
            [sys.executable, "-B", "-c", "import time;time.sleep(30)"],
            pass_fds=(kwargs["pass_fds"][1],),
            start_new_session=True,
        )
        children.append(child)
        raise KeyboardInterrupt("owned lost launch return")

    monkeypatch.setattr(subprocess, "Popen", lose_return)
    try:
        with pytest.raises(KeyboardInterrupt):
            runs.start(root, manifest["run_id"], expected=manifest["digest"])
        assert journal.execution_status()["status"] == "starting"
        assert journal.execution_status()["usage_complete"] is False
        assert not runs.execution_stopped(target, journal.execution_status())
        with pytest.raises(ForgeError, match="unit has not stopped"):
            runs._reconcile(root, manifest["run_id"], expected_attempt=1)
        with run_domain.lock() as (domain, config):
            other = tmp_path / "other"
            other.mkdir()
            with pytest.raises(ForgeError, match="capacity"):
                run_domain.reserve(
                    domain, config, other, {**manifest, "run_id": "r" + "2" * 24}, 1
                )
    finally:
        for child in children:
            child.kill()
            child.wait(timeout=5)
    recovered = runs._reconcile(root, manifest["run_id"], expected_attempt=1)
    assert (
        recovered["status"] == "interrupted"
        and recovered["state"]["usage_complete"] is False
    )


@pytest.mark.parametrize(
    "window", ["absent", "partial_files", "empty_journal", "initialized_journal"]
)
def test_r4_incomplete_prepare_is_failed_once_and_pending_capacity_recovers(
    tmp_path, window
):
    (tmp_path / ".forge").mkdir()
    identities = []

    def disappear(*args, **kwargs):
        target = runs.directory(tmp_path, kwargs["reserved_run_id"])
        if window != "absent":
            (target / "source").mkdir(parents=True)
            (target / "source/owned.py").write_text("# partial private seal\n")
        if window == "empty_journal":
            sqlite3.connect(target / "run.sqlite").close()
        if window == "initialized_journal":
            # Same reserved identity; initialization precedes prepared state.
            m = dict(
                format=runs.FORMAT,
                run_id=kwargs["reserved_run_id"],
                root=str(tmp_path),
                entry="flows/a.py:run",
                outputs={},
                limits=dict(runs.DEFAULT_LIMITS),
                prepare_request_id=kwargs["prepare_request_id"],
                runtime_contract="demiflow_forge_runtime_v1",
                report_digest="fixture",
                parameter_path=None,
                created_at=time.time(),
            )
            m["digest"] = content_digest(m)
            RunJournal(target).initialize(m, attempts=True)
        raise KeyboardInterrupt("owned preparation loss")

    for _ in range(16):
        identity = run_requests.request_id()
        identities.append(identity)
        with pytest.raises(KeyboardInterrupt):
            run_requests.prepare(
                tmp_path, "flows/a.py:run", {"params": {}}, disappear, identity
            )
    with pytest.raises(ForgeError, match="capacity"):
        run_requests.prepare(
            tmp_path,
            "flows/a.py:run",
            {"params": {}},
            disappear,
            run_requests.request_id(),
        )
    old = run_requests.read(tmp_path, identities[0])
    fixed = run_requests._reconcile(tmp_path, identities[0])
    assert fixed["status"] == "failed" and fixed["run_id"] == old["run_id"]
    assert not runs.directory(tmp_path, old["run_id"]).exists()
    assert run_requests._reconcile(tmp_path, identities[0]) == fixed
    with pytest.raises(ForgeError, match="failed"):
        run_requests.prepare(
            tmp_path, "flows/a.py:run", {"params": {}}, disappear, identities[0]
        )
    with pytest.raises(KeyboardInterrupt):
        run_requests.prepare(
            tmp_path,
            "flows/a.py:run",
            {"params": {}},
            disappear,
            run_requests.request_id(),
        )


def test_r4_execution_fence_prevents_discarding_unknown_preparation(tmp_path):
    (tmp_path / ".forge").mkdir()
    identity = run_requests.request_id()

    def disappear(*args, **kwargs):
        target = runs.directory(tmp_path, kwargs["reserved_run_id"])
        target.mkdir(parents=True)
        (target / "unit-1.lock").touch()
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        run_requests.prepare(
            tmp_path, "flows/a.py:run", {"params": {}}, disappear, identity
        )
    with pytest.raises(ForgeError, match="execution/ownership"):
        run_requests._reconcile(tmp_path, identity)
    assert run_requests.read(tmp_path, identity)["status"] == "pending"


@pytest.mark.parametrize("prepared", [False, True])
def test_r4_expired_pending_closes_with_same_identity(tmp_path, monkeypatch, prepared):
    (tmp_path / ".forge").mkdir()
    identity = run_requests.request_id()

    def disappear(*args, **kwargs):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        run_requests.prepare(tmp_path, "flows/a.py:run", {}, disappear, identity)
    old = run_requests.read(tmp_path, identity)
    if prepared:
        manifest, target, journal = control_fixture(tmp_path, request=identity)
        # Give the fixture the reserved identity before sealing it.
        import shutil

        shutil.rmtree(target)
        manifest["run_id"] = old["run_id"]
        manifest["digest"] = content_digest(
            {k: v for k, v in manifest.items() if k != "digest"}
        )
        target = runs.directory(tmp_path, old["run_id"])
        RunJournal(target).initialize(manifest, attempts=True)
        RunJournal(target, attempt=1).transition({None}, "prepared")
    future = time.time() + 8 * run_requests.DAY
    monkeypatch.setattr(run_requests.time, "time", lambda: future)
    with pytest.raises(ForgeError, match="expired"):
        run_requests.read(tmp_path, identity)
    if prepared:
        # Crash after the run is closed but before the receipt update must
        # remain recoverable with exactly the original identity.
        write = run_requests.atomic_write_json
        monkeypatch.setattr(
            run_requests,
            "atomic_write_json",
            lambda *a: (_ for _ in ()).throw(OSError("owned receipt loss")),
        )
        with pytest.raises(OSError, match="receipt loss"):
            run_requests._reconcile(tmp_path, identity)
        monkeypatch.setattr(run_requests, "atomic_write_json", write)
    fixed = run_requests._reconcile(tmp_path, identity)
    assert fixed["status"] == "failed" and fixed["run_id"] == old["run_id"]
    assert fixed["reason"] == (
        "preparation_expired" if prepared else "preparation_incomplete"
    )
    assert run_requests._reconcile(tmp_path, identity) == fixed
    with pytest.raises(ForgeError, match="expired"):
        run_requests.prepare(tmp_path, "flows/a.py:run", {}, disappear, identity)
    # Expired failed receipts can be pruned; the old ID remains non-replayable.
    with run_requests.locked(tmp_path) as parent:
        run_requests._capacity(parent)
    with pytest.raises(ForgeError, match="creation window"):
        run_requests.prepare(tmp_path, "flows/a.py:run", {}, disappear, identity)


def test_r4_public_reconcile_uses_bounded_preparation_worker(
    root, monkeypatch, tmp_path
):
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "domain"))
    identity = run_requests.request_id()

    def disappear(*args, **kwargs):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        run_requests.prepare(root, "flows/a.py:run", {}, disappear, identity)
    old = run_requests.read(root, identity)
    fixed = run_requests.reconcile(root, identity)
    assert fixed["status"] == "failed" and fixed["run_id"] == old["run_id"]
