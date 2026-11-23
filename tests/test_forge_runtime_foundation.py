"""Runtime control and the integrated native recovery contract share one journal."""

from demiflow.workbench import host_partition
from contextlib import closing
import sqlite3
import time
import pytest

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.workbench.runs import domain as run_domain
from test_forge_runtime_review_fixes import control_fixture


def test_control_fact_capacity_and_native_budget_reservations_coexist(tmp_path):
    _, target, journal = control_fixture(tmp_path)
    journal.reserve_actions(["checked-action"])
    assert journal.transition({"prepared"}, "starting")
    assert journal.transition({"starting"}, "running")
    with RunContext.attach(journal, limits={"max_requests": 2}) as native:
        assert native.attempt == journal.execution_status()["attempt"] == 1
        assert native.reserve("requests", 1, reservation_id="request-1") == 1
        assert native.reserve("requests", 1, reservation_id="request-1") == 1
        journal.action_event("checked-action", "intent", {"fixture": True})
        with closing(sqlite3.connect(target / "run.sqlite")) as db:
            assert db.execute(
                "SELECT attempt,action_id FROM forge_action_reservations"
            ).fetchall() == [(1, "checked-action")]
            assert db.execute(
                "SELECT name,reservation_id,amount FROM runtime_reservations"
            ).fetchall() == [("requests", "request-1", 1)]
    # Native scope releases its owner; the supervisor still owns lifecycle.
    assert journal.execution_status()["status"] == "running"
    assert journal.transition({"running"}, "stopping", cancel_requested_at=1)
    assert journal.transition(
        {"stopping"}, "cancelled", execution_stopped=True, cleanup="complete"
    )
    assert journal.execution_status()["attempt"] == 1


def test_retained_native_artifacts_are_charged_to_history(tmp_path):
    target = tmp_path / ".forge/runs/fixture"
    for name, count in [("state", 31), ("materializations", 47), ("scratch", 61)]:
        (target / name).mkdir(parents=True)
        (target / name / "owned.bin").write_bytes(b"x" * count)
    assert run_domain.history_admission(tmp_path, reserve=0, ceiling=200) == 139


def test_live_native_owner_cannot_be_replaced_by_cleanup(tmp_path):
    import pytest
    from demiflow.workbench.runs.storage import clean_stopped
    from demiflow.workbench.runs.storage import owned_objects
    from demiflow.workbench.runs.storage import scratch_bytes
    from demiflow.errors import ExecutionConflict
    from demiflow.workbench.runs.manager import execution_stopped

    location = tmp_path / "objects"
    _, target, journal = control_fixture(tmp_path / "project", objects=[str(location)])
    journal.transition({"prepared"}, "running")
    with RunContext.attach(journal) as native:
        namespace = native.object_namespace(location)
        assert owned_objects(target, [str(location)]) == [namespace]
        with pytest.raises(ExecutionConflict, match='lease is busy'):
            clean_stopped(journal, expected_revision=journal.execution_status()['revision'],
                confirm_stopped=lambda snapshot: execution_stopped(target, snapshot),
                deadline=time.monotonic() + 10)
        assert journal.execution_status().get("execution_stopped") is not True
        assert (
            scratch_bytes(target, target / "work/attempt-1", [str(location)])
            >= (namespace / ".owner").stat().st_size
        )


def test_native_crash_orphans_keep_slot_until_owned_cleanup(tmp_path, monkeypatch):
    import json
    import os
    import subprocess
    import sys
    import pytest
    from demiflow.workbench.runs.storage import clean_stopped
    from demiflow.workbench.runs.storage import owned_objects

    location = tmp_path / "objects"
    manifest, target, journal = control_fixture(
        tmp_path / "project", objects=[str(location)]
    )
    domain = tmp_path / "domain"
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(domain))
    with run_domain.lock() as (path, policy):
        run_domain.reserve(path, policy, tmp_path / "project", manifest, 1)
    journal.transition({"prepared"}, "running")
    (target / "unit-1.lock").touch()
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            """
import os,sys
from pathlib import Path
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
with RunContext.attach(RunJournal(Path(sys.argv[1]),attempt=1)) as native:
    namespace=native.object_namespace(sys.argv[2])
    orphan=namespace/'aa/orphan.bin'
    native.allocate_artifact(orphan)
    orphan.parent.mkdir()
    orphan.write_bytes(b'x'*73)
    os._exit(41)
""",
            str(target),
            str(location),
        ],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 41, child.stderr
    binding = journal.recovery_identity()
    (namespace,) = owned_objects(target, [str(location)])
    orphan = namespace / "aa/orphan.bin"
    assert orphan.exists()
    journal.transition({"running"}, "failed", execution_stopped=True, cleanup="pending")
    owner = json.loads((domain / "slot-0.json").read_text())
    assert not run_domain._owner_finished(owner)
    before = run_domain.history_admission(tmp_path / "project", reserve=0)
    from demiflow.execution.run_journal import RunJournal
    from demiflow.workbench.runs.manager import execution_stopped
    journal = RunJournal(target, attempt=1)

    def clean():
        return clean_stopped(journal, expected_revision=journal.execution_status()['revision'],
            confirm_stopped=lambda snapshot: execution_stopped(target, snapshot), deadline=time.monotonic() + 10)

    with monkeypatch.context() as scope:

        def fail(self, fence):
            raise OSError("owned native cleanup fault")

        scope.setattr(type(journal), "clean_orphans", fail)
        with pytest.raises(OSError, match="cleanup fault"):
            clean()
    assert orphan.exists() and not run_domain._owner_finished(owner)
    clean()
    assert not orphan.exists() and (namespace / ".owner").exists()
    assert run_domain.history_admission(tmp_path / "project", reserve=0) <= before - 73
    assert journal.recovery_identity() == binding
    assert journal.execution_status()["attempt"] == 1
    journal.transition({"failed"}, "failed", cleanup="complete")
    assert run_domain._owner_finished(owner)
    with closing(sqlite3.connect(target / "run.sqlite")) as db:
        assert db.execute("SELECT count(*) FROM runtime_owner").fetchone()[0] == 0


def test_object_devices_are_reserved_before_worker_allocation(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace
    import pytest
    from demiflow.forge.schema import ForgeError

    location = tmp_path / "objects"
    first = tmp_path / "first"
    second = tmp_path / "second"
    a, _, _ = control_fixture(first, objects=[str(location)])
    b, _, _ = control_fixture(second, digit="2", objects=[str(location)])
    original = run_domain._target
    device = original(tmp_path)["device"]

    def device_target(path):
        value = original(path)
        if Path(path) == location:
            value["device"] = device + 1
        elif Path(path).is_relative_to(second):
            value["device"] = device + 2
        return value

    monkeypatch.setattr(run_domain, "_target", device_target)
    monkeypatch.setattr(
        run_domain.shutil, "disk_usage", lambda _: SimpleNamespace(free=2**50)
    )
    domain = tmp_path / "domain"
    domain.mkdir()
    config = {
        **host_partition.DEFAULTS,
        "max_active": 2,
        "max_rss_bytes": 16 * 1024**3,
        "cpu_threads": 12,
        "device_bytes": 6 * 1024**3,
    }
    run_domain.reserve(domain, config, first, a, 1)
    owner = json.loads((domain / "slot-0.json").read_text())
    assert owner["devices"][str(device + 1)] == a["limits"]["max_scratch_bytes"]
    with pytest.raises(ForgeError, match="device reservation"):
        run_domain.reserve(domain, config, second, b, 1)
    assert not (domain / "slot-1.json").exists()


@pytest.mark.parametrize("damage", ["path", "run_id"])
def test_object_owner_record_cannot_redirect_accounting(tmp_path, damage):
    import pytest
    from demiflow.workbench.runs.storage import owned_objects
    from demiflow.forge.schema import ForgeError

    location = tmp_path / "objects"
    _, target, journal = control_fixture(tmp_path / "project", objects=[str(location)])
    journal.transition({"prepared"}, "running")
    with RunContext.attach(journal) as native:
        native.object_namespace(location)
    with closing(sqlite3.connect(target / "run.sqlite")) as db:
        if damage == "path":
            db.execute(
                "UPDATE runtime_object_roots SET physical_root=?",
                (str(tmp_path / "foreign"),),
            )
        else:
            import json

            binding = json.loads(
                db.execute("SELECT document FROM runtime_identity").fetchone()[0]
            )
            binding["run_id"] = "../foreign"
            db.execute("UPDATE runtime_identity SET document=?", (json.dumps(binding),))
            db.execute(
                "UPDATE runtime_object_roots SET run_id=?, physical_root=?",
                (
                    binding["run_id"],
                    str(location / ".demiflow-objects" / binding["run_id"]),
                ),
            )
        db.commit()
    with pytest.raises(ForgeError, match="checked locations|native (run )?identity"):
        owned_objects(target, [str(location)])


def test_unprepared_native_write_still_refuses_ambiguous_replay(tmp_path):
    import pytest
    from demiflow.execution import RecoveryContractError
    from demiflow.execution.run_journal import RunJournal

    with pytest.raises(RuntimeError, match="owned interruption"):
        with RunContext.start(tmp_path / "native") as native:
            reference = native.ref
            with native.action({"kind": "write_table"}):
                raise RuntimeError("owned interruption")
    with pytest.raises(RecoveryContractError, match="P3 native sink adapter"):
        with RunContext.resume(reference) as native:
            with native.action({"kind": "write_table"}):
                pytest.fail("ambiguous native write was replayed")
    assert RunJournal(reference.directory).execution_commits("action-1") == []


@pytest.mark.parametrize('original_keys', [None, []])
def test_legacy_managed_provenance_uses_recorded_identity_on_resume(tmp_path, original_keys):
    from demiflow.execution.host.control import DEFAULT_LIMITS, RunJournal
    _, target, journal = control_fixture(tmp_path)
    journal.transition({'prepared'}, 'running')
    # Legacy preparation without separate provenance retained all supplied fields.
    expected = set(DEFAULT_LIMITS if original_keys is None else original_keys)
    with RunContext.attach(journal, limits=DEFAULT_LIMITS, explicit_limits=original_keys) as run:
        assert run.explicit_limits == expected
    journal.transition({'running'}, 'failed', execution_stopped=True, cleanup='complete')
    journal.begin_attempt(expected_attempt=1, expected_revision=journal.execution_status()['revision'], nodes=[], actions=[])
    resumed = RunJournal(target, attempt=2)
    resumed.transition({'prepared'}, 'running')
    with RunContext.attach(resumed, limits=DEFAULT_LIMITS) as run:
        assert run.explicit_limits == expected
