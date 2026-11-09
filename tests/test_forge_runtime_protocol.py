"""Closed runtime protocol and loss of optional storage use owned fixtures."""

from pathlib import Path
import json
import os
import sqlite3
import time

import pytest

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.runs import domain as run_domain
from demiflow.workbench.runs import admission as run_admission
from demiflow.forge.schema import ForgeError
from demiflow.execution.run_journal import RunJournal
from demiflow.resources.process_observation import signal_owned
from test_forge_runs import base, root, prepared
from test_forge_public_api import service, request


def test_rm01_22_admission_never_executes_unsupported_protocols(root):
    before = set((root / ".forge/runs").glob("*"))
    ready = run_admission.describe(
        root, "flows/questions.py:select", params="flows/select_a.json"
    )
    assert ready["status"] == "requires_prepare" and ready["checks_pending"]
    with service(root, enable_runs=True) as server:
        for backend in ("ray", "gpu", "arbitrary_script", "config_run_pipeline"):
            status, value = request(
                server,
                "/api/run-prepare",
                {
                    "entry": "flows/questions.py:select",
                    "params": "flows/select_a.json",
                    "backend": backend,
                },
            )
            assert status == 400 and value["code"] == "invalid_request"
        for points in ("x", "0", "601", "-1"):
            status, value = request(
                server, "/api/run-metrics?run_id=not-a-run&points=" + points
            )
            assert status == 400 and value["code"] == "invalid_request"
        status, value = request(
            server,
            "/api/run-prepare",
            {"entry": "flows/unregistered.py:run", "params": "flows/select_a.json"},
        )
        assert status in (400, 413, 422)
    assert set((root / ".forge/runs").glob("*")) == before


def test_rm03_all_seal_directories_are_synced(tmp_path, monkeypatch):
    from demiflow.forge import io

    target = tmp_path / "seal"
    (target / "source/nested").mkdir(parents=True)
    (target / "source/nested/file").write_bytes(b"owned")
    seen = []
    original = io.fsync_directory

    def sync(path):
        seen.append(Path(path))
        original(path)

    monkeypatch.setattr(io, "_fsync_directory", sync)
    runs.durable_tree(target)
    assert seen == [target / "source/nested", target / "source", target, tmp_path]

    def failed(path):
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(io, "_fsync_directory", failed)
    with pytest.raises(OSError, match="fsync failure"):
        runs.durable_tree(target)


def test_rm09_pid_identity_mismatch_cannot_signal(monkeypatch):
    from demiflow.resources import process_observation as observation

    rows = observation.descendants(os.getpid(), include_owner=True)
    own = rows[os.getpid()]
    called = []
    monkeypatch.setattr(
        observation,
        "descendants",
        lambda owner: {os.getpid(): {**own, "start": str(int(own["start"]) + 1)}},
    )
    monkeypatch.setattr(observation, "_signal_fd", lambda *args: called.append(args))
    signal_owned(os.getpid(), 0)
    assert called == []
