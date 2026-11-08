"""Fail-closed entrypoint checks only; no actual sandbox was provisioned."""
from pathlib import Path

import pytest

from demiflow.workbench.analysis import sandbox
from demiflow.forge.schema import ForgeError


def test_missing_owned_manifest_refuses_instead_of_running_a_host_kernel(tmp_path, monkeypatch):
    monkeypatch.setattr(sandbox, "JOB_PATH", tmp_path/"missing-job.json")
    with pytest.raises(ForgeError, match="no host execution fallback") as error:
        sandbox.require_job("kernel")
    assert error.value.code == "operation_rejected"


def test_writable_fake_manifest_is_not_isolation_proof(tmp_path, monkeypatch):
    path = tmp_path/"job.json"; path.write_text('{"role":"kernel"}')
    monkeypatch.setattr(sandbox, "JOB_PATH", path)
    with pytest.raises(ForgeError, match="read-only"):
        sandbox.require_job("kernel")


def test_worker_cannot_request_unknown_resource_role():
    with pytest.raises(ForgeError):
        sandbox.require_job("root-shell")
