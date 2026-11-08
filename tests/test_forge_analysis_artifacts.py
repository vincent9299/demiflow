"""Owned file/real SQLite visibility and quota recovery; no OCI/volume proof."""
import hashlib
import os
from pathlib import Path
import shutil

import pytest

from demiflow.workbench.analysis import object_files
from test_forge_analysis_records import controller
from test_forge_analysis_object_files import arguments


def reserve(client, request_id="object-one", *, parents=None, controls=None):
    return client.call("artifact_reserve", request_id=request_id, execution_id="execution-one", generation=1,
        retention="ephemeral", parents=parents or [], source_binding=None, controls=controls or {})


def stage(client, state, identifier):
    ticket = client.call("artifact_receiving", identifier=identifier)["result"]
    kwargs, data = arguments(); kwargs.update(publish_id=ticket["publish_id"], seed=ticket["seed"])
    receiver = object_files.Ingress(state, **kwargs)
    for name, raw in data.items():
        receiver.start_file(name); receiver.write(raw); receiver.finish_file()
    prepared = receiver.seal()
    result = client.call("artifact_prepared", identifier=identifier, manifest=receiver.manifest, receipt=prepared)
    assert result["result"]["state"] == "prepared" and result["result"]["ready"] is False
    return receiver, prepared


def test_only_db_ready_exposes_handle_after_real_owned_rename_and_receipt_survives_restart(tmp_path):
    state = tmp_path/"state"; state.mkdir(mode=0o700)
    with controller(state/"database") as client:
        reserved = reserve(client)["result"]
        receiver, prepared = stage(client, state, reserved["id"])
        assert "handle" not in client.call("artifact_status", identifier=reserved["id"])["result"]
        started = client.call("artifact_begin_publish", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])["result"]
        assert started["state"] == "publishing"
        assert client.call("artifact_commit_ready", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])["code"] == "conflict"
        receiver.rename(authorize=lambda publish,digest: publish==reserved["publish_id"] and digest==started["manifest_digest"])
        ready = client.call("artifact_commit_ready", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])["result"]
        assert ready["ready"] and ready["held_bytes"] == prepared["charged_bytes"] < reserved["reserved_bytes"]
        assert ready["handle"]["digest"] == prepared["manifest_digest"]
    with controller(state/"database") as client:
        current = client.call("artifact_status", identifier=reserved["id"])["result"]
        assert current["handle"] == ready["handle"]
        replay = reserve(client)["result"]
        assert replay["replayed"] and replay["id"] == reserved["id"]
        assert reserve(client, "child", parents=[current["handle"] | {"rows": 2}])["code"] == "invalid_request"
        assert client.call("artifact_status", identifier=reserved["id"], project="w"+"1"*32)["code"] == "not_found"
        kept = client.call("artifact_system_cleanup_pending", identifier=reserved["id"], reason="later kernel stopped")["result"]
        assert kept["ready"] and kept["held_bytes"] == current["held_bytes"]


def test_cancel_race_before_ready_retains_renamed_object_charge_until_cleanup_proof(tmp_path):
    state = tmp_path/"state"; state.mkdir(mode=0o700)
    with controller(state/"database") as client:
        reserved = reserve(client)["result"]
        receiver, prepared = stage(client, state, reserved["id"])
        client.call("artifact_begin_publish", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])
        receiver.rename(authorize=lambda *_: True)
        cancelled = client.call("artifact_system_cleanup_pending", identifier=reserved["id"], reason="cancelled before ready")["result"]
        assert cancelled["held_bytes"] == reserved["reserved_bytes"] and not cancelled["ready"]
        assert client.call("artifact_commit_ready", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])["code"] == "conflict"
        assert client.call("artifact_system_cleanup_complete", identifier=reserved["id"])["code"] == "conflict"
        # File absence is real; the supplied process flag is deliberately not an
        # OCI proof. Neither part alone releases the reservation.
        assert client.call("artifact_system_cleanup_complete", identifier=reserved["id"], controls={"cleanup_proven": True})["code"] == "conflict"
        owned = receiver.objects_root/reserved["id"]
        for directory, _, _ in os.walk(owned):
            Path(directory).chmod(0o700)
        shutil.rmtree(owned)
        done = client.call("artifact_system_cleanup_complete", identifier=reserved["id"], controls={"cleanup_proven": True})["result"]
        assert done["state"] == "cleanup_complete" and done["held_bytes"] == 0


def test_pending_reservations_saturate_project_before_any_files_or_jobs_start(tmp_path):
    with controller(tmp_path/"database") as client:
        one = reserve(client)["result"]
        two = reserve(client, "two")["result"]
        failed = reserve(client, "three")
        assert failed["code"] == "resource_limit"
        assert client.call("receipt", request_id="three")["code"] == "not_found"
        assert not (tmp_path/"objects").exists()
        client.call("artifact_system_cleanup_pending", identifier=one["id"], reason="unused reservation")
        assert reserve(client, "still-full")["code"] == "resource_limit"
        client.call("artifact_system_cleanup_complete", identifier=one["id"], controls={"cleanup_proven": True})
        assert reserve(client, "after-cleanup")["result"]["state"] == "reserved"


def test_permission_revocation_before_ready_preserves_unexposed_file_and_reservation(tmp_path):
    state = tmp_path/"state"; state.mkdir(mode=0o700)
    with controller(state/"database") as client:
        reserved = reserve(client)["result"]
        receiver, prepared = stage(client, state, reserved["id"])
        client.call("artifact_begin_publish", identifier=reserved["id"], expected_digest=prepared["manifest_digest"])
        receiver.rename(authorize=lambda *_: True)
        rejected = client.call("artifact_commit_ready", identifier=reserved["id"], expected_digest=prepared["manifest_digest"], controls={"permissions": ["read"]})
        assert rejected["code"] == "operation_rejected"
        status = client.call("artifact_status", identifier=reserved["id"])["result"]
        assert status["state"] == "publishing" and not status["ready"] and status["held_bytes"] == reserved["reserved_bytes"]
