"""Host v2 accounting in private fixtures only; never edits the real v1 file."""
import copy
import json

import pytest

from demiflow.workbench import host_partition
from demiflow.workbench.runs import domain as run_domain
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.forge.schema import ForgeError
from test_forge_development_runtime import combined_configuration, GiB


def configured(tmp_path, monkeypatch):
    project = tmp_path/"business"; project.mkdir()
    config, registry, workspace, domain, value = combined_configuration(tmp_path, project, monkeypatch)
    resource, sha = profile()
    analysis = dict(memory_bytes=resource["memory"]["analysis"], state_root=str(tmp_path/"analysis-state"),
        state_disk_bytes=resource['payload_limits']['global_artifact_bytes']+resource['database']['region_reserved_bytes']+320*1024**2,
        image_roots=[dict(path=str(tmp_path/"worker-images"), bytes=2*GiB)], profile_sha256=sha)
    return domain, value, analysis


def test_v1_read_is_byte_preserving_and_cannot_admit_analysis(tmp_path, monkeypatch):
    domain, value, analysis = configured(tmp_path, monkeypatch)
    path = domain/"host-budget.json"; before=path.read_bytes()
    old = host_partition.load(domain, host_partition.DEFAULTS, required=True)
    assert "analysis_memory_bytes" not in old and "analysis_bytes" not in next(iter(old["devices"].values()))
    with pytest.raises(ForgeError, match="explicitly installed host-budget v2"):
        host_partition.load_analysis(domain, host_partition.DEFAULTS, state_root=analysis["state_root"], image_root=analysis["image_roots"][0]["path"])
    assert path.read_bytes() == before


def test_all_existing_gate_reads_charge_analysis_memory_and_every_disk_reservation(tmp_path, monkeypatch):
    domain, value, analysis = configured(tmp_path, monkeypatch)
    before = host_partition.load(domain, host_partition.DEFAULTS, required=True)
    value.update(format=host_partition.FORMAT_V2, analysis=analysis)
    (domain/"host-budget.json").write_text(json.dumps(value))
    checked = host_partition.load_analysis(domain, host_partition.DEFAULTS, state_root=analysis["state_root"], image_root=analysis["image_roots"][0]["path"])
    assert checked["memory_reserved_bytes"] == before["memory_reserved_bytes"] + analysis["memory_bytes"]
    device = checked["analysis_device"]
    assert checked["devices"][device]["analysis_bytes"] == analysis["state_disk_bytes"] + 2*GiB
    assert checked["devices"][device]["reserved_bytes"] == before["devices"][device]["reserved_bytes"] + analysis["state_disk_bytes"] + 2*GiB
    # Existing runtime policy reads use this same load(), without a second gate.
    assert host_partition.policy(domain) == host_partition.DEFAULTS
    value["memory_bytes"] = checked["memory_reserved_bytes"] - 1
    (domain/"host-budget.json").write_text(json.dumps(value))
    with pytest.raises(ForgeError, match="RSS reservations"):
        host_partition.policy(domain)


@pytest.mark.parametrize("change", ["profile", "small_memory", "small_disk", "business_root", "image_overlap", "unknown_field"])
def test_v2_bad_bindings_fail_closed_without_mutating_budget(tmp_path, monkeypatch, change):
    domain, value, analysis = configured(tmp_path, monkeypatch)
    if change == "profile": analysis["profile_sha256"] = "0"*64
    elif change == "small_memory": analysis["memory_bytes"] = 1
    elif change == "small_disk": analysis["state_disk_bytes"] = 1
    elif change == "business_root": analysis["state_root"] = value["workspaces"][0]
    elif change == "image_overlap": analysis["image_roots"][0]["path"] = analysis["state_root"] + "/image"
    else: analysis["grant"] = True
    value.update(format=host_partition.FORMAT_V2, analysis=analysis)
    path=domain/"host-budget.json"; path.write_text(json.dumps(value)); before=path.read_bytes()
    with pytest.raises(ForgeError):
        host_partition.load(domain, host_partition.DEFAULTS, required=True)
    assert path.read_bytes() == before
