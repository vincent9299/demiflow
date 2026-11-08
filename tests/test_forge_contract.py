import json

import pytest

from demiflow.forge.checks import check_project
from demiflow.forge.contract import build_context, load_contract
from demiflow.forge.schema import ForgeError, normalize_schema, validate_data
from forge_support import create_project, object_schema


def test_context_binds_real_platform_contracts():
    context = build_context()
    assert context["context_digest"] == build_context()["context_digest"]
    assert len(context["platform"]) == 3
    for item in context["platform"]:
        assert item["signature"]
        assert item["doc"].strip()
        assert len(item["source_sha256"]) == 64
    assert load_contract()["contract_id"] == "demiflow.pipeline.map"


def test_context_tampering_is_rejected(tmp_path):
    project = create_project(tmp_path)
    path = project / "contract.lock.json"
    value = json.loads(path.read_text())
    value["platform"][0]["signature"] = "forged"
    path.write_text(json.dumps(value))
    report = check_project(project)
    assert not report["passed"]
    assert "contract_context_stale" in {item["code"] for item in report["issues"]}


def test_closed_json_schema_and_data_validation():
    schema = normalize_schema(object_schema({"name": {"type": "string", "minLength": 1}}))
    assert validate_data({"name": "A"}, schema) == {"name": "A"}
    with pytest.raises(ForgeError, match="object fields"):
        validate_data({"name": "A", "extra": 1}, schema)
    with pytest.raises(ForgeError, match="minLength"):
        validate_data({"name": ""}, schema)
    with pytest.raises(ForgeError, match="object is invalid"):
        normalize_schema({"type": "object", "properties": {}, "required": [], "additionalProperties": True})
    with pytest.raises(ForgeError, match="object is invalid"):
        normalize_schema({"type": "object", "properties": {}, "required": [[]], "additionalProperties": False})


def test_snapshot_bounds_files_and_directories(tmp_path):
    from demiflow.forge.snapshot import snapshot

    project = create_project(tmp_path)
    contract = load_contract()
    contract["limits"]["max_files"] = 20
    for index in range(21):
        (project / "tests" / str(index)).mkdir()
    with pytest.raises(ForgeError, match="source entries"):
        snapshot(project, contract)
