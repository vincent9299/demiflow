"""Old bytes stay readable; reference admission never claims missing adapters."""
import hashlib
import json

import pytest

from demiflow.execution.contracts.dataflow import ArtifactRef, CutRequest, HistoricalBinding, RecoveryImage
from demiflow.execution.dataflow_history import SEALED_PLAN_FACTS_SCHEMA, validate_resume_compatibility
import runtime_v3_history_scenarios as scenarios


def test_prompt_identity_matches_original_git_golden():
    evidence = scenarios.prompt_identities()
    assert set(evidence["identities"]) == {"1024", "1025", "1500"}


def test_prompt_golden_drift_cannot_pass(tmp_path, monkeypatch):
    golden = json.loads(scenarios.GOLDEN.read_text())
    golden["identities"]["1025"]["operation_id"] = "0" * 64
    path = tmp_path / "corrupt-golden.json"
    path.write_text(json.dumps(golden))
    monkeypatch.setattr(scenarios, "GOLDEN", path)
    with pytest.raises(AssertionError):
        scenarios.prompt_identities()


@pytest.mark.parametrize("with_image", [False, True])
def test_native_compatibility_needs_image_and_verified_runtime_adapter(tmp_path, with_image):
    def artifact(value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        digest = hashlib.sha256(raw).hexdigest()
        (tmp_path / digest).write_bytes(raw)
        return ArtifactRef(digest, len(raw), "run", digest, "application/json")

    plan = artifact({"recovery": {"checkpoint_interval_ms": 1000}, "read_domain": "local"})
    code = artifact({"original_program": "available"})
    cut = CutRequest("1" * 64, 1, 0, 0, (), True)
    image = RecoveryImage("2" * 64, cut, (), {}, ()) if with_image else None
    binding = HistoricalBinding(SEALED_PLAN_FACTS_SCHEMA, plan, code, True, None, image)
    store = scenarios.OriginalCarrierReader(tmp_path)
    before = scenarios._file_digests(tmp_path)
    assert store.inspect(code)  # This unit isolates missing adapter evidence from file availability.
    report = validate_resume_compatibility(binding, current_runtime="different-runtime", store=store)
    assert not report.supported
    assert any("original engine environment" in reason for reason in report.reasons)
    assert any("journal-proven recovery image" in reason for reason in report.reasons) == (not with_image)
    assert binding.original_code_pin_ref == code and binding.original_plan_ref == plan
    assert scenarios._file_digests(tmp_path) == before


def test_original_native_history_read_refusal_and_original_runtime_resume(tmp_path):
    evidence = scenarios.original_native_history(tmp_path / "history")
    assert evidence["origin"]["commit"] == "b4dd41722130be3898a8a98057541d843777906e"
    assert evidence["cases"]["interrupted"]["original_environment_resumed"]["written_rows"] == 2
    for case in evidence["cases"].values():
        assert case["current_reference_admission"]["platform_provenance_differs"]
        assert case["current_reference_admission"]["context_entered"] is False
        assert not case["compatibility"]["supported"]
        assert case["read_only_before"] == case["read_only_after"]
    assert evidence["unsupported"]  # This does not turn the wider V029 gate green.
