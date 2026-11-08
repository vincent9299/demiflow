"""Template/admission refusal only; no resource grants are fabricated."""
import json
from pathlib import Path

import pytest

from demiflow.workbench.analysis import deployment
from demiflow.forge.schema import ForgeError


def test_old_template_inspection_is_read_only_and_does_not_claim_readiness(tmp_path):
    value = dict(format="demiflow_analysis_executor_binding_v1", site={}, owner_release={})
    path=tmp_path/"binding.json";path.write_text(json.dumps(value));before=path.read_bytes()
    result=deployment.inspect_binding(path)
    assert not result["activation_allowed"] and not result["jobs_started"] and result["missing_site_fields"]
    assert "allowed_web_uids" in result["missing_site_fields"]
    assert "helper_sha256" in result["missing_owner_release_fields"]
    assert path.read_bytes()==before and list(tmp_path.iterdir())==[path]
    with pytest.raises(ForgeError):
        deployment.Deployment.load(path)


def test_no_partial_or_candidate_binding_can_start_production_or_calibration(tmp_path):
    path=tmp_path/"binding.json"
    value=dict(format=deployment.FORMAT,binding_status="TEMPLATE_NOT_GRANTED",
        activation=dict(enabled=False,calibration_allowed=False,resource_candidate_is_authorization=False,calibration_receipt=None),
        profile_sha256=deployment.profile()[1],site={key:None for key in deployment.SITE_FIELDS},
        owner_release={key:None for key in deployment.RELEASE_FIELDS})
    path.write_text(json.dumps(value));path.chmod(0o600)
    for mode in ("production","calibration"):
        with pytest.raises(ForgeError,match="incomplete"):
            deployment.Deployment.load(path,mode=mode)


def test_hashed_executable_release_input_cannot_be_mutable_or_changed(tmp_path):
    import hashlib
    path=tmp_path/"runtime";path.write_bytes(b"owned fixture");path.chmod(0o555)
    sha=hashlib.sha256(path.read_bytes()).hexdigest()
    deployment._hash_file(path,sha,executable=True)
    path.chmod(0o777)
    with pytest.raises(ForgeError,match="mutable"):
        deployment._hash_file(path,sha,executable=True)
    path.chmod(0o555)
    with pytest.raises(ForgeError,match="differs"):
        deployment._hash_file(path,"0"*64,executable=True)
