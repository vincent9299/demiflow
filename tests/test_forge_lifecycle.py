import json

import pytest

from demiflow.forge.lifecycle import freeze_project, review_project, status
from demiflow.forge.schema import ForgeError
from demiflow.forge.verify import verify_project
from forge_support import create_project, review_and_freeze


def test_lifecycle_gates_and_stale_freeze(tmp_path):
    project = create_project(tmp_path)
    assert status(project)["state"] == "implemented"
    with pytest.raises(ForgeError, match="passing verification"):
        review_project(project, tmp_path / "missing.json")
    review_and_freeze(project)
    current = status(project)
    assert current["state"] == "frozen"
    assert all(value == "passed" for value in current["gates"].values())
    tests = project / "tests" / "test_pipeline.py"
    tests.write_text(tests.read_text() + "\n# source changed\n")
    stale = status(project)
    assert stale["state"] == "implemented"
    assert stale["gates"]["verification"] == "stale"
    assert stale["gates"]["freeze"] == "stale"


def test_review_requires_current_revision_and_no_blockers(tmp_path):
    project = create_project(tmp_path)
    verification = verify_project(project)
    assert verification["passed"]
    value = {
        "schema_version": "demiflow_forge_review_v1",
        "revision": verification["revision"],
        "reviewer": "reviewer",
        "review_type": "self",
        "status": "passed",
        "summary": "Self review only.",
        "findings": [{
            "path": "pipeline.py", "line": 1, "severity": "blocker",
            "message": "Unresolved contract problem.", "evidence": "pipeline.py",
        }],
    }
    path = tmp_path / "review.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ForgeError, match="blockers"):
        review_project(project, path)
    value["findings"] = []
    path.write_text(json.dumps(value))
    assert review_project(project, path)["review_type"] == "self"
    assert freeze_project(project)["status"] == "frozen"


def test_corrupt_verification_log_blocks_current_evidence(tmp_path):
    from pathlib import Path
    from demiflow.forge.store import read_record
    from demiflow.forge.lifecycle import require_frozen

    project = create_project(tmp_path)
    review_and_freeze(project)
    verification = read_record(project, "verification")
    log = verification["test"]["logs"]["stdout"]
    Path(log["path"]).write_text("tampered")
    assert status(project)["gates"]["verification"] == "corrupt"
    with pytest.raises(ForgeError, match="frozen"):
        require_frozen(project)
