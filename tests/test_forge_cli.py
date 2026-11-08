import json

from forge_support import complete_project, create_project, request_value, run_cli


def test_cli_help_and_legacy_run_contract_remain_available():
    code, payload, _ = run_cli("--help")
    assert code == 0
    assert "forge" in payload
    code, payload, _ = run_cli("run", "--help")
    assert code == 0
    assert "--root" in payload
    assert "--backend" in payload


def test_cli_check_status_and_json_errors(tmp_path):
    project = create_project(tmp_path)
    code, payload, _ = run_cli("forge", "check", project, "--json")
    assert code == 0
    assert payload["passed"] is True
    code, payload, _ = run_cli("forge", "status", project, "--json")
    assert code == 0
    assert payload["state"] == "implemented"
    code, payload, error = run_cli("forge", "run", project, "--config", project / "run.example.json", "--json")
    assert code == 1
    assert "frozen" in error


def test_cli_complete_lifecycle_and_real_run(tmp_path):
    from demiflow import data

    request = tmp_path / "request.json"
    request.write_text(json.dumps(request_value()))
    project = tmp_path / "cli-project"
    code, _, _ = run_cli("forge", "new", request, "--out", project, "--json")
    assert code == 0
    complete_project(project)
    assert run_cli("forge", "manifest", project, "--json")[0] == 0
    code, verification, _ = run_cli("forge", "verify", project, "--json")
    assert code == 0
    review = tmp_path / "review.json"
    review.write_text(json.dumps({
        "schema_version": "demiflow_forge_review_v1",
        "revision": verification["revision"],
        "reviewer": "cli-reviewer",
        "review_type": "independent",
        "status": "passed",
        "summary": "CLI lifecycle reviewed.",
        "findings": [],
    }))
    assert run_cli("forge", "review", project, "--file", review, "--json")[0] == 0
    assert run_cli("forge", "freeze", project, "--json")[0] == 0
    (project / "datasets").mkdir()
    data.from_items([{"id": 1, "text": " cli "}]).write_lance(
        str(project / "datasets" / "input.lance"), mode="overwrite",
    )
    code, result, _ = run_cli("forge", "run", project, "--config", project / "run.example.json", "--json")
    assert code == 0
    rows = data.read_lance(
        str(project / "datasets" / "output.lance"),
        version=result["receipt"]["committed_version"],
    ).take_all()
    assert rows == [{"id": 1, "text": "CLI"}]
