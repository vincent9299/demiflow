import json

import pytest

from demiflow.forge.checks import check_project
from demiflow.forge.definition import load_definition, normalize_request, normalize_run_config
from demiflow.forge.io import read_json
from demiflow.forge.manifest import write_manifest
from demiflow.forge.scaffold import new_project
from demiflow.forge.schema import ForgeError
from forge_support import complete_project, create_project, request_value


def test_scaffold_requires_implementation_then_passes(tmp_path):
    project = create_project(tmp_path, completed=False)
    initial = check_project(project)
    assert not initial["passed"]
    assert {"operator_unimplemented", "tests_unimplemented"} <= {item["code"] for item in initial["issues"]}
    complete_project(project)
    assert check_project(project)["passed"]


def test_scaffold_refuses_nonempty_and_unresolved_requests(tmp_path):
    request = request_value()
    request["unresolved_questions"] = ["Which output table?"]
    with pytest.raises(ForgeError, match="unresolved"):
        normalize_request(request)
    project = create_project(tmp_path)
    with pytest.raises(ForgeError, match="absent or empty"):
        new_project(tmp_path / "request.json", project)


def test_extra_module_dynamic_import_and_manifest_drift_are_rejected(tmp_path):
    project = create_project(tmp_path)
    (project / "runtime.py").write_text("pass\n")
    operator = project / "operators" / "normalize_text.py"
    operator.write_text("import importlib\n" + operator.read_text())
    write_manifest(project)
    report = check_project(project)
    codes = {item["code"] for item in report["issues"]}
    assert "layout_unregistered" in codes
    assert "contract_import_forbidden" in codes
    (project / "runtime.py").unlink()
    operator.write_text(operator.read_text().replace("import importlib\n", ""))
    (project / "manifest.json").write_text("{}")
    assert "manifest_stale" in {item["code"] for item in check_project(project)["issues"]}


def test_module_scope_calls_and_optional_defaults_are_rejected(tmp_path):
    project = create_project(tmp_path)
    operator = project / "operators" / "normalize_text.py"
    operator.write_text("VALUE = normalize_text({}, uppercase=True)\n" + operator.read_text())
    assert "operator_module_scope" in {item["code"] for item in check_project(project)["issues"]}
    request = request_value()
    request["map"]["config_schema"]["required"] = []
    request["map"]["config"] = {}
    with pytest.raises(ForgeError, match="explicit default"):
        normalize_request(request)


def test_request_drift_and_run_path_escape_are_rejected(tmp_path):
    project = create_project(tmp_path)
    request = read_json(project / "request.json")
    request["goal"] = "Changed without changing the Definition"
    (project / "request.json").write_text(json.dumps(request))
    assert "request_definition_drift" in {item["code"] for item in check_project(project)["issues"]}
    definition = load_definition(project)
    config = read_json(project / "run.example.json")
    config["inputs"]["source"]["uri"] = "../outside.lance"
    with pytest.raises(ForgeError, match="datasets"):
        normalize_run_config(config, definition)


def test_operator_without_config_is_supported(tmp_path):
    request = request_value()
    request["map"]["config"] = {}
    request["map"]["config_schema"] = {
        "type": "object", "properties": {}, "required": [], "additionalProperties": False,
    }
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request))
    project = tmp_path / "no-config"
    new_project(path, project)
    operator = project / "operators" / "normalize_text.py"
    operator.write_text(operator.read_text().replace(
        "    raise NotImplementedError('TODO: implement normalize_text')",
        "    return {**row, 'text': row['text'].strip()}",
    ))
    (project / "tests" / "test_pipeline.py").write_text(
        "from operators.normalize_text import normalize_text\n\n"
        "def test_no_config():\n"
        "    assert normalize_text({'id': 1, 'text': ' x '}) == {'id': 1, 'text': 'x'}\n"
    )
    write_manifest(project)
    assert check_project(project)["passed"]
