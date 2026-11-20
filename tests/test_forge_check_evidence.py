"""Read evidence survives failed inspection through the real public workers."""

import hashlib
import json
import shutil

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.projects.discovery import discover
from demiflow.forge.source import InspectionError, SOURCE_LIMITS, SourceTree


@pytest.fixture(scope="module")
def empty_workspace(tmp_path_factory):
    root = tmp_path_factory.mktemp("evidence-platform") / "workspace"
    ws.init_workspace(root, name="evidence-fixture")
    return root


@pytest.fixture
def project(tmp_path, empty_workspace):
    repo = tmp_path / "repository"
    root = repo / "orders"
    shutil.copytree(empty_workspace, root)
    (repo / ".git").mkdir()
    (repo / ".demiforge").mkdir()

    def create(*, dependency="processing.py", native=False, text="def clean(:\n"):
        source = root / dependency
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(text)
        module = dependency.removesuffix("/__init__.py").removesuffix(".py").replace("/", ".")
        (root / "recipe.py").write_text(
            f"from {module} import clean\ndef build():\n    return clean(1)\n"
        )
        (root / "unused.py").write_text("def unused(value):\n    return value\n")
        declaration = dict(
            schema="demiforge-project-v2", standard="1.0.0-candidate.9",
            pipelines={"orders": dict(
                path="orders", entry=dict(path="orders/recipe.py", symbol="build"),
                operators=["orders/" + dependency, "orders/unused.py"], notebooks=[],
            )},
        )
        (repo / ".demiforge/project.json").write_text(json.dumps(declaration))
        if native:
            (root / "params.json").write_text(json.dumps(
                {"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}
            ))
            ws.register_flow(root, "recipe.py:build", params="params.json")
        return root, source

    return create


@pytest.mark.parametrize("native", [False, True], ids=["recognized", "native"])
@pytest.mark.parametrize("dependency", ["processing.py", "helpers/__init__.py"])
def test_syntax_failure_is_bound_and_repair_stales_old_reports(project, dependency, native):
    root, source = project(dependency=dependency, native=native)
    checked = project_ops.check_project(root)
    assert checked["status"] == checked["static_status"] == "incomplete", checked
    assert set(checked["sources"]) == {"recipe.py", dependency}
    assert checked["sources"][dependency] == hashlib.sha256(source.read_bytes()).hexdigest()
    diagnostic = next(d for d in checked["diagnostics"] if d["rule"] == "FL-01")
    assert diagnostic["status"] == "incomplete" and diagnostic["path"] == dependency
    assert diagnostic["line"] == 1
    current = project_ops.project_check_status(root)
    assert current["status"] == "incomplete" and current["changes"] == []
    record = root / ".forge/project-check.json"
    saved = record.read_bytes()
    if native:
        digest = checked["entries"][0]["digest"]
        flow = project_ops.report_status(root, digest)
        assert flow["status"] == "incomplete"
        assert flow["report"]["binding"]["sources"] == checked["sources"]

    source.write_text("def clean(value):\n    return value\n")
    stale = project_ops.project_check_status(root)
    assert stale["status"] == stale["static_status"] == "stale", stale
    assert "source_changed:" + dependency in stale["changes"]
    assert record.read_bytes() == saved  # A status read cannot rewrite old evidence.
    if native:
        assert project_ops.report_status(root, digest)["status"] == "stale"
    else:
        rechecked = project_ops.check_project(root)
        assert rechecked["source_checks"][0]["status"] == "checked"
        assert set(rechecked["sources"]) == {"recipe.py", dependency}
        assert rechecked["status"] == "incomplete"  # Flow/behavior/review remain unproved.


def test_ast_budget_failure_is_bound_before_repair(project):
    text = "VALUES = [" + "0," * SOURCE_LIMITS["file_nodes"] + "]\ndef clean(value): return value\n"
    root, source = project(text=text)
    checked = project_ops.check_project(root)
    assert checked["status"] == checked["static_status"] == "incomplete", checked
    assert checked["sources"]["processing.py"] == hashlib.sha256(source.read_bytes()).hexdigest()
    diagnostic = next(d for d in checked["diagnostics"] if d["rule"] == "FL-10")
    assert diagnostic["path"] == "processing.py" and "AST budget" in diagnostic["detail"]
    assert checked["source_checks"][0]["status"] == "incomplete"
    assert project_ops.project_check_status(root)["changes"] == []
    source.write_text("def clean(value): return value\n")
    stale = project_ops.project_check_status(root)
    assert stale["status"] == "stale" and "source_changed:processing.py" in stale["changes"]


@pytest.mark.parametrize("limit", ["file_nodes", "ast_nodes", "total_bytes"])
def test_read_evidence_does_not_turn_a_budget_failure_into_a_loaded_module(tmp_path, limit):
    source = tmp_path / "processing.py"
    source.write_text("VALUES = [0, 1, 2]\n")
    tree = SourceTree(tmp_path, {limit: 1})
    with pytest.raises(InspectionError) as error:
        tree.load("processing")
    assert error.value.diagnostic["rule"] == "FL-10"
    assert error.value.diagnostic["status"] == "incomplete"
    assert error.value.diagnostic["path"] == "processing.py"
    assert tree.files == {"processing.py": hashlib.sha256(source.read_bytes()).hexdigest()}
    assert tree.modules == {} and tree.loading == set()


@pytest.mark.parametrize("failure", ["missing", "symlink", "file_bytes"])
def test_unreadable_source_has_no_content_evidence_and_never_passes(tmp_path, failure):
    root = tmp_path / "workspace"
    root.mkdir()
    source = root / "processing.py"
    limits = None
    if failure == "symlink":
        outside = tmp_path / "outside.py"
        outside.write_text("VALUE = 1\n")
        source.symlink_to(outside)
    elif failure == "file_bytes":
        source.write_text("VALUE = 1\n")
        limits = {"file_bytes": 1}
    tree = SourceTree(root, limits)
    with pytest.raises(InspectionError) as error:
        tree.load("processing")
    assert error.value.diagnostic["rule"] == "FL-01"
    assert error.value.diagnostic["status"] == "incomplete"
    assert tree.files == tree.modules == {} and tree.bytes == 0


def test_file_budget_still_stops_before_reading_another_dependency(tmp_path):
    (tmp_path / "recipe.py").write_text("from processing import clean\n")
    (tmp_path / "processing.py").write_text("def clean(value): return value\n")
    tree = SourceTree(tmp_path, {"files": 1})
    with pytest.raises(InspectionError) as error:
        tree.load("recipe")
    assert error.value.diagnostic["rule"] == "FL-10"
    assert error.value.diagnostic["status"] == "incomplete"
    assert set(tree.files) == {"recipe.py"}


def test_declared_operator_outside_read_closure_has_existence_only_coverage(project):
    root, _ = project(text="def clean(value): return value\n")
    recognized = discover(root)
    assert recognized["coverage"]["operator_checks"] == "declared_path_existence_only"
    checked = project_ops.check_project(root)
    assert set(checked["sources"]) == {"recipe.py", "processing.py"}
    assert checked["source_checks"][0]["status"] == "checked"
    (root / "unused.py").write_text("def unused(:\n")
    assert discover(root)["digest"] == recognized["digest"]
    status = project_ops.project_check_status(root)
    assert status["status"] == "incomplete" and status["changes"] == []


def test_documented_check_commands_reach_the_real_worker_without_json_option(project):
    root, _ = project(text="def clean(value): return value\n")
    for command in ("check", "check-status"):
        result = CliRunner().invoke(main, ["workbench", command, str(root)])
        assert result.exit_code == 1, result.output  # Missing obligations, not option rejection.
        value = json.loads(result.output)
        assert value["format"] == "demiflow_forge_project_check_v1"
        assert value["status"] == "incomplete"
        assert set(value["sources"]) == {"recipe.py", "processing.py"}
