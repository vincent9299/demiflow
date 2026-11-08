import pyarrow as pa
import pytest

from demiflow import data
from demiflow.forge.runner import run_project
from demiflow.forge.schema import ForgeError
from forge_support import create_project, review_and_freeze


def test_frozen_runner_reads_and_writes_real_lance(tmp_path):
    project = create_project(tmp_path)
    with pytest.raises(ForgeError, match="frozen"):
        run_project(project, project / "run.example.json")
    review_and_freeze(project)
    (project / "datasets").mkdir()
    data.from_items([{"id": 1, "text": " hello "}, {"id": 2, "text": " World "}]).write_lance(
        str(project / "datasets" / "input.lance"), mode="overwrite",
    )
    result = run_project(project, project / "run.example.json")
    version = result["receipt"]["committed_version"]
    assert data.read_lance(str(project / "datasets" / "output.lance"), version=version).take_all() == [
        {"id": 1, "text": "HELLO"}, {"id": 2, "text": "WORLD"},
    ]


def test_frozen_runner_supports_empty_input_with_declared_schema(tmp_path):
    project = create_project(tmp_path)
    review_and_freeze(project)
    (project / "datasets").mkdir()
    data.from_items([]).write_lance(
        str(project / "datasets" / "input.lance"), mode="overwrite",
        schema=pa.schema([("id", pa.int64()), ("text", pa.string())]),
    )
    result = run_project(project, project / "run.example.json")
    output = data.read_lance(str(project / "datasets" / "output.lance"), version=result["receipt"]["committed_version"])
    assert output.take_all() == []
    assert output.count() == 0


def test_runner_rejects_stale_freeze_and_operator_schema_failure(tmp_path):
    project = create_project(tmp_path)
    review_and_freeze(project)
    tests = project / "tests" / "test_pipeline.py"
    tests.write_text(tests.read_text() + "\n# stale\n")
    with pytest.raises(ForgeError, match="frozen"):
        run_project(project, project / "run.example.json")
    project = create_project(tmp_path / "bad-output")
    operator = project / "operators" / "normalize_text.py"
    operator.write_text(operator.read_text().replace("return {**row, 'text': text.upper() if uppercase else text}", "return {'id': 'wrong', 'text': 3}"))
    from demiflow.forge.manifest import write_manifest
    write_manifest(project)
    (project / "tests" / "test_pipeline.py").write_text(
        "from operators.normalize_text import normalize_text\n\n"
        "def test_operator_loads():\n"
        "    assert callable(normalize_text)\n"
    )
    review_and_freeze(project)
    (project / "datasets").mkdir()
    data.from_items([{"id": 1, "text": " hello "}]).write_lance(str(project / "datasets" / "input.lance"), mode="overwrite")
    with pytest.raises(ForgeError, match="expected"):
        run_project(project, project / "run.example.json")


def test_runner_requires_review_evidence_and_rejects_output_symlink(tmp_path):
    project = create_project(tmp_path)
    review_and_freeze(project)
    from demiflow.forge.store import evidence_root
    (evidence_root(project) / "review.json").unlink()
    with pytest.raises(ForgeError, match="Review"):
        run_project(project, project / "run.example.json")
    project = create_project(tmp_path / "symlink")
    review_and_freeze(project)
    (project / "datasets").mkdir()
    data.from_items([{"id": 1, "text": " hello "}]).write_lance(str(project / "datasets" / "input.lance"), mode="overwrite")
    outside = tmp_path / "outside.lance"
    data.from_items([]).write_lance(str(outside), mode="overwrite", schema=pa.schema([("id", pa.int64()), ("text", pa.string())]))
    (project / "datasets" / "output.lance").symlink_to(outside)
    with pytest.raises(ForgeError, match="Symlink"):
        run_project(project, project / "run.example.json")


def test_runner_uses_the_same_project_lock_as_verification(tmp_path):
    from demiflow.forge.store import locked

    project = create_project(tmp_path)
    review_and_freeze(project)
    with locked(project):
        with pytest.raises(ForgeError, match="busy"):
            run_project(project, project / "run.example.json")
