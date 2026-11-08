from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from demiflow.forge.lifecycle import freeze_project, review_project
from demiflow.forge.manifest import write_manifest
from demiflow.forge.scaffold import new_project
from demiflow.forge.verify import verify_project


def object_schema(properties=None, required=None):
    properties = properties or {}
    return {
        "type": "object",
        "properties": properties,
        "required": required if required is not None else list(properties),
        "additionalProperties": False,
    }


def request_value(*, timeout=60):
    row_schema = object_schema({"id": {"type": "integer"}, "text": {"type": "string"}})
    return {
        "schema_version": "demiflow_forge_request_v1",
        "name": "text-map",
        "version": "1.0.0",
        "source_text": "Normalize text in local Lance rows.",
        "goal": "Trim and uppercase text while preserving identity.",
        "row_grain": "One record per id and text pair.",
        "input": {"name": "source", "schema": row_schema},
        "output": {"name": "result", "mode": "overwrite", "schema": row_schema},
        "map": {
            "name": "normalize_text",
            "version": "1.0.0",
            "description": "Trim and optionally uppercase text.",
            "config": {"uppercase": True},
            "config_schema": object_schema({"uppercase": {"type": "boolean"}}),
        },
        "side_effects": {"model_calls": False, "network": False, "external_writes": False},
        "budgets": {"max_rows": 100, "test_timeout_seconds": timeout},
        "completion": "All selected rows are transformed and committed.",
        "unresolved_questions": [],
    }


def create_project(tmp_path: Path, *, completed=True, timeout=60):
    tmp_path.mkdir(parents=True, exist_ok=True)
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request_value(timeout=timeout)), encoding="utf-8")
    project = tmp_path / "text-map"
    new_project(request_path, project)
    if completed:
        complete_project(project)
    return project


def complete_project(project: Path) -> None:
    operator = project / "operators" / "normalize_text.py"
    operator.write_text(operator.read_text(encoding="utf-8").replace(
        "    raise NotImplementedError('TODO: implement normalize_text')",
        "    text = row['text'].strip()\n"
        "    return {**row, 'text': text.upper() if uppercase else text}",
    ), encoding="utf-8")
    (project / "tests" / "test_pipeline.py").write_text(
        "from operators.normalize_text import normalize_text\n\n"
        "def test_normalize():\n"
        "    assert normalize_text({'id': 1, 'text': ' hello '}, uppercase=True) == {'id': 1, 'text': 'HELLO'}\n",
        encoding="utf-8",
    )
    write_manifest(project)


def review_and_freeze(project: Path):
    verification = verify_project(project)
    assert verification["passed"], verification
    review_path = project.parent / "review.json"
    review_path.write_text(json.dumps({
        "schema_version": "demiflow_forge_review_v1",
        "revision": verification["revision"],
        "reviewer": "fixture-reviewer",
        "review_type": "independent",
        "status": "passed",
        "summary": "Implementation, contract, and behavior tests reviewed.",
        "findings": [],
    }), encoding="utf-8")
    review_project(project, review_path)
    return freeze_project(project)


def run_cli(*args):
    result = subprocess.run(
        [sys.executable, "-B", "-m", "demiflow.cli", *map(str, args)],
        text=True, capture_output=True, check=False,
    )
    try:
        payload = json.loads(result.stdout) if result.stdout else {}
    except json.JSONDecodeError:
        payload = result.stdout
    return result.returncode, payload, result.stderr
