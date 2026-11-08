import shutil
from pathlib import Path

from demiflow import data
from demiflow.forge.runner import run_project
from demiflow.forge.scaffold import new_project
from demiflow.forge.manifest import write_manifest
from forge_support import review_and_freeze


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "forge_text_map"


def test_canonical_example_end_to_end(tmp_path):
    project = tmp_path / "forge_text_map"
    # Published example bindings describe their original platform. Exercise the
    # unchanged candidate against fresh bindings without rewriting old evidence.
    new_project(EXAMPLE / "request.json", project)
    for name in ("pipeline.py", "run.example.json", "README.md"):
        shutil.copy2(EXAMPLE / name, project / name)
    for name in ("operators", "tests"):
        shutil.copytree(EXAMPLE / name, project / name, dirs_exist_ok=True)
    write_manifest(project)
    review_and_freeze(project)
    (project / "datasets").mkdir()
    data.from_items([{"id": 1, "text": "  Snow Leopard  "}]).write_lance(
        str(project / "datasets" / "input.lance"), mode="overwrite",
    )
    result = run_project(project, project / "run.example.json")
    rows = data.read_lance(
        str(project / "datasets" / "output.lance"),
        version=result["receipt"]["committed_version"],
    ).take_all()
    assert rows == [{"id": 1, "text": "SNOW LEOPARD"}]
