"""Create small owned fixtures. Candidate flows are copied, never executed.

Run with the same installed environment as the Forge CLI:
  python examples/forge_workspace/make_demo.py /tmp/forge-stage2-demo
"""

from demiflow.forge.project import initialize_project
from pathlib import Path
import json
import shutil
import sys


def create(root):
    import lance
    import pyarrow as pa
    from PIL import Image, ImageDraw
    from demiflow.workbench.projects import workspace
    from demiflow.workbench import project_operations as project_ops

    root = Path(root).absolute()
    initialize_project(root, name="question-experiments")
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    control = workspace.read_control(root / 'workspace.json')
    expected = workspace.content_digest(control)
    # This fixture represents an existing project with two published configurations.
    control['entries'] = {'flows/questions.py:select': ['flows/select_a.json', 'flows/select_b.json']}
    control['locations']['local'] = {'root': 'data', 'writable': True}
    workspace.write_control(root / 'workspace.json', control, expected=expected)
    source = Path(__file__).parent
    for directory in ("flows", "operators"):
        for path in (source / directory).iterdir():
            if path.suffix in {".py", ".json"}:
                shutil.copyfile(path, root / directory / path.name)
    shutil.copyfile(source / "README.md", root / "README.md")
    media = root / "data/media"
    media.mkdir()
    image = Image.new("RGB", (640, 400), "#f0f7f5")
    draw = ImageDraw.Draw(image)
    draw.polygon(
        [(100, 310), (540, 310), (320, 70)], fill="#c9e3dc", outline="#286f66", width=5
    )
    draw.line([(320, 70), (320, 310)], fill="#719b95", width=3)
    draw.text((285, 330), "base = 12", fill="#183e3c")
    draw.text((335, 200), "h = 8", fill="#183e3c")
    image.save(media / "triangle.png")
    candidate_schema = pa.schema(
        [
            pa.field("case_id", pa.string(), nullable=False),
            pa.field("question", pa.string()),
            pa.field("image_uri", pa.string()),
        ]
    )
    score_schema = pa.schema(
        [
            pa.field("case_id", pa.string(), nullable=False),
            pa.field("score", pa.float64(), nullable=False),
        ]
    )
    candidates = [
        {
            "case_id": "c01",
            "question": "  三角形的面积是多少？  ",
            "image_uri": "media/triangle.png",
        },
        {"case_id": "c02", "question": "增长率是多少？", "image_uri": None},
        {"case_id": "c03", "question": "这个结论一定成立吗？", "image_uri": None},
    ]
    scores = [
        {"case_id": key, "score": value}
        for key, value in [("c01", 0.95), ("c02", 0.84), ("c03", 0.72)]
    ]
    # An independent comparison fixture, explicitly not produced by the Flow.
    comparison = [
        {
            "case_id": "c01",
            "question": "三角形面积 = 12 × 8 ÷ 2 = 48。",
            "image_uri": "media/triangle.png",
        },
        {"case_id": "c02", "question": "审核版本一", "image_uri": None},
        {"case_id": "c02", "question": "审核版本二（重复业务键）", "image_uri": None},
    ]
    for alias, rows, schema, version in [
        ("candidates", candidates, candidate_schema, 3),
        ("scores", scores, score_schema, 2),
        ("comparison_fixture", comparison, candidate_schema, 1),
    ]:
        for index in range(version):
            lance.write_dataset(
                pa.Table.from_pylist(rows, schema=schema),
                str(root / "data" / (alias + ".lance")),
                mode="create" if index == 0 else "overwrite",
            )
        project_ops.attach_table(
            root,
            alias,
            relative_uri=alias + ".lance",
            version=version,
            dataset_id=alias,
            schema_name=alias,
            schema_version="1",
        )
    reports = {}
    for variant in ("a", "b"):
        report = project_ops.inspect_flow(
            root, "flows/questions.py:select", params=f"flows/select_{variant}.json"
        )
        if report["status"] != "checked":
            raise RuntimeError(report)
        reports[variant] = report["digest"]
    assert not (root / "data/selected_a.lance").exists()
    assert not (root / "data/selected_b.lance").exists()
    return {"workspace": str(root), "reports": reports, "candidate_executed": False}


if __name__ == "__main__":
    print(json.dumps(create(sys.argv[1]), ensure_ascii=False, indent=2))
