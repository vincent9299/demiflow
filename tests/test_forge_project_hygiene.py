"""Tests for full-project DF-02 structural hygiene checks and safe host layout tools."""
import json
import pytest

from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import initialize_project, register_flow
from demiflow.workbench.conversations.context import _run_project_layout


def _make_clean_project(root):
    initialize_project(root)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'operators').mkdir(exist_ok=True)
    (root / 'prompts').mkdir(exist_ok=True)
    (root / 'configs').mkdir(exist_ok=True)
    (root / 'data_tools').mkdir(exist_ok=True)
    (root / 'notebooks').mkdir(exist_ok=True)
    (root / 'data').mkdir(exist_ok=True)
    (root / 'runs' / 'historical_20260101').mkdir(parents=True, exist_ok=True)
    (root / 'runs' / 'historical_20260101' / 'receipt.json').write_text('{"ok": true}\n')

    (root / 'configs' / 'deployment.json').write_text(json.dumps({
        'prompt': 'active.yaml',
    }))
    (root / 'prompts' / 'active.yaml').write_text(
        'version: 1\nprompts:\n  p:\n    model: {name: demo}\n    user: "{{x}}"\n'
    )
    (root / 'operators' / 'ops.py').write_text(
        '__demiflow_operators__ = ["keep"]\n'
        'def keep(row):\n'
        '    """Keep row."""\n'
        '    return row\n'
    )
    (root / 'operators' / 'standalone_op.py').write_text(
        '__demiflow_operators__ = ["extra"]\n'
        'def extra(row):\n'
        '    """Standalone declared operator."""\n'
        '    return row\n'
    )
    (root / 'data_tools' / 'viewer.py').write_text(
        '__demiflow_data_tools__ = {"show": "view"}\n'
        'def show(rows):\n'
        '    return rows\n'
    )
    (root / 'notebooks' / 'explore.ipynb').write_text('{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}\n')
    (root / 'flows' / 'demo.py').write_text(
        'import json\n'
        'from pathlib import Path\n'
        'from demiflow import data\n'
        'from operators import ops\n'
        'ROOT = Path(__file__).resolve().parent.parent\n'
        'def run(*, output_directory):\n'
        '    cfg = json.loads((ROOT / "configs" / "deployment.json").read_text())\n'
        '    data.require(bool(cfg["prompt"]), "need prompt")\n'
        '    return data.from_items([{"id": 1, "out": output_directory}]).map(ops.keep).count()\n'
    )
    (root / 'configs' / 'demo.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'output_directory': str(root / 'data' / 'demo')},
    }))
    register_flow(root, 'flows/demo.py:run', params='configs/demo.json')
    return root


def test_clean_project_passes_full_hygiene_check(tmp_path):
    root = _make_clean_project(tmp_path / 'clean_proj')
    report = check_project(root)
    assert report['status'] == 'passed', report
    assert report['project_layout']['status'] == 'passed', report['project_layout']
    assert report['diagnostic_count'] == 0


def test_messy_project_fails_every_df02_hygiene_category(tmp_path):
    root = _make_clean_project(tmp_path / 'messy_proj')
    # Introduce all clutter categories
    (root / 'root_review.ipynb').write_text('{"cells": [{"outputs": ["keep"]}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}\n')
    (root / 'deployment.json').write_text('{"legacy": true}\n')
    (root / 'run_legacy.py').write_text('print("legacy")\n')
    (root / 'parameters').mkdir()
    (root / 'parameters' / 'old.json').write_text('{"format": "demiflow_forge_flow_parameters_v1", "arguments": {}}\n')
    (root / 'flows' / 'dead_flow.py').write_text('def run():\n    return 1\n')
    (root / 'operators' / 'notebook_helper.py').write_text('def view_it():\n    return 1\n')
    (root / 'operators' / 'template.html').write_text('<div>hi</div>\n')
    (root / 'data_tools' / 'undeclared.py').write_text('def helper():\n    return 1\n')
    (root / 'configs' / 'unused_exp.json').write_text('{"x": 1}\n')
    (root / 'configs' / 'old_batch').mkdir()
    (root / 'configs' / 'old_batch' / 'a.json').write_text('{"a": 1}\n')
    (root / 'configs' / 'old_batch' / 'b.json').write_text('{"b": 2}\n')
    (root / 'prompts' / 'obsolete.yaml').write_text('version: 1\n')
    # Point active param output_directory into legacy runs/
    (root / 'configs' / 'demo.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'output_directory': str(root / 'runs' / 'legacy_out')},
    }))

    report = check_project(root)
    assert report['status'] == 'failed'
    assert report['native_flow']['status'] == 'passed'
    assert report['project_layout']['status'] == 'failed'
    flagged_paths = {d['path'] for d in report['diagnostics'] if d['rule'] == 'DF-02'}
    assert 'root_review.ipynb' in flagged_paths
    assert 'deployment.json' in flagged_paths
    assert 'run_legacy.py' in flagged_paths
    assert 'parameters/old.json' in flagged_paths
    assert 'flows/dead_flow.py' in flagged_paths
    assert 'operators/notebook_helper.py' in flagged_paths
    assert 'operators/template.html' in flagged_paths
    assert 'data_tools/undeclared.py' in flagged_paths
    assert 'configs/unused_exp.json' in flagged_paths
    assert 'configs/old_batch' in flagged_paths  # collapsed to directory
    assert 'prompts/obsolete.yaml' in flagged_paths
    assert 'configs/demo.json' in flagged_paths
    # Historical runs/ itself must NOT be flagged
    assert not any(p.startswith('runs/') or p == 'runs' for p in flagged_paths)


def test_manage_project_layout_move_and_remove_safety_rails(tmp_path):
    root = _make_clean_project(tmp_path / 'layout_proj')
    nb_bytes = b'{"cells": [{"outputs": ["precious"]}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}\n'
    (root / 'legacy_nb.ipynb').write_bytes(nb_bytes)
    (root / 'legacy_cfg.json').write_text('{"ok": 1}\n')
    (root / 'legacy_script.py').write_text('x = 1\n')
    (root / 'parameters').mkdir()
    (root / 'parameters' / 'old.json').write_text('{}\n')
    (root / 'configs' / 'dead_dir').mkdir()
    (root / 'configs' / 'dead_dir' / 'x.json').write_text('{}\n')
    (root / 'configs' / 'dir_with_nb').mkdir()
    (root / 'configs' / 'dir_with_nb' / 'keep.ipynb').write_bytes(nb_bytes)

    # move_path preserves bytes and moves into standard dirs
    moved_nb = _run_project_layout(root, {'action': 'move_path', 'src': 'legacy_nb.ipynb', 'dst': 'notebooks/legacy_nb.ipynb'})
    assert moved_nb == {'status': 'moved', 'src': 'legacy_nb.ipynb', 'dst': 'notebooks/legacy_nb.ipynb'}
    assert (root / 'notebooks' / 'legacy_nb.ipynb').read_bytes() == nb_bytes
    assert not (root / 'legacy_nb.ipynb').exists()

    moved_cfg = _run_project_layout(root, {'action': 'move_path', 'src': 'legacy_cfg.json', 'dst': 'configs/legacy_cfg.json'})
    assert moved_cfg['status'] == 'moved' and (root / 'configs' / 'legacy_cfg.json').is_file()

    # move_path rejects overwriting existing destination or protected files
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'workspace.json', 'dst': 'configs/ws.json'})
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'configs/legacy_cfg.json', 'dst': 'configs/demo.json'})
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'configs/legacy_cfg.json', 'dst': 'data/legacy_cfg.json'})

    # remove_path deletes obsolete non-data files and directories cleanly
    removed = _run_project_layout(root, {
        'action': 'remove_path',
        'paths': ['legacy_script.py', 'parameters/old.json', 'configs/dead_dir', 'configs/legacy_cfg.json'],
    })
    assert removed['status'] == 'removed'
    assert not (root / 'legacy_script.py').exists()
    assert not (root / 'parameters' / 'old.json').exists()
    assert not (root / 'configs' / 'dead_dir').exists()

    # Empty legacy parameters/ dir is flagged by hygiene and removable via remove_path
    rep_empty_params = check_project(root)
    assert any(d['path'] == 'parameters' for d in rep_empty_params['diagnostics'])
    _run_project_layout(root, {'action': 'remove_path', 'paths': ['parameters']})
    assert not (root / 'parameters').exists()

    # remove_path STRICTLY blocks notebooks, data, runs, registered entries, and dirs containing notebooks
    for forbidden in (
        'notebooks/legacy_nb.ipynb',
        'notebooks',
        'runs',
        'runs/historical_20260101/receipt.json',
        'data',
        'workspace.json',
        'README.md',
        'flows/demo.py',
        'configs/demo.json',
        'configs/dir_with_nb',
    ):
        with pytest.raises(Exception):
            _run_project_layout(root, {'action': 'remove_path', 'paths': [forbidden]})
    assert (root / 'notebooks' / 'legacy_nb.ipynb').read_bytes() == nb_bytes
    assert (root / 'runs' / 'historical_20260101' / 'receipt.json').is_file()
