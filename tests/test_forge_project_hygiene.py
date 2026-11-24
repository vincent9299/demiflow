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


def test_hygiene_flags_notebook_ui_in_operators_and_dangling_test_imports(tmp_path):
    root = _make_clean_project(tmp_path / 'my_proj')
    (root / 'tests').mkdir(exist_ok=True)
    # Valid test passes cleanly
    (root / 'tests' / 'test_ok.py').write_text('from flows import demo\nfrom operators import ops\n')
    assert check_project(root)['status'] == 'passed'

    # Operator importing IPython/ipywidgets must be flagged even if __demiflow_operators__ is declared
    (root / 'operators' / 'widget_view.py').write_text(
        'import ipywidgets as widgets\n'
        '__demiflow_operators__ = ["show"]\n'
        'def show():\n'
        '    return widgets\n'
    )
    # Test importing deleted/moved project modules must be flagged
    (root / 'tests' / 'test_broken_direct.py').write_text('from flows.deleted_flow import run\n')
    (root / 'tests' / 'test_broken_pkg.py').write_text(f'from {tmp_path.name}.my_proj.operators import moved_mod\n')

    report = check_project(root)
    assert report['status'] == 'failed'
    flagged = {d['path']: d['detail'] for d in report['diagnostics'] if d['rule'] == 'DF-02'}
    assert 'operators/widget_view.py' in flagged and 'IPython/ipywidgets' in flagged['operators/widget_view.py']
    assert 'tests/test_broken_direct.py' in flagged and 'flows.deleted_flow' in flagged['tests/test_broken_direct.py']
    assert 'tests/test_broken_pkg.py' in flagged and 'moved_mod' in flagged['tests/test_broken_pkg.py']
    assert 'tests/test_ok.py' not in flagged


def test_hygiene_keeps_prompt_referenced_by_alternate_branch_declared_operator(tmp_path):
    root = _make_clean_project(tmp_path / 'branch_proj')
    (root / 'prompts' / 'alt_schema').mkdir(parents=True)
    (root / 'prompts' / 'alt_schema' / 'response.json').write_text('{"type": "object"}\n')
    (root / 'operators' / 'alt_op.py').write_text(
        '__demiflow_operators__ = ["adapt"]\n'
        'SCHEMA_REF = "prompts/alt_schema/response.json"\n'
        'def adapt(row):\n'
        '    return row\n'
    )
    report = check_project(root)
    assert report['status'] == 'passed', report
    assert report['diagnostic_count'] == 0


def test_move_path_blocks_moving_notebooks_out_and_docs_py_is_flagged(tmp_path):
    root = _make_clean_project(tmp_path / 'nb_guard_proj')
    nb_bytes = b'{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}\n'
    (root / 'root_nb.ipynb').write_bytes(nb_bytes)
    # Moving a root notebook into operators/ must be rejected
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'root_nb.ipynb', 'dst': 'operators/root_nb.ipynb'})
    # Moving a root notebook into notebooks/ succeeds
    moved = _run_project_layout(root, {'action': 'move_path', 'src': 'root_nb.ipynb', 'dst': 'notebooks/root_nb.ipynb'})
    assert moved['status'] == 'moved'
    # Moving any file OUT of notebooks/ must be rejected
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'notebooks/root_nb.ipynb', 'dst': 'operators/root_nb.ipynb'})
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'move_path', 'src': 'notebooks/root_nb.ipynb', 'dst': 'notebooks/renamed.ipynb'})

    # Smuggling obsolete .py code, archives, or mirrored role dirs under docs/ must fail DF-02
    (root / 'docs' / 'legacy_dump').mkdir(parents=True)
    (root / 'docs' / 'legacy_dump' / 'old_flow.py').write_text('def old():\n    return 1\n')
    (root / 'docs' / 'legacy_archive').mkdir(parents=True)
    (root / 'docs' / 'legacy_archive' / 'old_flows.tar.gz').write_bytes(b'fake-tar')
    (root / 'docs' / 'legacy_cfg' / 'configs').mkdir(parents=True)
    (root / 'docs' / 'legacy_cfg' / 'configs' / 'exp.json').write_text('{"x": 1}\n')
    (root / 'docs' / 'legacy_empty' / 'operators').mkdir(parents=True)
    # Legitimate verification evidence under docs/ must NOT be flagged
    (root / 'docs' / 'migration-20261118').mkdir(parents=True)
    (root / 'docs' / 'migration-20261118' / 'REPORT.md').write_text('# Report\n')
    (root / 'docs' / 'migration-20261118' / 'proof.json').write_text('{"ok": true}\n')
    (root / 'docs' / 'migration-20261118' / 'check.log').write_text('passed\n')
    (root / 'old_backup.tar.gz').write_bytes(b'fake-root-tar')

    report = check_project(root)
    assert report['status'] == 'failed'
    flagged = {d['path'] for d in report['diagnostics'] if d['rule'] == 'DF-02'}
    assert 'docs/legacy_dump' in flagged
    assert 'docs/legacy_archive' in flagged
    assert 'docs/legacy_cfg' in flagged
    assert 'docs/legacy_empty' in flagged
    assert 'old_backup.tar.gz' in flagged
    assert not any(p.startswith('docs/migration-20261118') for p in flagged)


def test_replay_anchors_user_message_on_long_turn(tmp_path):
    from demiflow.workbench.conversations.policy import Limits, root_identity
    from demiflow.workbench.conversations.store import Store

    proj = tmp_path / 'proj'
    proj.mkdir()
    ident = root_identity(proj)
    store = Store(tmp_path / 'replay_state', Limits(), initialize=True)
    sess = store.create('w1', ident, 'deepseek', 'req1', 'pol1')

    # Turn 1: long turn with many tool calls + final reply
    t1, _ = store.submit('w1', sess['id'], 'sub1', sess['revision'], '第一轮：帮我整理项目', ident, 'pol1')
    for i in range(40):
        store.event(t1['id'], 'item.updated', {'id': f'call_1_{i}', 'type': 'tool', 'title': 'bash', 'status': 'completed'})
    store.event(t1['id'], 'message.completed', {'item_id': 'a1', 'text': '第一轮已完成'})
    store.event(t1['id'], 'turn.state', {'state': 'completed', 'cleanup': 'complete'})
    store.update(t1['id'], state='completed', cleanup='complete')

    # Turn 2: long turn with 150 separate change.observed events after final answer
    t2, _ = store.submit('w1', sess['id'], 'sub2', sess['revision'] + 1, '第二轮：继续检查干净', ident, 'pol1')
    for i in range(30):
        store.event(t2['id'], 'item.updated', {'id': f'call_2_{i}', 'type': 'tool', 'title': 'bash', 'status': 'completed'})
    store.event(t2['id'], 'message.completed', {'item_id': 'a2', 'text': '第二轮全部清理完毕'})
    for i in range(150):
        store.event(t2['id'], 'change.observed', {'files': [{'path': f'f_{i}.py', 'diff': 'x' * 500}]})
    store.event(t2['id'], 'turn.state', {'state': 'completed', 'cleanup': 'complete'})
    store.update(t2['id'], state='completed', cleanup='complete')

    # Initial replay must coalesce the 150 change.observed events so Turn 2's user message
    # AND final assistant answer both appear in a small limit window.
    replayed = store.replay('w1', sess['id'], limit=20)
    user_texts = [ev['data']['text'] for ev in replayed['events'] if ev['type'] == 'message.user']
    ans_texts = [ev['data']['text'] for ev in replayed['events'] if ev['type'] == 'message.completed']
    change_evs = [ev for ev in replayed['events'] if ev['type'] == 'change.observed']
    assert '第二轮：继续检查干净' in user_texts
    assert '第二轮全部清理完毕' in ans_texts
    assert len(change_evs) == 1

    # Even with a tiny limit forcing upward pagination into an earlier long turn,
    # before_sequence replay MUST still anchor message.user for turns present in that page.
    t3, _ = store.submit('w1', sess['id'], 'sub3', sess['revision'] + 2, '第三轮：测试分页锚定', ident, 'pol1')
    for i in range(60):
        store.event(t3['id'], 'item.updated', {'id': f'call_3_{i}', 'type': 'tool', 'title': 'bash', 'status': 'completed'})
    store.event(t3['id'], 'message.completed', {'item_id': 'a3', 'text': '第三轮回复'})
    store.event(t3['id'], 'turn.state', {'state': 'running', 'cleanup': 'pending'})
    # Active (non-complete) Turn 3 keeps all 60 events verbatim, pushing Turn 3's user msg out of limit=10
    active_tail = store.replay('w1', sess['id'], limit=10)
    assert any(ev['type'] == 'message.user' and ev['data']['text'] == '第三轮：测试分页锚定' for ev in active_tail['events'])
    assert active_tail['first_sequence'] > next(ev['sequence'] for ev in active_tail['events'] if ev['type'] == 'message.user')
    # Upward pagination into Turn 3's middle events must ALSO anchor its message.user
    older_page = store.replay('w1', sess['id'], limit=10, before_sequence=active_tail['first_sequence'])
    assert any(ev['type'] == 'message.user' and ev['data']['text'] == '第三轮：测试分页锚定' for ev in older_page['events'])
