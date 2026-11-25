"""Regression tests for the 2026-11-25 consolidated Forge/workbench gap fixes."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from demiflow.forge.api_reference import build_reference
from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import initialize_project, load_project, register_flow
from demiflow.forge.project_rules import _stale_operator_example_docs, check_project_hygiene
from demiflow.workbench.conversations.worker import (
    _build_rework_prompt,
    _capture_protected_files,
    _enforce_protected_files,
    _root_entry_names,
)


@pytest.fixture(scope='module')
def reference():
    return build_reference()


def _project(tmp_path, name='gap-proj'):
    proj = tmp_path / name
    initialize_project(proj, name=name)
    (proj / 'data_tools').mkdir()
    (proj / 'data_tools' / 'view.py').write_text('def view(rows):\n    """查看样本。"""\n    return rows\n', encoding='utf-8')
    return proj


def test_hook_guards_data_tools_and_workspace_json(tmp_path):
    proj = _project(tmp_path)
    ws_before = (proj / 'workspace.json').read_text(encoding='utf-8')
    snap = _capture_protected_files(proj)
    assert 'workspace.json' in snap and 'data_tools/view.py' in snap

    (proj / 'data_tools' / 'view.py').write_text('def view(rows):\n    return []\n', encoding='utf-8')
    (proj / 'data_tools' / 'new_tool.py').write_text('def t():\n    return 1\n', encoding='utf-8')
    (proj / 'data_tools' / 'pkg').mkdir()
    (proj / 'data_tools' / 'pkg' / 'x.py').write_text('X = 1\n', encoding='utf-8')
    (proj / 'workspace.json').write_text('{"name": "x", "entries": {}, "locations": {"a": {}}}', encoding='utf-8')

    violations = _enforce_protected_files(proj, snap)
    assert set(violations) == {'data_tools/view.py', 'data_tools/new_tool.py', 'data_tools/pkg/x.py', 'workspace.json'}
    assert '查看样本' in (proj / 'data_tools' / 'view.py').read_text(encoding='utf-8')
    assert not (proj / 'data_tools' / 'new_tool.py').exists()
    assert not (proj / 'data_tools' / 'pkg').exists()
    assert (proj / 'workspace.json').read_text(encoding='utf-8') == ws_before
    prompt = _build_rework_prompt(None, None, project_root=proj, protected_violations=violations)
    assert 'data_tools/' in prompt and 'manage_project_layout' in prompt


def test_hook_keeps_host_layout_writes(tmp_path):
    proj = _project(tmp_path)
    (proj / 'flows').mkdir()
    (proj / 'flows' / 'demo.py').write_text('from demiflow.data import range\ndef run():\n    return range(1).count()\n', encoding='utf-8')
    (proj / 'review_tool.py').write_text('def review(r):\n    """复核。"""\n    return r\n', encoding='utf-8')
    snap = _capture_protected_files(proj)

    # Legitimate host actions during the turn: register_flow (workspace.json + .forge/state.json)
    # and move_path of a stray tool into data_tools/.
    register_flow(proj, 'flows/demo.py:run')
    (proj / 'review_tool.py').rename(proj / 'data_tools' / 'review_tool.py')
    authorized = ['workspace.json', '.forge/state.json', 'review_tool.py', 'data_tools/review_tool.py']
    assert _enforce_protected_files(proj, snap, authorized=authorized) == []
    assert 'flows/demo.py:run' in json.loads((proj / 'workspace.json').read_text(encoding='utf-8'))['entries']
    assert (proj / 'data_tools' / 'review_tool.py').is_file()


def test_ensure_dirs_authorization_does_not_cover_descendants(tmp_path):
    proj = tmp_path / 'fresh'
    initialize_project(proj, name='fresh')
    snap = _capture_protected_files(proj)
    (proj / 'data_tools').mkdir()
    (proj / 'data_tools' / 'sneaky.py').write_text('X = 1\n', encoding='utf-8')
    # ensure_dirs records 'data_tools'; that authorizes the directory only, not files written into it.
    assert _enforce_protected_files(proj, snap, authorized=['data_tools']) == ['data_tools/sneaky.py']
    assert (proj / 'data_tools').is_dir() and not any((proj / 'data_tools').iterdir())
    # Without authorization a data_tools/ created by the developer is removed entirely.
    proj2 = tmp_path / 'fresh2'
    initialize_project(proj2, name='fresh2')
    snap2 = _capture_protected_files(proj2)
    (proj2 / 'data_tools').mkdir()
    (proj2 / 'data_tools' / 'a.py').write_text('A = 1\n', encoding='utf-8')
    assert _enforce_protected_files(proj2, snap2) == ['data_tools/a.py']
    assert not (proj2 / 'data_tools').exists()


def test_preexisting_empty_data_tools_is_kept(tmp_path):
    proj = tmp_path / 'empty-tools'
    initialize_project(proj, name='empty-tools')
    (proj / 'data_tools').mkdir()
    snap = _capture_protected_files(proj)
    assert _enforce_protected_files(proj, snap) == []
    assert (proj / 'data_tools').is_dir()


def test_root_entry_names_reports_new_visible_entries(tmp_path):
    proj = _project(tmp_path)
    before = _root_entry_names(proj)
    (proj / 'tables').mkdir()
    (proj / 'debug.py').write_text('print(1)\n', encoding='utf-8')
    (proj / '.cache').mkdir()
    (proj / 'data').mkdir()
    assert _root_entry_names(proj) - before == {'tables', 'debug.py'}


def test_hygiene_runs_for_projects_without_entries(tmp_path, reference):
    proj = tmp_path / 'new-proj'
    initialize_project(proj, name='new-proj')
    clean = check_project(proj, reference=reference)
    assert not [d for d in clean['diagnostics'] if d['rule'] == 'DF-02'], clean

    (proj / 'flows').mkdir()
    (proj / 'flows' / 'build.py').write_text('from demiflow.data import range\ndef run():\n    return range(1).count()\n', encoding='utf-8')
    (proj / 'tables').mkdir()
    (proj / 'tables' / 'answers_fixture.json').write_text('{"uri": "x"}', encoding='utf-8')
    _, control = load_project(proj)
    assert control['entries'] == {}

    _, diags = check_project_hygiene(proj, control, checked_sources={}, parameter_digests={})
    flagged = {d['path'] for d in diags if d['rule'] == 'DF-02'}
    assert 'tables' in flagged and 'flows/build.py' in flagged

    hook = check_project(proj, paths=['tables/answers_fixture.json'], reference=reference, include_hygiene=True)
    assert 'tables' in {d['path'] for d in hook['diagnostics'] if d['rule'] == 'DF-02'}, hook


def test_legacy_param_format_scans_unregistered_configs(tmp_path):
    proj = tmp_path / 'cfg-proj'
    initialize_project(proj, name='cfg-proj')
    (proj / 'configs').mkdir()
    (proj / 'configs' / 'draft.json').write_text(
        json.dumps({'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {'n': 1}}), encoding='utf-8')
    (proj / 'configs' / 'business.json').write_text(json.dumps({'format': 'csv', 'threshold': 0.5}), encoding='utf-8')
    (proj / 'configs' / 'flat.json').write_text(json.dumps({'source': {'uri': 'data/in.lance', 'version': 1}}), encoding='utf-8')
    _, control = load_project(proj)
    _, diags = check_project_hygiene(proj, control, checked_sources={}, parameter_digests={})
    flagged = {d['path'] for d in diags if d['rule'] == 'DF-02'}
    assert 'configs/draft.json' in flagged
    assert 'configs/business.json' not in flagged and 'configs/flat.json' not in flagged


def test_op_e1_flags_example_input_disjoint_from_reads(tmp_path):
    module = tmp_path / 'scoring.py'
    module.write_text(
        'def score(row):\n'
        '    """为样本打分。\n\n'
        '    Example:\n'
        '        输入: {"item_id": 1}\n'
        '        输出: {"score": 0.5}\n'
        '    """\n'
        '    return {"score": row["concept_id"] * 0.5}\n'
        '\n'
        'def rank(row):\n'
        '    """排序。\n\n'
        '    Example:\n'
        '        输入: {"concept_id": 1, "extra": 2}\n'
        '        输出: {"rank": 1}\n'
        '    """\n'
        '    return {"rank": row["concept_id"]}\n',
        encoding='utf-8')
    issues = _stale_operator_example_docs(module)
    assert [(name, '示例输入' in detail) for _, name, detail in issues] == [('score', True)]


def _deepseek_stream(home, final_text):
    events = [
        {'type': 'session', 'sessionId': 'session-0123abcd-0123-4567-89ab-0123456789ab', 'cwd': str(home)},
        {'type': 'status', 'phase': 'turn_end', 'reason': {'kind': 'completed'}},
        {'type': 'final', 'text': final_text},
    ]
    return b'\n'.join(json.dumps(e).encode() for e in events) + b'\n'


def test_deepseek_invalid_json_report_keeps_redacted_excerpt(tmp_path):
    from demiflow.forge.review_deepseek import _result
    from demiflow.forge.schema import ForgeError
    with pytest.raises(ForgeError) as caught:
        _result(0, _deepseek_stream(tmp_path, 'Here is my review: {"status": "passed",'), 'm', tmp_path)
    assert 'invalid JSON report' in str(caught.value) and 'excerpt: Here is my review' in str(caught.value)
    with pytest.raises(ForgeError) as caught:
        _result(0, _deepseek_stream(tmp_path, 'leaked sk-abc123 {'), 'm', tmp_path)
    assert 'excerpt' not in str(caught.value)


def test_prompt_docstrings_document_temperature_and_cli():
    from demiflow.data.dataset import Dataset
    for name in ('map_prompt', 'map_prompt_async'):
        doc = getattr(Dataset, name).__doc__
        assert '``temperature``' in doc and "options={'cli'" in doc, name
    assert '{output_file}' in Dataset.map_prompt_async.__doc__
