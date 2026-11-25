"""Tests for the simplified Forge protocol: slim workspace.json, .forge/state.json,
flat configs/*.json with direct Lance URIs, DF-02 root directory checks, and optional data_tools exports.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from demiflow import data
from demiflow.forge.api_reference import build_reference
from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import (
    STATE_PATH,
    initialize_project,
    load_project,
    load_project_snapshot,
    register_flow,
    save_project,
)
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_worker import describe_fixed_table
from demiflow.forge.project_bindings import extract_arguments, resolve_arguments
from demiflow.forge.project_rules import check_project_hygiene
from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.context import _run_project_layout
from demiflow.workbench.graph import parameters as graph_params
from demiflow.workbench.projects.definitions import catalog as definitions_catalog
from demiflow.workbench.projects.discovery import discover


@pytest.fixture(scope='module')
def reference():
    return build_reference()


def _make_lance(path: Path, rows=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    items = rows or [{'id': 1, 'score': 0.9}, {'id': 2, 'score': 0.3}]
    data.from_items(items).write_lance(str(path), mode='create')
    return path


def test_slim_workspace_manifest_and_lazy_forge_bootstrap(tmp_path, reference):
    proj = tmp_path / 'fresh-proj'
    res = initialize_project(proj, name='Fresh Project')
    assert STATE_PATH in res['created_paths']
    assert 'workspace.json' in res['created_paths']
    assert '.gitignore' in res['created_paths']

    # workspace.json only holds readable project manifest fields.
    raw_ws = json.loads((proj / 'workspace.json').read_text(encoding='utf-8'))
    assert raw_ws == {'name': 'Fresh Project', 'entries': {}}

    # Forge internal state lives under .forge/state.json, omitting empty audit.
    raw_state = json.loads((proj / STATE_PATH).read_text(encoding='utf-8'))
    assert set(raw_state) == {'format', 'id', 'platform'}
    assert 'audit' not in raw_state

    # .gitignore ignores both .env and .forge/
    gi = (proj / '.gitignore').read_text(encoding='utf-8')
    assert '.env' in gi and '.forge/' in gi

    # Simulate fresh git clone with no .forge/ directory: Forge lazily reconstructs it.
    shutil.rmtree(proj / '.forge')
    assert not (proj / '.forge').exists()
    _, merged, snapshot = load_project_snapshot(proj)
    assert merged['name'] == 'Fresh Project'
    assert merged['entries'] == {}
    assert (proj / STATE_PATH).is_file()
    assert snapshot['digests']['snapshot'] == merged['platform']['digest']

    # Registering a flow writes flat empty {} params and keeps workspace.json slim.
    (proj / 'flows').mkdir()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'def run(n: int = 3):\n'
        '    return range(n).count()\n',
        encoding='utf-8',
    )
    reg = register_flow(proj, 'flows/demo.py:run')
    assert reg['status'] == 'registered'
    assert json.loads((proj / 'configs' / 'demo.json').read_text(encoding='utf-8')) == {}
    raw_ws_after = json.loads((proj / 'workspace.json').read_text(encoding='utf-8'))
    assert raw_ws_after == {'name': 'Fresh Project', 'entries': {'flows/demo.py:run': ['configs/demo.json']}}


def test_flat_params_and_direct_uri_tables_and_outputs(tmp_path, reference, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    proj = tmp_path / 'uri-proj'
    initialize_project(proj, name='URI Project')
    (proj / 'flows').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'data').mkdir()

    local_tbl = _make_lance(proj / 'data' / 'candidates.lance')
    ext_tbl = _make_lance(tmp_path / 'external_shared' / 'ref.lance')

    (proj / 'flows' / 'filter_flow.py').write_text(
        'from demiflow import data\n'
        'def run(*, source, external, target, min_score: float = 0.5):\n'
        '    ds = data.read_lance(source["uri"], version=source["version"])\n'
        '    ext = data.read_lance(external["uri"], version=external["version"])\n'
        '    return ds.union(ext).write_lance(target["uri"], mode=target["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    flat_cfg = {
        'source': {'uri': 'data/candidates.lance', 'version': 1},
        'external': {'kind': 'table', 'uri': str(ext_tbl)},
        'target': {'kind': 'output', 'uri': 'data/filtered.lance'},
        'min_score': 0.5,
    }
    (proj / 'configs' / 'filter.json').write_text(json.dumps(flat_cfg, indent=2), encoding='utf-8')

    # Direct resolution without tables/ or workspace.json locations
    _, control = load_project(proj)
    assert control['locations'] == {}
    assert not (proj / 'tables').exists()

    args, tables, targets, controls = resolve_arguments(
        proj, control, flat_cfg,
        table_resolver=lambda *a: (_ for _ in ()).throw(AssertionError('should not use alias resolver')),
        table_describer=describe_fixed_table,
    )
    assert args['source'] == {'uri': str(local_tbl), 'version': 1}
    assert args['external'] == {'uri': str(ext_tbl), 'version': 1}
    assert args['target'] == {'uri': str(proj / 'data' / 'filtered.lance'), 'mode': 'create', 'expected_version': None}
    assert controls == {}
    assert len(tables) == 2
    assert targets == {str(proj / 'data' / 'filtered.lance'): {'mode': 'create', 'expected_version': None}}

    # Register & run full Forge check: zero diagnostics, no tables/ required.
    reg = register_flow(proj, 'flows/filter_flow.py:run', params='configs/filter.json')
    assert reg['status'] == 'registered'
    checked = check_project(proj, reference=reference)
    assert checked['status'] == 'passed', checked
    assert checked['diagnostic_count'] == 0

    # Discovery finds the local table directly from flat configs/*.json without tables/
    disc = discover(proj)
    assert any(t['path'] == 'data/candidates.lance' and t['version'] == 1 for t in disc['tables']), disc['tables']
    assert any(c['path'] == 'configs/filter.json' and c['status'] == 'native_parameters' for c in disc['configurations'])

    # Parameter projection handles flat JSON and table_uri/output_uri fields cleanly.
    _, control = load_project(proj)
    proj_model = graph_params.public_model(proj, control, 'flows/filter_flow.py:run', 'configs/filter.json', [], readonly=True)
    assert proj_model['available'] is True
    by_ptr = {f['pointer']: f for f in proj_model['fields']}
    assert by_ptr['/source/uri']['kind'] == 'table_uri'
    assert by_ptr['/external/uri']['kind'] == 'table_uri'
    assert by_ptr['/target/uri']['kind'] == 'output_uri'
    assert by_ptr['/min_score']['value'] == 0.5

    # Safety guards: output outside data/ or forbidden external paths are rejected.
    with pytest.raises(ForgeError, match='data/'):
        resolve_arguments(proj, control, {'out': {'kind': 'output', 'uri': 'runs/escape.lance'}}, table_resolver=None)
    with pytest.raises(ForgeError, match='forbidden'):
        resolve_arguments(proj, control, {'src': {'uri': '/etc/fake.lance'}}, table_resolver=None)

    # End-to-end managed local run with flat params + direct URI input & output (no tables/, no locations).
    from demiflow.workbench.runs import manager as runs
    from test_forge_runs import wait as wait_run
    prepared = runs.prepare(proj, 'flows/filter_flow.py:run', params='configs/filter.json')
    runs.start(proj, prepared['run_id'], expected=prepared['digest'])
    done = wait_run(proj, prepared['run_id'])
    assert done['status'] == 'succeeded', done
    assert not (proj / 'tables').exists()
    assert json.loads((proj / 'workspace.json').read_text(encoding='utf-8')) == {
        'name': 'URI Project',
        'entries': {'flows/filter_flow.py:run': ['configs/filter.json']},
    }


def test_df02_root_directory_hygiene_and_legacy_tables_cleanup(tmp_path):
    proj = tmp_path / 'hygiene-proj'
    initialize_project(proj, name='Hygiene Project')
    (proj / 'flows').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\ndef run():\n    return range(2).count()\n',
        encoding='utf-8',
    )
    (proj / 'configs' / 'demo.json').write_text('{}', encoding='utf-8')
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')
    _, control = load_project(proj)

    # Non-standard root dir and legacy tables/ dir are flagged under DF-02.
    (proj / 'misc_junk').mkdir()
    (proj / 'tables').mkdir()
    (proj / 'tables' / 'old.json').write_text('{}', encoding='utf-8')
    _, diags = check_project_hygiene(proj, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    paths = {d['path']: d['detail'] for d in diags if d['rule'] == 'DF-02'}
    assert 'tables' in paths
    assert 'misc_junk' in paths

    # manage_project_layout remove_path can clean up deprecated tables/ and misc_junk/.
    rm = _run_project_layout(proj, {'action': 'remove_path', 'paths': ['tables', 'misc_junk']})
    assert rm['status'] == 'removed'
    assert not (proj / 'tables').exists()
    assert not (proj / 'misc_junk').exists()

    _, clean_diags = check_project_hygiene(proj, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert clean_diags == []


def test_optional_data_tools_export_and_auto_discovery(tmp_path):
    proj = tmp_path / 'tools-proj'
    initialize_project(proj, name='Tools Project')
    (proj / 'flows').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'data_tools').mkdir()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\ndef run():\n    return range(2).count()\n',
        encoding='utf-8',
    )
    (proj / 'configs' / 'demo.json').write_text('{}', encoding='utf-8')
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')

    # Module without __demiflow_data_tools__ passes DF-02 and auto-discovers public functions as 'tool'.
    (proj / 'data_tools' / 'quick_view.py').write_text(
        'def inspect_rows(rows):\n    """Inspect sample rows."""\n    return len(rows)\n\n'
        'def _private_helper():\n    return 1\n',
        encoding='utf-8',
    )
    # Module with explicit __demiflow_data_tools__ keeps explicit role tags.
    (proj / 'data_tools' / 'reviewer.py').write_text(
        '__demiflow_data_tools__ = {"review_case": "annotation"}\n'
        'def review_case(row):\n    return row\n'
        'def internal_fn():\n    return 0\n',
        encoding='utf-8',
    )

    _, control = load_project(proj)
    _, diags = check_project_hygiene(proj, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert diags == []

    cat = definitions_catalog(proj, data_tools=True)
    found = {(d['path'], d['symbol']): d['tool_kind'] for d in cat['definitions'] if d['kind'] == 'data_tool'}
    assert found[('data_tools/quick_view.py', 'inspect_rows')] == 'tool'
    assert ('data_tools/quick_view.py', '_private_helper') not in found
    assert found[('data_tools/reviewer.py', 'review_case')] == 'annotation'
    assert ('data_tools/reviewer.py', 'internal_fn') not in found


def test_legacy_workspace_v3_and_wrapped_params_remain_compatible(tmp_path, reference):
    proj = tmp_path / 'legacy-proj'
    initialize_project(proj, name='Legacy Project')
    # Rewrite workspace.json back into legacy monolithic v3 format and remove .forge/state.json.
    _, merged = load_project(proj)
    (proj / STATE_PATH).unlink()
    (proj / 'workspace.json').write_text(json.dumps(merged, indent=2), encoding='utf-8')

    _, loaded = load_project(proj)
    assert loaded['format'] == 'demiflow_forge_workspace_v3'
    assert loaded['id'] == merged['id']

    # save_project preserves legacy monolithic file when .forge/state.json is absent.
    expected = content_digest(loaded)
    loaded['name'] = 'Legacy Renamed'
    save_project(proj, loaded, expected=expected)
    assert not (proj / STATE_PATH).exists()
    assert json.loads((proj / 'workspace.json').read_text(encoding='utf-8'))['format'] == 'demiflow_forge_workspace_v3'

    # extract_arguments accepts both legacy wrapped and flat formats.
    args_v2, fmt_v2 = extract_arguments({'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {'k': 1}})
    assert args_v2 == {'k': 1} and fmt_v2 == 'demiflow_forge_flow_parameters_v2'
    args_flat, fmt_flat = extract_arguments({'k': 1})
    assert args_flat == {'k': 1} and fmt_flat == 'flat'
    with pytest.raises(ForgeError):
        extract_arguments({'format': 'unknown_format', 'arguments': {}})


def test_post_turn_hygiene_hook_and_slim_manifest_migration(tmp_path, reference):
    proj = tmp_path / 'migrate-proj'
    initialize_project(proj, name='Migrate Project')
    (proj / 'flows').mkdir()
    (proj / 'operators').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'data').mkdir()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'from operators.helper import add_one\n'
        'def run(n: int = 2):\n'
        '    return range(n).map(add_one, inputs={"x": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'flows' / 'other.py').write_text(
        'from demiflow.data import range\n'
        'from operators.other_op import double\n'
        'def run():\n'
        '    return range(2).map(double, inputs={"x": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'helper.py').write_text('def add_one(x: int) -> int:\n    """将输入整数加一。"""\n    return x + 1\n', encoding='utf-8')
    (proj / 'operators' / 'other_op.py').write_text('def double(x: int) -> int:\n    """将输入整数翻倍。"""\n    return x * 2\n', encoding='utf-8')
    # Legacy wrapped config & legacy bloated workspace.json + unused location
    (proj / 'configs' / 'demo.json').write_text(
        json.dumps({'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {'n': 3}}),
        encoding='utf-8',
    )
    (proj / 'configs' / 'other.json').write_text('{}', encoding='utf-8')
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')
    register_flow(proj, 'flows/other.py:run', params='configs/other.json')

    # Rewrite workspace.json into legacy monolithic v3 with an unused location
    _, merged = load_project(proj)
    merged['locations']['unused_in'] = {'root': str(proj / 'data'), 'writable': False}
    (proj / STATE_PATH).unlink()
    (proj / 'workspace.json').write_text(json.dumps(merged, indent=2), encoding='utf-8')
    (proj / 'tables').mkdir()

    # Post-turn incremental hook (paths + include_hygiene=True) catches tables/, legacy config,
    # and bloated workspace.json WITHOUT falsely flagging unrelated operators/other_op.py!
    hook_res = check_project(proj, paths=['flows/demo.py'], reference=reference, include_hygiene=True)
    assert hook_res['status'] == 'failed', hook_res
    diag_paths = {d['path'] for d in hook_res['diagnostics'] if d['rule'] == 'DF-02'}
    assert 'tables' in diag_paths
    assert 'configs/demo.json' in diag_paths
    assert 'workspace.json' in diag_paths
    assert 'operators/other_op.py' not in diag_paths

    # Migrate: flatten configs/demo.json, remove tables/, and slim workspace.json via manage_project_layout
    (proj / 'configs' / 'demo.json').write_text(json.dumps({'n': 3}), encoding='utf-8')
    rm = _run_project_layout(proj, {'action': 'remove_path', 'paths': ['tables']})
    assert rm['status'] == 'removed'
    slim = _run_project_layout(proj, {'action': 'slim_manifest'})
    assert slim['status'] == 'slimmed'
    assert 'unused_in' in slim['removed_locations']
    assert json.loads((proj / 'workspace.json').read_text(encoding='utf-8')) == {
        'name': 'Migrate Project',
        'entries': {
            'flows/demo.py:run': ['configs/demo.json'],
            'flows/other.py:run': ['configs/other.json'],
        },
    }
    assert (proj / STATE_PATH).is_file()

    # Now post-turn hook passes cleanly (0 diagnostics -> no rework message emitted, 0 extra tokens)
    clean_hook = check_project(proj, paths=['flows/demo.py'], reference=reference, include_hygiene=True)
    assert clean_hook['status'] == 'passed', clean_hook
    assert clean_hook['diagnostic_count'] == 0
    full_clean = check_project(proj, reference=reference)
    assert full_clean['status'] == 'passed', full_clean
    assert full_clean['diagnostic_count'] == 0


def test_thin_operator_wrapper_shim_detection(tmp_path):
    proj = tmp_path / 'shim-proj'
    initialize_project(proj, name='Shim Project')
    (proj / 'flows').mkdir()
    (proj / 'operators').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'from operators import wrapped\n'
        'def run():\n'
        '    return range(2).map(wrapped.pool_group, inputs={"row": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'operators' / '_util.py').write_text(
        'def pack(row):\n'
        '    return {"packed": row}\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'base.py').write_text(
        'MAX_ITEMS = 100\n'
        'def pool_group(row, maximum=MAX_ITEMS):\n'
        '    """按概念分组整理候选池。"""\n'
        '    return {"row": row, "maximum": maximum}\n'
        'def helper(row):\n'
        '    """保留原始行。"""\n'
        '    return row\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'wrapped.py').write_text(
        'from . import base as hybrid\n'
        'POOL_MAX = hybrid.MAX_ITEMS\n'
        'reexported = hybrid.helper\n'
        'def _private_shim(row):\n'
        '    return hybrid.pool_group(row, maximum=50)\n'
        'def pool_group(row):\n'
        '    """Thin shim only changing default."""\n'
        '    return hybrid.pool_group(row, maximum=200)\n'
        'def missing_doc(row):\n'
        '    out = hybrid.pool_group(row, maximum=200)\n'
        '    out["tag"] = "custom"\n'
        '    return out\n',
        encoding='utf-8',
    )
    (proj / 'configs' / 'demo.json').write_text('{}', encoding='utf-8')
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')
    _, control = load_project(proj)

    _, diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/_util.py', 'operators/base.py', 'operators/wrapped.py'},
        parameter_digests={},
    )
    wrapped_diags = [d['detail'] for d in diags if d['path'] == 'operators/wrapped.py']
    assert any('垫片' in msg and 'reexported' in msg and '等 2 处' in msg for msg in wrapped_diags), diags
    assert any('公开业务算子模块导入了另一公开算子模块' in msg for msg in wrapped_diags), diags
    assert any('missing_doc' in msg and 'docstring' in msg for msg in wrapped_diags), diags

    # Self-contained operator importing only private _util.py and having a docstring passes cleanly.
    (proj / 'operators' / 'wrapped.py').write_text(
        'from . import _util\n'
        'def _private_helper(row):\n'
        '    return _util.pack(row)\n'
        'def pool_group(row):\n'
        '    """打包候选池条目。"""\n'
        '    out = _util.pack(row)\n'
        '    out["tag"] = "custom"\n'
        '    return out\n',
        encoding='utf-8',
    )
    _, clean_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/_util.py', 'operators/base.py', 'operators/wrapped.py'},
        parameter_digests={},
    )
    assert clean_diags == [], clean_diags
