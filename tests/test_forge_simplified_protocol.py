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
from demiflow.forge.bridge.tools import run_project_layout as _run_project_layout
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
    (proj / 'operators' / 'helper.py').write_text(
        'def add_one(x: int) -> int:\n'
        '    """将输入整数加一。\n'
        '\n'
        '    Example:\n'
        '        输入: 1\n'
        '        输出: 2\n'
        '    """\n'
        '    return x + 1\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'other_op.py').write_text(
        'def double(x: int) -> int:\n'
        '    """将输入整数翻倍。\n'
        '\n'
        '    Example:\n'
        '        输入: 2\n'
        '        输出: 4\n'
        '    """\n'
        '    return x * 2\n',
        encoding='utf-8',
    )
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
        '    """按概念分组整理候选池。\n'
        '\n'
        '    Args:\n'
        '        maximum: 单组最大候选图数量。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"row": {"id": 1}, "maximum": 100}\n'
        '    """\n'
        '    return {"row": row, "maximum": maximum}\n'
        'def helper(row):\n'
        '    """保留原始行。\n'
        '\n'
        '    Example:\n'
        '        {"id": 1} -> {"id": 1}\n'
        '    """\n'
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
        '    """打包候选池条目。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"packed": {"id": 1}, "tag": "custom"}\n'
        '    """\n'
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


def test_mece_operator_richness_anti_bypass_rules(tmp_path):
    """Verify MECE OP-L1/OP-L2/OP-O1/OP-O2/OP-S1 checks block shadow private mirrors and re-export workarounds."""
    proj = tmp_path / 'mece-proj'
    initialize_project(proj, name='MECE Project')
    (proj / 'flows').mkdir()
    (proj / 'operators').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'configs' / 'demo.json').write_text('{}', encoding='utf-8')

    # OP-L2c: Flow directly importing a private operators/_*.py utility is blocked.
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'from operators import wrapped, _codec\n'
        'def run():\n'
        '    return range(2).map(wrapped.pool_group, inputs={"row": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'operators' / '_codec.py').write_text(
        'def pack(row):\n'
        '    return {"packed": row}\n'
        'def unpack(row):\n'
        '    return row.get("packed")\n',
        encoding='utf-8',
    )
    # OP-L2a (1:1 shadow mirror of wrapped.py) + OP-L2b (private importing another private module)
    (proj / 'operators' / '_wrapped.py').write_text(
        'from ._codec import pack\n'
        'def hidden_op(row):\n'
        '    return pack(row)\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'base.py').write_text(
        'def seed(row):\n'
        '    """生成基础种子行。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"seed": {"id": 1}}\n'
        '    """\n'
        '    return {"seed": row}\n',
        encoding='utf-8',
    )
    # OP-O2 (re-exporting unpack without internal use) + OP-O1 (thin wrapper to _codec.pack)
    # + OP-L1 (nested lazy import of public base inside function body)
    (proj / 'operators' / 'wrapped.py').write_text(
        'from ._codec import pack, unpack\n'
        'def fwd(row):\n'
        '    """仅转调私有工具的薄壳。"""\n'
        '    return pack(row)\n'
        'def pool_group(row):\n'
        '    """在函数内延迟导入另一公开算子模块。"""\n'
        '    from . import base\n'
        '    out = base.seed(row)\n'
        '    out["p"] = pack(row)\n'
        '    return out\n',
        encoding='utf-8',
    )
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')
    _, control = load_project(proj)

    _, diags = check_project_hygiene(
        proj, control,
        checked_sources={
            'flows/demo.py', 'operators/_codec.py', 'operators/_wrapped.py',
            'operators/base.py', 'operators/wrapped.py',
        },
        parameter_digests={},
    )
    by_path: dict[str, list[str]] = {}
    for d in diags:
        by_path.setdefault(d['path'], []).append(d['detail'])

    assert any('Flow 直接导入了私有工具模块' in m for m in by_path.get('flows/demo.py', [])), diags
    assert any('1:1 影子私仓' in m for m in by_path.get('operators/_wrapped.py', [])), diags
    assert any('纯底层叶节点工具' in m for m in by_path.get('operators/_wrapped.py', [])), diags
    assert any('无逻辑垫片' in m and 'fwd' in m for m in by_path.get('operators/wrapped.py', [])), diags
    assert any('公开业务算子模块导入了另一公开算子模块' in m and 'base' in m for m in by_path.get('operators/wrapped.py', [])), diags
    assert any('直接重导出了私有模块函数/类' in m and 'unpack' in m for m in by_path.get('operators/wrapped.py', [])), diags

    # Clean fix: remove shadow mirror, remove Flow private import, keep only internal _codec.pack call.
    (proj / 'operators' / '_wrapped.py').unlink()
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'from operators import wrapped\n'
        'def run():\n'
        '    return range(2).map(wrapped.pool_group, inputs={"row": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'wrapped.py').write_text(
        'from ._codec import pack\n'
        'def pool_group(row):\n'
        '    """打包并标记候选池条目。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"packed": {"id": 1}, "tag": "clean"}\n'
        '    """\n'
        '    out = pack(row)\n'
        '    out["tag"] = "clean"\n'
        '    return out\n',
        encoding='utf-8',
    )
    _, clean_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/_codec.py', 'operators/base.py', 'operators/wrapped.py'},
        parameter_digests={},
    )
    assert clean_diags == [], clean_diags

    # OP-P1: Extra non-row parameters must be documented in Args:/参数:.
    (proj / 'operators' / 'wrapped.py').write_text(
        'from ._codec import pack\n'
        'def pool_group(row, limit=12, tag="clean"):\n'
        '    """打包并标记候选池条目。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"packed": {"id": 1}, "limit": 12, "tag": "clean"}\n'
        '    """\n'
        '    out = pack(row)\n'
        '    out["limit"] = limit\n'
        '    out["tag"] = tag\n'
        '    return out\n',
        encoding='utf-8',
    )
    _, p1_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/_codec.py', 'operators/base.py', 'operators/wrapped.py'},
        parameter_digests={},
    )
    assert any(
        d['path'] == 'operators/wrapped.py' and 'OP-P1' in d['detail'] and 'limit' in d['detail']
        for d in p1_diags
    ), p1_diags

    # Documenting both non-row parameters in 参数:/Args: clears OP-P1 cleanly.
    (proj / 'operators' / 'wrapped.py').write_text(
        'from ._codec import pack\n'
        'def pool_group(row, limit=12, tag="clean"):\n'
        '    """打包并标记候选池条目。\n'
        '\n'
        '    Args:\n'
        '        limit: 每个概念最多保留的候选条数。\n'
        '        tag: 写入结果行的业务标签。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": 1}\n'
        '        输出: {"packed": {"id": 1}, "limit": 12, "tag": "clean"}\n'
        '    """\n'
        '    out = pack(row)\n'
        '    out["limit"] = limit\n'
        '    out["tag"] = tag\n'
        '    return out\n',
        encoding='utf-8',
    )
    _, p1_clean = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/_codec.py', 'operators/base.py', 'operators/wrapped.py'},
        parameter_digests={},
    )
    assert p1_clean == [], p1_clean


def test_op_e1_example_and_ast_field_flow_extraction(tmp_path):
    """Verify OP-E1 enforces Example:/示例: on public operators and AST extracts field_flow + example."""
    import ast as _ast
    from demiflow.workbench.definition_contracts import code_definition

    proj = tmp_path / 'example-proj'
    initialize_project(proj, name='Example Project')
    (proj / 'flows').mkdir()
    (proj / 'operators').mkdir()
    (proj / 'configs').mkdir()
    (proj / 'configs' / 'demo.json').write_text('{}', encoding='utf-8')
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow.data import range\n'
        'from operators import transforms\n'
        'def run():\n'
        '    return range(2).map(transforms.enrich, inputs={"row": "id"}, output="y").count()\n',
        encoding='utf-8',
    )
    (proj / 'operators' / 'transforms.py').write_text(
        'class ConfigRecord:\n'
        '    """非可调用配置数据结构，不应触发 OP-E1。"""\n'
        '    pass\n'
        '\n'
        'def enrich(row):\n'
        '    """缺少变换示例的公开算子。"""\n'
        '    return {**row, "score": row.get("raw_score", 0) + 1}\n'
        '\n'
        'class ScorerActor:\n'
        '    """缺少变换示例的可调用 Actor。"""\n'
        '    def __call__(self, item):\n'
        '        return {"id": item["id"], "passed": item.get("score", 0) > 0}\n',
        encoding='utf-8',
    )
    register_flow(proj, 'flows/demo.py:run', params='configs/demo.json')
    _, control = load_project(proj)

    _, e1_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/transforms.py'},
        parameter_digests={},
    )
    e1_msgs = [d['detail'] for d in e1_diags if d['path'] == 'operators/transforms.py']
    assert any('OP-E1' in m and 'enrich' in m and '等 2 个算子' in m for m in e1_msgs), e1_diags

    # Supplying Example:/示例: clears OP-E1 and powers workbench definition_contracts.
    clean_src = (
        'class ConfigRecord:\n'
        '    """非可调用配置数据结构，不应触发 OP-E1。"""\n'
        '    pass\n'
        '\n'
        'def enrich(row):\n'
        '    """补齐归一化分数字段。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": "r1", "raw_score": 4}\n'
        '        输出: {"id": "r1", "raw_score": 4, "score": 5}\n'
        '    """\n'
        '    return {**row, "score": row.get("raw_score", 0) + 1}\n'
        '\n'
        'class ScorerActor:\n'
        '    """按阈值判定样本是否通过。\n'
        '\n'
        '    示例:\n'
        '        {"id": "r1", "score": 5} -> {"id": "r1", "passed": True}\n'
        '    """\n'
        '    def __call__(self, item):\n'
        '        return {"id": item["id"], "passed": item.get("score", 0) > 0}\n'
    )
    (proj / 'operators' / 'transforms.py').write_text(clean_src, encoding='utf-8')
    _, clean_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/transforms.py'},
        parameter_digests={},
    )
    assert clean_diags == [], clean_diags

    tree = _ast.parse(clean_src)
    fn_node = next(n for n in tree.body if isinstance(n, _ast.FunctionDef) and n.name == 'enrich')
    cls_node = next(n for n in tree.body if isinstance(n, _ast.ClassDef) and n.name == 'ScorerActor')

    fn_def = code_definition(fn_node, clean_src)
    assert fn_def['field_flow'] == {'reads': ['raw_score'], 'writes': ['score'], 'passes_through': True}
    assert fn_def['example'] == {
        'input': '{"id": "r1", "raw_score": 4}',
        'output': '{"id": "r1", "raw_score": 4, "score": 5}',
    }

    cls_def = code_definition(cls_node, clean_src)
    assert cls_def['field_flow'] == {'reads': ['id', 'score'], 'writes': ['id', 'passed'], 'passes_through': False}
    assert cls_def['example'] == {
        'input': '{"id": "r1", "score": 5}',
        'output': '{"id": "r1", "passed": True}',
    }

    # Drift detection: renaming output field or leaving deleted parameter in Args: must fail check.
    drifted_src = (
        'def enrich(row, scale=2):\n'
        '    """补齐归一化分数字段。\n'
        '\n'
        '    Args:\n'
        '        scale: 分数缩放倍数。\n'
        '        old_limit: 已删除的旧参数。\n'
        '\n'
        '    Example:\n'
        '        输入: {"id": "r1", "raw_score": 4}\n'
        '        输出: {"id": "r1", "old_score": 8}\n'
        '    """\n'
        '    return {**row, "normalized_score": row.get("raw_score", 0) * scale}\n'
    )
    (proj / 'operators' / 'transforms.py').write_text(drifted_src, encoding='utf-8')
    _, drift_diags = check_project_hygiene(
        proj, control,
        checked_sources={'flows/demo.py', 'operators/transforms.py'},
        parameter_digests={},
    )
    drift_msgs = [d['detail'] for d in drift_diags if d['path'] == 'operators/transforms.py']
    assert any('OP-P1' in m and 'old_limit' in m for m in drift_msgs), drift_diags
    assert any('OP-E1' in m and 'old_score' in m for m in drift_msgs), drift_diags


def test_slim_workspace_read_object_without_locations_and_static_raise_handling(tmp_path):
    """Slim workspaces without locations support read_object & clear static raise diagnostics."""
    import hashlib
    import lance
    import pyarrow as pa
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project
    from demiflow.forge.project_bindings import resolve_flow_resources
    from demiflow.forge.schema import ForgeError

    proj = tmp_path / 'slim_obj_proj'
    initialize_project(proj, name='Slim Object Project')
    for d in ('flows', 'configs', 'tests/fixtures', 'data'):
        (proj / d).mkdir(parents=True, exist_ok=True)

    # Verify workspace.json has NO locations key.
    raw_ws = json.loads((proj / 'workspace.json').read_text(encoding='utf-8'))
    assert 'locations' not in raw_ws

    img = proj / 'tests' / 'fixtures' / 'sample.bin'
    raw = b'fake-png-bytes'
    img.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    tbl = proj / 'tests' / 'fixtures' / 'answers.lance'
    img_type = pa.struct([
        pa.field('uri', pa.string(), nullable=False),
        pa.field('sha256', pa.string(), nullable=False),
    ])
    lance.write_dataset(
        pa.table({'image': pa.array([{'uri': img.as_uri(), 'sha256': digest}], type=img_type)}),
        str(tbl),
    )

    ext_imgs = tmp_path / 'external_images'
    ext_imgs.mkdir()
    ext_img = ext_imgs / 'ext.bin'
    ext_img.write_bytes(raw)

    (proj / 'flows' / 'score.py').write_text(
        'from demiflow import data\n'
        'def run(answers: dict, image_root: str, expected_cases: int = 1, execution_approved: bool = False):\n'
        '    """读取本地对象并按静态配置校验。"""\n'
        '    if type(expected_cases) is not int or expected_cases < 1:\n'
        '        raise ValueError("expected_cases 必须是正整数")\n'
        '    data.require(execution_approved, "execution_approved=false：拒绝运行")\n'
        '    return data.read_lance(answers["uri"], version=answers["version"]).read_object(\n'
        '        reference="image", output="image_bytes", root=image_root, max_bytes=65536\n'
        '    ).materialize()\n',
        encoding='utf-8',
    )
    # 1. Valid config with project tests/fixtures root passes WITHOUT locations in workspace.json.
    (proj / 'configs' / 'score.json').write_text(json.dumps({
        'answers': {'uri': 'tests/fixtures/answers.lance', 'version': 1},
        'image_root': str(proj / 'tests' / 'fixtures'),
        'expected_cases': 1,
        'execution_approved': False,
    }), encoding='utf-8')
    res = check_project(proj, entry='flows/score.py:run', params='configs/score.json')
    assert res['status'] == 'passed', res
    assert res['entries'][0]['status'] == 'checked', res

    # 2. Valid config with external read-only image directory also passes without locations.
    (proj / 'configs' / 'score_ext.json').write_text(json.dumps({
        'answers': {'uri': 'tests/fixtures/answers.lance', 'version': 1},
        'image_root': str(ext_imgs),
        'expected_cases': 1,
        'execution_approved': False,
    }), encoding='utf-8')
    res_ext = check_project(proj, entry='flows/score.py:run', params='configs/score_ext.json')
    assert res_ext['status'] == 'passed', res_ext

    # 3. Static if ...: raise triggered by invalid param reports clear FL-06 (not FL-07 Unsupported Raise).
    (proj / 'configs' / 'score_bad.json').write_text(json.dumps({
        'answers': {'uri': 'tests/fixtures/answers.lance', 'version': 1},
        'image_root': str(proj / 'tests' / 'fixtures'),
        'expected_cases': 0,
        'execution_approved': False,
    }), encoding='utf-8')
    res_bad = check_project(proj, entry='flows/score.py:run', params='configs/score_bad.json')
    assert res_bad['status'] == 'failed', res_bad
    entry_diags = res_bad['diagnostics']
    assert any(
        d['rule'] == 'FL-06'
        and 'ValueError' in d['detail']
        and 'expected_cases 必须是正整数' in d['detail']
        and 'demiflow.data.require' in d['detail']
        for d in entry_diags
    ), entry_diags
    assert not any('Unsupported wiring statement: Raise' in d['detail'] for d in entry_diags), entry_diags

    # 4. Security boundaries on direct object roots in slim workspaces.
    for bad_root in (str(proj), str(tmp_path), '/etc', str(proj / '.forge')):
        with pytest.raises(ForgeError):
            resolve_flow_resources(
                proj, None,
                [{'kind': 'objects', 'parameter': 'objects', 'root': bad_root, 'relocations': [], 'layout': 'path'}],
                {},
            )
    # Write object roots in slim workspaces must stay inside data/.
    with pytest.raises(ForgeError, match='data/'):
        resolve_flow_resources(
            proj, None,
            [{'kind': 'objects', 'parameter': 'objects', 'root': str(proj / 'tests' / 'fixtures'),
              'relocations': [], 'layout': 'path', 'access': 'write'}],
            {},
        )
    ok_write = resolve_flow_resources(
        proj, None,
        [{'kind': 'objects', 'parameter': 'objects', 'root': str(proj / 'data' / 'imgs'),
          'relocations': [], 'layout': 'path', 'access': 'write'}],
        {},
    )
    assert ok_write['objects']['access'] == 'write'
    assert ok_write['objects']['root'] == str((proj / 'data' / 'imgs').absolute())
