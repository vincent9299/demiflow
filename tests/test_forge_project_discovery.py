"""Original declarations are evidence, never imports, execution or copied code."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

from click.testing import CliRunner
import lance
import pyarrow as pa
import pytest

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.projects.discovery import discover
from demiflow.forge.schema import ForgeError
from test_forge_project_management import snapshot, cached_snapshot
from test_forge_public_api import request, service


@pytest.fixture
def original(tmp_path):
    repo = tmp_path / 'retail'
    root = repo / 'shipping'
    root.mkdir(parents=True)
    (repo / '.git').mkdir()
    (repo / '.demiforge').mkdir()
    declaration = dict(schema='demiforge-project-v2', standard='1.0.0-candidate.5',
        pipelines={'shipments': dict(path='shipping', entry='dispatch.py',
                                    verify=[{'argv': ['must-never-execute']}])})
    (repo / '.demiforge/project.json').write_text(json.dumps(declaration))
    (root / 'operators').mkdir()
    (root / 'operators/clean.py').write_text('def clean(row):\n    return row\n')
    (root / 'dispatch.py').write_text('from operators.clean import clean\n'
        'def config(*, label="shipping"):\n    return {"label": label}\n'
        'def run_pipeline(cfg):\n    return clean(cfg)\n')
    (root / 'settings').mkdir()
    (root / 'records').mkdir()
    lance.write_dataset(pa.table({'order': ['alpha', 'beta'], 'amount': [3, 8]}), str(root / 'records/orders.lance'))
    (root / 'settings/small.json').write_text(json.dumps({'source': {'uri': 'records/orders.lance', 'version': 1}}))
    (root / 'README.md').write_text('[small](settings/small.json)\n`records/orders.lance`\n`history/RUN/calls.sqlite`\n')
    return root


def declarations(root):
    path = root.parent / '.demiforge/project.json'
    return path, json.loads(path.read_text())


def business_bytes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file() and '.forge' not in p.parts and p.name != 'workspace.json'}


def test_register_recognizes_original_without_execution_or_copies(original, tmp_path):
    source = original / 'dispatch.py'
    source.write_text(source.read_text() + '\nraise RuntimeError("NEVER IMPORT")\n')
    before = business_bytes(original)
    console.add(tmp_path / 'console.json', original)
    value = ws.workspace_context(original)
    result = value['project']
    assert result['entries'][0]['entry'] == 'dispatch.py:run_pipeline'
    assert result['entries'][0]['configuration_entry'] == 'dispatch.py:config'
    assert result['entries'][0]['protocol'] == 'config_run_pipeline'
    assert result['configurations'][0]['path'] == 'settings/small.json'
    assert result['operators'][0]['path'] == 'operators'
    assert len(result['tables']) == 2  # documented head plus fixed configuration
    assert result['runs'][0]['status'] == 'unverified'
    assert result['coverage']['candidate_code_executed'] is False
    assert value['flows'] == []  # recognized legacy entry is not an executable native Flow
    assert any(d['code'] == 'unsupported_flow_protocol' for d in result['diagnostics'])
    checked = project_ops.check_project(original)
    assert checked['static_status'] == 'invalid'
    assert checked['coverage']['source_inspected'] == 1
    assert any(d.get('scope') == 'source_prerequisites' and 'initialization' in d['detail'] for d in checked['diagnostics'])
    assert business_bytes(original) == before
    assert not (original / 'tables').exists() and not (original / 'flows').exists()


def test_partial_source_check_is_bound_to_originals_and_declarations(original):
    ws.init_workspace(original, existing=True)
    result = project_ops.check_project(original)
    assert result['status'] == result['static_status'] == 'incomplete'
    assert result['coverage']['recognized_entries'] == result['coverage']['source_inspected'] == 1
    assert result['coverage']['declared'] == 0
    assert set(result['sources']) == {'dispatch.py', 'operators/clean.py'}
    assert project_ops.project_check_status(original)['status'] == 'incomplete'
    operator = original / 'operators/clean.py'
    operator.write_text(operator.read_text() + '\n# modified\n')
    assert 'source_changed:operators/clean.py' in project_ops.project_check_status(original)['changes']
    project_ops.check_project(original)
    path, value = declarations(original)
    value['pipelines']['shipments']['standard'] = 'unsupported-version'
    path.write_text(json.dumps(value))
    assert 'original_project_evidence_changed' in project_ops.project_check_status(original)['changes']


def test_project_check_tracks_the_checker_revision(original, monkeypatch):
    from demiflow.workbench.projects import checks as project_check
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.table_bindings import make_resolver
    ws.init_workspace(original, existing=True)
    result = project_ops.check_project(original)
    assert result['checker_platform_digest']
    monkeypatch.setattr(ws, 'check_snapshot', lambda path: {'status': 'current', 'current_digest': '0'*64})
    resolver = make_resolver(ws.table_binding, runs.result_table)
    status = project_check.status(original, table_resolver=resolver)
    assert 'platform_changed' not in status['changes']
    assert status['platform_provenance']['current_digest'] == '0'*64
    assert status['static_status'] == result['static_status']


@pytest.mark.parametrize('change,code', [
    ('unsupported', 'unsupported_standard'), ('duplicate_owner', 'conflicting_owner'),
    ('outside_entry', 'declaration_unreadable'), ('missing_entry', 'entry_unreadable'),
    ('wrong_symbol', 'entry_missing'), ('duplicate_json', 'declaration_unreadable'),
])
def test_explicit_gaps_are_not_empty_success(original, change, code):
    path, value = declarations(original)
    row = value['pipelines']['shipments']
    if change == 'unsupported':
        row['standard'] = 'v-next'
    elif change == 'duplicate_owner':
        value['pipelines']['other'] = dict(row)
    elif change == 'missing_entry':
        (original / 'dispatch.py').unlink()
    elif change == 'wrong_symbol':
        (original / 'dispatch.py').write_text('def different():\n    pass\n')
    elif change == 'outside_entry':
        row.update(standard='1.0.0-candidate.9', entry={'path': 'elsewhere/recipe.py', 'symbol': 'build'}, operators=[])
    path.write_text('{"schema":1,"schema":2}' if change == 'duplicate_json' else json.dumps(value))
    result = discover(original, {'entries': {}})
    assert result['status'] == 'partial'
    assert code in {d['code'] for d in result['diagnostics']}


def test_native_project_roles_need_no_directory_spelling(original):
    path, value = declarations(original)
    value['pipelines']['shipments'].update(standard='1.0.0-candidate.9',
        entry={'path': 'shipping/dispatch.py', 'symbol': 'run_pipeline'}, operators=['shipping/processing.py'])
    path.write_text(json.dumps(value))
    (original / 'processing.py').write_text('def process(row):\n    return row\n')
    result = discover(original, {'entries': {'dispatch.py:run_pipeline': ['settings/small.json']}})
    assert len(result['entries']) == 1
    assert result['entries'][0]['parameters'] == ['settings/small.json']
    assert result['operators'][0]['path'] == 'processing.py'
    assert not any(d['code'] == 'parameters_unbound' for d in result['diagnostics'])


def test_conflicting_protocol_is_reported(original):
    value = discover(original, {'entries': {'dispatch.py:run_pipeline': ['settings/small.json']}})
    assert 'conflicting_entry' in {d['code'] for d in value['diagnostics']}


def test_format_version_mismatch_and_sibling_paths(original, tmp_path):
    path, value = declarations(original)
    (original.parent / 'sibling').symlink_to(tmp_path)
    value['pipelines']['sibling'] = dict(path='sibling', entry='other.py')
    path.write_text(json.dumps(value))
    assert len(discover(original, {'entries': {}})['entries']) == 1
    value['pipelines']['shipments']['standard'] = '1.0.0-candidate.9'
    path.write_text(json.dumps(value))
    result = discover(original, {'entries': {}})
    assert 'unsupported_entry' in {d['code'] for d in result['diagnostics']}


def test_config_reference_budget_is_visible_without_hiding_the_project(original):
    config = {'entries': {'dispatch.py:run_pipeline': ['settings/p'+str(n)+'.json' for n in range(65)]}}
    value = discover(original, config)
    assert len(value['configurations']) == 64
    assert any(d['code'] == 'configuration_limit' for d in value['diagnostics'])


@pytest.mark.parametrize('raw', ['{"value": 1e309}', '{"value": NaN}',
                               '{"uri":"records/orders.lance","version":null}'])
def test_malformed_configuration_fields_are_diagnostics(original, raw):
    (original / 'settings/small.json').write_text(raw)
    result = discover(original, {'entries': {}})
    assert result['configurations'][0]['status'] == 'unreadable'
    assert any(d['code'] == 'configuration_unreadable' for d in result['diagnostics'])


def test_reference_shape_does_not_turn_arbitrary_uris_into_tables(original):
    (original / 'README.md').write_text('[settings](settings/small.json)\n`root_table.lance`')
    (original / 'settings/small.json').write_text(json.dumps({'format': [],
        'image': {'uri': 'https://example.test/image.png', 'version': None},
        'document': {'uri': 'document.pdf', 'version': 'edition-two'}}))
    result = discover(original, {'entries': {}})
    assert [t['path'] for t in result['tables']] == ['root_table.lance']
    assert result['tables'][0]['status'] == 'missing'
    assert result['configurations'][0]['status'] == 'original_configuration'
    assert any(d['code'] == 'configuration_format_unknown' for d in result['diagnostics'])


def test_owner_path_must_be_normalized(original):
    path, value = declarations(original)
    value['pipelines']['shipments']['path'] = './shipping'
    path.write_text(json.dumps(value))
    result = discover(original, {'entries': {}})
    assert result['entries'] == []
    assert any(d['code'] == 'invalid_owner' for d in result['diagnostics'])


def test_unknown_project_and_nearest_repository_boundary(tmp_path):
    parent = tmp_path / '.demiforge'; parent.mkdir()
    (parent / 'project.json').write_text('invalid')
    root = tmp_path / 'unrelated'; root.mkdir(); (root / '.git').mkdir()
    (root / 'main.py').write_text('raise RuntimeError("never import")')
    value = discover(root, {'entries': {}})
    assert value['entries'] == [] and value['declarations'] == []
    assert [d['code'] for d in value['diagnostics']] == ['entry_not_declared']


def test_symlink_private_and_external_references_are_not_read(original, tmp_path):
    outside = tmp_path / 'private.json'; outside.write_text('{"secret":"do not read"}')
    (original / 'settings/link.json').symlink_to(outside)
    (original / '.private.json').write_text('{"secret":1}')
    (original / 'settings/small.json').write_text(json.dumps({'source': {'uri': str(tmp_path / 'outside.lance'), 'version': 1}}))
    (original / 'README.md').write_text('[link](settings/link.json)\n[private](.private.json)\n[escape](../private.json)\n[ok](settings/small.json)')
    value = discover(original, {'entries': {}})
    assert value['tables'][0]['status'] == 'outside_project'
    assert not any(e['path'].endswith('private.json') or e['path'].endswith('link.json') for e in value['evidence'])
    assert not any(c.get('sha256') for c in value['configurations'] if c['path'] != 'settings/small.json')
    assert 'secret' not in json.dumps(value)
    ws.init_workspace(original, existing=True)
    with pytest.raises(ForgeError):
        project_ops.project_table(original, value['tables'][0]['id'], expected=value['digest'])


def test_configuration_budget_and_invalid_json_remain_visible(original):
    config = original / 'settings/small.json'
    config.write_text('{"value":1,"value":2}')
    value = discover(original, {'entries': {}})
    assert value['configurations'][0]['status'] == 'unreadable'
    assert any(d['code'] == 'configuration_unreadable' for d in value['diagnostics'])
    config.write_text(' ' * 65537)
    value = discover(original, {'entries': {}})
    assert value['configurations'][0]['status'] == 'unreadable'
    assert len(value['evidence']) < 128
    ws.init_workspace(original, existing=True)
    (original / 'README.md').write_text('x' * (ws.README_BYTES + 1))
    context = ws.workspace_context(original)
    assert '无法读取' in context['readme']
    assert any(d['code'] == 'documentation_unreadable' for d in context['project']['diagnostics'])


def test_preview_fixes_version_and_rejects_changed_evidence(original):
    ws.init_workspace(original, existing=True)
    before = business_bytes(original)
    value = ws.discover_project(original)
    row = next(t for t in value['tables'] if t['version'] is None)
    metadata = project_ops.project_table(original, row['id'], expected=value['digest'])
    assert metadata['row_count'] == 2 and metadata['version'] == 1
    args = dict(expected=value['digest'], version=1, binding=metadata['binding'])
    page = project_ops.project_sample(original, row['id'], columns=['order'], limit=1, **args)
    assert page['rows'][0]['order'] == 'alpha'
    assert business_bytes(original) == before
    lance.write_dataset(pa.table({'order': ['gamma'], 'amount': [99]}), str(original / 'records/orders.lance'), mode='append')
    assert project_ops.project_sample(original, row['id'], **args)['row_count'] == 2
    assert project_ops.project_table(original, row['id'], expected=value['digest'])['row_count'] == 3
    fixed = next(t for t in value['tables'] if t['version'] == 1)
    with pytest.raises(ForgeError, match='declared fixed reference'):
        project_ops.project_table(original, fixed['id'], expected=value['digest'], version=2)
    with pytest.raises(ForgeError, match='binding'):
        project_ops.project_sample(original, row['id'], **{**args, 'binding': '0'*64})
    (original / 'README.md').write_text('changed')
    with pytest.raises(ForgeError, match='declarations changed'):
        project_ops.project_sample(original, row['id'], **args)
    assert not (original / 'tables').exists()


def test_http_cli_and_change_observation_share_recognition(original, tmp_path):
    registry = tmp_path / 'console.json'
    registered = console.add(registry, original)
    first = project_ops.workspace_changes(original)
    with service(None, registry=registry, api_only=True) as app:
        prefix = '/api/workspaces/' + registered['workspace_id'] + '/'
        code, value = request(app, prefix + 'project-discovery')
        assert code == 200
        cli = CliRunner().invoke(main, ['workbench', 'discover', str(original), '--json'])
        assert cli.exit_code == 0 and json.loads(cli.output) == value
        config = value['configurations'][0]
        assert request(app, prefix + 'project-document?' + urlencode({'path': config['path'], 'expected': config['sha256']}))[0] == 200
        row = value['tables'][0]
        query = {'id': row['id'], 'expected': value['digest']}
        code, metadata = request(app, prefix + 'project-table?' + urlencode(query))
        assert code == 200, metadata
        query.update(version=metadata['version'], binding=metadata['binding'], limit=1)
        code, page = request(app, prefix + 'project-sample?' + urlencode(query))
        assert code == 200 and page['rows'][0]['order'] == 'alpha'
        assert request(app, prefix + 'project-sample?' + urlencode({**query, 'version': 0}))[0] == 400
        (original / 'settings/small.json').write_text('{}')
        assert request(app, prefix + 'project-sample?' + urlencode(query))[0] == 409
        assert first['discovery'] != request(app, prefix + 'changes')[1]['discovery']
