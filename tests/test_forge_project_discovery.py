"""Original declarations are evidence, never imports, execution or copied code."""
from demiflow.forge.project import initialize_project
from _forge_project_fixtures import declare_existing_flow
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


def register_fixture_flow(root, params='settings/preview.json'):
    """Register the fixture entry explicitly under the selected project."""
    if not (root / 'workspace.json').exists():
        initialize_project(root, existing=True)
    path = root / params
    if not path.exists():
        path.write_text('{"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}')
    declare_existing_flow(root, 'dispatch.py:run_pipeline', params=params)


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
    assert result['entries'] == []
    assert result['configurations'][0]['path'] == 'settings/small.json'
    assert result['operators'][0]['path'] == 'operators'
    assert len(result['tables']) == 2  # documented head plus fixed configuration
    assert result['runs'][0]['status'] == 'unverified'
    assert result['coverage']['candidate_code_executed'] is False
    assert value['flows'] == []
    assert any(d['code'] == 'entry_not_declared' for d in result['diagnostics'])
    assert business_bytes(original) == before
    assert not (original / 'tables').exists() and not (original / 'flows').exists()


def test_project_check_tracks_the_checker_revision(tmp_path, monkeypatch):
    from demiflow.forge import project_rules
    from demiflow.forge.project import register_flow
    from demiflow.workbench.projects import checks as project_check
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.table_bindings import make_resolver
    initialize_project(tmp_path)
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'flows/counts.py').write_text('from demiflow import data\ndef run():\n    return data.range(2).count()\n')
    register_flow(tmp_path, 'flows/counts.py:run')
    report = project_ops.check_project(tmp_path)
    assert report['status'] == 'checked'
    saved = ws.load_workspace(tmp_path)[2]
    current = ws.current_platform_snapshot(saved)
    resolver = make_resolver(ws.table_binding, runs.result_table)
    def status():
        return project_check.status(tmp_path, table_resolver=resolver)
    assert status()['status'] == 'checked'
    with monkeypatch.context() as m:
        m.setattr(ws, 'current_platform_snapshot', lambda _: {
            **current, 'digests': {**current['digests'], 'snapshot': '0'*64}})
        result = status()
        assert result['status'] == 'stale' and 'checker_platform_changed' in result['changes']
        assert result['platform_provenance']['status'] == 'stale'
    with monkeypatch.context() as m:
        m.setattr(ws, 'current_platform_snapshot', lambda _: {
            **current, 'catalog': {**current['catalog'], 'inspection_complete': False}})
        result = status()
        assert result['status'] == 'incomplete' and 'checker_platform_incomplete' in result['changes']
    with monkeypatch.context() as m:
        m.setattr(project_rules, 'RULES_ID', 'other-revision')
        result = status()
        assert result['status'] == 'stale' and 'authoring_rules_changed' in result['changes']
    # Reformatting a bound document changes the raw bytes recorded by the check.
    manifest = tmp_path / 'workspace.json'
    manifest.write_bytes(manifest.read_bytes() + b'\n')
    result = status()
    assert result['status'] == 'stale' and 'source_changed:workspace.json' in result['changes']


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


def test_unknown_project_and_nearest_repository_boundary(tmp_path):
    parent = tmp_path / '.demiforge'; parent.mkdir()
    (parent / 'project.json').write_text('invalid')
    root = tmp_path / 'unrelated'; root.mkdir()
    (root / 'main.py').write_text('raise RuntimeError("never import")')
    value = discover(root, {'entries': {}})
    assert value['entries'] == []
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
    initialize_project(original, existing=True)
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
    initialize_project(original, existing=True)
    (original / 'README.md').write_text('x' * (ws.README_BYTES + 1))
    context = ws.workspace_context(original)
    assert '无法读取' in context['readme']
    assert any(d['code'] == 'documentation_unreadable' for d in context['project']['diagnostics'])


def test_preview_fixes_version_and_rejects_changed_evidence(original):
    initialize_project(original, existing=True)
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
