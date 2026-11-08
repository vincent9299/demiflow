"""Project onboarding never imports or rearranges candidate business code."""
import json

import pytest
from click.testing import CliRunner

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.schema import ForgeError


@pytest.fixture(scope='module')
def snapshot():
    return ws.build_snapshot(('demiflow.data',))


@pytest.fixture(autouse=True)
def cached_snapshot(monkeypatch, snapshot):
    monkeypatch.setattr(ws, 'build_snapshot', lambda *a, **kw: snapshot)


def test_create_only_control_files_and_no_false_pass(tmp_path):
    registry, root = tmp_path / 'console.json', tmp_path / 'new'
    result = console.create(registry, root, name='新项目')
    assert set(p.name for p in root.iterdir()) == {'.forge', 'workspace.json'}
    value = ws.load_workspace(root)[1]
    assert value['locations'] == {} and value['entries'] == {}
    assert not result['allow_runs']
    assert ws.workspace_context(root)['flows'] == []
    assert ws.workspace_context(root)['tables'] == []
    assert ws.workspace_changes(root)['workspace_id'] == result['workspace_id']
    with pytest.raises(ForgeError):
        console.create(registry, root, name='new')


def test_register_keeps_business_bytes_and_is_idempotent(tmp_path):
    root = tmp_path / 'existing'; root.mkdir()
    original = {'pipeline.py': b'raise RuntimeError("must never import")\n',
                'flows': b'An ordinary business file',
                'tables': b'An ordinary business file, not a Forge registry',
                'README.md': b'Original documentation', 'result.json': b'{"value": 7}'}
    for name, data in original.items():
        (root / name).write_bytes(data)
    registry = tmp_path / 'console.json'
    first = console.add(registry, root)
    assert console.add(registry, root)['status'] == 'unchanged'
    assert len(console.projects(console.load_registry(registry))['projects']) == 1
    for name, data in original.items():
        assert (root / name).read_bytes() == data
    assert first['root'] == str(root)
    assert not (root / 'operators').exists()
    assert ws.workspace_context(root)['tables'] == []


def test_registration_controls_conflict_does_not_overwrite(tmp_path):
    root = tmp_path / 'existing'; root.mkdir(); (root / '.forge').mkdir()
    (root / '.forge/owned').write_text('keep')
    with pytest.raises(ForgeError, match='already exist'):
        console.add(tmp_path / 'console.json', root)
    assert (root / '.forge/owned').read_text() == 'keep'
    assert not (root / 'workspace.json').exists()


def test_large_original_project_keeps_browsing_and_sync_without_data_configuration(tmp_path):
    root = tmp_path / 'existing'; root.mkdir()
    history = root / 'arbitrary-history'; history.mkdir()
    for number in range(1025):
        (history / f'{number}.json').write_text('{}')
    original = b'raise RuntimeError("must not execute")\n'
    (root / 'pipeline.py').write_bytes(original)
    (root / 'processing').mkdir()
    (root / 'processing/rows.py').write_text('def normalize(value): return value\n')
    console.add(tmp_path / 'console.json', root)
    context = ws.workspace_context(root)
    assert context['source_files'] == ['pipeline.py', 'processing/rows.py']
    assert context['locations'] == {} and context['flows'] == []
    assert any('arbitrary-history' in item for item in context['source_diagnostics'])
    before = ws.workspace_changes(root)
    assert before['source_diagnostics'] == context['source_diagnostics']
    (root / 'processing/rows.py').write_text('def normalize(value): return str(value)\n')
    assert ws.workspace_changes(root)['sources'] != before['sources']
    assert (root / 'pipeline.py').read_bytes() == original
    assert len(list(history.iterdir())) == 1025


def test_failed_publication_is_recoverable_without_reinitializing(tmp_path):
    root, registry = tmp_path / 'new', tmp_path / 'console.json'
    registry.write_text('{}')
    with pytest.raises(ForgeError):
        console.create(registry, root, name='example')
    identity = ws.load_workspace(root)[1]['id']
    registry.unlink()
    assert console.add(registry, root)['workspace_id'] == identity


def test_cli_create_and_add_share_registration(tmp_path):
    root, registry = tmp_path / 'new', tmp_path / 'console.json'
    cli = CliRunner()
    prefix = ['workbench', 'console', '--registry', str(registry)]
    created = cli.invoke(main, [*prefix, 'create', str(root), '--name', 'cli-project'])
    assert created.exit_code == 0, created.output
    added = cli.invoke(main, [*prefix, 'add', str(root)])
    assert added.exit_code == 0, added.output
    assert json.loads(created.output)['workspace_id'] == json.loads(added.output)['workspace_id']
