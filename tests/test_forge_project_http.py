"""The HTTP client shares project creation, registration and rule checks."""
from demiflow.forge.project import initialize_project
import json

from click.testing import CliRunner

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from test_forge_project_management import snapshot, cached_snapshot
from test_forge_public_api import request, service


def test_http_project_management_and_headless_visibility(tmp_path):
    registry = tmp_path / 'console.json'
    registry.write_text(json.dumps(dict(format=console.FORMAT, projects={})))
    existing = tmp_path / 'existing'; existing.mkdir()
    (existing / 'pipeline.py').write_text('raise RuntimeError("must never execute")\n')
    before = (existing / 'pipeline.py').read_bytes()
    with service(None, registry=registry, api_only=True) as app:
        assert request(app, '/api/capabilities')[1]['permissions']['project_manage']
        assert request(app, '/api/project-create', {'root':str(tmp_path/'bad'),'name':'bad'},
                       headers={'X-Forge-Token':'wrong'})[0] == 403
        assert not (tmp_path/'bad').exists()
        assert request(app, '/api/project-register', {'root':'relative'})[0] == 400
        status, new = request(app, '/api/project-create', {'root':str(tmp_path/'new'),'name':'new'})
        assert status == 200, new
        code, registered = request(app, '/api/project-register', {'root':str(existing)})
        assert code == 200, registered
        assert (existing / 'pipeline.py').read_bytes() == before
        cli = CliRunner().invoke(main, ['workbench','console','--registry',str(registry),'list'])
        assert cli.exit_code == 0
        assert len(json.loads(cli.output)['projects']) == 2
        key = registered['workspace_id']
        assert request(app, '/api/project-register', {'root':str(existing)})[1]['status'] == 'unchanged'
        assert request(app, f'/api/workspaces/{key}/project-check', {'skip_rules':True})[0] == 400
        code, check = request(app, f'/api/workspaces/{key}/project-check', {})
        assert code == 200 and check['status'] == 'incomplete', check
        assert check['coverage']['declared'] == 0
        assert check['diagnostics'][0]['rule'] == 'FL-01'
        assert request(app, f'/api/workspaces/{key}/run-prepare', {})[0] == 403
        assert (existing / 'pipeline.py').read_bytes() == before
        assert set(p.name for p in (tmp_path/'new').iterdir()) == {'.forge','workspace.json'}


def test_missing_project_check_cli_is_nonzero_without_fixing(tmp_path):
    root=tmp_path/'project'; initialize_project(root)
    result=CliRunner().invoke(main,['workbench','check',str(root)])
    assert result.exit_code == 1
    assert json.loads(result.output)['status'] == 'incomplete'
    assert set(p.name for p in root.iterdir()) == {'.forge','workspace.json'}


def test_http_project_create_grants_default_permissions_up_to_ceiling(tmp_path):
    registry = tmp_path / 'console.json'
    registry.write_text(json.dumps(dict(format=console.FORMAT_V3, projects={})))
    existing = tmp_path / 'existing'
    existing.mkdir()
    with service(None, registry=registry, api_only=True, enable_runs=True, enable_config_write=True) as app:
        status, created = request(app, '/api/project-create', {'root': str(tmp_path / 'fresh'), 'name': 'fresh'})
        assert status == 200, created
        new_id = created['workspace_id']
        saved = console.load_registry(registry)['projects'][new_id]['permissions']
        assert saved['config_write'] is True
        assert saved['run_submit'] is True
        assert saved['run_control'] is True
        assert saved['attach'] is False
        assert saved['case_annotate'] is False
        # Newly created project can create .env immediately without manual permission setup.
        c_status, c_body = request(app, f'/api/workspaces/{new_id}/project-file-create', {'path': '.env', 'text': 'KEY=\n'})
        assert c_status == 200, c_body
        # Registering an existing directory remains read-only by default and returns a friendly Chinese hint.
        r_status, reg = request(app, '/api/project-register', {'root': str(existing), 'name': 'existing'})
        assert r_status == 200, reg
        reg_id = reg['workspace_id']
        reg_perms = console.load_registry(registry)['projects'][reg_id]['permissions']
        assert reg_perms['config_write'] is False and reg_perms['run_submit'] is False
        deny_status, deny_body = request(app, f'/api/workspaces/{reg_id}/project-file-create', {'path': '.env', 'text': ''})
        assert deny_status == 403, deny_body
        assert '文件与配置写入' in deny_body.get('error', '') and '项目设置' in deny_body.get('error', '')
