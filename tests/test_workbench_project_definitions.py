"""Project definitions are independent from executions; current flow inputs are unique."""
import json
import subprocess
from pathlib import Path
import pytest
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.projects import definitions, flow_arguments
from demiflow.forge.schema import ForgeError
from test_forge_project_discovery import original, snapshot, cached_snapshot


@pytest.fixture
def project(original):
    (original / 'flows').mkdir()
    for name, default in [('select', 3), ('evaluate', 8)]:
        (original / f'flows/{name}.py').write_text(f'def run(*, limit: int = {default}, label: str = "demo"):\n    """{name}"""\n    raise RuntimeError("NEVER IMPORT")\n')
        (original / f'settings/{name}.json').write_text(json.dumps({'format':'demiflow_forge_flow_parameters_v1','arguments':{'limit':default}}))
    (original / 'prompts').mkdir()
    (original / 'prompts/review.yaml').write_text('schema_version: demiflow_prompt_pack_v2\nprompts:\n  review:\n    version: v1\n    template: "Review {{ value }}"\n    response_schema: {type: object}\n')
    declaration=dict(schema='demiforge-project-v3',standard='1.0.0-candidate.10',projects={'shipping':dict(path='shipping',flows={n:dict(path=f'shipping/flows/{n}.py',symbol='run') for n in ('select','evaluate')},operators=['shipping/operators'],prompts=['shipping/prompts'])})
    (original.parent / '.demiforge/project.json').write_text(json.dumps(declaration))
    ws.init_workspace(original, existing=True)
    for name in ('select','evaluate'): ws.register_flow(original, f'flows/{name}.py:run', params=f'settings/{name}.json')
    return original


def test_catalog_does_not_import_and_is_project_scoped(project):
    catalog=definitions.catalog(project)
    assert {(d['kind'],d['symbol']) for d in catalog['definitions']}=={('code','clean'),('prompt','review')}
    prompt=next(d for d in catalog['definitions'] if d['kind']=='prompt')
    actual=definitions.read(project,id=prompt['id'])
    assert actual['template']=='Review {{ value }}'
    assert actual['output_schema']=={'type':'object'}
    assert definitions.history(project,id=prompt['id'])['available'] is False
    with pytest.raises(ForgeError): definitions.read(project,id='../dispatch.py')


def test_repository_workspace_discovers_only_explicit_unique_flow_owner(project):
    from demiflow.workbench.projects.discovery import discover
    root = project.parent
    ws.init_workspace(root, existing=True)
    # No guessing from a nested directory or merely having one declaration.
    assert definitions.catalog(root)['definitions'] == []
    ws.register_flow(root, 'shipping/flows/select.py:run', params='shipping/settings/select.json')
    catalog = definitions.catalog(root)
    assert {(d['kind'], d['symbol']) for d in catalog['definitions']} == {('code','clean'), ('prompt','review')}
    (project/'operators/quality.py').write_text('raise RuntimeError("NEVER IMPORT")\ndef quality(row):\n    return row\n')
    assert {d['symbol'] for d in definitions.catalog(root)['definitions']} == {'clean', 'quality', 'review'}
    # A second referenced owner is ambiguous: do not silently choose or merge.
    path = root/'.demiforge/project.json'
    value = json.loads(path.read_text())
    value['projects']['other'] = dict(value['projects']['shipping'], path='another')
    path.write_text(json.dumps(value))
    assert definitions.catalog(root)['definitions'] == []
    assert any(d['code'] == 'conflicting_owner' for d in discover(root)['diagnostics'])


def test_real_versions_from_nested_repository_and_current_changes(project):
    repo=project.parent
    def git(*args): return subprocess.check_output(['git','-C',str(repo),*args],text=True).strip()
    git('init','-q');git('config','user.email','test@example.invalid');git('config','user.name','test')
    git('add','shipping/operators');git('commit','-qm','original')
    old=git('rev-parse','HEAD')
    (project/'operators/clean.py').write_text('def clean(row):\n    return {**row, "v": 2}\n')
    git('add','shipping/operators');git('commit','-qm','second')
    now=git('rev-parse','HEAD')
    item=next(d for d in definitions.catalog(project)['definitions'] if d['kind']=='code')
    versions=definitions.history(project,id=item['id'])
    assert [v['revision'] for v in versions['versions']]==[now,old]
    assert '"v": 2' in definitions.read(project,id=item['id'])['source']
    assert definitions.read(project,id=item['id'],revision=old)['source']=='def clean(row):\n    return row\n'
    assert (project/'operators/clean.py').read_text().endswith('"v": 2}\n')
    with pytest.raises(ForgeError):definitions.read(project,id=item['id'],revision='0'*40)


def test_current_arguments_are_per_flow_defaults_and_cas(project):
    entry='flows/select.py:run';current=flow_arguments.read(project,entry=entry)
    assert [(f['name'],f['value'],f['origin']) for f in current['fields']]==[('limit',3,'current'),('label','demo','default')]
    calls=[]
    result=flow_arguments.save(project,entry=entry,revision=current['revision'],source_sha256=current['source_sha256'],arguments={'limit':5},authorize=lambda:calls.append(True))
    assert len(calls)==2 and result['arguments']=={'limit':5}
    assert flow_arguments.read(project,entry='flows/evaluate.py:run')['arguments']=={'limit':8}
    with pytest.raises(ForgeError,match='已变化'):flow_arguments.save(project,entry=entry,revision=current['revision'],source_sha256=current['source_sha256'],arguments={'limit':6},authorize=lambda:None)
    with pytest.raises(ForgeError,match='类型'):flow_arguments.save(project,entry=entry,revision=result['revision'],source_sha256=result['source_sha256'],arguments={'limit':True},authorize=lambda:None)
    ws.register_flow(project,entry,params='settings/evaluate.json')
    with pytest.raises(ForgeError,match='多份参数'):flow_arguments.read(project,entry=entry)


def test_invalid_prompt_reports_source_without_losing_code(project):
    (project/'prompts/review.yaml').write_text('prompts: [malformed')
    value=definitions.catalog(project)
    assert [d['symbol'] for d in value['definitions']]==['clean']
    assert value['diagnostics'][0]['path']=='prompts/review.yaml'


def test_author_metadata_remains_available_while_native_reader_is_busy(project):
    from test_forge_public_api import service, request
    # Parameter I/O never steals run cancellation's reserved control slot.
    with service(project,enable_config_write=True,workspace_config_write=True) as app:
        assert app.workers.acquire(blocking=False)
        assert app.control_workers.acquire(blocking=False)
        try:
            status,value=request(app,'/api/flow-arguments?entry=flows%2Fselect.py%3Arun')
            assert status==200,value
            status,saved=request(app,'/api/flow-arguments-save',dict(entry=value['entry'],revision=value['revision'],source_sha256=value['source_sha256'],arguments={'limit':7}))
            assert status==200,saved
            assert saved['arguments']=={'limit':7}
        finally:
            app.control_workers.release();app.workers.release()


def test_parameter_write_is_denied_without_project_permission(project):
    from test_forge_public_api import service, request
    value=flow_arguments.read(project,entry='flows/select.py:run')
    with service(project,enable_config_write=True,workspace_config_write=False) as app:
        status,detail=request(app,'/api/flow-arguments-save',dict(entry=value['entry'],revision=value['revision'],source_sha256=value['source_sha256'],arguments={'limit':9}))
        assert status==403,detail
    assert flow_arguments.read(project,entry=value['entry'])['arguments']=={'limit':3}
