"""Project definitions are independent from executions; current flow inputs are unique."""
from demiflow.forge.project import initialize_project
from _forge_project_fixtures import declare_existing_flow
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
    initialize_project(original, existing=True)
    for name in ('select','evaluate'): declare_existing_flow(original, f'flows/{name}.py:run', params=f'settings/{name}.json')
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


def test_prompt_and_agent_tasks_share_the_catalog_and_select_their_own_source(project):
    import yaml
    from demiflow.forge.project import create_role_file
    from test_map_prompt_async import PACK

    task = yaml.safe_load(PACK)['prompts']['enrich']
    model = task.pop('model')
    document = dict(schema_version='demiflow_agent_v2', runtime='demiflow', model=model,
                    tasks={'review': task, 'independent': {**task, 'template': 'Second {{ payload | json }}'}},
                    budgets={'max_requests': 2}, operators={'custom': {
                        'fn': 'operators.would_raise:run', 'description': 'Inspect one item', 'replay':'verify',
                        'arguments': {'type': 'object', 'properties': {}, 'additionalProperties': False}}})
    (project/'operators/would_raise.py').write_text("raise AssertionError('must not import project code')\n")
    create_role_file(project, 'prompt', 'judge', content=yaml.safe_dump(document))
    index = definitions.catalog(project)
    assert not index['diagnostics']
    prompts = {(row['path'], row['symbol']): row for row in index['definitions'] if row['kind'] == 'prompt'}
    assert set(prompts) == {('prompts/review.yaml', 'review'), ('prompts/judge.yaml', 'review'),
                            ('prompts/judge.yaml', 'independent')}
    assert len({row['id'] for row in prompts.values()}) == 3
    agent = definitions.read(project, id=prompts['prompts/judge.yaml', 'independent']['id'])
    assert agent['model']['name'] == model['name'] and agent['template'] == 'Second {{ payload | json }}'
    assert yaml.safe_load(agent['source']) == document['tasks']['independent']
    assert agent['file_sha256'] == index['sources']['prompts/judge.yaml']
    pack = definitions.read(project, id=prompts['prompts/review.yaml', 'review']['id'])
    assert pack['template'] == 'Review {{ value }}'


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
    declare_existing_flow(project,entry,params='settings/evaluate.json')
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
