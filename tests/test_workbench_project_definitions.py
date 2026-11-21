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


def test_explicit_exports_do_not_depend_on_wiring_and_legacy_evidence_is_scoped(project):
    (project/'operators/review.py').write_text(
        '__demiflow_operators__ = ("ready",)\n'
        'def ready(row):\n    """Return review status."""\n    return row["ok"]\n'
        'def card(row):\n    return str(row)\n')
    (project/'operators/legacy.py').write_text(
        'class AllPoolReview:\n    pass\n'
        'def normalize(row):\n    return row\n')
    (project/'flows/select.py').write_text(
        'from demiflow import data as d\nfrom operators import legacy as old, review\n'
        'def run(*, limit=3):\n'
        '    old.AllPoolReview()\n'
        '    return d.from_items([]).map(old.normalize).map(review.card).limit(limit)\n')
    items={i['symbol']:i for i in definitions.catalog(project)['definitions']}
    assert items['ready']['export_status']=='declared' and not items['ready']['flow_references']
    assert items['card']['export_status']=='internal' and items['card']['flow_references']
    assert items['AllPoolReview']['export_status']=='legacy' and not items['AllPoolReview']['flow_references']
    assert items['normalize']['flow_references']==[dict(entry='flows/select.py:run',line=5,api='map',port='fn')]


def test_data_tools_have_an_independent_catalog_and_do_not_execute(project):
    folder=project/'data_tools'
    folder.mkdir()
    file=folder/'review.py'
    file.write_text('__demiflow_data_tools__ = ["AllPoolReview"]\n'
        'raise RuntimeError("never import")\nclass AllPoolReview:\n    """Review a page."""\n    pass\n'
        'def card():\n    pass\n')
    data=definitions.catalog(project,data_tools=True)
    assert [i['symbol'] for i in data['definitions']]==['AllPoolReview']
    item=data['definitions'][0]
    assert item['kind']=='data_tool' and item['definition_type']=='class'
    actual=definitions.read(project,data_tools=True,id=item['id'])
    assert actual['source'].startswith('class AllPoolReview:') and actual['line']==3
    with pytest.raises(ForgeError):
        definitions.read(project,id=item['id'])
    file.write_text('__demiflow_data_tools__=["missing"]\n')
    invalid=definitions.catalog(project,data_tools=True)
    assert not invalid['definitions'] and invalid['diagnostics'][0]['path']=='data_tools/review.py'


@pytest.mark.parametrize('declaration', ['["ready","ready"]', '["typo"]', 'create()', '["_private"]'])
def test_invalid_operator_exports_report_the_module(project,declaration):
    (project/'operators/bad.py').write_text(f'__demiflow_operators__={declaration}\ndef ready(row):\n    return row\n')
    index=definitions.catalog(project)
    assert any(d['path']=='operators/bad.py' for d in index['diagnostics'])
    assert not any(d['symbol']=='ready' for d in index['definitions'])


def test_data_tool_authoring_checks_exports_without_a_flow(project):
    from demiflow.forge.pipeline_check import check_changes
    directory=project/'data_tools'
    directory.mkdir()
    source=directory/'review.py'
    path=source.relative_to(project).as_posix()
    def check():
        return check_changes(project,changed={path},variants=[],layout_diagnostics=[],
                             coverage=[],catalog={},catalog_id='fixture')
    source.write_text('__demiflow_data_tools__ = ["review"]\ndef review():\n    pass\n')
    valid=check()
    assert valid['native_flow']['status']=='not_applicable'
    assert valid['project_layout']['status']=='passed'
    source.write_text('__demiflow_data_tools__ = ["typo"]\n')
    assert check()['project_layout']['status']=='failed'
