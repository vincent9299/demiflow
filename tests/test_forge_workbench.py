"""Real registered metadata, CAS faults and sealed-source/occurrence identities."""
from _forge_project_fixtures import declare_existing_flow
import hashlib
import json
import shutil
from urllib.parse import urlencode

import pytest

from demiflow.workbench.projects import registry as console
from demiflow.forge.runs import manager as runs

from demiflow.workbench.runs import view as run_view
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest,bounded_json
from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph import model
from demiflow.workbench.graph import parameters
from demiflow.workbench.graph import source_view
from test_forge_workspace import demo, isolated
from test_forge_public_api import service, request
from test_map_prompt_async import server

ENTRY='flows/questions.py:select'
PARAMS='flows/select_a.json'


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


def graph(root):
    return model.get(root,basis_kind='source',entry=ENTRY,params=PARAMS)


def replacement(root,value=0.7):
    g=graph(root)
    return dict(path=PARAMS,expected_sha256=g['parameters']['sha256'],graph_binding=g['binding'],
                replace=[dict(op='replace',path='/arguments/minimum',value=value)])


def test_explicit_v3_migration_preserves_grants_and_old_updates(tmp_path):
    path=tmp_path/'registry.json';key='w'+'1'*32
    original=dict(format=console.FORMAT,projects={key:dict(root=str(tmp_path),name='fixture',allow_runs=True)})
    path.write_text(json.dumps(original));before=path.read_bytes()
    assert not any(console.capabilities(console.load_registry(path)['projects'][key]).values())
    assert path.read_bytes()==before
    old=content_digest(original);result=console.migrate(path,expected=old)
    current=console.load_registry(path)
    assert current['format']==console.FORMAT_V3
    assert console.permissions(current['projects'][key])==dict(run_submit=True,run_control=True,attach=True)
    assert not any(console.capabilities(current['projects'][key]).values())
    assert ws.read_control(result['backup'])==original
    grants={k:k=='config_write' for k in console.CAPABILITIES}
    console.set_capabilities(path,key,expected=result['digest'],capabilities=grants)
    current=console.load_registry(path)
    console.set_permissions(path,key,expected=content_digest(current),run_submit=False,run_control=True,attach=False)
    current=console.load_registry(path)
    assert current['format']==console.FORMAT_V3
    assert console.capabilities(current['projects'][key])==grants
    with pytest.raises(ForgeError) as caught:
        console.set_capabilities(path,key,expected=old,capabilities=grants)
    assert caught.value.code=='conflict'
    with pytest.raises(ForgeError):
        console.set_capabilities(path,key,expected=content_digest(current),capabilities={**grants,'analysis_execute':True})
    assert console.load_registry(path)==current


def test_source_graph_is_not_check_and_save_has_no_parameter_receipt(isolated):
    g=graph(isolated)
    assert g['status']=='not_checked' and g['completeness']['semantic']=='unchecked'
    assert all(e['type']=='reference' for e in g['edges'])
    assert len({n['id'] for n in g['nodes']})==len(g['nodes'])
    body=replacement(isolated);before=(isolated/PARAMS).read_bytes()
    preview=parameters.preview(isolated,**body)
    assert (isolated/PARAMS).read_bytes()==before
    result=parameters.update(isolated,authorize=lambda:None,**body,proposed_sha256=preview['proposed_sha256'])
    assert result['applied'] and result['needs_check']
    assert hashlib.sha256((isolated/PARAMS).read_bytes()).hexdigest()==result['new_sha256']
    state=parameters.state(isolated,path=PARAMS)
    assert state['observed_sha256']==preview['proposed_sha256']
    assert state['attribution']=='unknown' and state['durability']=='not_proven_by_readback'
    assert 'receipt' not in state and 'request_id' not in result
    with pytest.raises(ForgeError) as caught:
        parameters.update(isolated,authorize=lambda:None,**body,proposed_sha256=preview['proposed_sha256'])
    assert caught.value.code=='conflict'


def test_two_clients_share_base_but_cannot_overwrite_after_conflict(isolated):
    a=replacement(isolated,0.7);b=replacement(isolated,0.6)
    pa=parameters.preview(isolated,**a);pb=parameters.preview(isolated,**b)
    parameters.update(isolated,authorize=lambda:None,**a,proposed_sha256=pa['proposed_sha256'])
    with pytest.raises(ForgeError) as caught:
        parameters.update(isolated,authorize=lambda:None,**b,proposed_sha256=pb['proposed_sha256'])
    assert caught.value.code=='conflict'
    assert parameters.state(isolated,path=PARAMS)['observed_sha256']==pa['proposed_sha256']
    assert b['expected_sha256']==a['expected_sha256']


def test_revoke_and_external_change_at_last_checkpoint_preserve_file(isolated):
    body=replacement(isolated);p=parameters.preview(isolated,**body);before=(isolated/PARAMS).read_bytes()
    calls=0
    def revoked():
        nonlocal calls
        calls+=1
        if calls==2:raise PermissionError('revoked')
    with pytest.raises(PermissionError):
        parameters.update(isolated,authorize=revoked,**body,proposed_sha256=p['proposed_sha256'])
    assert (isolated/PARAMS).read_bytes()==before
    assert not list((isolated/'flows').glob('.forge-parameters-*'))
    calls=0
    def external():
        nonlocal calls
        calls+=1
        if calls==2:
            value=json.loads(before);value['arguments']['minimum']=0.55
            (isolated/PARAMS).write_text(json.dumps(value))
    with pytest.raises(ForgeError) as caught:
        parameters.update(isolated,authorize=external,**body,proposed_sha256=p['proposed_sha256'])
    assert caught.value.code=='conflict'
    assert json.loads((isolated/PARAMS).read_text())['arguments']['minimum']==0.55


def test_fsync_failure_after_replace_is_not_reported_applied(isolated,monkeypatch):
    body=replacement(isolated);p=parameters.preview(isolated,**body)
    def fail(_):raise OSError('owned fault injection')
    monkeypatch.setattr(parameters,'fsync_directory',fail)
    with pytest.raises(ForgeError) as caught:
        parameters.update(isolated,authorize=lambda:None,**body,proposed_sha256=p['proposed_sha256'])
    assert caught.value.code=='operation_failed' and 'unknown after replace' in str(caught.value)
    assert parameters.state(isolated,path=PARAMS)['observed_sha256']==p['proposed_sha256']
    assert not list((isolated/'flows').glob('.forge-parameters-*'))


def test_native_provenance_and_real_author_source_are_bound_to_report(isolated):
    report=project_ops.inspect_flow(isolated,ENTRY,params=PARAMS)
    assert report['status']=='checked',report
    threshold=next(n for n in report['nodes'] if (n.get('operator') or {}).get('name')=='above_threshold')
    assert threshold['parameter_origins']['fn_kwargs/minimum']==dict(argument='minimum',path=[])
    normalizer=next(n for n in report['nodes'] if (n.get('operator') or {}).get('name')=='normalize_question')
    assert 'fn_kwargs/max_chars' not in normalizer['parameter_origins']
    g=model.get(isolated,basis_kind='report',report_digest=report['digest'])
    field=next(f for f in g['parameters']['fields'] if f['pointer']=='/arguments/minimum')
    assert threshold['id'] in field['affected_nodes']
    source=source_view.get(isolated,graph_binding=g['binding'],node_id=normalizer['id'],kind='implementation')
    assert source['path']=='operators/question_rules.py' and 'def normalize_question' in source['text']
    (isolated/source['path']).write_text('# changed author source\n')
    with pytest.raises(ForgeError) as caught:
        source_view.get(isolated,graph_binding=g['binding'],node_id=normalizer['id'],kind='implementation')
    assert caught.value.code=='conflict'


def test_sealed_graph_source_survives_current_source_and_params_removal(isolated):
    run=runs.prepare(isolated,ENTRY,params=PARAMS)
    g=model.get(isolated,basis_kind='run',run_id=run['run_id'],attempt=1)
    node=next(n for n in g['nodes'] if n.get('operator'))
    source=source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='implementation')
    (isolated/PARAMS).unlink()
    (isolated/source['path']).unlink()
    (isolated/'flows/questions.py').write_text('raise RuntimeError("must not import")\n')
    sealed=model.get(isolated,basis_kind='run',run_id=run['run_id'],attempt=1)
    assert sealed['binding']==g['binding'] and sealed['parameters']['readonly']
    assert source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='implementation')['text']==source['text']
    assert all(not f['writable'] for f in sealed['parameters']['fields'])


def test_report_source_reads_bound_revision_without_rechecking(isolated,monkeypatch):
    report=project_ops.inspect_flow(isolated,ENTRY,params=PARAMS)
    g=model.get(isolated,basis_kind='report',report_digest=report['digest'])
    node=next(n for n in report['nodes'] if (n.get('operator') or {}).get('name')=='above_threshold')
    original=report['binding']['parameters']['arguments']['minimum']
    changed=json.loads((isolated/PARAMS).read_text());changed['arguments']['minimum']=0.95
    (isolated/PARAMS).write_text(json.dumps(changed))
    def forbidden(*args,**kwargs):raise AssertionError('Source preview must not launch freshness verification')
    monkeypatch.setattr(project_ops,'report_status',forbidden)
    value=source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='configuration')
    assert value['source_basis']=='report_bound' and value['read_only']
    field=next(f for f in value['fields'] if f['pointer']=='/arguments/minimum')
    assert field['value']==original and not field['writable']
    assert 'def above_threshold' in source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='implementation')['text']
    with pytest.raises(ForgeError):
        source_view.get(isolated,graph_binding={**g['binding'],'workspace_id':'w'+'0'*32},node_id=node['id'],kind='implementation')


def test_native_prompt_schema_public_preview_and_sealed_source_never_fall_back(isolated,server):
    from test_forge_runtime_mock_http import current_prompt
    run=current_prompt(isolated,server)
    g=model.get(isolated,basis_kind='run',run_id=run['run_id'],attempt=1)
    node=next(n for n in g['nodes'] if n['kind']=='prompt')
    value=source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='prompt')
    projection=value['public_projection']
    assert value['historical'] and projection['schema_version']=='demiflow_prompt_pack_v2'
    assert list(projection['prompts'])==['enrich']
    assert '{{ payload | json }}' in projection['prompts']['enrich']['template']
    assert projection['prompts']['enrich']['model']['api_key_env']=='TEST_PROMPT_KEY'
    assert 'base_url' not in projection['prompts']['enrich']['model']
    path=isolated/'flows/prompt.yaml';path.write_text(path.read_text()+'\n# changed current copy\n')
    assert source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='prompt')['public_projection']==projection
    (runs.directory(isolated,run['run_id'])/'source/flows/prompt.yaml').unlink()
    with pytest.raises((ForgeError,OSError)):
        source_view.get(isolated,graph_binding=g['binding'],node_id=node['id'],kind='prompt')
    assert server['requests']==[]  # Preparation/preview did not call a model.


def test_prompt_preview_agent_v2_redacts_opaque_options_and_rejects_yaml_aliases():
    import yaml
    value=dict(schema_version='demiflow_agent_v2',runtime='codex',
               model=dict(name='public-model',api_key_env='PUBLIC_ENV_REFERENCE',api_key='owned-private-secret'),
               tasks={'enrich':dict(template='Read {{ payload }}',response_schema={'type':'string'})},
               options={'secret':'owned-private-secret'})
    projected=source_view._public_prompt(yaml.safe_dump(value).encode(),prompt='enrich')
    assert projected['tasks']['enrich']['template']=='Read {{ payload }}'
    assert projected['model']['api_key_env']=='PUBLIC_ENV_REFERENCE'
    assert 'owned-private-secret' not in json.dumps(projected)
    for raw in (b'schema_version: demiflow_prompt_pack_v2\nprompts: &a {x: {template: public}}\nother: *a\n',
                b'schema_version: demiflow_prompt_pack_v2\nprompts: {}\nprompts: {}\n'):
        with pytest.raises(ForgeError):source_view._public_prompt(raw)


def test_run_graph_configuration_uses_effective_sealed_output(isolated):
    run=runs.prepare(isolated,ENTRY,params=PARAMS,new_outputs=True)
    graph=model.get(isolated,basis_kind='run',run_id=run['run_id'],attempt=1)
    field=next(f for f in graph['parameters']['fields'] if f['pointer']=='/arguments/target/relative_uri')
    effective=run['report']['binding']['parameters']['arguments']['target']['relative_uri']
    assert field['value']==effective
    assert field['value']!=run['original_parameters']['arguments']['target']['relative_uri']
    assert not field['writable']


def test_nested_parameter_paths_correlate_only_the_actual_consumers(isolated):
    source=(isolated/'flows/questions.py').read_text().split('\ndef select_a(')[0]
    source=source.replace('def _select(*, candidates, scores, target, minimum):','def run(*, candidates, scores, target, settings):')
    source=source.replace('"max_chars": 4096','"max_chars": settings["max_chars"]').replace('"minimum": minimum','"minimum": settings["minimum"]')
    (isolated/'flows/nested.py').write_text(source)
    value=json.loads((isolated/PARAMS).read_text())
    minimum=value['arguments'].pop('minimum')
    value['arguments']['settings']=dict(minimum=minimum,max_chars=1024)
    (isolated/'flows/nested.json').write_text(json.dumps(value))
    declare_existing_flow(isolated,'flows/nested.py:run',params='flows/nested.json')
    report=project_ops.inspect_flow(isolated,'flows/nested.py:run',params='flows/nested.json')
    assert report['status']=='checked',report['diagnostics']
    graph=model.get(isolated,basis_kind='report',report_digest=report['digest'])
    fields={f['pointer']:f for f in graph['parameters']['fields']}
    normalizer=next(n for n in graph['nodes'] if (n.get('operator') or {}).get('name')=='normalize_question')
    threshold=next(n for n in graph['nodes'] if (n.get('operator') or {}).get('name')=='above_threshold')
    assert fields['/arguments/settings/max_chars']['affected_nodes']==[normalizer['id']]
    assert fields['/arguments/settings/minimum']['affected_nodes']==[threshold['id']]
    assert fields['/arguments/settings/minimum']['writable']
    table=next(n for n in graph['nodes'] if n['kind']=='read_table')
    assert table['id'] in fields['/arguments/candidates/alias']['affected_nodes']


def test_observation_pages_are_occurrence_and_attempt_bound(isolated):
    run=runs.prepare(isolated,ENTRY,params=PARAMS);journal=runs.journal(isolated,run['run_id'])
    action='a'*16;node='b'*16
    journal.reserve_nodes([(action,node,index) for index in range(3)])
    compact=runs.show(isolated,run['run_id'],attempt=1,compact=True)
    assert compact['observation_scope']=='paged_run_observations'
    assert 'observations' not in compact and 'actions' not in compact
    assert len([o for o in runs.show(isolated,run['run_id'],attempt=1)['observations'] if o['node_id']==node])==3
    page=run_view.observations(isolated,run['run_id'],attempt=1,node_id=node,limit=1)
    assert page['scope']=='correlated_node_observations' and page['has_more']
    assert page['observations'][0]['occurrence']==0
    next_page=run_view.observations(isolated,run['run_id'],attempt=1,node_id=node,cursor=page['cursor'],limit=1)
    assert next_page['observations'][0]['occurrence']==1
    with pytest.raises(ForgeError) as caught:
        run_view.observations(isolated,run['run_id'],attempt=1,node_id='c'*16,cursor=page['cursor'])
    assert caught.value.code=='conflict'
    journal.transition({'prepared'},'failed',recoverable=True)
    old=journal.execution_status()
    target, observed = runs._load(isolated, run['run_id'], detail='identity')
    manifest = observed['manifest']
    journal.begin_attempt(expected_attempt=1,expected_revision=old['revision'],
        nodes=[(item['action_id'], item['node_id'], item['occurrence']) for item in manifest['observation_instances']],
        actions=runs._fact_actions(runs._report(target, manifest), manifest['outputs']))
    with pytest.raises(ForgeError) as caught:
        run_view.observations(isolated,run['run_id'],attempt=2,node_id=node,cursor=page['cursor'])
    assert caught.value.code=='conflict'
    assert len(run_view.observations(isolated,run['run_id'],attempt=1,node_id=node)['observations'])==3


def test_http_config_write_requires_both_grant_and_deployment_ceiling(isolated):
    body=replacement(isolated)
    for flags in ({},{'enable_runs':True},{'enable_config_write':True},{'workspace_config_write':True}):
        with service(isolated,**flags) as app:
            assert request(app,'/api/parameter-preview',body)[0]==403
    with service(isolated,enable_config_write=True,workspace_config_write=True) as app:
        assert request(app,'/api/parameter-preview',{**body,'request_id':'invented'})[0]==400
        status,p=request(app,'/api/parameter-preview',body)
        assert status==200,p
        assert request(app,'/api/parameters-update',{**body,'proposed_sha256':p['proposed_sha256']})[1]['applied']
        assert request(app,'/api/context')[1]['run_permissions']==dict(run_submit=False,run_control=False,attach=False)
        assert not request(app,'/api/context')[1]['capabilities']['analysis_execute']


def test_console_http_workbench_projection_keeps_v1_permission_keys(isolated,tmp_path):
    registry=tmp_path/'registry.json';key=console.add(registry,isolated)['workspace_id']
    prefix='/api/workspaces/'+key+'/'
    grants={k:k=='config_write' for k in console.CAPABILITIES}
    before=registry.read_bytes()
    with service(None,registry=registry,enable_config_write=True) as app:
        capabilities=request(app,'/api/capabilities')[1]
        assert set(capabilities['permissions'])=={'read','inspect','audit_check','runs','run_submit','run_control','attach','project_manage','project_check'}
        assert capabilities['workbench_permissions']=={k:k=='config_write' for k in console.CAPABILITIES}
        assert not request(app,prefix+'context')[1]['capabilities']['config_write']
        assert registry.read_bytes()==before
        expected=content_digest(console.load_registry(registry))
        assert request(app,'/api/project-capabilities',dict(workspace_id=key,expected=expected,capabilities=grants))[0]==422
        console.migrate(registry,expected=expected)
        expected=content_digest(console.load_registry(registry))
        assert request(app,'/api/project-capabilities',dict(workspace_id=key,expected=expected,capabilities=grants))[0]==200
        assert request(app,prefix+'context')[1]['capabilities']['config_write']
        assert request(app,prefix+'parameter-preview',replacement(isolated))[0]==200
    with service(None,registry=registry,enable_runs=True) as app:
        assert not request(app,prefix+'context')[1]['capabilities']['config_write']
        assert request(app,prefix+'parameter-preview',replacement(isolated))[0]==403
        expected=content_digest(console.load_registry(registry))
        assert request(app,'/api/project-permissions',dict(workspace_id=key,expected=expected,run_submit=True,run_control=False,attach=False))[0]==200
        assert console.capabilities(console.load_registry(registry)['projects'][key])==grants


def test_unknown_argument_and_unsafe_js_integer_are_not_editable(isolated):
    path=isolated/PARAMS;value=json.loads(path.read_text())
    value['arguments']['unknown_field']=1
    value['arguments']['minimum']=2**60+7
    path.write_text(json.dumps(value))
    g=graph(isolated);fields={f['pointer']:f for f in g['parameters']['fields']}
    assert not fields['/arguments/unknown_field']['writable']
    assert not fields['/arguments/minimum']['writable']
    assert fields['/arguments/minimum']['value']==str(2**60+7)
    body=dict(path=PARAMS,expected_sha256=g['parameters']['sha256'],graph_binding=g['binding'],replace=[dict(op='replace',path='/arguments/unknown_field',value=2)])
    with pytest.raises(ForgeError) as caught:parameters.preview(isolated,**body)
    assert caught.value.code=='invalid_request'


def test_unknown_resource_fields_are_not_returned_as_public_configuration(isolated):
    path=isolated/PARAMS;value=json.loads(path.read_text())
    value['arguments']['opaque']={'kind':'unrecognized','credential':'owned-fixture-secret'}
    value['arguments']['public_reference']={'kind':'config','path':'flows/model.yaml','credential':'owned-fixture-secret'}
    path.write_text(json.dumps(value))
    projected=graph(isolated)['parameters']
    assert 'owned-fixture-secret' not in json.dumps(projected)
    fields={f['pointer']:f for f in projected['fields']}
    assert fields['/arguments/opaque']['value']=={'kind':'unsupported'}
    assert fields['/arguments/public_reference']['value']=={'kind':'config','path':'flows/model.yaml'}
    assert not fields['/arguments/public_reference']['writable']


def test_observation_revision_detects_add_update_rollback_and_cleanup(isolated):
    run=runs.prepare(isolated,ENTRY,params=PARAMS);run_id=run['run_id'];journal=runs.journal(isolated,run_id)
    action='a'*16;node='b'*16
    journal.reserve_nodes([(action,node,index) for index in range(70)])
    document=dict(sequence=1,state='running',timing_scope='group',rows_output=10,
                  detail='x'*1024,group_members=[('member'+str(i)).ljust(48,'x') for i in range(100)])
    journal.observe_nodes([dict(action_id=action,node_id=node,occurrence=i,document=document) for i in range(70)])
    page=run_view.observations(isolated,run_id,attempt=1,node_id=node,limit=64)
    assert len(bounded_json(page,96*1024))<=96*1024 and len(page['observations'])<64 and page['has_more']
    control=journal.execution_status()['revision'];before=run_view.snapshot(isolated,run_id,attempt=1)
    journal.reserve_nodes([(action,node,70)])
    after=run_view.snapshot(isolated,run_id,attempt=1)
    assert journal.execution_status()['revision']==control
    assert before['node_sequence']==after['node_sequence']
    assert before['observation_revision']!=after['observation_revision'] and before['revision']!=after['revision']
    with pytest.raises(run_view.ObservationResetRequired) as caught:
        run_view.observations(isolated,run_id,attempt=1,node_id=node,cursor=page['cursor'])
    assert caught.value.reason=='observations_changed' and caught.value.reset_required
    page=run_view.observations(isolated,run_id,attempt=1,node_id=node,limit=1)
    journal.observe_nodes([dict(action_id=action,node_id=node,occurrence=0,document={**document,'sequence':2,'rows_output':11})])
    with service(isolated) as app:
        status,error=request(app,'/api/run-observations?'+urlencode(dict(run_id=run_id,attempt=1,node_id=node,cursor=page['cursor'])))
    assert status==409 and error['reset_required'] and error['reason']=='observations_changed'
    updated=run_view.observations(isolated,run_id,attempt=1,node_id=node,limit=1)
    # Owned SQL fault injection models restoring an earlier metadata snapshot;
    # it is not a new observation producer or a claim of real execution.
    with journal._transaction() as db:
        db.execute('UPDATE node_observations SET sequence=1,document=? WHERE attempt=1 AND action_id=? AND node_id=? AND occurrence=0',
                   (json.dumps(document),action,node))
    with pytest.raises(run_view.ObservationResetRequired):
        run_view.observations(isolated,run_id,attempt=1,node_id=node,cursor=updated['cursor'])
    restored=run_view.observations(isolated,run_id,attempt=1,node_id=node,limit=1)
    with journal._transaction() as db:
        db.execute('DELETE FROM node_observations WHERE attempt=1 AND action_id=? AND node_id=? AND occurrence=70',(action,node))
    with pytest.raises(run_view.ObservationResetRequired):
        run_view.observations(isolated,run_id,attempt=1,node_id=node,cursor=restored['cursor'])


def test_observation_cursor_rejects_same_run_reference_in_another_project(isolated,tmp_path):
    from demiflow.forge.runs import domain as run_domain

    from demiflow.workbench.runs.authority import LocalAuthority
    from demiflow.workbench.worker import operation
    first=runs.prepare(isolated,ENTRY,params=PARAMS)
    page=run_view.observations(isolated,first['run_id'],attempt=1,limit=1)
    other=tmp_path/'other-project'
    shutil.copytree(isolated,other,ignore=shutil.ignore_patterns('runs','requests'))
    control=ws.read_control(other/'workspace.json');expected=content_digest(control);control['id']='w'+'f'*32
    ws.write_control(other/'workspace.json',control,expected=expected)
    with run_domain.preparation_lock():
        second=operation('run_prepare',other,entry=ENTRY,params=PARAMS,reserved_run_id=first['run_id'],
                         _authority=LocalAuthority(str(other.absolute())).bootstrap())
    assert second['run_id']==first['run_id'] and second['digest']!=first['digest']
    with pytest.raises(run_view.ObservationResetRequired) as caught:
        run_view.observations(other,second['run_id'],attempt=1,cursor=page['cursor'])
    assert caught.value.reason=='cursor_binding_changed'


def test_source_display_clipping_does_not_claim_semantic_coverage(isolated):
    path=isolated/'flows/unexpanded.py'
    path.write_text('def run():\n'+''.join('    unknown()\n' for _ in range(260)))
    params=isolated/'flows/unexpanded.json'
    params.write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments={})))
    declare_existing_flow(isolated,'flows/unexpanded.py:run',params='flows/unexpanded.json')
    g=model.get(isolated,basis_kind='source',entry='flows/unexpanded.py:run',params='flows/unexpanded.json')
    assert g['completeness']['display']=='clipped' and g['completeness']['total_nodes']==260
    assert len(g['nodes'])==128 and g['completeness']['semantic']=='unchecked'
    assert g['diagnostics'] and g['status']!='checked'
    index=model.get(isolated,basis_kind='source',entry='flows/unexpanded.py:run',params='flows/unexpanded.json',node_offset=192)
    assert index['node_index']['offset']==192 and len(index['node_index']['nodes'])==64
    selected=index['node_index']['nodes'][30]['id']
    local=model.get(isolated,basis_kind='source',entry='flows/unexpanded.py:run',params='flows/unexpanded.json',node_id=selected)
    assert local['binding']==g['binding'] and local['completeness']['scope_node']==selected
    assert len(local['nodes'])==1 and local['nodes'][0]['id']==selected
    assert source_view.get(isolated,graph_binding=g['binding'],node_id=selected,kind='callsite')['text'].strip()=='unknown()'


def test_source_edges_do_not_guess_last_branch_or_cross_nested_scopes(isolated):
    source=isolated/'flows/syntax.py';params=isolated/'flows/syntax.json'
    source.write_text('''def run(*, flag):
    def later():
        not_executed_here()
    if flag:
        selected = left()
    else:
        selected = right()
    return consume(selected, lambda selected: also_deferred(selected))
''')
    params.write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments={'flag':True})))
    declare_existing_flow(isolated,'flows/syntax.py:run',params='flows/syntax.json')
    observed=model.get(isolated,basis_kind='source',entry='flows/syntax.py:run',params='flows/syntax.json')
    assert {n['api'] for n in observed['nodes']}=={'left','right','consume'}
    assert observed['edges']==[]
    assert any(d['code']=='source_control_flow_unchecked' for d in observed['diagnostics'])
    source.write_text('def run(*, flag):\n    value = left()\n    result = consume(value)\n    return result\n')
    linear=model.get(isolated,basis_kind='source',entry='flows/syntax.py:run',params='flows/syntax.json')
    assert len(linear['edges'])==1 and linear['edges'][0]['type']=='reference'
    assert linear['status']=='not_checked'
