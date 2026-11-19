"""Natural project flow creation uses one conversation and a drained registration."""
import json
from pathlib import Path
import sys

from demiflow.workbench.conversations.service import Service
from demiflow.workbench.projects import workspace as ws
from test_forge_development_chat_managed import managed_project, gate_done, task_file
from test_forge_development_chat_repair import independent_reviewer
from test_forge_project_management import snapshot, cached_snapshot


def test_actual_restricted_add_flow_after_acceptance_reuses_project_and_baseline(managed_project, independent_reviewer, monkeypatch, tmp_path):
    from test_forge_development_runtime import combined_configuration
    from demiflow.forge.development.project import Project
    from demiflow.workbench.projects.discovery import discover
    root,path,config=managed_project
    declared=root/'.demiforge/project.json'; value=json.loads(declared.read_text())
    old=value.pop('pipelines')['orders']
    value.update(schema='demiforge-project-v3',standard='1.0.0-candidate.10',projects=dict(orders=dict(
        path='orders',flows=dict(existing=dict(path='orders/orders_pipeline.py',symbol='run_pipeline')),
        operators=['orders/operators'],prompts=[],notebooks=['orders/orders_debug.ipynb'],verify=[dict(
            id='unit',argv=[sys.executable,'-B','-m','unittest','discover','-s','orders/tests','-v'],
            env={'PYTHONPATH':'{project}'},timeout=10)])))
    ws.init_workspace(root,existing=True)
    params=root/'orders/existing.json';params.write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments=dict(options={}))))
    ws.register_flow(root,'orders/orders_pipeline.py:run_pipeline',params='orders/existing.json')
    value['readonly'].append('workspace.json');declared.write_text(json.dumps(value))
    executable=path.parent/'project-provider'
    fixture=Path(__file__).with_name('devchat_fake_project_provider.py')
    executable.write_text('#!'+sys.executable+'\nimport runpy\nrunpy.run_path('+repr(str(fixture))+',run_name="__main__")\n');executable.chmod(0o700)
    host=tmp_path/'host';host.mkdir()
    host_config,_,_,domain,_=combined_configuration(host,root,monkeypatch)
    partition=json.loads(host_config.read_text());domain.chmod(0o700)
    config.update({key:partition[key] for key in ('state_root','runtime_domain','host_budget')})
    endpoint,calls=independent_reviewer;monkeypatch.setenv('FIXTURE_REVIEW_KEY','offline-fixture-only')
    config['mode']='restricted';config['limits']['turn_seconds']=45
    config['providers']['codex'].update(executable=str(executable),base_url=endpoint,credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config))
    service=Service(path)
    try:
        sid=service.create_project('wfixture',root,'create','codex')['id']
        first=service.start_project('wfixture',root,sid,'first',0,'repair-fixture:project-initial')
        first_saved,accepted=gate_done(service,first['id'],timeout=45)
        assert accepted['state']=='accepted' and accepted['assurance']=='enforced',(accepted,first_saved['result'])
        original=json.loads(task_file(service,sid).read_text());original_history=first_saved['development']
        controls={name:(root/name).read_bytes() for name in ('.demiforge/project.json','workspace.json')}
        service.close();service=Service(path)
        second=service.start_project('wfixture',root,sid,'second',1,'Add an image quality flow and its reusable operator')
        saved,result=gate_done(service,second['id'],timeout=45)
        assert result['state']=='accepted' and result['assurance']=='enforced',(result,saved['result'])
        control=json.loads(saved['development_control'])
        assert control['repair_round']==1 and control['review_attempts']==1
        assert control['deadline']==saved['created']+45 and len(control['segments'])==2
        assert control['registration_intents'][0]['status']=='applied'
        updated=json.loads(task_file(service,sid).read_text())
        assert updated['baseline']==original['baseline'] and updated['readonly_baseline']==original['readonly_baseline']
        assert updated['goal']==original['goal'] and updated['binding']==original['binding']
        assert service.store.turn(first['id'])['development']==original_history
        revision=updated['managed']['registration_revisions'][0]
        assert revision['status']=='committed' and len(updated['managed']['registration_revisions'])==1
        for kind,name in (('project','.demiforge/project.json'),('workspace','workspace.json')):
            assert revision['plan'][kind]['before_raw'].encode()==controls[name]
            assert revision['plan'][kind]['after_raw'].encode()==(root/name).read_bytes()
        assert set(Project(root).entries('orders'))=={'existing','image_quality'}
        assert 'orders/flows/image_quality.py:run' in {entry['entry'] for entry in discover(root)['entries']}
        assert len(calls)==2
        events=service.store.events('wfixture',sid)['events']
        assert sum(event['type']=='project.registration.applied' for event in events)==1
        assert sum(event['type']=='message.user' for event in events)==2
        prompt=(service.root/sid/'provider/received-context.txt').read_text()
        assert 'flow-entry-image_quality' in prompt and 'Add an image quality flow' in prompt
        service.close();service=Service(path)
        assert service.show('wfixture',sid)['turn']['development']['state']=='accepted'
    finally:service.close()
