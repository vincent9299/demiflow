"""One host setting, real per-project grants and immutable conversation history."""
import json
import os
from pathlib import Path
import tempfile
import urllib.error
import urllib.request

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.global_settings import GlobalSettings, automatic_project
from demiflow.workbench.conversations.service import Service
from test_forge_development_chat import configuration, wait_turn
from test_forge_development_chat_http import app


def payload(settings, **changes):
    current=settings.public()
    return dict(expected=current['revision'],provider='codex',model='fixture-global',effort='high',
                authentication='api_key',base_url='https://example.invalid/v1',api_key='fixture-private-key',**changes)


def setup(tmp_path):
    root,path,_=configuration(tmp_path)
    service=Service(path)
    inventory={'projects':{'wfixture':{'root':str(root)}}}
    settings=GlobalSettings(service,lambda:inventory);service.settings=settings
    return root,path,service,settings,inventory


def test_save_inherits_registered_and_new_projects_without_touching_sources(tmp_path):
    root,path,service,settings,inventory=setup(tmp_path)
    try:
        before=(root/'code.py').read_bytes()
        result=settings.save(payload(settings))
        assert result['inherited'] and result['has_credentials']
        assert 'fixture-private-key' not in json.dumps(result)
        assert not (settings.path.stat().st_mode & 0o077)
        assert service.project('wfixture',root)['writes']==['code.py']  # Preserve original narrower grants.
        original=service.current_hash('wfixture',root)
        other=tmp_path/'other';other.mkdir();(other/'main.py').write_text('x=1\n')
        inventory['projects']['wnew']={'root':str(other)}
        assert service.capabilities('wnew',other)['start']
        grant=service.project('wnew',other)
        assert 'main.py' in grant['writes'] and 'AGENTS.md' not in grant['writes']
        assert service.current_hash('wfixture',root)==original
        with pytest.raises(ForgeError):service.create('outsider',other,'bad','codex')
        with pytest.raises(ForgeError):service.create('wnew',root,'bad','codex')
        sid=service.create_project('wnew',other,'new','codex')['id']
        tid=service.start('wnew',other,sid,'turn',0,'A small normal task')['id']
        assert wait_turn(service,tid)['state']=='completed'
        assert (root/'code.py').read_bytes()==before
        assert list(other.iterdir())==[other/'main.py']
        inventory['projects'].pop('wnew')
        with pytest.raises(ForgeError):service.project('wnew',other)
    finally:service.close()


def test_restart_and_changes_preserve_history_and_pinned_policies(tmp_path):
    root,path,service,settings,inventory=setup(tmp_path)
    try:
        old=service.create('wfixture',root,'old','codex')['id']
        settings.save(payload(settings))
        assert service.show('wfixture',old)['writable'] is False
        new=service.create_project('wfixture',root,'new','codex')['id']
        tid=service.start('wfixture',root,new,'turn',0,'A small normal task')['id']
        assert wait_turn(service,tid)['state']=='completed'
        events=service.store.events('wfixture',new)
        selected=service.current_hash('wfixture',root)
        credential=settings.value['config']['credential_env']
    finally:service.close()
    os.environ.pop(credential)
    service=Service(path);settings=GlobalSettings(service,lambda:inventory);service.settings=settings
    try:
        assert settings.public()['model']=='fixture-global'
        assert os.environ[credential]=='fixture-private-key'
        assert service.current_hash('wfixture',root)==selected
        request=payload(settings);request.update(model='fixture-next',api_key='')
        settings.save(request)
        assert service.store.events('wfixture',new)==events
        assert service.show('wfixture',new)['writable'] is False
        assert service.show('wfixture',new)['model']=='fixture-global'
        snapshot=service.session_policy(service.store.session(new))
        assert snapshot.providers['codex']['model']=='fixture-global'
    finally:service.close()


def test_compare_and_swap_invalid_and_busy_save_are_atomic(tmp_path):
    root,path,service,settings,_=setup(tmp_path)
    try:
        request=payload(settings);settings.save(request);raw=settings.path.read_bytes()
        with pytest.raises(ForgeError,match='其他页面'):settings.save(request)
        for bad in ({'base_url':'https://user:secret@example.com'}, {'provider':'shell'}, {'model':''}, {'api_key':'key\nsecond'}):
            invalid=payload(settings);invalid.update(bad)
            with pytest.raises(ForgeError):settings.save(invalid)
            assert settings.path.read_bytes()==raw
        sid=service.create_project('wfixture',root,'new','codex')['id']
        tid=service.start('wfixture',root,sid,'turn',0,'input')['id'];wait_turn(service,tid,False)
        with pytest.raises(ForgeError,match='正在运行'):settings.save(payload(settings))
        assert settings.path.read_bytes()==raw
        service.cancel('wfixture',root,sid,tid,'cancel');wait_turn(service,tid)
    finally:service.close()


def test_automatic_managed_project_keeps_parent_forge_gates(tmp_path,monkeypatch):
    from test_forge_development import DevelopmentTests
    from demiflow.workbench.conversations.development import prepare
    from demiflow.workbench.conversations.context import assemble
    monkeypatch.setattr(tempfile,'tempdir',str(tmp_path))
    fixture=DevelopmentTests();fixture.setUp()
    host=tmp_path/'host';host.mkdir();root,path,service,settings,inventory=setup(host)
    try:
        selected=fixture.root/'orders';grant=automatic_project(selected)
        assert grant['managed'] and grant['development_root']==str(fixture.root)
        assert grant['pipeline']=='orders'
        inventory['projects']['worders']={'root':str(selected)}
        settings.save(payload(settings))
        sid=service.create_project('worders',selected,'managed','codex')['id']
        session=service.store.session(sid);project=service.project('worders',selected)
        from demiflow.workbench.conversations.common import root_identity
        turn,_=service.store.submit('worders',sid,'turn',0,'Preserve orders',root_identity(selected),session['policy_hash'])
        policy=service.session_policy(session);tasks=prepare(policy,service.store,turn,session,project)
        scratch=tmp_path/'scratch';scratch.mkdir()
        manifest,prompt=assemble(project,'Preserve orders',scratch,policy.limits,managed_tasks=tasks,task_id=sid,
                                current_request={'turn_id':turn['id'],'text':turn['text'],'event_ids':[turn['id']+':0']})
        assert manifest['kind']=='demiforge_registered'
        assert all(p.startswith('orders/') for p in manifest['binding']['writes'])
        assert 'Preserve orders' in prompt
    finally:service.close();fixture.doCleanups()


def test_automatic_scope_preserves_nested_data_and_notebooks(tmp_path):
    root=tmp_path/'code';root.mkdir();src=root/'src';src.mkdir()
    (src/'data').mkdir();(src/'data/raw.txt').write_text('business data')
    (src/'result.ipynb').write_text('{}');(src/'AGENTS.md').write_text('rules')
    (src/'app.py').write_text('x=1')
    grant=automatic_project(root)
    assert set(grant['protected'])=={'src/data','src/result.ipynb','src/AGENTS.md'}


def call(server,body=None,token=None):
    request=urllib.request.Request(server.origin+'/api/development-settings',
        data=json.dumps(body).encode() if body is not None else None,
        headers={'X-Forge-Token':server.token if token is None else token,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request) as response:return response.status,json.load(response)
    except urllib.error.HTTPError as exc:return exc.code,json.load(exc)


def test_global_http_auth_contract_and_secret_non_disclosure(tmp_path):
    with app(tmp_path) as (server,workspace,root):
        assert call(server,token='wrong')[0]==403
        status,current=call(server);assert status==200
        status,value=call(server,payload(server.development_settings));assert status==200,value
        assert value['inherited']
        status,value=call(server);assert status==200 and value['has_credentials']
        assert 'fixture-private-key' not in json.dumps(value)
        request=payload(server.development_settings);request['executable']='/bin/sh'
        assert call(server,request)[0]==400


def test_existing_connection_automatically_applies_to_new_registered_project(tmp_path):
    root,path,service,settings,inventory=setup(tmp_path)
    try:
        other=tmp_path/'added';other.mkdir();(other/'entry.py').write_text('x=1')
        inventory['projects']['wadded']={'root':str(other)}
        assert settings.public()['inherited']
        assert service.capabilities('wadded',other)['enabled']
        assert service.project('wadded',other)['writes']
        assert service.current_hash('wfixture',root)==service.policy_hash
    finally:service.close()


def test_save_waits_for_forge_verification_after_provider_finishes(tmp_path):
    root,path,service,settings,_=setup(tmp_path)
    try:
        settings.save(payload(settings));before=settings.path.read_bytes()
        with service.lock:
            sid=service.create_project('wfixture',root,'gate','codex')['id']
            from demiflow.workbench.conversations.common import root_identity
            turn,_=service.store.submit('wfixture',sid,'turn',0,'gate pending',root_identity(root),service.current_hash('wfixture',root))
            service.store.update(turn['id'],cleanup='complete',gate_cleanup='pending',gate_phase='claimed')
            try:
                with pytest.raises(ForgeError,match='正在运行'):settings.save(payload(settings))
                assert settings.path.read_bytes()==before
            finally:service.store.update(turn['id'],gate_cleanup='complete',gate_phase='finished')
    finally:service.close()


def test_project_reregistration_does_not_broaden_original_grant(tmp_path):
    root,path,service,settings,inventory=setup(tmp_path)
    try:
        inventory['projects']={'wreregistered':{'root':str(root)}}
        grant=service.project('wreregistered',root)
        assert grant['writes']==['code.py'] and grant['kind']=='platform'
    finally:service.close()
