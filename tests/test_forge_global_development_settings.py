"""One host setting, real per-project grants and immutable conversation history."""
import json
import os
from pathlib import Path
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


def test_automatic_project_does_not_inherit_parent_forge_tasks(tmp_path):
    parent = tmp_path / '.demiforge'
    parent.mkdir()
    (parent / 'project.json').write_text('intentionally invalid legacy declaration')
    selected = tmp_path / 'orders'
    selected.mkdir()
    for directory in ('flows', 'operators', 'prompts'):
        (selected / directory).mkdir()
    grant = automatic_project(selected)
    assert grant['kind'] == 'pipeline'
    assert not {'managed', 'development_root', 'pipeline'} & grant.keys()
    assert {'flows', 'operators', 'prompts'} <= set(grant['writes'])
    assert all(not Path(p).is_absolute() and '..' not in Path(p).parts for p in grant['writes'])


def test_automatic_scope_preserves_nested_data_and_notebooks(tmp_path):
    root=tmp_path/'code';root.mkdir();src=root/'src';src.mkdir()
    (src/'data').mkdir();(src/'data/raw.txt').write_text('business data')
    (src/'result.ipynb').write_text('{}');(src/'AGENTS.md').write_text('rules')
    cache=src/'__pycache__';cache.mkdir();(cache/'app.pyc').write_bytes(b'cache')
    (src/'app.py').write_text('x=1')
    (src/'models').mkdir()
    grant=automatic_project(root)
    assert set(grant['protected'])=={'src/data','src/result.ipynb','src/AGENTS.md','src/__pycache__'}
    assert grant['writes'] == ['src/app.py', 'src/models']
    assert 'src' in grant['watch']
    from demiflow.workbench.conversations.sandbox import SandboxSpec, specification
    home = tmp_path / 'private'; home.mkdir(mode=0o700)
    spec = SandboxSpec(read_paths=(str(root),), runtime_paths=('/usr', '/lib', '/lib64'),
                       write_paths=tuple(str(root / path) for path in grant['writes']),
                       protected_paths=tuple(str(root / path) for path in grant['protected']), private_home=str(home))
    assert specification(spec, ['/usr/bin/true'], {}, root)['source_roots'] == [
        str(src/'app.py'), str(src/'models')]


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


def test_project_reregistration_does_not_broaden_original_grant(tmp_path):
    root,path,service,settings,inventory=setup(tmp_path)
    try:
        inventory['projects']={'wreregistered':{'root':str(root)}}
        grant=service.project('wreregistered',root)
        assert grant['writes']==['code.py'] and grant['kind']=='platform'
    finally:service.close()
