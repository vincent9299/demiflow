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
        assert service.show('wfixture',old)['writable'] is True
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
        assert service.show('wfixture',new)['writable'] is True
        assert service.show('wfixture',new)['model']=='fixture-global'
        snapshot=service.session_policy(service.store.session(new))
        assert snapshot.providers['codex']['model']=='fixture-global'
        # Continuing an existing session after a settings update seamlessly
        # rebinds it to the new Forge-constrained policy while preserving history.
        tid2=service.start_project('wfixture',root,new,'turn-2',1,'Follow-up after model update')['id']
        assert wait_turn(service,tid2)['state']=='completed'
        assert service.show('wfixture',new)['model']=='fixture-next'
        assert len(service.store.events('wfixture',new)['events']) > len(events['events'])
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


def test_automatic_scope_allows_business_configs_and_blocks_control_and_data(tmp_path):
    root = tmp_path / 'pipeline_proj'
    root.mkdir()
    (root / 'workspace.json').write_text('{"name":"demo"}')
    (root / 'AGENTS.md').write_text('rules')
    (root / 'PIPELINE_SPEC.md').write_text('spec')
    (root / 'deployment.json').write_text('{"mode":"http"}')
    (root / 'execute.py').write_text('print("ok")\n')
    (root / 'review.ipynb').write_text('{}')
    for d in ('flows', 'operators', 'prompts', 'parameters', 'configs', 'tests', 'docs', 'runs', 'notebooks', 'data'):
        (root / d).mkdir()
    (root / 'parameters/default.yaml').write_text('limit: 5\n')
    (root / 'configs/model.yaml').write_text('model: demo\n')
    (root / 'flows/main.py').write_text('def run(): return 1\n')
    grant = automatic_project(root)
    writes = set(grant['writes'])
    assert {'deployment.json', 'execute.py', 'parameters', 'configs', 'flows', 'operators', 'prompts', 'tests', 'docs'} <= writes
    for forbidden in ('workspace.json', 'AGENTS.md', 'PIPELINE_SPEC.md', 'runs', 'notebooks', 'data', 'review.ipynb'):
        assert forbidden not in writes
        assert not any(w.startswith(forbidden + '/') for w in writes)
    # Bytecode caches created during runs/tests must not pollute change observation.
    from demiflow.workbench.conversations.changes import capture
    cache = root / 'flows/__pycache__'
    cache.mkdir()
    (cache / 'main.cpython-310.pyc').write_bytes(b'pyc')
    snap = capture(root, ['flows'])
    assert 'flows/main.py' in snap
    assert not any('__pycache__' in p or p.endswith('.pyc') for p in snap)


def test_project_git_changes_and_file_diff(tmp_path):
    import subprocess
    from demiflow.workbench.projects import documents
    repo = tmp_path / 'repo'
    repo.mkdir()
    subprocess.run(['git', '-C', str(repo), 'init', '-q'], check=True)
    subprocess.run(['git', '-C', str(repo), 'config', 'user.email', 'test@example.com'], check=True)
    subprocess.run(['git', '-C', str(repo), 'config', 'user.name', 'Test'], check=True)
    (repo / 'flows').mkdir()
    (repo / 'flows/a.py').write_text('x = 1\ny = 2\n')
    subprocess.run(['git', '-C', str(repo), 'add', 'flows/a.py'], check=True)
    subprocess.run(['git', '-C', str(repo), 'commit', '-q', '-m', 'init'], check=True)
    (repo / 'flows/a.py').write_text('x = 1\ny = 3\nz = 4\n')
    (repo / 'flows/b.py').write_text('print("new")\n')
    summary = documents.git_changes(repo)
    assert summary['is_git'] is True
    by_path = {row['path']: row for row in summary['changes']}
    assert by_path['flows/a.py']['status'] == 'M'
    assert by_path['flows/a.py']['added'] == 2 and by_path['flows/a.py']['removed'] == 1
    assert by_path['flows/b.py']['status'] == 'A'
    diff = documents.file_diff(repo, 'flows/a.py')
    assert diff['changed'] is True and diff['status'] == 'M'
    assert diff['original'] == 'x = 1\ny = 2\n'
    assert diff['modified'] == 'x = 1\ny = 3\nz = 4\n'
    # Untracked subdirectory project inside a parent git repo must not report parent repo untracked noise.
    sub = repo / 'untracked_sub'
    sub.mkdir()
    (sub / 'note.py').write_text('a = 1\n')
    assert documents.git_changes(sub)['is_git'] is False


def test_per_project_python_configuration_is_readonly_and_configurable(tmp_path):
    import sys
    from demiflow.workbench.projects import registry
    from demiflow.workbench.conversations.execution import runtime_paths
    from demiflow.workbench.conversations.deepseek import DeepSeek
    from demiflow.workbench.conversations.codex import Codex
    proj = tmp_path / 'proj_a'
    proj.mkdir()
    reg = tmp_path / 'registry.json'
    added = registry.add(reg, proj)
    wid = added['workspace_id']
    # Default is unset (None) -> inherits workbench default.
    loaded = registry.projects(registry.load_registry(reg))
    assert loaded['projects'][0]['python'] is None
    # Create a fake custom project venv executable.
    venv_bin = tmp_path / 'custom_venv/bin'
    venv_bin.mkdir(parents=True)
    custom_py = venv_bin / 'python'
    custom_py.write_text('#!/bin/sh\nexit 0\n')
    custom_py.chmod(0o755)
    # Set custom python via CAS.
    res = registry.set_python(reg, wid, expected=loaded['digest'], python=str(custom_py))
    assert res['python'] == str(custom_py)
    loaded2 = registry.projects(registry.load_registry(reg))
    assert loaded2['projects'][0]['python'] == str(custom_py)
    # Invalid path rejected.
    with pytest.raises(ForgeError):
        registry.set_python(reg, wid, expected=loaded2['digest'], python='/nonexistent/bin/python')
    # Disable python explicitly ('').
    res3 = registry.set_python(reg, wid, expected=loaded2['digest'], python='')
    assert res3['python'] == ''
    # Verify runtime_paths mounts custom venv prefix read-only.
    rp = runtime_paths({'executable': sys.executable}, project_python=str(custom_py))
    assert str(tmp_path / 'custom_venv') in rp
    assert str(venv_bin) in rp
    # Verify DeepSeek & Codex prepend configured python bin to PATH.
    dsh_home = tmp_path / 'dsh_home'; dsh_home.mkdir()
    _, dsh_env = DeepSeek.launch(
        {'executable': str(custom_py), 'model': 'deepseek-v4-flash', 'version': '0.2.1-rc.2', '_project_python': str(custom_py)},
        dsh_home, str(proj))
    assert dsh_env['PATH'].startswith(str(venv_bin) + ':')
    assert dsh_env['DEMIFLOW_PROJECT_PYTHON'] == str(custom_py)
    cdx_home = tmp_path / 'cdx_home'; cdx_home.mkdir()
    cdx_cmd, _ = Codex.launch(
        {'executable': str(custom_py), 'model': 'gpt-5', 'version': '0.1.0', '_project_python': str(custom_py)},
        cdx_home, str(proj))
    assert any(str(venv_bin) in arg and 'DEMIFLOW_PROJECT_PYTHON' in arg for arg in cdx_cmd)


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


def test_automatic_scope_allows_more_than_128_split_writable_paths(tmp_path):
    root, path, service, settings, inventory = setup(tmp_path)
    try:
        big = tmp_path / 'big_project'
        big.mkdir()
        (big / 'workspace.json').write_text('{"entries":[]}\n')
        prompts = big / 'prompts'
        prompts.mkdir()
        (prompts / '.probe_write').write_text('')
        cache = prompts / '__pycache__'
        cache.mkdir()
        (cache / 'x.pyc').write_bytes(b'cache')
        for idx in range(160):
            (prompts / f'p_{idx:03d}.md').write_text(f'# prompt {idx}\n')
        grant = automatic_project(big)
        assert len(grant['writes']) == 160
        assert set(grant['protected']) == {'prompts/.probe_write', 'prompts/__pycache__'}
        inventory['projects']['wbig'] = {'root': str(big)}
        caps = service.capabilities('wbig', big)
        assert caps['enabled'] and caps['start']
        _, revision, config_path = settings.binding('wbig', big, seal=True)
        assert revision and config_path.is_file()
        from demiflow.workbench.conversations.sandbox import SandboxSpec, specification
        home = tmp_path / 'big_private'
        home.mkdir(mode=0o700)
        spec = SandboxSpec(
            read_paths=(str(big),),
            runtime_paths=('/usr', '/lib', '/lib64'),
            write_paths=tuple(str(big / p) for p in grant['writes']),
            protected_paths=tuple(str(big / p) for p in grant['protected']),
            private_home=str(home),
        )
        launched = specification(spec, ['/usr/bin/true'], {}, big)
        assert len(launched['source_roots']) == 160
        assert str(prompts / '.probe_write') not in launched['write_paths']
        assert str(cache) not in launched['write_paths']
    finally:
        service.close()
