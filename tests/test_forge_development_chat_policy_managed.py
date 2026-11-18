"""Managed admission must not migrate old grants or imply execution isolation."""
import copy

import pytest

from demiflow.workbench.conversations.policy import Deployment
from demiflow.forge.schema import ForgeError
from test_forge_development_chat import configuration


def registered(tmp_path):
    root, _, value = configuration(tmp_path)
    value['projects']['wfixture'].update(kind='demiforge_registered', pipeline='orders')
    return root, value


def test_new_registered_grant_automatically_uses_managed_task(tmp_path):
    root, value = registered(tmp_path)
    deployment = Deployment(value)
    assert deployment.project('wfixture', root)['managed'] is True
    capabilities = deployment.capabilities('wfixture', root)
    assert capabilities['development_gate'] == dict(enabled=True, assurance='gate_only',
        run_policy='observe', independent_review='unavailable', automatic_repair=True,repair_attempts=3,review_attempts=2)
    assert capabilities['filesystem_enforcement'] == 'unsupported'
    assert not (root / '.demiforge').exists()


def test_explicit_local_task_preserves_original_policy_snapshot(tmp_path):
    root, value = registered(tmp_path)
    grant = value['projects']['wfixture']
    grant['task_id'] = 'old-task'
    before = copy.deepcopy(value)
    deployment = Deployment(value)
    snapshot = copy.deepcopy(deployment.value)
    snapshot['projects']['wfixture'].pop('identity')
    assert snapshot == before
    assert deployment.capabilities('wfixture', root)['development_gate']['enabled'] is False


@pytest.mark.parametrize('update', [
    {'managed': True, 'task_id': 'old-task'},
    {'managed': False},
    {'managed': 'true'},
    {'allow_tests': 'yes'},
    {'managed': True, 'kind': 'platform'},
    {'managed': True, 'phase': 'design', 'writes': ['design.md'], 'watch': ['design.md']},
])
def test_managed_authority_cannot_be_inferred_from_invalid_grant(tmp_path, update):
    _, value = registered(tmp_path)
    value['projects']['wfixture'].update(update)
    with pytest.raises(ForgeError):
        Deployment(value)


def test_managed_does_not_enable_unavailable_restricted_execution(tmp_path, monkeypatch):
    monkeypatch.setattr('demiflow.workbench.conversations.policy.execution_facts',lambda _:dict(
        available=False,filesystem_enforcement='unverified',reason='fixture unavailable'))
    root, value = registered(tmp_path)
    value['mode'] = 'restricted'
    value['runtime_domain'] = str(tmp_path/'domain')
    monkeypatch.setattr(Deployment,'validate_runtime',lambda *_args,**_kwargs:None)
    capabilities = Deployment(value).capabilities('wfixture', root)
    assert capabilities['development_gate']['enabled']
    assert not capabilities['start']
    assert not any(p['available'] for p in capabilities['providers'])
    assert capabilities['filesystem_enforcement'] == 'unverified'


def test_restricted_legacy_launcher_is_never_admitted(tmp_path, monkeypatch):
    root,value=registered(tmp_path)
    value['mode']='restricted';value['projects']['wfixture']['task_id']='legacy'
    monkeypatch.setattr('demiflow.workbench.conversations.policy.execution_facts',lambda _:dict(
        available=True,filesystem_enforcement='landlock_seccomp',reason='fixture ready'))
    monkeypatch.setattr('demiflow.workbench.conversations.model_broker.capabilities',lambda *_:dict(available=True,reason=''))
    capabilities=Deployment(value).capabilities('wfixture',root)
    assert not capabilities['start'] and not any(p['available'] for p in capabilities['providers'])
    assert capabilities['development_gate']['enabled'] is False


@pytest.mark.parametrize('mode,managed,domain', [('trusted_local',True,True),('restricted',True,False),('restricted',False,True)])
def test_require_cannot_bypass_managed_external_boundary(tmp_path,mode,managed,domain):
    _,value=registered(tmp_path);value['mode']=mode
    grant=value['projects']['wfixture'];grant['run_policy']='require'
    if not managed:grant['task_id']='legacy'
    if domain:value['runtime_domain']=str(tmp_path/'domain')
    with pytest.raises(ForgeError,match='Required development acceptance'):
        Deployment(value)


def test_managed_restricted_observe_also_requires_protected_scope_binding(tmp_path):
    _,value=registered(tmp_path);value['mode']='restricted'
    with pytest.raises(ForgeError,match='all source execution entrypoints'):
        Deployment(value)
