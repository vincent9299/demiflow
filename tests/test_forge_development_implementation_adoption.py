"""Explicit implementation upgrades preserve the existing task and source basis."""
import copy
import json
from unittest.mock import patch

import pytest

from demiflow.forge.development import project as project_module, registration
from demiflow.forge.development.project import ForgeError, digest
from test_forge_development_managed import managed, pass_gates

FILE = 'demiflow/forge/development/managed.py'


def upgrade(managed, monkeypatch):
    api, project, authority = managed
    pass_gates(api, authority); accepted = api.evaluate_acceptance('session')
    original = json.loads((api.store.root / 'session/task.json').read_text())
    real = project_module.binding_components
    before = real(project.path, project.config)
    def upgraded(*args, **kwargs):
        value = real(*args, **kwargs)
        value['tool'][FILE] = '0' * 64
        return value
    monkeypatch.setattr(project_module, 'binding_components', upgraded)
    monkeypatch.setattr(registration, 'binding_components', upgraded)
    after = upgraded(project.path, project.config)
    assert api.status('session')['source_fingerprint'] is None
    return api, authority, before, after, original, accepted


def test_explicit_tool_adoption_keeps_old_task_history_and_requires_new_gates(managed, monkeypatch):
    api, authority, before, after, original, accepted = upgrade(managed, monkeypatch)
    arguments = dict(request_id='reviewed-upgrade', before_components=before, after_components=after,
                     allowed_files=[FILE], authorization='Human-approved functionality repair; independently reviewed fixture')
    result = api.adopt_implementation('session', **arguments)
    assert result['ready'] is False
    assert result['source_fingerprint'] and result['source_fingerprint'] != accepted['source_fingerprint']
    saved = json.loads((api.store.root / 'session/task.json').read_text())
    for key in ('binding', 'baseline', 'readonly_baseline', 'goal', 'writes', 'check', 'verification', 'review'):
        assert saved[key] == original[key]
    assert saved['managed']['acceptance'] == original['managed']['acceptance']
    assert api.adopt_implementation('session', **arguments)['source_fingerprint'] == result['source_fingerprint']
    assert len(json.loads((api.store.root / 'session/task.json').read_text())['managed']['registration_revisions']) == 1
    pass_gates(api, authority)
    assert api.evaluate_acceptance('session')['development_state'] == 'accepted'


@pytest.mark.parametrize('bad', ['allowlist', 'actual', 'old_binding', 'config', 'standard', 'authorization'])
def test_adoption_rejects_unreviewed_or_nonimplementation_changes(managed, monkeypatch, bad):
    api, _, before, after, original, _ = upgrade(managed, monkeypatch)
    args = dict(request_id='reviewed-upgrade', before_components=copy.deepcopy(before),
                after_components=copy.deepcopy(after), allowed_files=[FILE], authorization='Explicit reviewed upgrade')
    if bad == 'allowlist': args['allowed_files'] = []
    if bad == 'actual': args['after_components']['tool'][FILE] = '1' * 64
    if bad == 'old_binding': args['before_components']['tool'][FILE] = '2' * 64
    if bad == 'config': args['after_components']['config'] = '3' * 64
    if bad == 'standard':
        path = next(k for k in after['tool'] if '/standards/' in k)
        args['after_components']['tool'][path] = '4' * 64
        args['allowed_files'].append(path)
    if bad == 'authorization': args['authorization'] = ''
    with pytest.raises(ForgeError): api.adopt_implementation('session', **args)
    assert json.loads((api.store.root / 'session/task.json').read_text()) == original


def test_adoption_journal_tamper_cannot_become_current(managed, monkeypatch):
    api, _, before, after, _, _ = upgrade(managed, monkeypatch)
    api.adopt_implementation('session', request_id='reviewed-upgrade', before_components=before,
        after_components=after, allowed_files=[FILE], authorization='Explicit reviewed upgrade')
    path = api.store.root / 'session/task.json'; value = json.loads(path.read_text())
    value['managed']['registration_revisions'][0]['plan']['request']['authorization'] = 'Changed after signing'
    path.write_text(json.dumps(value))
    assert api.status('session')['source_fingerprint'] is None
    assert api.status('session')['ready'] is False


from test_forge_development_registration_boundary import registered_project
from test_forge_development_multiflow import workspace as multiflow_workspace


def test_adoption_then_registration_share_one_chain_and_original_baselines(registered_project, monkeypatch):
    from test_forge_development_registration_boundary import _new_flow, _prepare, _publish_exact
    case = registered_project
    api, _, before, after, original, _ = upgrade((case.api, case.project, case.authority), monkeypatch)
    api.adopt_implementation('session', request_id='reviewed-upgrade', before_components=before,
        after_components=after, allowed_files=[FILE], authorization='Explicit reviewed upgrade')
    flow = _new_flow(case)
    plan = _prepare(case, [flow])
    _publish_exact(case, plan, 'project'); _publish_exact(case, plan, 'workspace')
    committed = api.commit_registration('session', plan['revision_id'])
    assert committed['source_fingerprint'] is not None and committed['ready'] is False
    assert 'flow-entry-added' in api.context('session')['sources']
    saved = json.loads(case.task_file.read_text())
    assert [r['plan']['kind'] for r in saved['managed']['registration_revisions']] == ['implementation', 'registration']
    for key in ('binding', 'baseline', 'readonly_baseline', 'goal', 'writes'):
        assert saved[key] == original[key]
    assert saved['managed']['acceptance'] == original['managed']['acceptance']
