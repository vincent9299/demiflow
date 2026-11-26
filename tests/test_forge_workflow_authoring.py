"""Engineering contracts for a workflow-only change and host-owned author capabilities."""
import json
from types import SimpleNamespace

import pytest

from demiflow.forge.context import build_context
from demiflow.forge.development import assess_turn, has_code_changes
from demiflow.forge.project import load_project, register_workflow
from demiflow.forge.project_guard import capture_protected_files, enforce_protected_files
from demiflow.forge.review_evidence import capture_evidence, review_units, verify_evidence
from demiflow.forge.schema import ForgeError
from demiflow.forge.workflow import discover_workflows, validate_workflow
from demiflow.workbench.conversations.context import BridgeServer
from demiflow.workbench.projects import registry
from demiflow.workbench.workflows.resolver import registered_project_catalog, workflow_review_observations
from test_forge_development_loop import response_for
from test_forge_workflow import _make_project


ENTRY = 'flows/step_a.py:run'
PARAMS = 'configs/step_a.json'
PATH = 'workflows/draft.json'


def write_workflow(root, *, path=PATH, nodes=None, **fields):
    target = root / path
    target.parent.mkdir(exist_ok=True)
    doc = dict(format='demiflow_workflow_v1', name='验收编排',
               nodes=nodes or [dict(id='a', entry=ENTRY, params=PARAMS)], **fields)
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    return doc


def test_authorized_registration_survives_then_restores_exact_published_version(tmp_path):
    root = _make_project(tmp_path/'project')
    write_workflow(root)
    snap = capture_protected_files(root)
    manifest = root/'workspace.json'
    original = manifest.read_bytes()
    active = SimpleNamespace(tid='first', epoch='grant-a', require_active=lambda:None)
    bridge = BridgeServer(active, project_root=root, protected_snapshot=snap)
    try:
        request = dict(name='manage_project_layout', arguments=dict(action='register_workflow', path=PATH))
        manifest.write_bytes(original + b' ')
        with pytest.raises(ForgeError, match='outside the authorized'):
            bridge.dispatch('tools/call', request, active)
        manifest.write_bytes(original)
        answer = bridge.dispatch('tools/call', request, active)
        assert json.loads(answer['content'][0]['text'])['status'] == 'registered'
        published = manifest.read_bytes()
        authorized = bridge.finish_turn()
        assert enforce_protected_files(root, snap, authorized) == []
        assert PATH in load_project(root)[1]['workflows']
        manifest.write_bytes(original)
        assert enforce_protected_files(root, snap, authorized) == ['workspace.json']
        assert manifest.read_bytes() == published
        later = SimpleNamespace(tid='second', epoch='grant-b', require_active=lambda:None)
        bridge.set_turn(later)
        assert [t['name'] for t in bridge.dispatch('tools/list', {}, later)['tools']] == ['read_input']
        with pytest.raises(ForgeError):
            bridge.dispatch('tools/call', request, active)
        with pytest.raises(ForgeError):
            bridge.dispatch('tools/call', request, later)
    finally:
        bridge.close()


def test_slim_migration_through_tool_protects_both_published_controls(tmp_path):
    root = _make_project(tmp_path/'project')
    manifest, state = root/'workspace.json', root/'.forge/state.json'
    full = load_project(root)[1]
    manifest.write_text(json.dumps(full))
    state.unlink()
    snap = capture_protected_files(root)
    active = SimpleNamespace(tid='turn', epoch='grant', require_active=lambda:None)
    bridge = BridgeServer(active, project_root=root, protected_snapshot=snap)
    try:
        reply = bridge.dispatch('tools/call', dict(name='manage_project_layout',
                                arguments=dict(action='slim_manifest')), active)
        assert json.loads(reply['content'][0]['text'])['status'] == 'slimmed'
        versions = manifest.read_bytes(), state.read_bytes()
        allowed = bridge.finish_turn()
        assert enforce_protected_files(root, snap, allowed) == []
        manifest.write_text('{}')
        state.unlink()
        assert set(enforce_protected_files(root, snap, allowed)) == {'workspace.json', '.forge/state.json'}
        assert (manifest.read_bytes(), state.read_bytes()) == versions
        assert load_project(root)[1]['id'] == full['id']
    finally:
        bridge.close()


def test_registration_rechecks_definition_and_draft_errors_reach_original_author(tmp_path):
    root = _make_project(tmp_path/'project')
    write_workflow(root)
    def edit():
        write_workflow(root, nodes=[dict(id='a', entry=ENTRY, after=['missing'])])
    with pytest.raises(ForgeError) as changed:
        register_workflow(root, PATH, authorize=edit)
    assert changed.value.code == 'inputs_changed'
    assert PATH not in load_project(root)[1].get('workflows', [])
    changes = [dict(path=PATH, state='modified')]
    assert has_code_changes(changes)
    author = dict(provider='codex', session='original', completed=True, acknowledged=True, cleanup='complete')
    receipt = assess_turn(root, author=author, goal='修正工作流', changes=changes)
    diagnostics = receipt['checks']['project_layout']['diagnostics']
    assert sum(d['rule'] == 'WF-03' for d in diagnostics) == 1
    assert receipt['feedback']['author']['session'] == 'original'
    good = 'workflows/registered.json'
    write_workflow(root, path=good)
    register_workflow(root, good)
    write_workflow(root, nodes=[dict(id='a', entry=ENTRY, project=[])], policy=dict(on_failure={}))
    rows = {r['path']:r for r in discover_workflows(root)}
    assert rows[good]['status'] == 'passed' and rows[PATH]['status'] == 'failed'
    assert all(d['rule'] == 'WF-01' for d in validate_workflow(root, PATH)['diagnostics'])
    context = build_context(root, topic='workflow')
    assert [(r['path'], r['registered']) for r in context['workflows']['items']] == [(good, True), (PATH, False)]


def test_workflow_review_links_only_local_bindings_and_rechecks_the_candidate(tmp_path):
    root = _make_project(tmp_path/'project')
    write_workflow(root, projects={'peer':'remote'}, nodes=[
        dict(id='a', entry=ENTRY, params=PARAMS),
        dict(id='b', project='peer', entry='flows/step_b.py:run', params='configs/step_b.json')])
    spec = validate_workflow(root, PATH)
    assert spec['status'] == 'passed' and spec['unverified_projects'] == ['peer']
    evidence = capture_evidence(root, paths=[PATH])
    expected = {PATH, ENTRY.split(':')[0], PARAMS}
    assert set(evidence['files']) == expected
    registration = dict(manifest='workspace.json', registered=False, local_bindings=[
        dict(entry=ENTRY, entry_registered=True, params=PARAMS, params_bound=True)])
    assert evidence['files'][PATH]['workspace_registration'] == registration
    assert any(expected <= unit['files'].keys() for unit in review_units(evidence))
    for path in (PARAMS, ENTRY.split(':')[0]):
        assert PATH in capture_evidence(root, paths=[path])['files']
    assert PATH not in capture_evidence(root, paths=['configs/step_b.json'])['files']
    author = dict(provider='deepseek', session='original', completed=True, acknowledged=True, cleanup='complete')
    calls = []
    def reviewer(prompt):
        calls.append(prompt)
        return response_for(prompt)
    receipt = assess_turn(root, author=author, goal='查看编排的静态接线', changes=[dict(path=PATH)],
                          auto_review=True, reviewer=reviewer)
    assert receipt['checks']['coverage']['external_projects_pending'] == ['peer']
    assert calls and receipt['review']['status'] == 'passed'
    request = json.loads(calls[0].split('\n\n')[-1])
    assert request['workflow_authoring'] and request['files'][PATH]['workspace_registration'] == registration
    checks = {row['source']: json.loads(row['detail']) for row in request['host_observations']}
    assert checks['project_layout']['stage'] == 'author_submission'
    assert checks['project_layout']['workflow_checks'] == [dict(path=PATH, status='passed')]
    register_workflow(root, PATH)
    assert not verify_evidence(root, evidence)
    evidence = capture_evidence(root, paths=[PATH])
    assert evidence['files'][PATH]['workspace_registration']['registered']
    (root/PARAMS).write_text('{"scale":3}')
    assert not verify_evidence(root, evidence)


def test_review_rechecks_manifest_binding_without_a_workflow_edit(tmp_path):
    root = _make_project(tmp_path/'project')
    write_workflow(root, nodes=[dict(id='a', entry=ENTRY)])
    evidence = capture_evidence(root, paths=[PATH])
    local = evidence['files'][PATH]['workspace_registration']['local_bindings']
    assert local == [dict(entry=ENTRY, entry_registered=True, params=PARAMS, params_bound=True)]
    manifest = root/'workspace.json'
    control = json.loads(manifest.read_text())
    control['entries'][ENTRY].append('configs/step_b.json')
    manifest.write_text(json.dumps(control))
    assert not verify_evidence(root, evidence)
    receipt = assess_turn(root, round=1, author=dict(provider='codex', session='original', completed=True,
                          acknowledged=True, cleanup='complete'), goal='修复编排', changes=[dict(path=PATH)],
                          reviewer=response_for, auto_review=True)
    checks = {row['source']: json.loads(row['detail']) for row in receipt['review']['observations']}
    assert checks['project_layout']['stage'] == 'recheck_after_feedback'
    assert checks['project_layout']['workflow_checks'] == [dict(path=PATH, status='failed')]
    assert any(d['rule'] == 'WF-04' and d['path'] == PATH for d in checks['project_layout']['diagnostics'])


@pytest.mark.parametrize('malformed', [[], {}])
def test_invalid_external_binding_still_returns_static_feedback_with_review(tmp_path, malformed):
    root = _make_project(tmp_path/'project')
    write_workflow(root, projects={'peer':'other'}, nodes=[dict(id='bad', project='peer', entry=ENTRY, params=malformed)])
    def catalog(arguments):
        pytest.fail('Malformed external bindings cannot be queried')
    receipt = assess_turn(root, author=dict(provider='codex', session='original', completed=True,
                                             acknowledged=True, cleanup='complete'),
                          goal='核验项目绑定', changes=[dict(path=PATH)], auto_review=True,
                          reviewer=response_for, observations=lambda definitions: workflow_review_observations(catalog, definitions))
    assert any(d['rule'] == 'WF-01' for d in receipt['checks']['project_layout']['diagnostics'])
    assert receipt['feedback']['author']['session'] == 'original'


def test_project_catalog_is_paged_read_only_and_rechecks_membership(tmp_path):
    host = _make_project(tmp_path/'host', 'host')
    peer = _make_project(tmp_path/'peer', 'peer')
    path = tmp_path/'registry.json'
    host_id = registry.add(path, host)['workspace_id']
    peer_id = registry.add(path, peer, allow_runs=True)['workspace_id']
    authority = dict(path=str(path), run_submit=False)
    def query(**arguments):
        return registered_project_catalog(authority, host_id, host, arguments)
    assert query(limit=1)['next_offset'] == 1
    assert all(not row['run_submit'] for row in query()['projects'])
    authority['run_submit'] = True
    view = query(project=peer_id, limit=1)
    assert view['name'] == 'peer' and view['run_submit'] and view['next_offset'] == 1
    row, = view['flows']
    assert row['entry'] == ENTRY and row['params'] == PARAMS and set(row['keys']) == {'scale', 'output_table'}
    text = json.dumps(view)
    assert str(tmp_path) not in text and 'data/a.lance' not in text and 'from_items' not in text
    assert query(project='peer', offset=1)['flows'][0]['entry'] != ENTRY
    definitions = [dict(external_bindings=[dict(project='peer', entry=ENTRY, params=PARAMS)])]
    def observe():
        facts = workflow_review_observations(lambda args: query(**args), definitions)
        return json.loads(facts[0]['detail'])
    assert observe()['keys'] == ['output_table', 'scale']
    (peer/PARAMS).write_text('{"changed_public_key": 5}')
    assert observe()['keys'] == ['changed_public_key']
    with pytest.raises(ForgeError):
        query(project=str(peer))
    inventory = registry.load_registry(path)
    del inventory['projects'][host_id]
    path.write_text(json.dumps(inventory))
    with pytest.raises(ForgeError):
        query(project='peer')
    assert observe()['unavailable']
