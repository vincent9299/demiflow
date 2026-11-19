"""Managed L1 ownership and evidence gates; only owned temporary fixtures run."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from unittest.mock import patch

import pytest

from demiflow.forge.development import tasks
from demiflow.forge.development.managed import ManagedTasks, ReviewAuthority
from demiflow.forge.development.project import ForgeError, REVIEW_RULES, file_hash
from test_forge_development_migration import fixture
from test_forge_development_multiflow import workspace as multiflow_workspace


@pytest.fixture
def managed(tmp_path):
    root = tmp_path / 'project'
    root.mkdir()
    project = fixture(root)
    authority = ReviewAuthority()
    api = ManagedTasks(tmp_path / 'state', 'fixture', root, review_authority=authority)
    api.prepare_task('session', 'orders', 'Develop fixture', ['orders'], ['No production'],
                     contract=dict(allow_tests=True, max_verification_seconds=5,
                                   developer_session_id='developer'))
    return api, project, authority


def read_all(api, task_id='session'):
    listing = api.context(task_id)
    for source, info in listing['sources'].items():
        for page in range(1, info['pages'] + 1):
            assert api.context(task_id, source, page)


def claims():
    return {rule: dict(status='pass', reason='Isolated fixture inspected',
                       evidence=['orders/README.md']) for rule in REVIEW_RULES}


def review(api, authority):
    fingerprint = api.status('session')['source_fingerprint']
    value = claims()
    receipt = authority.issue('session', fingerprint, value, provider='fixture', model='fixture',
                              session_id='reviewer', mode='no-tools')
    return api.record_review('session', fingerprint, value, receipt=receipt)


def pass_gates(api, authority):
    read_all(api)
    result = api.check('session')
    assert result['gates']['check'] == 'passed', result
    result = api.verify('session', expected_fingerprint=result['source_fingerprint'])
    assert result['gates']['verify'] == 'passed', result
    return review(api, authority)


def test_external_only_and_local_history_unchanged(managed):
    api, project, _ = managed
    tasks.start(project, 'old', 'orders', 'Legacy local goal', ['orders'], ['No production'])
    local = project.control / 'tasks' / 'old' / 'task.json'
    before = local.read_bytes()
    read_all(api)
    api.check('session')
    api.verify('session')
    assert local.read_bytes() == before
    assert not (project.control / 'tasks' / 'session').exists()
    assert (api.store.root / 'session' / 'task.json').is_file()
    assert api.store.root.stat().st_mode & 0o777 == 0o700
    assert (api.store.root / 'session').stat().st_mode & 0o777 == 0o700
    state = json.loads((api.store.root / 'session' / 'task.json').read_text())
    command = state['verification']['commands'][0]
    assert command['log_base'] == 'task'
    assert not Path(command['log']).is_absolute()
    assert (api.store.root / 'session' / command['log']).read_text() == 'fixture\n'
    assert state['verification']['sandbox'] is False
    assert state['verification']['verification_source'] == 'live_tree_rechecked'


def test_prepare_and_user_decisions_are_idempotent_without_rebasing(managed):
    api, project, _ = managed
    path = api.store.root / 'session' / 'task.json'
    original = json.loads(path.read_text())
    contract = original['managed']['request']['contract']
    (project.root / 'orders/rules.py').write_text('def keep(row): return False\n')
    api.prepare_task('session', 'orders', 'Develop fixture', ['orders'], ['No production'], contract=contract)
    before = api.status('session')['source_fingerprint']
    first = api.note('session', 'decision', 'Keep the new filter', event_id='user-message-2')
    second = api.note('session', 'decision', 'Keep the new filter', event_id='user-message-2')
    assert first == dict(note_index=0, duplicate=False)
    assert second == dict(note_index=0, duplicate=True)
    after = json.loads(path.read_text())
    assert after['baseline'] == original['baseline']
    assert api.status('session')['source_fingerprint'] != before
    with pytest.raises(ForgeError, match='different goal'):
        api.prepare_task('session', 'orders', 'Changed goal', ['orders'], ['No production'], contract=contract)
    with pytest.raises(ForgeError, match='different content'):
        api.note('session', 'decision', 'Different decision', event_id='user-message-2')
    with pytest.raises(ForgeError, match='stable controller'):
        api.note('session', 'decision', 'No genuine user event')


def test_no_synthetic_context_reads(managed):
    api, _, _ = managed
    assert api.context('session')['read_pages'] == {}
    result = api.check('session')
    assert result['gates']['check'] == 'failed'
    assert any('unread' in text for text in result['violations'])
    api.context('session', 'project-rules')
    assert api.context('session')['read_pages'] == {'project-rules': [1]}


def test_acceptance_requires_exact_current_evidence_and_explicit_evaluation(managed):
    api, project, authority = managed
    passed = pass_gates(api, authority)
    assert passed['ready'] is True
    assert passed['development_state'] != 'accepted'
    accepted = api.evaluate_acceptance('session', expected_fingerprint=passed['source_fingerprint'])
    assert accepted['development_state'] == 'accepted'
    assert accepted['assurance'] == 'gate_only'
    assert api.status('session')['development_state'] == 'accepted'
    (project.root / 'orders/rules.py').write_text('def keep(row): return False\n')
    stale = api.status('session')
    assert stale['ready'] is False
    assert stale['development_state'] == 'stale'
    assert stale['accepted_fingerprint'] == accepted['source_fingerprint']
    assert 'orders/rules.py' in stale['changed']
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('stale command ran')):
        refused = api.verify('session', expected_fingerprint=accepted['source_fingerprint'])
    assert refused['development_state'] == 'stale'


def test_missing_reviewer_never_falls_back_to_legacy_self_review(managed):
    api, project, _ = managed
    no_reviewer = ManagedTasks(api.store.root.parent.parent, 'fixture', project.root)
    read_all(no_reviewer)
    no_reviewer.check('session')
    no_reviewer.verify('session')
    result = no_reviewer.evaluate_acceptance('session')
    assert result['gates']['review'] == 'unavailable'
    assert result['development_state'] == 'review_pending'
    assert result['ready'] is False
    with pytest.raises(ForgeError, match='unavailable'):
        no_reviewer.record_review('session', result['source_fingerprint'], claims(), receipt='self')
    with tasks.locked(api._project(), 'session') as (_, state):
        with pytest.raises(ForgeError, match='trusted controller'):
            tasks.review(api._project(), state, project.root / 'unused.json', 'fake reviewer')
        with pytest.raises(ForgeError, match='trusted controller'):
            tasks.handoff(api._project(), state, api.store.root / 'session', True)


def test_review_rejects_forged_wrong_authority_or_developer_receipts(managed):
    api, _, authority = managed
    fingerprint = api.status('session')['source_fingerprint']
    value = claims()
    with pytest.raises(ForgeError, match='opaque'):
        api.record_review('session', fingerprint, value, receipt={'reviewer': 'fake'})
    for issuer, session, expected in ((ReviewAuthority(), 'reviewer', 'authority'),
                                      (authority, 'developer', 'distinct')):
        receipt = issuer.issue('session', fingerprint, value, provider='fixture', model='fixture',
                               session_id=session, mode='no-tools')
        with pytest.raises(ForgeError, match=expected):
            api.record_review('session', fingerprint, value, receipt=receipt)
    receipt = authority.issue('session', fingerprint, value, provider='fixture', model='fixture',
                              session_id='reviewer', mode='no-tools')
    value['DF-01']['reason'] = 'Changed result after receipt'
    with pytest.raises(ForgeError, match='candidate/result'):
        api.record_review('session', fingerprint, value, receipt=receipt)


def test_legacy_review_attestation_is_not_managed_review(managed):
    api, _, _ = managed
    read_all(api)
    api.check('session')
    api.verify('session')
    with tasks.locked(api._project(), 'session') as (_, state):
        state['review'] = dict(fingerprint=tasks.current(api._project(), state)[3], passed=True,
                               type='reviewer-attestation', reviewer='I am independent', claims=claims())
    result = api.evaluate_acceptance('session')
    assert result['gates']['review'] == 'failed'
    assert result['ready'] is False


@pytest.mark.parametrize('mutation', ['log', 'verifier', 'rule', 'tool'])
def test_log_rule_verifier_and_platform_changes_cannot_stay_accepted(managed, mutation):
    api, project, authority = managed
    pass_gates(api, authority)
    api.evaluate_acceptance('session')
    if mutation == 'log':
        state = json.loads((api.store.root / 'session' / 'task.json').read_text())
        path = api.store.root / 'session' / state['verification']['commands'][0]['log']
        path.write_text('forged success')
    elif mutation == 'verifier':
        config = json.loads(project.path.read_text())
        config['pipelines']['orders']['verify'][0]['argv'] = ['true']
        project.path.write_text(json.dumps(config))
    elif mutation == 'rule':
        (project.root / 'AGENTS.md').write_text('Changed applicable rules')
    if mutation == 'tool':
        original = file_hash
        with patch('demiflow.forge.development.project.file_hash',
                   side_effect=lambda path: 'changed' if Path(path).name == 'managed.py' else original(path)):
            result = api.status('session')
    else:
        result = api.status('session')
    assert result['ready'] is False
    assert result['development_state'] != 'accepted'


def test_git_index_is_only_read_and_changes_block_acceptance(tmp_path):
    root = tmp_path / 'project'
    root.mkdir()
    project = fixture(root)
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'add', 'orders/rules.py'], check=True)
    index = root / '.git/index'
    before_bytes = index.read_bytes()
    before_stat = index.stat()
    authority = ReviewAuthority()
    api = ManagedTasks(tmp_path / 'state', 'fixture', root, review_authority=authority)
    api.prepare_task('session', 'orders', 'Develop fixture', ['orders'], ['No production'],
                     contract=dict(allow_tests=True, developer_session_id='developer'))
    pass_gates(api, authority)
    api.evaluate_acceptance('session')
    assert index.read_bytes() == before_bytes
    assert (index.stat().st_mtime_ns, index.stat().st_ctime_ns) == (before_stat.st_mtime_ns, before_stat.st_ctime_ns)
    assert not (root / '.git/index.lock').exists()
    subprocess.run(['git', '-C', str(root), 'add', 'orders/README.md'], check=True)
    result = api.status('session')
    assert result['gates']['check'] == 'failed'
    assert any('Git staging changed' in item for item in result['violations'])
    assert result['repairable'] is False
    assert result['ready'] is False


def test_external_location_and_project_identity_are_fixed(managed, tmp_path):
    api, project, _ = managed
    with pytest.raises(ForgeError, match='outside'):
        ManagedTasks(project.root / 'state', 'inside', project.root)
    other = tmp_path / 'other'
    other.mkdir()
    with pytest.raises(ForgeError, match='different project'):
        ManagedTasks(api.store.root.parent.parent, 'fixture', other)
    linked = tmp_path / 'linked'
    linked.symlink_to(api.store.root.parent.parent)
    with pytest.raises(ForgeError, match='Symlink'):
        ManagedTasks(linked, 'linked', project.root)
    (api.store.root / 'session').chmod(0o755)
    with pytest.raises(ForgeError, match='0700'):
        api.status('session')


def test_replaced_physical_root_invalidates_even_identical_sources(managed, tmp_path):
    api, project, authority = managed
    pass_gates(api, authority)
    api.evaluate_acceptance('session')
    saved = tmp_path / 'saved-project'
    project.root.rename(saved)
    shutil.copytree(saved, project.root)
    result = api.status('session')
    assert result['development_state'] == 'stale'
    assert result['ready'] is False
    assert 'Physical project root' in result['violations'][0]
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('wrong root verifier ran')):
        assert api.verify('session')['ready'] is False
    with pytest.raises(ForgeError, match='different project'):
        ManagedTasks(api.store.root.parent.parent, 'fixture', project.root)


def test_split_index_shared_bytes_are_bound_and_never_written(tmp_path):
    from demiflow.forge.development.managed import _index_snapshot
    root = tmp_path / 'project'
    root.mkdir()
    fixture(root)
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'add', 'orders/rules.py'], check=True)
    subprocess.run(['git', '-C', str(root), 'update-index', '--split-index'], check=True)
    indexes = [root / '.git/index', *sorted((root / '.git').glob('sharedindex.*'))]
    identities = [(p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in indexes]
    before = _index_snapshot(root)
    assert before['shared_indexes']
    shared = root / '.git' / next(iter(before['shared_indexes']))
    original = shared.read_bytes()
    assert _index_snapshot(root) == before
    assert shared.read_bytes() == original
    assert [(p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in indexes] == identities
    # Appending a byte leaves the shared path intact but must invalidate bytes.
    shared.write_bytes(original + b'X')
    assert _index_snapshot(root) != before


def test_unbound_contract_copy_or_changed_baseline_is_rejected(managed):
    api, _, _ = managed
    path = api.store.root / 'session' / 'task.json'
    original = json.loads(path.read_text())
    state = json.loads(json.dumps(original))
    state['managed']['request']['contract']['allow_tests'] = False
    path.write_text(json.dumps(state))
    with pytest.raises(ForgeError, match='identity/contract'):
        api.status('session')
    state = json.loads(json.dumps(original))
    state['baseline']['orders/rules.py'] = 'changed'
    path.write_text(json.dumps(state))
    with pytest.raises(ForgeError, match='identity/contract'):
        api.status('session')


def test_timeout_only_tightens_and_failed_fixture_does_not_pass(managed):
    api, _, _ = managed
    read_all(api)
    with pytest.raises(ForgeError, match='tighten'):
        api.verify('session', timeout_seconds=6)
    with patch('demiflow.forge.development.tasks.run', return_value=dict(
            id='fixture', passed=False, failure='timeout', output='')):
        result = api.verify('session', timeout_seconds=0.01)
    assert result['gates']['verify'] == 'failed'
    assert result['ready'] is False


def authority_source(path):
    return dict(canonical_path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def test_required_context_binding_and_fake_sha_block_verification(managed):
    api, project, _ = managed
    api.prepare_task('required', 'orders', 'Bind the delivered rules', ['orders'], ['No production'],
                     contract=dict(allow_tests=True, require_context_binding=True))
    read_all(api, 'required')
    assert api.check('required')['gates']['check'] == 'failed'
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('unbound verification ran')):
        assert api.verify('required')['gates']['check'] == 'failed'
    assert api.evaluate_acceptance('required')['ready'] is False
    extra = project.root.parent / 'AGENTS.md'
    extra.write_text('Required ancestor instructions')
    with pytest.raises(ForgeError, match='context changed'):
        api.bind_context('required', [dict(canonical_path=str(extra), sha256='0' * 64)])
    assert api.check('required')['gates']['check'] == 'failed'
    bound = api.bind_context('required', [authority_source(extra)])
    assert api.context('required')['fingerprint'] == bound['source_fingerprint']
    assert api.check('required')['gates']['check'] == 'passed'


@pytest.mark.parametrize('mutation', ['changed', 'deleted'])
def test_ancestor_authority_change_invalidates_current_evidence(managed, mutation):
    api, project, authority = managed
    extra = project.root.parent / 'AGENTS.md'
    extra.write_text('Original authoritative instructions')
    path = api.store.root / 'session' / 'task.json'
    baseline = json.loads(path.read_text())['baseline']
    source = authority_source(extra)
    bound = api.bind_context('session', [source])
    assert api.bind_context('session', [source])['source_fingerprint'] == bound['source_fingerprint']
    assert json.loads(path.read_text())['baseline'] == baseline
    pass_gates(api, authority)
    assert api.evaluate_acceptance('session')['development_state'] == 'accepted'
    if mutation == 'changed': extra.write_text('Changed after provider dispatch')
    else: extra.unlink()
    stale = api.status('session')
    assert stale['development_state'] == 'stale'
    assert stale['source_fingerprint'] is None
    assert stale['repairable'] is False
    assert stale['ready'] is False
    assert all(value != 'passed' for value in stale['gates'].values())
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('changed authority verification ran')):
        with pytest.raises((ForgeError, OSError)):
            api.verify('session')


@pytest.mark.parametrize('mutation', ['remove', 'add'])
def test_supplementary_authority_set_cannot_be_silently_rebound(managed, mutation):
    api, project, authority = managed
    one, two = project.root.parent / 'AGENTS.md', project.root.parent / 'PIPELINE_SPEC.md'
    one.write_text('One authority'); two.write_text('Another authority')
    api.bind_context('session', [authority_source(one)])
    pass_gates(api, authority)
    api.evaluate_acceptance('session')
    sources = [] if mutation == 'remove' else [authority_source(one), authority_source(two)]
    with pytest.raises(ForgeError, match='source set cannot change'):
        api.bind_context('session', sources)
    stale = api.status('session')
    assert stale['development_state'] == 'stale'
    assert stale['source_fingerprint'] is None
    assert stale['repairable'] is False
    assert stale['ready'] is False
    saved = json.loads((api.store.root / 'session' / 'task.json').read_text())
    assert saved['managed']['bound_contract']['context_baseline'] == [authority_source(one)]
    assert saved['managed']['context_binding_problem'] == 'supplementary_source_set_changed'
    # Replaying the previously valid list cannot clear the refused policy change
    # or restore an acceptance recorded before that change was proposed.
    with pytest.raises(ForgeError, match='explicit contract update'):
        api.bind_context('session', [authority_source(one)])
    with pytest.raises(ForgeError, match='explicit contract update'):
        api.check('session')
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('blocked verification ran')):
        with pytest.raises(ForgeError, match='explicit contract update'):
            api.verify('session')
    after = api.status('session')
    assert after['development_state'] == 'stale'
    assert after['source_fingerprint'] is None
    assert after['ready'] is False


def test_full_snapshot_sources_are_not_frozen_as_supplementary_norms(managed):
    api, project, _ = managed
    source = project.root / 'orders/rules.py'
    api.bind_context('session', [authority_source(source)])
    saved = json.loads((api.store.root / 'session' / 'task.json').read_text())
    assert saved['managed']['bound_contract']['context_baseline'] == []
    read_all(api)
    before = api.check('session')['source_fingerprint']
    source.write_text('def keep(row): return False\n')
    bound = api.bind_context('session', [authority_source(source)])
    assert bound['source_fingerprint'] != before
    checked = api.check('session')
    assert checked['gates']['check'] == 'passed', checked
    assert api.verify('session')['gates']['verify'] == 'passed'


def test_managed_candidate10_keeps_all_flows_shared_evidence_and_local_history(multiflow_workspace, tmp_path_factory):
    root, _, write, save = multiflow_workspace
    project = save()
    assert project.config['schema'] == 'demiforge-project-v3'
    assert project.standard('orders') == '1.0.0-candidate.10'
    tasks.start(project, 'local-before-managed', 'orders', 'Preserve the local task',
                ['orders'], ['No model or production execution'])
    local_path = project.control / 'tasks/local-before-managed/task.json'
    local_before = local_path.read_bytes()

    api = ManagedTasks(tmp_path_factory.mktemp('managed-v3-state'), 'multi-flow', root)
    api.prepare_task('session', 'orders', 'Develop both explicit flows', ['orders'],
                     ['No model or production execution'],
                     contract=dict(allow_tests=True, require_context_binding=True))
    api.bind_context('session', [authority_source(root / 'AGENTS.md')])
    listing = api.context('session')
    assert {'flow-entry-select', 'flow-entry-evaluate'} <= set(listing['sources'])
    assert 'pipeline-entry' not in listing['sources']
    assert listing['source_coverage']['policy'] == 'candidate.10-multi-flow-source-v1'
    read_all(api)
    checked = api.check('session')
    assert checked['gates']['check'] == 'passed', checked
    verified = api.verify('session', expected_fingerprint=checked['source_fingerprint'])
    assert verified['gates']['verify'] == 'passed', verified
    saved = json.loads((api.store.root / 'session/task.json').read_text())
    assert [command['id'] for command in saved['verification']['commands']] == ['both-flows']
    assert saved['verification']['commands'][0]['log_base'] == 'task'

    write('orders/prompts/review.yaml', 'schema_version: demiflow_prompt_pack_v2\nprompts: {}\n# changed\n')
    stale = api.status('session')
    assert stale['source_fingerprint'] != verified['source_fingerprint']
    assert stale['gates']['verify'] == 'stale'
    assert 'orders/prompts/review.yaml' in stale['changed']

    # Refresh delivery so a context-read failure cannot hide the actual v3
    # cross-entry rule. The second flow must remain subject to that rule.
    write('orders/flows/evaluate.py', 'from .select import run as other\ndef run(value): return other(value)\n')
    api.refresh_context('session', 'Read the changed second flow for mechanical review')
    read_all(api)
    rejected = api.check('session')
    assert rejected['gates']['check'] == 'failed'
    assert any('cross-entry' in violation for violation in rejected['violations'])
    with patch('demiflow.forge.development.tasks.run', side_effect=AssertionError('invalid v3 verifier ran')):
        assert api.verify('session')['gates']['check'] == 'failed'
    assert local_path.read_bytes() == local_before
    assert not (project.control / 'tasks/session').exists()


def test_current_failure_can_be_repaired_without_rebasing_old_acceptance(managed):
    api, project, authority = managed
    pass_gates(api, authority)
    accepted = api.evaluate_acceptance('session')
    path = api.store.root / 'session/task.json'
    original = json.loads(path.read_text())
    api.note('session', 'decision', 'Update the documentation for the second requirement', event_id='second-user-turn')
    (project.root / 'orders/README.md').write_text('Updated DemiForge fixture documentation.\n')
    failed = api.check('session')
    assert failed['development_state'] == 'stale'
    assert failed['gates'] == dict(check='failed', verify='stale', review='stale')
    assert failed['repairable'] is True
    assert failed['accepted_fingerprint'] == accepted['source_fingerprint']
    assert [f['reason'] for f in failed['feedback']] == ['DF-01 context changed: refresh and read changed sources']
    api.refresh_context('session', 'Read the authorized changed documentation')
    pass_gates(api, authority)
    final = api.evaluate_acceptance('session')
    assert final['development_state'] == 'accepted'
    assert final['source_fingerprint'] != accepted['source_fingerprint']
    saved = json.loads(path.read_text())
    for key in ('baseline', 'readonly_baseline', 'goal', 'writes'):
        assert saved[key] == original[key]
    assert saved['managed']['bound_contract'] == original['managed']['bound_contract']


@pytest.mark.parametrize('prior', ['timeout', 'running', 'review_failure'])
def test_previous_candidate_failures_do_not_contaminate_new_failure_feedback(managed, prior):
    api, project, authority = managed
    read_all(api)
    if prior == 'review_failure':
        value = claims(); value['DF-07']['status'] = 'fail'; value['DF-07']['reason'] = 'Old candidate failure'
        fingerprint = api.check('session')['source_fingerprint']
        receipt = authority.issue('session', fingerprint, value, provider='fixture', model='fixture',
                                  session_id='reviewer', mode='no-tools')
        api.record_review('session', fingerprint, value, receipt=receipt)
    else:
        with patch('demiflow.forge.development.tasks.run', return_value=dict(
                id='fixture', passed=False, failure='timeout', output='old timeout')):
            assert api.verify('session')['repairable'] is False
        if prior == 'running':
            with tasks.locked(api._project(), 'session') as (_, state):
                state['verification']['status'] = 'running'
            assert api.status('session')['repairable'] is False
    api.note('session', 'decision', 'A new explicit request', event_id='new-request')
    (project.root / 'orders/README.md').write_text('New authorized DemiForge documentation.\n')
    current = api.check('session')
    assert current['repairable'] is True
    assert all(f['allowed_next_action'] == 'fix_within_scope' for f in current['feedback'])
    assert [f['reason'] for f in current['feedback']] == ['DF-01 context changed: refresh and read changed sources']


@pytest.mark.parametrize('violation', ['out_of_scope', 'readonly', 'corrupt_log', 'forged_review', 'expected_candidate'])
def test_current_check_failure_does_not_make_protected_drift_repairable(tmp_path, violation):
    root = tmp_path / 'project'; root.mkdir()
    project = fixture(root)
    (root / 'data').mkdir(); (root / 'data/input.txt').write_text('Protected data')
    config = json.loads(project.path.read_text()); config['readonly'] = ['data']
    project.path.write_text(json.dumps(config))
    authority = ReviewAuthority()
    api = ManagedTasks(tmp_path / 'state', 'fixture', root, review_authority=authority)
    api.prepare_task('session', 'orders', 'Develop fixture', ['orders'], ['No production'],
                     contract=dict(allow_tests=True, developer_session_id='developer'))
    pass_gates(api, authority)
    old = api.evaluate_acceptance('session')
    (root / 'orders/README.md').write_text('Authorized update needing context refresh')
    if violation == 'out_of_scope':
        (root / 'AGENTS.md').write_text('Changed rules outside scope')
    elif violation == 'readonly':
        (root / 'data/input.txt').write_text('Unauthorized data mutation')
    elif violation == 'corrupt_log':
        state = json.loads((api.store.root / 'session/task.json').read_text())
        (api.store.root / 'session' / state['verification']['commands'][0]['log']).write_text('Changed log')
    elif violation == 'forged_review':
        with tasks.locked(api._project(), 'session') as (_, state):
            state['review']['receipt']['signature'] = 'forged'
    current = api.check('session', expected_fingerprint=old['source_fingerprint'] if violation == 'expected_candidate' else None)
    assert current['repairable'] is False
    assert current['ready'] is False
    assert current['accepted_fingerprint'] == old['accepted_fingerprint']


def test_current_trusted_review_failure_is_repairable(managed):
    api, _, authority = managed
    pass_gates(api, authority)
    accepted = api.evaluate_acceptance('session')
    api.note('session', 'decision', 'Check the next requirement', event_id='next-request')
    api.check('session'); api.verify('session')
    fingerprint = api.status('session')['source_fingerprint']
    value = claims(); value['DF-07']['status'] = 'fail'; value['DF-07']['reason'] = 'Add the requested test assertion'
    receipt = authority.issue('session', fingerprint, value, provider='fixture', model='fixture',
                              session_id='reviewer', mode='no-tools')
    current = api.record_review('session', fingerprint, value, receipt=receipt)
    assert current['repairable'] is True
    assert current['accepted_fingerprint'] == accepted['accepted_fingerprint']
    assert current['development_state'] == 'stale' and current['ready'] is False
    assert [f['reason'] for f in current['feedback']] == ['Add the requested test assertion']


def test_enforced_repair_requires_current_developer_receipt_and_backend(managed):
    api, project, authority = managed
    api.prepare_task('enforced', 'orders', 'Develop fixture', ['orders'], ['No production'],
        contract=dict(allow_tests=True, developer_session_id='developer', require_enforcement=True))
    read_all(api, 'enforced')
    (project.root / 'orders/README.md').write_text('Updated DemiForge documentation')
    current = api.check('enforced')
    assert current['repairable'] is False
    authority.execution_identity = dict(backend='fixture', implementation='v1')
    receipt = authority.issue_execution('enforced', current['source_fingerprint'], phase='developer',
        backend='fixture', implementation='v1', execution_id='completed-developer')
    current = api.record_execution('enforced', current['source_fingerprint'], receipt=receipt)
    assert current['repairable'] is True
    authority.execution_identity = dict(backend='fixture', implementation='v2')
    assert api.status('enforced')['repairable'] is False


def test_checked_readonly_dialogue_needs_developer_evidence_but_does_not_accept(managed):
    api, project, authority = managed
    api.prepare_task('readonly', 'orders', 'Inspect available inputs', ['orders'], ['No production'],
        contract=dict(allow_tests=True, developer_session_id='developer', require_enforcement=True))
    read_all(api, 'readonly')
    current = api.check('readonly')
    assert current['changed_total'] == 0 and current['gates']['check'] == 'passed'
    assert current['unchanged_checked'] is False
    authority.execution_identity = dict(backend='fixture', implementation='v1')
    receipt = authority.issue_execution('readonly', current['source_fingerprint'], phase='developer',
        backend='fixture', implementation='v1', execution_id='completed-reader')
    current = api.record_execution('readonly', current['source_fingerprint'], receipt=receipt)
    assert current['unchanged_checked'] is True
    assert current['ready'] is False and current['accepted_fingerprint'] is None
    assert current['gates']['verify'] == 'missing' and current['execution_evidence']['verifier'] == 'missing'
    authority.execution_identity = dict(backend='fixture', implementation='v2')
    assert api.status('readonly')['unchanged_checked'] is False
    authority.execution_identity = dict(backend='fixture', implementation='v1')
    (project.root / 'orders/rules.py').write_text('def keep(row): return False\n')
    assert api.check('readonly')['unchanged_checked'] is False


def test_readonly_dialogue_shortcut_rejects_staging_drift(managed):
    api, project, _ = managed
    read_all(api)
    assert api.check('session')['unchanged_checked'] is True
    with patch('demiflow.forge.development.managed._index_snapshot', return_value={'changed': 'fixture'}):
        current = api.check('session')
        assert current['unchanged_checked'] is False and current['gates']['check'] == 'failed'
