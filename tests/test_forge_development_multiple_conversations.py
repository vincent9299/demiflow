"""Explicit project conversations retain history and share one physical writer lease."""
from concurrent.futures import ThreadPoolExecutor
import json
from types import SimpleNamespace
import threading

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import http
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.policy import Deployment, Limits
from demiflow.workbench.conversations.service import Service
from demiflow.workbench.conversations.store import Store
from test_forge_development_chat import configuration, wait_turn


@pytest.fixture
def case(tmp_path):
    root = tmp_path / 'project'
    root.mkdir()
    store = Store(tmp_path / 'state', Limits(control_reserve=4096, output_bytes=256 * 1024), initialize=True)
    return SimpleNamespace(root=root, identity=root_identity(root), store=store, workspace='workspace', policy='policy')


def create(case, request, *, new=False, workspace=None):
    method = case.store.create_conversation if new else case.store.ensure_project
    return method(workspace or case.workspace, case.identity, 'codex', request, case.policy)['id']


def listing(case, **kwargs):
    return case.store.list(case.workspace, identity=case.identity, policy_hash=case.policy, providers={'codex'}, **kwargs)


def submit(case, sid, key='message', revision=0):
    return case.store.submit(case.workspace, sid, key, revision, 'First conversation message', case.identity, case.policy)[0]


def finish(case, turn):
    case.store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})


def service_for(case):
    """Use production service routing with no worker, admission snapshot or model."""
    service = Service.__new__(Service)
    service.store, service.policy_hash = case.store, case.policy
    service.lock = threading.Lock()
    service._reap = lambda: None
    project = dict(root=str(case.root), identity=case.identity)

    def project_grant(workspace, root):
        if workspace != case.workspace or root_identity(root) != case.identity:
            raise ForgeError('Project does not belong to this grant', code='conflict')
        return project

    service.deployment = SimpleNamespace(projects={case.workspace: project}, providers={'codex': {}}, project=project_grant)
    return service


def test_concurrent_default_stays_primary_and_explicit_creation_is_idempotent(case):
    with ThreadPoolExecutor(max_workers=4) as pool:
        main_ids = list(pool.map(lambda n: create(case, 'old-tab-' + str(n)), range(8)))
        new_ids = list(pool.map(lambda _: create(case, 'new-request', new=True), range(8)))
    assert len(set(main_ids)) == len(set(new_ids)) == 1
    assert main_ids[0] != new_ids[0]
    assert create(case, 'different-tab') == main_ids[0]
    assert create(case, 'new-request') == main_ids[0]
    assert create(case, 'new-request', new=True) == new_ids[0]
    assert len(listing(case)['sessions']) == 2
    assert all(row['writable'] and row['turn'] is None for row in listing(case)['sessions'])
    assert not listing(case)['writer_busy'] and listing(case)['active_session_id'] is None


def test_first_explicit_conversation_pins_primary_and_persists_on_restart(case):
    first = create(case, 'first', new=True)
    second = create(case, 'second', new=True)
    assert create(case, 'legacy-client') == first
    reopened = Store(case.store.root, case.store.limits, initialize=True)
    assert reopened.primary(case.workspace, case.identity, case.policy)['id'] == first
    assert reopened.writable(case.workspace, second, case.identity, case.policy, {'codex'})
    assert reopened.create_conversation(case.workspace, case.identity, 'codex', 'second', case.policy)['id'] == second


def test_legacy_migration_keeps_only_main_writable_and_history_unchanged(case):
    old = case.store.create(case.workspace, case.identity, 'codex', 'old', case.policy)['id']
    old_turn = submit(case, old)
    finish(case, old_turn)
    main = case.store.create(case.workspace, case.identity, 'codex', 'main', case.policy)['id']
    finish(case, submit(case, main))
    empty = case.store.create(case.workspace, case.identity, 'codex', 'unused', case.policy)['id']
    history = {sid: case.store.events(case.workspace, sid) for sid in (old, main, empty)}
    with case.store.connect(write=True) as db:
        before = [tuple(row) for row in db.execute('SELECT * FROM sessions ORDER BY id')]
        db.execute('DROP TABLE project_conversation_members')
    case.store = Store(case.store.root, case.store.limits, initialize=True)
    with case.store.connect() as db:
        assert [tuple(row) for row in db.execute('SELECT * FROM sessions ORDER BY id')] == before
        assert db.execute('SELECT count(*) FROM project_conversation_members').fetchone()[0] == 0
    new = create(case, 'new-after-migration', new=True)
    writable = {row['id'] for row in listing(case)['sessions'] if row['writable']}
    assert writable == {main, new}
    assert create(case, 'default-after-migration') == main
    assert {sid: case.store.events(case.workspace, sid) for sid in history} == history
    with pytest.raises(ForgeError, match='membership'):
        create(case, 'old', new=True)
    assert not case.store.writable(case.workspace, old, case.identity, case.policy, {'codex'})


def test_fallback_never_promotes_an_explicit_member(case):
    main = create(case, 'main')
    member = create(case, 'member', new=True)
    finish(case, submit(case, member))
    # A missing legacy binding must not reinterpret explicit membership as the
    # legacy migration choice, even when the member has newer messages.
    with case.store.connect(write=True) as db:
        db.execute('DELETE FROM project_conversations WHERE workspace=?', (case.workspace,))
    assert case.store.primary(case.workspace, case.identity, case.policy)['id'] == main


@pytest.mark.parametrize('changed', ['root', 'policy', 'provider'])
def test_writable_and_creation_retries_require_exact_current_grant(case, changed):
    sid = create(case, 'member', new=True)
    identity, policy, providers = dict(case.identity), case.policy, {'codex'}
    provider = 'codex'
    if changed == 'root':
        identity['inode'] += 1
    elif changed == 'policy':
        policy = 'replacement-policy'
    else:
        providers, provider = {'deepseek'}, 'deepseek'
    assert not case.store.writable(case.workspace, sid, identity, policy, providers)
    page = case.store.list(case.workspace, identity=identity, policy_hash=policy, providers=providers)
    assert page['sessions'][0]['writable'] is False
    with pytest.raises(ForgeError, match='idempotency'):
        case.store.create_conversation(case.workspace, identity, provider, 'member', policy)


def test_new_current_grant_member_does_not_upgrade_old_primary(case):
    main = create(case, 'main')
    changed = 'new-policy'
    member = case.store.create_conversation(case.workspace, case.identity, 'codex', 'new-grant', changed)['id']
    assert not case.store.writable(case.workspace, main, case.identity, changed, {'codex'})
    assert case.store.writable(case.workspace, member, case.identity, changed, {'codex'})
    assert case.store.primary(case.workspace, case.identity, changed)['id'] == main


def test_paged_minimal_list_and_writer_identity_are_workspace_scoped(case):
    main = create(case, 'main')
    members = [create(case, 'member-' + str(n), new=True) for n in range(21)]
    alias = create(case, 'alias', new=True, workspace='alias-workspace')
    turn = submit(case, main)
    page = listing(case)
    assert len(page['sessions']) == page['next_offset'] == 20
    assert {row['id'] for row in page['sessions']}.isdisjoint({main, alias})
    assert page['writer_busy'] is True and page['active_session_id'] == main
    tail = listing(case, offset=20)
    assert len(tail['sessions']) == 2 and tail['next_offset'] is None
    row = next(row for row in tail['sessions'] if row['id'] == main)
    assert row['title'] == 'First conversation message'
    assert row['turn'] == dict(id=turn['id'], state='preparing', cleanup='pending', development=dict(busy=False))
    assert set(row) == {'id', 'title', 'provider', 'created', 'revision', 'writable', 'turn'}
    alias_page = case.store.list('alias-workspace', identity=case.identity, policy_hash=case.policy, providers={'codex'})
    assert alias_page['writer_busy'] and alias_page['active_session_id'] is None
    assert main not in json.dumps(alias_page)
    for sid, workspace in ((members[0], case.workspace), (alias, 'alias-workspace')):
        with pytest.raises(ForgeError, match='physical project'):
            case.store.submit(workspace, sid, 'busy', 0, 'Should not launch', case.identity, case.policy)
    finish(case, turn)
    assert not listing(case)['writer_busy']
    finish(case, submit(case, members[0]))
    with pytest.raises(ForgeError):
        listing(case, limit=21)


@pytest.mark.parametrize('changes', [dict(gate_cleanup='pending'), dict(gate_phase='repair_pending')])
def test_completed_provider_keeps_writer_busy_until_gate_cleanup(case, changes):
    sid = create(case, 'main')
    member = create(case, 'member', new=True)
    turn = submit(case, sid)
    finish(case, turn)
    case.store.update(turn['id'], **changes, development=json.dumps(dict(busy=True)))
    page = listing(case)
    assert page['writer_busy'] and page['active_session_id'] == sid
    assert next(row for row in page['sessions'] if row['id'] == sid)['turn']['development']['busy']
    with pytest.raises(ForgeError, match='physical project'):
        submit(case, member)


def test_session_scoped_project_turns_preserve_aggregate_and_ownership(case):
    main = create(case, 'main')
    member = create(case, 'member', new=True)
    turns = []
    for sid in (main, member, main):
        revision = case.store.session(sid)['revision']
        turn = submit(case, sid, 'm-' + str(revision), revision)
        finish(case, turn)
        turns.append(turn)
    foreign = create(case, 'foreign', new=True, workspace='foreign-workspace')
    old_events = case.store.events(case.workspace, main)
    assert len(case.store.project_turns(case.workspace)['turns']) == 3
    page = case.store.project_turns(case.workspace, limit=1, session_id=main)
    assert page['turns'][0]['id'] == turns[-1]['id'] and page['next_offset'] == 1
    assert case.store.project_turns(case.workspace, offset=1, session_id=main)['turns'][0]['id'] == turns[0]['id']
    assert case.store.project_turns(case.workspace, session_id=member, kind='timeline')['turns'] == []
    with pytest.raises(ForgeError, match='not found'):
        case.store.project_turns(case.workspace, session_id=foreign)
    assert case.store.events(case.workspace, main) == old_events


def test_routes_authorize_members_without_reconciling_each_list_row(case):
    service = service_for(case)
    server = SimpleNamespace(development=service)
    main = http.post(server, case.workspace, case.root, 'dev-session-create', dict(request_id='main', provider_id='codex'))
    member = http.post(server, case.workspace, case.root, 'dev-session-create',
                       dict(request_id='member', provider_id='codex', new_conversation=True))
    old = service.create(case.workspace, case.root, 'old-history', 'codex')['id']
    started = []
    service.start = lambda *args, **kwargs: started.append((args, kwargs)) or {'id': 'fixture-turn'}
    for sid in (main['id'], member['id']):
        assert http.get(server, case.workspace, case.root, 'dev-session', dict(session_id=sid))['writable']
        http.post(server, case.workspace, case.root, 'dev-turn-start',
                  dict(session_id=sid, submission_id='send', expected_revision=0, text='Hello'))
    assert len(started) == 2
    assert not http.get(server, case.workspace, case.root, 'dev-session', dict(session_id=old))['writable']
    for operation, fields in [('dev-turn-start', dict(text='No')), ('dev-compact', {})]:
        with pytest.raises(ForgeError, match='read-only'):
            http.post(server, case.workspace, case.root, operation,
                      dict(session_id=old, submission_id='no', expected_revision=0, **fields))
    service._reap = lambda: pytest.fail('Listing must not reconcile workers')
    page = http.get(server, case.workspace, case.root, 'dev-sessions', {})
    assert {row['id'] for row in page['sessions'] if row['writable']} == {main['id'], member['id']}
    assert 'new_conversation' in http.POST_FIELDS['dev-session-create'][1]
    assert 'session_id' in http.GET_FIELDS['dev-project-turns'][1]
    assert http.get(server, case.workspace, case.root, 'dev-project-turns', dict(session_id=member['id']))['turns'] == []


@pytest.mark.parametrize('invalid', [None, 0, 1, 'true', 'false', [], {}])
def test_new_conversation_is_strict_boolean_before_local_or_remote_dispatch(case, invalid):
    service = service_for(case)
    with pytest.raises(ForgeError, match='boolean'):
        service.create_project(case.workspace, case.root, 'new', 'codex', new_conversation=invalid)
    remote = SimpleNamespace(remote=True, forward=lambda *args, **kwargs: pytest.fail('Invalid flag reached remote'))
    with pytest.raises(ForgeError, match='boolean'):
        http.post(SimpleNamespace(development=remote), case.workspace, case.root, 'dev-session-create',
                  dict(request_id='new', provider_id='codex', new_conversation=invalid))


def test_capability_preserves_statistics_and_configured_effort(tmp_path):
    root, _, value = configuration(tmp_path)
    capability = Deployment(value).capabilities('wfixture', root)
    assert capability['multiple_conversations'] is True and capability['conversation_stats'] is True
    assert capability['providers'][0]['effort'] == 'xhigh'


def test_real_service_protocol_fixture_keeps_member_history_and_compaction_separate(tmp_path):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    server = SimpleNamespace(development=service)
    workspace = 'wfixture'
    try:
        primary = service.create_project(workspace, root, 'main', 'codex')['id']
        member = service.create_project(workspace, root, 'member', 'codex', new_conversation=True)['id']
        for sid, prompt in ((primary, 'Primary decision'), (member, 'Independent decision')):
            turn = service.start_project(workspace, root, sid, 'first', 0, prompt)
            assert wait_turn(service, turn['id'])['state'] == 'completed'
        saved = service.store.events(workspace, primary)
        first_member = service.store.events(workspace, member)['events']
        compacted = service.compact(workspace, root, member, 'archive', 1)
        assert wait_turn(service, compacted['id'])['state'] == 'completed'
        assert service.show(workspace, member)['archive_sequence'] > 0
        assert service.show(workspace, primary)['archive_sequence'] == 0
        assert service.store.events(workspace, primary) == saved
        assert service.store.events(workspace, member)['events'][:len(first_member)] == first_member
        for sid in (primary, member):
            stats = http.get(server, workspace, root, 'dev-stats', dict(session_id=sid))
            assert stats['session_id'] == sid and stats['turns'] == 1
        assert not service.sessions(workspace, root)['writer_busy']
        assert service.conversation(workspace, root)['session']['id'] == primary
    finally:
        service.close()
