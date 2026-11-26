"""Regression tests for multi-turn history preservation and development chat performance."""
from demiflow.forge.bridge import model_channel as channel_module
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

import pytest

from demiflow.forge import source_snapshot as changes
from demiflow.workbench.conversations import model_broker as broker
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.deepseek import DeepSeek
from demiflow.workbench.conversations.policy import Limits
from test_forge_development_chat_http import app, request
from test_forge_development_sessions_browser import browser_page, conversation_app, send


def test_replay_compacts_heavy_completed_turns_and_preserves_active_turn(tmp_path):
    with app(tmp_path, acp=True) as (server, workspace, root):
        service = server.development
        store = service.store
        sid = service.create_project(workspace, root, 'heavy-history', 'deepseek')['id']
        for turn_idx in range(5):
            turn, _ = store.submit(
                workspace, sid, f'heavy-{turn_idx}', turn_idx,
                f'第 {turn_idx + 1} 轮需求', root_identity(root), service.policy_hash,
            )
            batch = []
            for d in range(25):
                batch.append(('message.delta', dict(item_id='think-1', channel='process', text=f'step {d} '), False))
                batch.append(('usage.observed', dict(format='demiflow_native_metrics_v1', kind='step_started', event_sequence=d, native_turn=1), False))
            batch.append(('message.completed', dict(item_id='think-1', channel='process', text=''.join(f'step {d} ' for d in range(25))), False))
            for t in range(10):
                batch.append(('item.updated', dict(id=f'tool-{t}', type='tool', title=f'Tool {t}', status='running'), False))
                batch.append(('item.updated', dict(id=f'tool-{t}', type='tool', title=f'Tool {t}', status='completed', output=f'ok-{t}'), False))
            for d in range(15):
                batch.append(('message.delta', dict(item_id='ans-1', text=f'part {d} '), False))
            batch.append(('message.completed', dict(item_id='ans-1', text=f'第 {turn_idx + 1} 轮最终结论'), False))
            store.events_batch(turn['id'], batch)
            store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})

        # Add an active in-progress 6th turn with live streaming deltas.
        active_turn, _ = store.submit(
            workspace, sid, 'heavy-active', 5, '第 6 轮进行中', root_identity(root), service.policy_hash,
        )
        store.events_batch(active_turn['id'], [
            ('message.delta', dict(item_id='live-ans', text='正在输出第6轮'), False),
        ])

        total_seq = store.show(workspace, sid)['sequence']
        assert total_seq > 400

        status, replay = request(server, workspace, 'dev-replay', query='?' + urlencode(dict(session_id=sid)))
        assert status == 200, replay
        assert replay['next_sequence'] == replay['last_sequence'] == total_seq
        assert replay['has_earlier'] is False
        assert len(replay['events']) <= 100
        # All 6 user prompts, all 5 completed answers, and live turn delta must be present.
        user_texts = [e['data']['text'] for e in replay['events'] if e['type'] == 'message.user']
        assert user_texts == [f'第 {i + 1} 轮需求' for i in range(5)] + ['第 6 轮进行中']
        answers = [e['data']['text'] for e in replay['events'] if e['type'] == 'message.completed' and e['data'].get('channel') != 'process']
        assert answers == [f'第 {i + 1} 轮最终结论' for i in range(5)]
        assert any(e['type'] == 'message.delta' and e['data'].get('text') == '正在输出第6轮' for e in replay['events'])
        assert not any(e['type'] == 'usage.observed' for e in replay['events'])

        # Combined dev-events with include_session=1 returns both events and session state.
        status, ev_page = request(
            server, workspace, 'dev-events',
            query='?' + urlencode(dict(session_id=sid, after_sequence=total_seq, limit=50, wait_ms=0, include_session=1)),
        )
        assert status == 200 and ev_page['session']['id'] == sid and ev_page['events'] == []
        assert request(server, workspace, 'dev-events', query='?' + urlencode(dict(session_id=sid, include_session=2)))[0] == 400


def test_browser_multiturn_history_survives_new_message_and_reload(tmp_path):
    """Sending a new message in a heavy multi-turn session keeps prior turns' Q&A visible."""
    with conversation_app(tmp_path) as (server, workspace, root):
        service = server.development
        store = service.store
        sid = service.create_project(workspace, root, 'multiturn-keep', 'codex')['id']
        # Seed 4 heavy turns (each with >30 tool/process items and >100 raw events -> >120 items, >450 events).
        for turn_idx in range(4):
            turn, _ = store.submit(
                workspace, sid, f'seed-{turn_idx}', turn_idx,
                f'历史问题-{turn_idx + 1}', root_identity(root), service.policy_hash,
            )
            batch = []
            for d in range(20):
                batch.append(('message.delta', dict(item_id=f'think-{turn_idx}', channel='process', text=f't{d} '), False))
                batch.append(('usage.observed', dict(format='demiflow_native_metrics_v1', kind='step_started', event_sequence=d, native_turn=1), False))
            batch.append(('message.completed', dict(item_id=f'think-{turn_idx}', channel='process', text=f'思考-{turn_idx + 1}'), False))
            for t in range(30):
                batch.append(('item.updated', dict(id=f'tool-{turn_idx}-{t}', type='tool', title=f'Cmd {t}', status='completed', output='ok'), False))
            for d in range(15):
                batch.append(('message.delta', dict(item_id=f'ans-{turn_idx}', text=f'chunk {d} '), False))
            batch.append(('message.completed', dict(item_id=f'ans-{turn_idx}', text=f'历史回答-{turn_idx + 1}'), False))
            store.events_batch(turn['id'], batch)
            store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})

        with browser_page(server) as (page, expect):
            # Opening the session must show all 4 historical turns' user prompts and answers.
            for i in range(4):
                expect(page.locator('.dfc-user').nth(i)).to_contain_text(f'历史问题-{i + 1}')
                expect(page.locator('.dfc-answers>.dfc-answer').nth(i)).to_contain_text(f'历史回答-{i + 1}')

            # Sending a new 5th message must keep all 4 historical turns visible too.
            send(page, '最新第5轮提问')
            expect(page.locator('.dfc-user')).to_have_count(5)
            for i in range(4):
                expect(page.locator('.dfc-user').nth(i)).to_contain_text(f'历史问题-{i + 1}')
                expect(page.locator('.dfc-answers>.dfc-answer').nth(i)).to_contain_text(f'历史回答-{i + 1}')
            expect(page.locator('.dfc-user').nth(4)).to_contain_text('最新第5轮提问')
            expect(page.locator('.dfc-answers>.dfc-answer').nth(4)).to_contain_text('fixture answer')

            # Reloading the page must still show all 5 turns cleanly.
            page.reload()
            page.locator('.development-status[data-turn-state="completed"][data-turn-cleanup="complete"]').wait_for(state='attached')
            expect(page.locator('.dfc-user')).to_have_count(5)
            for i in range(4):
                expect(page.locator('.dfc-user').nth(i)).to_contain_text(f'历史问题-{i + 1}')
                expect(page.locator('.dfc-answers>.dfc-answer').nth(i)).to_contain_text(f'历史回答-{i + 1}')
            expect(page.locator('.dfc-user').nth(4)).to_contain_text('最新第5轮提问')


def test_model_broker_deferred_handshake_overlaps_and_propagates_errors(tmp_path, monkeypatch):
    import shutil
    from types import SimpleNamespace
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is needed for deferred handshake test')
    plugin = channel_module.plugin_path('deepseek_model_broker.cjs')
    driver = tmp_path / 'slow_llm.cjs'
    driver.write_text('''
const plugin=require(process.argv[2]);
setTimeout(() => {
  plugin.apply({effect(){},llm:{
    async resolveModelInfo(provider,id){return {provider,id,name:id,defaultMaxTokens:65536,inputModalities:['text']};},
    async *stream(){yield {type:'text-delta',index:0,text:'ok'};yield {type:'finish',reason:{kind:'stop'}};}
  }}, {provider:'fixture',model:'fixture-model',reasoningEffort:'high'});
}, 150);
''')
    monkeypatch.setattr(channel_module, 'capabilities', lambda *_: dict(available=True))
    monkeypatch.setattr(DeepSeek, 'launch', lambda config, home, root: ([node, str(driver), str(plugin)], dict(PATH='/usr/bin:/bin', HOME=str(home))))
    deployment = SimpleNamespace(limits=Limits(), providers={'deepseek': dict(model='fixture-model', model_provider='fixture', effort='high', native_credentials='/not-exposed')})
    home = tmp_path / 'session/control/turn-defer/model'
    t0 = time.monotonic()
    channel = broker.start_model_broker(deployment, dict(id='turn', created=time.time()), dict(provider='deepseek'), home, defer_handshake=True)
    try:
        # Constructor returns before the 150ms delayed Node handshake completes.
        assert time.monotonic() - t0 < 0.14
        assert channel.socket_path.exists()
        channel.wait_ready(timeout=10)
    finally:
        channel.close()
    assert not channel.socket_path.exists()


def test_single_pass_capture_preserves_hashes_and_preview_budgets(tmp_path):
    (tmp_path / 'a.py').write_text('print("hello")\n', encoding='utf-8')
    (tmp_path / 'big.py').write_text('x' * 20000, encoding='utf-8')
    (tmp_path / 'many.py').write_text('a\n' * 600, encoding='utf-8')
    (tmp_path / 'bin.dat').write_bytes(b'\x00\x01\x02')
    (tmp_path / 'bad.txt').write_bytes(b'\xff\xfe')
    facts = changes.capture(tmp_path, ['a.py', 'big.py', 'many.py', 'bin.dat', 'bad.txt', 'missing.py'])
    assert facts['a.py']['preview'] == 'print("hello")\n'
    assert facts['a.py']['bytes'] == 15 and len(facts['a.py']['sha256']) == 64
    assert facts['big.py']['preview_unavailable'] == 'preview_byte_budget'
    assert facts['many.py']['preview_unavailable'] == 'preview_line_budget'
    assert facts['bin.dat']['preview_unavailable'] == 'not_plain_source'
    assert facts['bad.txt']['preview_unavailable'] == 'not_utf8'
    assert facts['missing.py'] is None


def test_warm_worker_reuses_live_provider_across_turns_and_drains_on_close(tmp_path):
    from demiflow.workbench.conversations.service import Service
    from test_forge_development_chat import configuration, wait_turn

    root, path, _ = configuration(tmp_path)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'warm-session', 'codex')['id']
        t1 = service.start_project('wfixture', root, sid, 'warm-1', 0, 'First turn')['id']
        r1 = wait_turn(service, t1)
        assert r1['state'] == 'completed' and r1['cleanup'] == 'complete'
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            warm = service.warm_workers.get(sid)
            if warm is not None and warm.idle:
                break
            time.sleep(0.05)
        warm = service.warm_workers.get(sid)
        assert warm is not None and warm.idle and warm.process.poll() is None
        first_pid = warm.process.pid
        assert not service.workers

        t2 = service.start_project('wfixture', root, sid, 'warm-2', 1, 'Second warm turn')['id']
        r2 = wait_turn(service, t2)
        assert r2['state'] == 'completed' and r2['cleanup'] == 'complete'
        events = service.store.events('wfixture', sid, 0, 50)['events']
        bound_t2 = [e for e in events if e['turn_id'] == t2 and e['type'] == 'provider.bound']
        assert bound_t2 and bound_t2[0]['data'].get('warm_reused') is True
        assert service.warm_workers[sid].process.pid == first_pid
        warm_proc = service.warm_workers[sid].process
    finally:
        service.close()
    assert warm_proc.poll() is not None


def test_model_broker_coalesces_many_deltas_without_20k_frame_limit(tmp_path, monkeypatch):
    import shutil
    from types import SimpleNamespace
    from demiflow.workbench.conversations import model_broker as broker
    from demiflow.workbench.conversations.deepseek import DeepSeek
    from demiflow.workbench.conversations.policy import Limits
    from test_forge_development_model_broker import exchange, options

    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required')
    plugin = channel_module.plugin_path('deepseek_model_broker.cjs')
    driver = tmp_path / 'llm_many.cjs'
    driver.write_text('''
const plugin=require(process.argv[2]);
plugin.apply({effect(){},llm:{
 async resolveModelInfo(provider,id){return {provider,id,name:id,defaultMaxTokens:65536,inputModalities:['text']};},
 async *stream(options){
  for(let i=0;i<25000;i++) yield {type:'text-delta',index:0,text:'a'};
  yield {type:'finish',reason:{kind:'stop'}};
 }
}}, {provider:'fixture',model:'fixture-model',reasoningEffort:'high'});
''')
    monkeypatch.setattr(channel_module, 'capabilities', lambda *_: dict(available=True))
    monkeypatch.setattr(DeepSeek, 'launch', lambda config, home, root: ([node, str(driver), str(plugin)], dict(PATH='/usr/bin:/bin', HOME=str(home))))
    deployment = SimpleNamespace(
        limits=Limits(),
        providers={'deepseek': dict(model='fixture-model', model_provider='fixture', effort='high')},
    )
    home = tmp_path / 'session/control/turn-many/model'
    channel = broker.ModelBroker(deployment, dict(id='turnmany', created=time.time()), dict(provider='deepseek'), home)
    try:
        rows = exchange(channel, 'stream', options())
        assert rows[-1] == {'done': True}
        text = ''.join(r['value']['text'] for r in rows if isinstance(r.get('value'), dict) and r['value'].get('type') == 'text-delta')
        assert len(text) == 25000
        assert len(rows) < 1000
    finally:
        channel.close()


def test_standby_prewarm_accelerates_new_session_first_turn_and_subsequent_turns(tmp_path):
    from demiflow.workbench.conversations.service import Service
    from test_forge_development_chat import configuration, wait_turn

    root, path, _ = configuration(tmp_path)
    service = Service(path)
    try:
        status = service.prewarm('wfixture', root, provider_id='codex')
        assert status['status'] in ('warming', 'ready') and status.get('standby') is True
        deadline = time.monotonic() + 10.0
        warm = None
        while time.monotonic() < deadline:
            warm = next((w for w in service.warm_workers.values() if w.reserved), None)
            if warm is not None and warm.idle:
                break
            time.sleep(0.05)
        assert warm is not None and warm.idle and warm.process.poll() is None
        standby_pid = warm.process.pid
        reserved_sid = warm.session_id

        session = service.create_project('wfixture', root, 'prewarm-new-1', 'codex', new_conversation=True)
        assert session['id'] == reserved_sid
        assert warm.reserved is False

        t1 = service.start_project('wfixture', root, reserved_sid, 'pw-1', 0, 'First standby turn')['id']
        r1 = wait_turn(service, t1)
        assert r1['state'] == 'completed' and r1['cleanup'] == 'complete'
        events = service.store.events('wfixture', reserved_sid, 0, 50)['events']
        bound_t1 = [e for e in events if e['turn_id'] == t1 and e['type'] == 'provider.bound']
        assert bound_t1 and bound_t1[0]['data'].get('standby_prewarmed') is True
        assert bound_t1[0]['data'].get('warm_reused') is True
        assert service.warm_workers[reserved_sid].process.pid == standby_pid

        # Wait until warm worker parks again and verify Turn 2 still reuses exact same PID.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not service.warm_workers[reserved_sid].idle:
            time.sleep(0.05)
        t2 = service.start_project('wfixture', root, reserved_sid, 'pw-2', 1, 'Second turn after standby')['id']
        r2 = wait_turn(service, t2)
        assert r2['state'] == 'completed' and r2['cleanup'] == 'complete'
        events2 = service.store.events('wfixture', reserved_sid, 0, 50)['events']
        bound_t2 = [e for e in events2 if e['turn_id'] == t2 and e['type'] == 'provider.bound']
        assert bound_t2 and bound_t2[0]['data'].get('warm_reused') is True
        assert not bound_t2[0]['data'].get('standby_prewarmed')
        assert service.warm_workers[reserved_sid].process.pid == standby_pid
        warm_proc = service.warm_workers[reserved_sid].process
    finally:
        service.close()
    assert warm_proc.poll() is not None


def test_standby_prewarm_existing_session_and_unused_reserved_cleanup(tmp_path):
    from demiflow.workbench.conversations.service import Service
    from test_forge_development_chat import configuration, wait_turn

    root, path, _ = configuration(tmp_path)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'existing-pw', 'codex')['id']
        st = service.prewarm('wfixture', root, session_id=sid)
        assert st['status'] in ('warming', 'ready') and st['session_id'] == sid
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            w = service.warm_workers.get(sid)
            if w is not None and w.idle:
                break
            time.sleep(0.05)
        w = service.warm_workers.get(sid)
        assert w is not None and w.idle
        pid = w.process.pid

        t1 = service.start_project('wfixture', root, sid, 'epw-1', 0, 'Existing session turn 1')['id']
        r1 = wait_turn(service, t1)
        assert r1['state'] == 'completed' and r1['cleanup'] == 'complete'
        events = service.store.events('wfixture', sid, 0, 50)['events']
        bound = [e for e in events if e['turn_id'] == t1 and e['type'] == 'provider.bound']
        assert bound and bound[0]['data'].get('standby_prewarmed') is True
        assert service.warm_workers[sid].process.pid == pid

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not service.warm_workers[sid].idle:
            time.sleep(0.05)
        st2 = service.prewarm('wfixture', root, provider_id='codex')
        assert st2['status'] in ('warming', 'ready')
        reserved = next((x for x in service.warm_workers.values() if x.reserved), None)
        assert reserved is not None
        reserved_dir = service.root / reserved.session_id
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not reserved.idle:
            time.sleep(0.05)
        assert reserved.idle and reserved_dir.is_dir()
    finally:
        service.close()
    assert not reserved_dir.exists()


def test_direct_model_sandbox_enforces_landlock_write_boundary_without_model_broker(tmp_path):
    import socket
    from demiflow.forge.sandbox.sandbox import SandboxSpec, launch_sandbox, specification
    from demiflow.workbench.conversations.policy import Deployment
    from test_forge_development_chat import configuration

    root, path, _ = configuration(tmp_path)
    raw = json.loads(Path(path).read_text())
    raw['direct_model'] = True
    dep = Deployment(raw)
    assert dep.direct_model is True
    assert dep.capabilities('wfixture', root)['direct_model'] is True

    bad = dict(raw, direct_model='yes')
    with pytest.raises(Exception):
        Deployment(bad)

    home = tmp_path / 'dm_home'; home.mkdir(mode=0o700)
    src = tmp_path / 'dm_src'; src.mkdir()
    cred = tmp_path / 'dm_creds.yaml'; cred.write_text('token: test\n')
    ro = tmp_path / 'dm_ro.txt'; ro.write_text('keep')

    # Verifier role must never be granted direct_network.
    with pytest.raises(ValueError):
        specification(
            SandboxSpec(read_paths=(str(ro),), write_paths=(), runtime_paths=('/usr', '/lib', '/lib64'),
                        protected_paths=(str(ro),), private_home=str(home), role='verifier',
                        seconds=5, direct_network=True),
            ['/usr/bin/python3', '-c', 'pass'], {}, str(src),
        )

    spec = SandboxSpec(
        read_paths=(str(ro), str(cred)),
        write_paths=(str(src),),
        runtime_paths=('/usr', '/lib', '/lib64'),
        protected_paths=(str(ro), str(cred)),
        denied_paths=(),
        private_home=str(home),
        role='developer',
        seconds=10,
        direct_network=True,
    )
    code = '''import os, pathlib, socket, sys
src, ro, cred = sys.argv[1:]
pathlib.Path(src, 'written.txt').write_text('ok')
assert pathlib.Path(ro).read_text() == 'keep'
assert pathlib.Path(cred).read_text() == 'token: test\\n'
for target in (ro, cred):
    for fn in (lambda p=target: pathlib.Path(p).write_text('bad'),
               lambda p=target: os.truncate(p, 0),
               lambda p=target: os.open(p, os.O_RDONLY | os.O_TRUNC)):
        try:
            fn()
        except OSError:
            continue
        raise RuntimeError('Protected file write was allowed: ' + target)
# Direct network mode allows IPv4 TCP + UDP sockets and preserves explicit proxy env.
for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
    s = socket.socket(socket.AF_INET, kind)
    s.close()
assert os.environ.get('HTTPS_PROXY') == 'http://127.0.0.1:7897'
print('direct_ok')
'''
    proc = launch_sandbox(['/usr/bin/python3', '-I', '-B', '-c', code, str(src), str(ro), str(cred)],
                          spec=spec, env={'HTTPS_PROXY': 'http://127.0.0.1:7897'}, cwd=str(src))
    out, err = proc.communicate(timeout=10)
    assert proc.returncode == 0, err.decode(errors='replace')
    assert out.strip() == b'direct_ok'
    assert (src / 'written.txt').read_text() == 'ok'
    assert ro.read_text() == 'keep'
    assert cred.read_text() == 'token: test\n'


def test_notebook_cell_commands_standby_set_turn_and_role_isolation(tmp_path):
    from demiflow.workbench.notebooks.cell_commands import CellCommands
    from demiflow.workbench.conversations.service import Service
    from demiflow.workbench.conversations.store import Store
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.forge.control_client import call as control_call
    from test_forge_development_chat import configuration

    # 1. CellCommands standby -> set_turn -> park_idle lifecycle
    store = Store(tmp_path / 'cell_state', Limits(), initialize=True)
    bridge = CellCommands(store, None, 0.0)
    try:
        assert json.loads((bridge.bin / 'binding.json').read_text()) == ['', '', '']
        with pytest.raises(Exception):
            control_call(str(bridge.path), bridge.token, 't1', 'e1', 'cell.show', {})

        session = store.create_conversation('w1', {'path': str(tmp_path), 'device': 1, 'inode': 1},
                                            'deepseek', 'nb-req-1', 'h' * 64, session_kind='notebook')
        turn, _ = store.submit('w1', session['id'], 'sub-1', 0, 'show cell',
                               {'path': str(tmp_path), 'device': 1, 'inode': 1}, 'h' * 64, operation='notebook_cell_cli')
        bridge.set_turn(turn, time.time() + 60)
        assert json.loads((bridge.bin / 'binding.json').read_text()) == [bridge.token, turn['id'], turn['epoch']]
        res = control_call(str(bridge.path), bridge.token, turn['id'], turn['epoch'], 'cell.show', {})
        assert 'id' in res

        bridge.park_idle()
        assert json.loads((bridge.bin / 'binding.json').read_text()) == ['', '', '']
        with pytest.raises(Exception):
            control_call(str(bridge.path), bridge.token, turn['id'], turn['epoch'], 'cell.show', {})
    finally:
        bridge.close()

    # 2. Developer standby worker is never adopted by a notebook session (and vice versa).
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    try:
        st = service.prewarm('wfixture', root, provider_id='codex', session_kind='development')
        assert st['status'] in ('warming', 'ready')
        dev_warm = next(w for w in service.warm_workers.values() if w.reserved)
        assert dev_warm.operation == 'message'
        # Notebook session must NOT adopt the developer standby worker's reserved session_id.
        nb_session = service.create_project('wfixture', root, 'nb-iso-1', 'codex', new_conversation=True, session_kind='notebook')
        assert nb_session['id'] != dev_warm.session_id
        assert dev_warm.reserved is True
        # Trusted-local deployment has notebook_cells=False -> notebook prewarm returns unavailable cleanly.
        nb_pw = service.prewarm('wfixture', root, provider_id='codex', session_kind='notebook')
        assert nb_pw['status'] == 'unavailable'
    finally:
        service.close()


def test_restricted_notebook_cell_cli_standby_prewarm_and_warm_reuse(tmp_path, monkeypatch):
    from demiflow.workbench.conversations.service import Service
    from test_forge_development_chat import configuration, wait_turn

    monkeypatch.setenv('DEMIFLOW_FAKE_KEY', 'sk-test-notebook-direct')
    root, path, cfg = configuration(tmp_path)
    cfg['mode'] = 'restricted'
    cfg['direct_model'] = True
    cfg['providers']['codex']['base_url'] = 'http://127.0.0.1:1'
    cfg['providers']['codex']['credential_env'] = 'DEMIFLOW_FAKE_KEY'
    Path(path).write_text(json.dumps(cfg))

    service = Service(path)
    try:
        caps = service.capabilities('wfixture', root)
        assert caps['notebook_cells'] is True and caps['notebook_cli'] is True
        st = service.prewarm('wfixture', root, provider_id='codex', session_kind='notebook')
        assert st['status'] in ('warming', 'ready') and st.get('standby') is True
        deadline = time.monotonic() + 10.0
        warm = None
        while time.monotonic() < deadline:
            warm = next((w for w in service.warm_workers.values() if w.reserved and w.operation == 'notebook_cell_cli'), None)
            if warm is not None and warm.idle:
                break
            time.sleep(0.05)
        assert warm is not None and warm.idle and warm.process.poll() is None
        standby_pid = warm.process.pid
        reserved_sid = warm.session_id

        # Development session must NOT adopt this readonly notebook standby worker.
        dev_sess = service.create_project('wfixture', root, 'dev-not-nb', 'codex', new_conversation=True, session_kind='development')
        assert dev_sess['id'] != reserved_sid and warm.reserved is True

        # Notebook session DOES adopt it!
        nb_sess = service.create_project('wfixture', root, 'nb-adopt-1', 'codex', new_conversation=True, session_kind='notebook')
        assert nb_sess['id'] == reserved_sid and warm.reserved is False

        t1 = service.start_project('wfixture', root, reserved_sid, 'nb-t1', 0, 'Cell turn 1', operation='notebook_cell_cli')['id']
        r1 = wait_turn(service, t1)
        assert r1['state'] == 'completed' and r1['cleanup'] == 'complete'
        events1 = service.store.events('wfixture', reserved_sid, 0, 50)['events']
        bound1 = [e for e in events1 if e['turn_id'] == t1 and e['type'] == 'provider.bound']
        assert bound1 and bound1[0]['data'].get('standby_prewarmed') is True
        assert bound1[0]['data'].get('warm_reused') is True
        assert bound1[0]['data'].get('execution_assurance') == 'enforced'
        assert service.warm_workers[reserved_sid].process.pid == standby_pid

        # Wait until parked, then Turn 2 reuses the exact same readonly Landlock worker PID!
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not service.warm_workers[reserved_sid].idle:
            time.sleep(0.05)
        assert service.warm_workers[reserved_sid].idle
        t2 = service.start_project('wfixture', root, reserved_sid, 'nb-t2', 1, 'Cell turn 2', operation='notebook_cell_cli')['id']
        r2 = wait_turn(service, t2)
        assert r2['state'] == 'completed' and r2['cleanup'] == 'complete'
        events2 = service.store.events('wfixture', reserved_sid, 0, 50)['events']
        bound2 = [e for e in events2 if e['turn_id'] == t2 and e['type'] == 'provider.bound']
        assert bound2 and bound2[0]['data'].get('warm_reused') is True
        assert not bound2[0]['data'].get('standby_prewarmed')
        assert service.warm_workers[reserved_sid].process.pid == standby_pid
        warm_proc = service.warm_workers[reserved_sid].process
    finally:
        service.close()
    assert warm_proc.poll() is not None
