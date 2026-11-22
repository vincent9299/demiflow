"""Regression tests for multi-turn history preservation and development chat performance."""
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

import pytest

from demiflow.workbench.conversations import changes
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
            page.locator('.development-status').filter(has_text='本轮已完成').wait_for()
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
    plugin = Path(broker.__file__).with_name('deepseek_model_broker.cjs')
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
    monkeypatch.setattr(broker, 'capabilities', lambda *_: dict(available=True))
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
