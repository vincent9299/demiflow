"""Coding materials cross the actual provider protocol before source changes."""
import json

import pytest

from test_forge_development_chat import wait_turn
from test_forge_development_chat_http import app
from test_forge_markdown_reading import browser


@pytest.mark.parametrize('acp', [False, True])
def test_guidance_crosses_start_and_resume_without_a_parent_task(tmp_path, acp):
    legacy = tmp_path / '.demiforge'
    legacy.mkdir()
    (legacy / 'project.json').write_text('invalid parent should not be opened')
    with app(tmp_path, acp=acp, guided=True) as (server, workspace, root):
        service = server.development
        provider = 'deepseek' if acp else 'codex'
        session = service.create_project(workspace, root, 'new', provider)['id']
        for revision in range(2):
            user = f'Explain this project ({revision})'
            turn = service.start_project(workspace, root, session, f'send-{revision}', revision, user)
            done = wait_turn(service, turn['id'])
            assert done['state'] == 'completed' and done['dispatch'] == 'acknowledged', done['result']
            manifest = json.loads(done['context'])
            assert manifest['coding_guidance']['catalog_id']
            assert 'task_id' not in manifest
            received = (service.root / session / 'received-context.txt').read_text()
            assert '# DemiForge 项目开发' in received and 'demiflow forge context' in received
            assert 'demiflow forge check' in received
            assert manifest['requirement'] == user
            if acp:
                if revision == 0:
                    assert received.count(user) == 1
                else:
                    # Resumed ACP turns send only the user's message with zero extra context.
                    assert (service.root / session / 'received-request.txt').read_text() == user
            else:
                assert user not in received
                assert (service.root / session / 'received-request.txt').read_text() == user
            assert 'pipeline_checks' not in json.loads(done['result'])
        assert not (service.root / 'forge-development').exists()


def test_guided_browser_reports_independent_pipeline_checks(tmp_path, browser):
    with app(tmp_path, guided=True) as (server, workspace, root):
        from test_map_prompt_async import PACK
        (root / 'prompts/guided.yaml').write_text(
            PACK.replace('base_url_env: TEST_PROMPT_URL', 'base_url: http://127.0.0.1:9/v1'))
        helper = root / 'lib'
        helper.mkdir()
        (helper / 'normalize.py').write_text(
            "from typing import TypedDict\nclass Row(TypedDict):\n    name: str\n"
            "def normalize(row: Row) -> Row:\n    return {'name': row['name'].upper()}\n")
        page = browser.new_page()
        page.set_default_timeout(20000)
        page.goto(server.origin + '/#token=' + server.token)
        page.get_by_role('button', name='开发', exact=True).click()

        for revision, (scenario, native, layout) in enumerate([
                ('misplaced', 'passed', 'failed'), ('prompt', 'passed', 'passed'), ('manual', 'failed', 'passed'),
                ('unproved', 'incomplete', 'passed'),
                ('layout', 'passed', 'failed'), ('crossflow', 'passed', 'failed')], start=1):
            if scenario == 'crossflow':
                (root / 'flows/other.py').write_text(
                    "from demiflow.data import from_items\ndef source():\n    return from_items([{'id': 1}])\n")
            page.get_by_label('开发要求').fill('guided fixture edit ' + scenario)
            with page.expect_response(lambda response: response.url.endswith('/dev-turn-start')) as submitted:
                page.get_by_role('button', name='发送', exact=True).click()
            response = submitted.value
            assert response.status == 202, response.json()
            done = wait_turn(server.development, response.json()['id'])
            saved = json.loads(done['result'])
            assert done['state'] == 'completed', saved
            checks = saved['pipeline_checks']
            assert checks['native_flow']['status'] == native, checks
            assert checks['project_layout']['status'] == layout, checks
            if scenario == 'misplaced':
                assert any(item['rule'] == 'DF-02' and item['path'] == 'prompts/misplaced.py'
                           for item in checks['project_layout']['diagnostics']), checks
            assert checks['catalog_id'] == json.loads(done['context'])['coding_guidance']['catalog_id']
            page.get_by_text('项目目录检查', exact=True).nth(revision - 1).wait_for(state='attached')
            page.get_by_text('原生数据流检查', exact=True).nth(revision - 1).wait_for(state='attached')
        page.reload()
        page.get_by_text('项目目录检查', exact=True).last.wait_for(state='attached')
        assert (root / 'flows/guided.py').exists()
        page.close()


def test_new_root_flow_is_reported_without_guessing_from_function_names(tmp_path):
    from demiflow.forge.api_reference import build_reference
    from demiflow.forge.pipeline_check import check_project

    (tmp_path / 'misplaced.py').write_text(
        "from demiflow.data import from_items\ndef compute():\n    return from_items([{'id': 1}]).count()\n")
    (tmp_path / 'helper.py').write_text('def run():\n    return 42\n')
    (tmp_path / 'source.py').write_text(
        "from demiflow.data import from_items\ndef build_source():\n    return from_items([{'id': 1}])\n")
    report = check_project(tmp_path, paths=['helper.py', 'source.py', 'misplaced.py'], reference=build_reference())
    assert report['native_flow']['status'] == 'passed', report
    assert report['native_flow']['inspected_count'] == 1
    assert report['project_layout']['status'] == 'failed'
    assert [(item['rule'], item['path']) for item in report['project_layout']['diagnostics']] == [
        ('DF-02', 'misplaced.py')
    ]


def test_old_development_database_is_refused_and_untouched(tmp_path):
    import sqlite3

    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.workbench.conversations.store import Store

    state = tmp_path / 'old-state'
    state.mkdir(mode=0o700)
    path = state / 'chat.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE turns (id TEXT, text TEXT)')
        db.execute('INSERT INTO turns VALUES (?, ?)', ('archived', 'Original record'))
    before = path.read_bytes()
    with pytest.raises(ForgeError, match='fresh directory'):
        Store(state, Limits(control_reserve=4096), initialize=True)
    assert path.read_bytes() == before



def test_cancel_wins_over_a_late_pipeline_check_result(tmp_path):
    from demiflow.workbench.conversations.common import root_identity
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.workbench.conversations.store import Store

    project = tmp_path / 'project'
    project.mkdir()
    identity = root_identity(project)
    store = Store(tmp_path / 'state', Limits(control_reserve=4096), initialize=True)
    session = store.create('workspace', identity, 'codex', 'new', 'policy')['id']
    turn, _ = store.submit('workspace', session, 'send', 0, 'Write a flow', identity, 'policy')
    store.cancel('workspace', session, turn['id'])
    store.finish_provider(turn['id'], state='completed', cleanup='complete',
                          result={'pipeline_checks': {'native_flow': {'status': 'passed'}}})
    saved = store.turn(turn['id'])
    assert saved['state'] == 'cancelled' and saved['cleanup'] == 'complete'
    assert 'pipeline_checks' not in json.loads(saved['result'])
