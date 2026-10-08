"""Exercise the real CLI subprocess boundary with a local, non-model executable."""
import asyncio
import json
import os
from pathlib import Path
import sys

import pytest

from demiflow import data
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from demiflow.objects import ObjectRef
from demiflow.operator_llm.codex_exec import CodexExecPromptClient
from demiflow.operator_llm.model import OperatorLLMRequest, TextPart
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.template import render_template

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  author:
    version: v1
    model: {name: fixture-model, transport: openai_compatible, base_url: 'http://localhost:1/v1', api_key_env: UNUSED_CODEX_TEST_KEY}
    template: "{{ task }}\\n{{ images | numbered_image }}"
    response_schema:
      type: object
      required: [result]
      additionalProperties: false
      properties: {result: {type: integer}}
'''


@pytest.fixture
def fake_cli(tmp_path, monkeypatch):
    path = tmp_path / 'fake-codex'
    path.write_text('#!' + sys.executable + '\n' + r'''
import json, os, pathlib, sys, time
args = sys.argv[1:]
payload = sys.stdin.read()
capture = pathlib.Path(os.environ['FAKE_CODEX_CAPTURE'])
with capture.open('a') as stream:
    stream.write(json.dumps({'args': args, 'cwd': os.getcwd(), 'prompt': payload,
        'schema': json.loads(pathlib.Path(args[args.index('--output-schema') + 1]).read_text()),
        'images': [pathlib.Path(args[i + 1]).read_bytes().hex() for i, a in enumerate(args) if a == '--image'],
        'pid': os.getpid()}) + '\n')
artifact_kind = os.environ.get('FAKE_CODEX_ARTIFACT', '')
if 'CODEX_EXEC_FILE_CONTEXT\n' in payload:
    context = json.loads(payload.rsplit('CODEX_EXEC_FILE_CONTEXT\n', 1)[1])
    directory = pathlib.Path(context['artifact_directory'])
    target = directory / 'source.bin'
    if artifact_kind in ('file', 'large'):
        target.write_bytes(b'generated file bytes' if artifact_kind == 'file' else b'x' * 32)
    elif artifact_kind == 'many':
        target.write_bytes(b'one')
        (directory / 'second.bin').write_bytes(b'two')
    elif artifact_kind in ('symlink', 'hardlink'):
        other = directory.parent / 'outside.bin'
        other.write_bytes(b'outside export scope')
        target.symlink_to(other) if artifact_kind == 'symlink' else os.link(other, target)
    elif artifact_kind == 'directory_symlink':
        other = directory.parent / 'outside'
        other.mkdir()
        (other / 'secret.bin').write_bytes(b'outside export scope')
        directory.rmdir()
        directory.symlink_to(other, target_is_directory=True)
    elif artifact_kind == 'fifo':
        os.mkfifo(target)
    elif artifact_kind == 'nested':
        (directory / 'nested').mkdir()
behavior = os.environ.get('FAKE_CODEX_BEHAVIOR', 'ok')
if behavior == 'sleep':
    print(json.dumps({'type': 'thread.started'}), flush=True)
    time.sleep(20)
output = pathlib.Path(args[args.index('--output-last-message') + 1])
if behavior != 'missing':
    output.write_text('bad-json' if behavior == 'bad-json' else os.environ.get('FAKE_CODEX_CONTENT', '{"result":7}'))
if behavior != 'incomplete':
    print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 12, 'output_tokens': 3}}))
if behavior == 'failed':
    print(json.dumps({'type': 'turn.failed', 'error': {'message': 'fixture failure'}}))
    print('fixture error', file=sys.stderr)
    sys.exit(2)
''')
    path.chmod(0o755)
    capture = tmp_path / 'capture.jsonl'
    monkeypatch.setenv('FAKE_CODEX_CAPTURE', str(capture))
    return path, capture


def evaluate(tmp_path, cli, *, effort='low', model='fixture-model', limit=1, task='One task', images=(),
             web_search='cached', codex_settings=None, timeout_s=2):
    pack = parse_prompt_pack(PACK.replace('fixture-model', model))
    options = {'codex_exec': {'bin': str(cli), 'reasoning_effort': effort, 'web_search': web_search},
               'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}, 'timeout_s': timeout_s}
    options['codex_exec'].update(codex_settings or {})
    return (data.from_items([{'task': task, 'images': list(images)}])
            .map_prompt_async('author', config=pack, options=options, max_requests=limit,
                              inputs=['task', 'images'], output='answer', call_output='call', error_output='error')
            .materialize().take_all()[0])


def test_cli_roundtrip_schema_images_and_cache(tmp_path, fake_cli):
    cli, capture = fake_cli
    images = ['data:image/png;base64,aGVsbG8=', 'data:image/png;base64,d29ybGQ=']
    row = evaluate(tmp_path, cli, images=images)
    assert row['answer'] == 7
    assert row['call']['transport'] == 'codex_exec'
    assert row['call']['usage']['input_tokens'] == 12
    event = json.loads(capture.read_text())
    assert event['images'] == [b'hello'.hex(), b'world'.hex()]
    assert event['prompt'].index('Image 1:') < event['prompt'].index('Image 2:')
    assert '--ephemeral' in event['args'] and '--ignore-user-config' in event['args']
    assert event['args'][event['args'].index('--sandbox') + 1] == 'read-only'
    assert 'project_doc_max_bytes=0' in event['args']
    assert event['cwd'] != str(tmp_path) and not Path(event['cwd']).exists()
    assert event['schema']['properties']['result']['type'] == 'integer'
    assert 'One task' in event['prompt']
    again = evaluate(tmp_path, cli, images=images, limit=0)
    assert again['answer'] == 7 and again['call']['reused']
    assert len(capture.read_text().splitlines()) == 1
    # Effort and model are part of the durable identity, so neither can reuse the old call.
    assert evaluate(tmp_path, cli, effort='high')['answer'] == 7
    assert evaluate(tmp_path, cli, model='second-model')['answer'] == 7
    assert len(capture.read_text().splitlines()) == 3
    skipped = evaluate(tmp_path, cli, task='another task', limit=0)
    assert skipped['error']['type'] == 'PromptBudgetExceededError'
    assert len(capture.read_text().splitlines()) == 3


def test_web_search_modes_reach_cli_and_do_not_share_cached_answers(tmp_path, fake_cli):
    """检索能力参与请求身份；相同模式可复用，切换模式必须执行新的 CLI。"""
    cli, capture = fake_cli
    for mode in ('cached', 'live', 'disabled'):
        row = evaluate(tmp_path, cli, web_search=mode)
        assert row['answer'] == 7 and row['call']['web_search'] == mode
        event = json.loads(capture.read_text().splitlines()[-1])
        assert 'web_search=' + json.dumps(mode) in event['args']
        again = evaluate(tmp_path, cli, web_search=mode, limit=0)
        assert again['answer'] == 7 and again['call']['reused']
    assert len(capture.read_text().splitlines()) == 3


@pytest.mark.parametrize('behavior', ['failed', 'missing', 'incomplete', 'bad-json'])
def test_cli_failures_remain_technical_and_raw_logs_are_retained(tmp_path, fake_cli, monkeypatch, behavior):
    cli, _ = fake_cli
    monkeypatch.setenv('FAKE_CODEX_BEHAVIOR', behavior)
    row = evaluate(tmp_path, cli)
    assert 'answer' not in row and row['error']['type'].startswith('PromptResponse')
    records = journal_rows(tmp_path, 'response')
    assert len(records) == 1
    saved = next(iter(records.values()))
    assert 'stdout' in saved and 'stderr' in saved and 'content' in saved
    if behavior == 'failed':
        assert saved['exit_code'] == 2 and 'fixture error' in saved['stderr']


@pytest.mark.parametrize('cancel', [False, True])
def test_timeout_and_cancellation_reap_process_and_keep_logs(tmp_path, fake_cli, monkeypatch, cancel):
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_BEHAVIOR', 'sleep')
    definition = parse_prompt_pack(PACK).prompt_definitions['author']
    request = OperatorLLMRequest('author', 'v1', 'fixture-model', (TextPart('test'),),
                                 response_schema=definition.response_schema)
    async def run():
        client = CodexExecPromptClient(definition.model, {
            'codex_exec': {'bin': str(cli)}, 'timeout_s': 0.3,
            'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}})
        task = asyncio.create_task(client.execute(request))
        if cancel:
            while not capture.exists():
                await asyncio.sleep(0.01)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else Exception):
            await task
        assert not client.processes
        await client.aclose()
    asyncio.run(run())
    pid = json.loads(capture.read_text())['pid']
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    saved = next(iter(journal_rows(tmp_path, 'response').values()))
    assert saved['status'] == ('cancelled' if cancel else 'timed_out')


def exports(tmp_path, **kwargs):
    return {'image_generation': True, 'artifact_store': {'directory': str(tmp_path / 'objects')}, **kwargs}


def test_artifacts_are_durable_and_reused_after_temporary_directory_removed(tmp_path, fake_cli, monkeypatch):
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', 'file')
    cfg = exports(tmp_path)
    images = ['data:image/jpeg;base64,aGVsbG8=']
    first = evaluate(tmp_path, cli, images=images, codex_settings=cfg)
    assert first['answer'] == 7
    call = first['call']
    assert call['image_generation'] is True
    artifact = call['artifacts'][0]
    assert artifact['name'] == 'source.bin'
    assert ObjectRef(**artifact['object_ref']).read() == b'generated file bytes'
    executed = json.loads(capture.read_text())
    assert not Path(executed['cwd']).exists()
    assert executed['args'][executed['args'].index('--sandbox') + 1] == 'workspace-write'
    assert 'features.image_generation=true' in executed['args']
    assert 'sandbox_workspace_write.exclude_slash_tmp=true' in executed['args']
    context = json.loads(executed['prompt'].rsplit('CODEX_EXEC_FILE_CONTEXT\n', 1)[1])
    assert context['input_images'] == [{'image_number': 1, 'mime': 'image/jpeg',
                                       'path': executed['cwd'] + '/image-1.jpg'}]
    assert context['artifact_directory'] == executed['cwd'] + '/artifacts'
    again = evaluate(tmp_path, cli, images=images, codex_settings=cfg, limit=0)
    assert again['call']['artifacts'] == call['artifacts'] and again['call']['reused']
    assert len(capture.read_text().splitlines()) == 1
    # Byte/file limits affect the generated prompt and must affect cache identity.
    changed = evaluate(tmp_path, cli, images=images, codex_settings=exports(tmp_path, max_artifact_files=3), limit=0)
    assert changed['error']['type'] == 'PromptBudgetExceededError'


@pytest.mark.parametrize('kind,limits', [
    ('symlink', {}), ('hardlink', {}), ('directory_symlink', {}), ('fifo', {}), ('nested', {}),
    ('many', {'max_artifact_files': 1}), ('large', {'max_artifact_bytes': 16}),
    ('many', {'max_artifact_bytes': 5}),
])
def test_artifact_boundary_and_limits_fail_without_losing_cli_logs(tmp_path, fake_cli, monkeypatch, kind, limits):
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', kind)
    cfg = exports(tmp_path, **limits)
    first = evaluate(tmp_path, cli, codex_settings=cfg)
    assert first['error']['type'] == 'PromptResponseContractError'
    call = first['error']['call']
    assert call['execution_status'] == 'artifact_failed' and call['artifact_error']
    assert call['artifacts'] == []
    record = next(iter(journal_rows(tmp_path, 'response').values()))
    assert 'turn.completed' in record['stdout'] and record['content'] == '{"result":7}'
    assert not (tmp_path / 'artifacts.lance').exists()
    again = evaluate(tmp_path, cli, codex_settings=cfg, limit=0)
    assert again['error']['call']['reused']
    assert len(capture.read_text().splitlines()) == 1


def test_missing_cached_blob_does_not_trigger_a_new_paid_request(tmp_path, fake_cli, monkeypatch):
    import shutil
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', 'file')
    cfg = exports(tmp_path)
    assert evaluate(tmp_path, cli, codex_settings=cfg)['answer'] == 7
    shutil.rmtree(tmp_path / 'objects')
    again = evaluate(tmp_path, cli, codex_settings=cfg)
    assert again['error']['type'] == 'PromptResponseContractError'
    assert 'Cached Codex artifact' in again['error']['detail']
    assert len(capture.read_text().splitlines()) == 1


def test_timeout_retains_exported_files_but_is_not_a_success(tmp_path, fake_cli, monkeypatch):
    cli, _ = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', 'file')
    monkeypatch.setenv('FAKE_CODEX_BEHAVIOR', 'sleep')
    row = evaluate(tmp_path, cli, codex_settings=exports(tmp_path), timeout_s=0.3)
    call = row['error']['call']
    assert call['execution_status'] == 'timed_out'
    assert ObjectRef(**call['artifacts'][0]['object_ref']).read() == b'generated file bytes'


def test_blob_write_failure_is_journaled_as_delivery_failure(tmp_path, fake_cli, monkeypatch):
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', 'file')
    def fail_put(self, content):
        raise OSError('fixture disk full')
    monkeypatch.setattr('demiflow.operator_llm.codex_exec.LocalObjectStore.put', fail_put)
    row = evaluate(tmp_path, cli, codex_settings=exports(tmp_path))
    assert row['error']['call']['execution_status'] == 'artifact_failed'
    assert 'disk full' in row['error']['call']['artifact_error']
    assert not Path(json.loads(capture.read_text())['cwd']).exists()


def test_explicit_image_disable_is_recorded_and_old_default_unchanged(tmp_path, fake_cli):
    cli, capture = fake_cli
    assert evaluate(tmp_path, cli)['answer'] == 7
    records = journal_rows(tmp_path, 'request')
    execution = next(iter(records.values()))['execution']
    assert execution == {'sandbox': 'read-only', 'ephemeral': True, 'ignore_user_config': True,
                         'project_doc_max_bytes': 0, 'web_search': 'cached'}
    row = evaluate(tmp_path, cli, codex_settings={'image_generation': False})
    assert row['answer'] == 7 and row['call']['image_generation'] is False
    captured = [json.loads(line) for line in capture.read_text().splitlines()]
    assert len(captured) == 2
    assert 'features.image_generation=false' in captured[1]['args']


def test_cancellation_drains_artifact_storage_before_removing_work_directory(tmp_path, fake_cli, monkeypatch):
    import threading
    cli, capture = fake_cli
    monkeypatch.setenv('FAKE_CODEX_ARTIFACT', 'file')
    started, release = threading.Event(), threading.Event()
    original = CodexExecPromptClient._collect_artifacts
    def slow_collect(self, directory, record):
        started.set()
        assert release.wait(5)
        assert directory.exists()
        original(self, directory, record)
    monkeypatch.setattr(CodexExecPromptClient, '_collect_artifacts', slow_collect)
    definition = parse_prompt_pack(PACK).prompt_definitions['author']
    request = OperatorLLMRequest('author', 'v1', 'fixture-model', (TextPart('test'),),
                                 response_schema=definition.response_schema)
    async def run():
        client = CodexExecPromptClient(definition.model, {
            'codex_exec': {'bin': str(cli), **exports(tmp_path)}, 'timeout_s': 2,
            'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}})
        task = asyncio.create_task(client.execute(request))
        try:
            for _ in range(300):
                if started.is_set(): break
                await asyncio.sleep(0.01)
            assert started.is_set()
            work = Path(json.loads(capture.read_text())['cwd'])
            task.cancel()
            await asyncio.sleep(0.03)
            assert not task.done() and work.exists()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not work.exists()
        await client.aclose()
    asyncio.run(run())
    record = next(iter(journal_rows(tmp_path, 'response').values()))
    assert record['status'] == 'cancelled'
    assert ObjectRef(**record['artifacts'][0]['object_ref']).read() == b'generated file bytes'


def journal_rows(root, kind):
    journal = SQLitePromptJournal(root / 'calls.sqlite')
    try:
        return {row['request_id']: row[kind] for row in journal.calls() if row[kind] is not None}
    finally:
        journal.close()
