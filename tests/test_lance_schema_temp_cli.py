"""Tests for Lance schema mismatch reporting, explicit temperature, and configurable CLI mode."""
from __future__ import annotations

import base64
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import stat
import sys
import threading

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.outputs.batch_conversion import table_from_pylist
from demiflow.operator_llm.client import (
    AsyncOperatorLLMClient,
    OpenAICompatibleOperatorLLMClient,
    create_async_operator_llm_client,
)
from demiflow.operator_llm.model import OperatorLLMRequest, PromptModel
from demiflow.operator_llm.parser import parse_prompt_pack


_PNG_1X1 = base64.b64decode(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII='
)


def _prompt_yaml(*, base_url: str = 'https://example.invalid/v1', with_image: bool = False) -> str:
    img_part = '\n      Image: {{ img | image }}' if with_image else ''
    return f"""schema_version: demiflow_prompt_pack_v2
prompts:
  sum_prompt:
    version: sum-v1
    model:
      name: test-model
      transport: openai_compatible
      base_url: {base_url}
      api_key_env: TEST_API_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [answer]
      properties:
        answer: {{type: string}}
    template: |
      Solve: {{{{ text }}}}{img_part}
"""


# ---------------------------------------------------------------------------
# 1. Lance schema error diagnostics (column/type/row, never INTERNAL_PROTOCOL)
# ---------------------------------------------------------------------------

def test_table_from_pylist_reports_column_type_row_without_leaking_values():
    schema = pa.schema([('id', pa.string()), ('author_error', pa.string()), ('count', pa.int64())])
    secret_payload = {'category': 'http_error', 'detail': 'sk-super-secret-token-value-1234567890'}
    with pytest.raises(ValueError) as exc_info:
        table_from_pylist(
            [
                {'id': 'a', 'author_error': None, 'count': 1},
                {'id': 'b', 'author_error': secret_payload, 'count': 2},
            ],
            schema,
        )
    message = str(exc_info.value)
    assert "column 'author_error'" in message
    assert 'expected string' in message
    assert 'got dict' in message
    assert 'at row 1' in message
    assert 'sk-super-secret-token-value' not in message


def test_stream_lance_schema_mismatch_preserves_value_error_not_internal_protocol(tmp_path):
    schema = pa.schema([('case_id', pa.string()), ('author_error', pa.string())])
    target = str(tmp_path / 'outputs__author.lance')
    ds = data.from_items([{'case_id': 'c1', 'author_error': {'category': 'http_error', 'detail': '400'}}])
    ds = ds.save_lance(target, schema=schema, key='case_id', stage='author', mode='append', max_batch=4, max_rows=10)
    with pytest.raises(ValueError) as exc_info:
        ds.take_all()
    assert not isinstance(exc_info.value, DataflowError)
    message = str(exc_info.value)
    assert 'INTERNAL_PROTOCOL' not in message
    assert "column 'author_error'" in message
    assert 'expected string' in message
    assert 'got dict' in message


def test_write_lance_schema_mismatch_reports_column(tmp_path):
    schema = pa.schema([('case_id', pa.string()), ('author_error', pa.string())])
    target = str(tmp_path / 'delivery.lance')
    ds = data.from_items([{'case_id': 'c1', 'author_error': {'bad': 'dict'}}])
    with pytest.raises((TypeError, ValueError)) as exc_info:
        ds.write_lance(target, mode='create', schema=schema)
    message = str(exc_info.value)
    assert 'INTERNAL_PROTOCOL' not in message
    assert "column 'author_error'" in message
    assert 'expected string' in message


# ---------------------------------------------------------------------------
# 2. Explicit temperature (Option A: no hidden default injection)
# ---------------------------------------------------------------------------

def test_http_clients_omit_temperature_by_default(monkeypatch):
    monkeypatch.setenv('TEST_API_KEY', 'test-key')
    req = OperatorLLMRequest(
        prompt_name='p', prompt_version='v1', model='gpt-6.1-sol',
        parts=({'type': 'text', 'text': 'hi'},),
        response_schema={'type': 'object', 'required': ['answer'], 'properties': {'answer': {'type': 'string'}}},
    )

    for transport, extra in (
        ('openai_compatible', {'base_url': 'https://api.example.com/v1', 'api_key_env': 'TEST_API_KEY'}),
        ('azure_openai', {'base_url': 'https://azure.example.com', 'api_key_env': 'TEST_API_KEY', 'api_version': '2024-06-01'}),
        ('systemone', {'base_url': 'https://sys.example.com', 'api_key_env': ''}),
    ):
        model = PromptModel('gpt-6.1-sol', transport, **extra)
        client = AsyncOperatorLLMClient(model, options={})
        payload = client.record(req)['payload']
        assert 'temperature' not in payload

        client_zero = AsyncOperatorLLMClient(model, options={'request_options': {'temperature': 0}})
        assert client_zero.record(req)['payload']['temperature'] == 0

        client_none = AsyncOperatorLLMClient(model, options={'request_options': {'temperature': None}})
        assert 'temperature' not in client_none.record(req)['payload']

    # Sync OpenAI-compatible client omits temperature by default and passes it when explicit
    captured = []
    class _FakeResp:
        status_code = 200
        url = 'https://api.example.com/v1/chat/completions'
        def raise_for_status(self): pass
        def json(self): return {'choices': [{'message': {'content': '{"answer": "ok"}'}}]}

    import requests
    def fake_post(url, *, headers, json, timeout):
        captured.append(json)
        return _FakeResp()
    monkeypatch.setattr(requests, 'post', fake_post)
    model = PromptModel('gpt-6.1-sol', 'openai_compatible', base_url='https://api.example.com/v1', api_key_env='TEST_API_KEY')
    OpenAICompatibleOperatorLLMClient(model).execute(req)
    assert 'temperature' not in captured[-1]
    OpenAICompatibleOperatorLLMClient(model, temperature=0.25).execute(req)
    assert captured[-1]['temperature'] == 0.25


def test_map_prompt_async_explicit_temperature_and_replay_key_separation(monkeypatch, tmp_path):
    seen_payloads = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            seen_payloads.append(body)
            reply = json.dumps({'choices': [{'message': {'content': json.dumps({'answer': 'ok'})}}]}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)
        def log_message(self, *args): pass

    server = HTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv('TEST_API_KEY', 'test-key')
        cfg = tmp_path / 'prompt.yaml'
        cfg.write_text(_prompt_yaml(base_url=f'http://127.0.0.1:{server.server_port}/v1'))
        journal_path = str(tmp_path / 'calls.sqlite')

        # 1) Default (temperature omitted) -> no 'temperature' in HTTP payload
        rows = data.from_items([{'text': '1+1'}]).map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            options={'sqlite_journal': {'path': journal_path}}, max_requests=4,
        ).take_all()
        assert rows[0]['out'] == 'ok'
        assert len(seen_payloads) == 1
        assert 'temperature' not in seen_payloads[0]

        # 2) Explicit temperature=0 -> distinct replay key, sends temperature=0
        rows2 = data.from_items([{'text': '1+1'}]).map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            temperature=0,
            options={'sqlite_journal': {'path': journal_path}}, max_requests=4,
        ).take_all()
        assert rows2[0]['out'] == 'ok'
        assert len(seen_payloads) == 2
        assert seen_payloads[1]['temperature'] == 0

        # 3) Repeat with temperature=0 -> replays from journal (0 new HTTP calls)
        rows3 = data.from_items([{'text': '1+1'}]).map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            temperature=0,
            options={'sqlite_journal': {'path': journal_path}}, max_requests=4,
        ).take_all()
        assert rows3[0]['out'] == 'ok'
        assert len(seen_payloads) == 2
    finally:
        server.shutdown()
        server.server_close()


def test_temperature_validation_and_conflicts(tmp_path):
    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml())
    ds = data.from_items([{'text': 'x'}])

    for bad in (True, -0.1, float('nan'), float('inf'), '0'):
        with pytest.raises(ValueError, match='temperature'):
            ds.map_prompt_async('sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out', temperature=bad)

    with pytest.raises(ValueError, match='Conflicting temperature'):
        ds.map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            temperature=0, options={'request_options': {'temperature': 1}},
        )

    with pytest.raises(ValueError, match='only supported for HTTP'):
        ds.map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            temperature=0, options={'cli': {'command': [sys.executable, '-c', 'pass']}},
        )


def test_managed_prompt_contract_accepts_explicit_temperature():
    from demiflow.data.flow_contract import field, scalar
    from demiflow.operator_llm.flow_contract import analyze_prompt, normalize_prompt

    raw_args = {
        'prompt': 'sum_prompt', 'config': 'prompt.yaml',
        'inputs': {'text': 'text'}, 'output': 'out', 'outputs': None,
        'concurrency': 1, 'queue_depth': 1, 'catch': (), 'label': None,
        'when': None, 'call_output': None, 'error_output': None,
        'options': {'timeout_s': 30, 'trust_env': False, 'max_request_bytes': 4096, 'max_response_bytes': 2 * 1024 * 1024},
        'temperature': 0.5,
        'max_requests': 2, 'service': None,
        'request_policy': None, 'token_budget': None,
        'routes': None, 'route': None, 'isolate_route_failures': False,
    }
    declared = normalize_prompt(raw_args)
    assert declared['options']['request_options'] == {'temperature': 0.5}
    result = analyze_prompt(
        declared,
        inputs=[{'text': field(scalar('str'))}],
        callables=[],
        resources={'config': {'text': _prompt_yaml()}},
    )
    assert result['native']['options']['request_options'] == {'temperature': 0.5}


# ---------------------------------------------------------------------------
# 3. Configurable CLI mode (command templates, adapters, replay, errors)
# ---------------------------------------------------------------------------

def _write_script(path: Path, body: str) -> str:
    path.write_text('#!' + sys.executable + '\n' + body, encoding='utf-8')
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def test_cli_command_stdin_stdout_fenced_json_and_replay(tmp_path):
    from demiflow.execution import RunContext
    counter = tmp_path / 'count.txt'
    script = _write_script(
        tmp_path / 'fake_cli.py',
        f"""import json, pathlib, sys
prompt = sys.stdin.read()
p = pathlib.Path({str(counter)!r})
n = int(p.read_text()) if p.exists() else 0
p.write_text(str(n + 1))
assert 'Solve: hello' in prompt
print('```json')
print(json.dumps({{'answer': f'seen:{{n + 1}}'}}))
print('```')
""",
    )
    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml())
    opts = {
        'cli': {'command': [script], 'input': 'stdin', 'output': 'stdout'},
        'timeout_s': 30,
    }

    def pipeline():
        return data.from_items([{'text': 'hello'}]).map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            call_output='call', error_output='err', options=opts, max_requests=4,
        )

    class _Interrupt(Exception):
        pass

    limits = {'memory_bytes': 2 * 1024**3, 'max_state_bytes': 16 * 1024**2}
    with pytest.raises(_Interrupt):
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=4):
            ref = run.ref
            rows1 = pipeline().take_all()
            assert rows1[0].get('err') is None
            assert rows1[0]['out'] == 'seen:1'
            assert rows1[0]['call']['transport'] == 'cli'
            assert rows1[0]['call']['reused'] is False
            assert counter.read_text() == '1'
            raise _Interrupt()

    # Second run replays from native RunContext journal without launching CLI again
    with RunContext.resume(ref) as run, data.local_execution(workers=4):
        rows2 = pipeline().take_all()
        assert rows2 == rows1
        assert counter.read_text() == '1'
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1


def test_cli_command_file_placeholders_images_and_artifacts(tmp_path):
    artifact_root = tmp_path / 'objects'
    script = _write_script(
        tmp_path / 'file_cli.py',
        """import argparse, json, pathlib
ap = argparse.ArgumentParser()
ap.add_argument('--model', required=True)
ap.add_argument('--prompt', required=True)
ap.add_argument('--messages', required=True)
ap.add_argument('--schema', required=True)
ap.add_argument('--out', required=True)
ap.add_argument('--images', required=True)
ap.add_argument('--artifacts', required=True)
a = ap.parse_args()
assert a.model == 'test-model'
assert 'Solve: img-case' in pathlib.Path(a.prompt).read_text()
msgs = json.loads(pathlib.Path(a.messages).read_text())
assert any(
    part.get('type') == 'image_file' and pathlib.Path(part['path']).exists()
    for m in msgs for part in m['content']
)
assert len(list(pathlib.Path(a.images).iterdir())) == 1
assert json.loads(pathlib.Path(a.schema).read_text())['required'] == ['answer']
(pathlib.Path(a.artifacts) / 'report.txt').write_text('artifact-body')
pathlib.Path(a.out).write_text(json.dumps({'answer': 'with-artifact'}))
""",
    )
    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml(with_image=True))
    opts = {
        'cli': {
            'command': [
                script,
                '--model', '{model}',
                '--prompt', '{prompt_file}',
                '--messages', '{messages_file}',
                '--schema', '{schema_file}',
                '--out', '{output_file}',
                '--images', '{images_dir}',
                '--artifacts', '{artifacts_dir}',
            ],
            'input': 'file',
            'output': 'file',
            'artifact_store': {'directory': str(artifact_root)},
        },
        'timeout_s': 30,
    }
    rows = data.from_items([{'text': 'img-case', 'img': _PNG_1X1}]).map_prompt_async(
        'sum_prompt', config=str(cfg), inputs={'text': 'text', 'img': 'img'},
        output='out', call_output='call', error_output='err', options=opts, max_requests=2,
    ).take_all()
    assert rows[0].get('err') is None
    assert rows[0]['out'] == 'with-artifact'
    from demiflow.objects import ObjectRef
    artifacts = rows[0]['call']['artifacts']
    assert len(artifacts) == 1
    assert artifacts[0]['name'] == 'report.txt'
    assert ObjectRef(**artifacts[0]['object_ref']).read() == b'artifact-body'


def test_cli_nonzero_exit_and_timeout_captured_in_error_output(tmp_path):
    fail_script = _write_script(tmp_path / 'fail_cli.py', 'import sys; sys.stderr.write("boom\\n"); sys.exit(2)\n')
    slow_script = _write_script(tmp_path / 'slow_cli.py', 'import time; time.sleep(30)\n')
    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml())

    rows_fail = data.from_items([{'text': 'x'}]).map_prompt_async(
        'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
        call_output='call', error_output='err',
        options={'cli': {'command': [fail_script]}, 'timeout_s': 10}, max_requests=2,
    ).take_all()
    assert rows_fail[0]['err'] is not None
    assert rows_fail[0]['err']['call']['exit_code'] == 2
    assert rows_fail[0]['err']['call']['execution_status'] == 'completed'

    rows_timeout = data.from_items([{'text': 'x'}]).map_prompt_async(
        'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
        call_output='call', error_output='err',
        options={'cli': {'command': [slow_script]}, 'timeout_s': 0.3}, max_requests=2,
    ).take_all()
    assert rows_timeout[0]['err'] is not None
    assert rows_timeout[0]['err']['call']['execution_status'] == 'timed_out'


def test_cli_command_dsh_legacy_codex_and_validation(tmp_path):
    # 1) dsh-style CLI works directly through the generic command template
    fake_dsh = _write_script(
        tmp_path / 'fake_dsh.py',
        """import json, sys
assert sys.argv[1:] == ['--profile', 'headless', '--extra']
text = sys.stdin.read()
assert 'Solve: dsh-test' in text
print(json.dumps({'answer': 'from-dsh'}))
""",
    )
    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml())
    rows = data.from_items([{'text': 'dsh-test'}]).map_prompt_async(
        'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
        call_output='call', error_output='err',
        options={'cli': {'command': [fake_dsh, '--profile', 'headless', '--extra']}, 'timeout_s': 15},
        max_requests=2,
    ).take_all()
    assert rows[0].get('err') is None
    assert rows[0]['out'] == 'from-dsh'
    assert rows[0]['call']['transport'] == 'cli'

    # 2) Legacy codex_exec remains backward compatible
    fake_codex = _write_script(
        tmp_path / 'fake_codex.py',
        """import json, pathlib, sys
assert '--custom-flag' in sys.argv
out = sys.argv[sys.argv.index('--output-last-message') + 1]
pathlib.Path(out).write_text(json.dumps({'answer': 'from-codex-legacy'}))
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 3, 'output_tokens': 2}}))
""",
    )
    rows_codex = data.from_items([{'text': 'codex-test'}]).map_prompt_async(
        'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
        call_output='call', error_output='err',
        options={'codex_exec': {'bin': fake_codex, 'extra_args': ['--custom-flag']}, 'timeout_s': 15},
        max_requests=2,
    ).take_all()
    assert rows_codex[0].get('err') is None
    assert rows_codex[0]['out'] == 'from-codex-legacy'
    assert rows_codex[0]['call']['transport'] == 'codex_exec'

    # 3) Eager validation rejects malformed or non-command cli options (no adapter presets)
    ds = data.from_items([{'text': 'x'}])
    for bad_opts in (
        {'cli': {}},
        {'cli': {'adapter': 'dsh'}},
        {'cli': {'adapter': 'codex', 'command': ['codex']}},
        {'cli': {'command': []}},
        {'cli': {'command': [sys.executable, '{unknown_placeholder}']}},
        {'cli': {'command': [sys.executable, '{artifacts_dir}']}},
        {'cli': {'command': [sys.executable, '-c', 'pass'], 'output': 'file'}},
        {'cli': {'command': [sys.executable]}, 'codex_exec': {}},
        {'cli': {'command': [sys.executable], 'env_passthrough': ['bad-name']}},
    ):
        with pytest.raises(ValueError):
            ds.map_prompt_async('sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out', options=bad_opts)


def test_cli_managed_native_resources_and_env_passthrough(tmp_path):
    from demiflow.execution.pipeline.managed_native import capture_managed_pipeline

    cfg = tmp_path / 'prompt.yaml'
    cfg.write_text(_prompt_yaml())
    art_dir = str((tmp_path / 'cli_objects').resolve())
    target = str((tmp_path / 'out.lance').resolve())
    schema = pa.schema([('text', pa.string()), ('out', pa.string())])

    def entry():
        return data.from_items([{'text': 'hello'}]).map_prompt_async(
            'sum_prompt', config=str(cfg), inputs={'text': 'text'}, output='out',
            options={
                'cli': {
                    'command': [sys.executable, '-c', 'print("{}")'],
                    'env_passthrough': ['CUSTOM_CLI_TOKEN'],
                    'artifact_store': {'directory': art_dir},
                },
                'timeout_s': 30,
            },
            max_requests=2,
        ).write_lance(target, mode='create', schema=schema)

    captured = capture_managed_pipeline(
        entry, {},
        limits={'batch_rows': 16, 'memory_bytes': 2 * 1024**3, 'max_state_bytes': 16 * 1024**2},
    )
    contract = captured.submission
    assert 'TEST_API_KEY' not in contract['credential_names']
    assert 'CUSTOM_CLI_TOKEN' in contract['credential_names']
    assert art_dir in contract['object_roots']
