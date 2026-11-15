"""Public Dataset -> real worker -> native driver journal -> loopback HTTP."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: v1
    model:
      name: local-fixture
      transport: openai_compatible
      base_url_env: DF_V3_NATIVE_TEST_URL
      api_key_env: DF_V3_NATIVE_TEST_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: '{{ payload | json }}'
'''


@pytest.fixture
def native_server(monkeypatch):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(request)
            if '__drop_response__' in json.dumps(request):
                # Provider accepted the request, but no complete response can be
                # confirmed. An attempt must never turn this into a cache miss.
                self.close_connection = True
                return
            response = {'choices': [{'message': {'content': '{"result":"ok"}'}}],
                        'usage': {'prompt_tokens': 3, 'completion_tokens': 1}}
            if self.path.endswith('/embeddings'):
                response = {'data': [{'index': i, 'embedding': [1., 0., 0.]}
                                     for i in range(len(request['input']))], 'usage': {'prompt_tokens': 1}}
            encoded = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('DF_V3_NATIVE_TEST_URL', f'http://127.0.0.1:{server.server_port}/v1')
    monkeypatch.setenv('DF_V3_NATIVE_TEST_KEY', 'local-test')
    try:
        yield requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize('asynchronous', [False, True])
def test_prompt_worker_native_journal_and_replay(tmp_path, native_server, asynchronous):
    def pipeline():
        dataset = data.from_items([{'item': 'a'}, {'item': 'b'}])
        method = dataset.map_prompt_async if asynchronous else dataset.map_prompt
        options = {'concurrency': 1, 'request_policy': {'initial_concurrency': 1, 'max_concurrency': 1}} if asynchronous else {}
        return method('enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
                      output='answer', max_requests=2, **options)
    class Interrupted(Exception):
        pass
    expected = [{'item': 'a', 'answer': 'ok'}, {'item': 'b', 'answer': 'ok'}]
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            assert pipeline().take_all() == expected
            assert len(native_server) == 2
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            with run.journal._connection() as db:
                assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
                assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 2
            raise Interrupted()
    with RunContext.resume(ref), data.local_execution():
        assert pipeline().take_all() == expected
    assert len(native_server) == 2


def test_embedding_worker_native_journal(tmp_path, native_server):
    from demiflow.embeddings import EmbeddingModel
    model = EmbeddingModel('fixture', 'v1', 3, os.environ['DF_V3_NATIVE_TEST_URL'])
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        result = data.from_items([{'text': 'a'}, {'text': 'b'}]).map_embeddings(
            model=model, inputs={'text': 'text'}, batch_size=2, concurrency=1,
            max_requests=1).take_all()
        assert [row['embedding'] for row in result] == [[1., 0., 0.], [1., 0., 0.]]
        assert len(native_server) == 1
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1


def test_prompt_run_budget_is_atomic_before_second_http(tmp_path, native_server):
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(RecoveryContractError, match='unconfirmed'), \
            RunContext.start(tmp_path / 'run', limits={'max_requests': 1}) as run, data.local_execution():
        with pytest.raises(DataflowError):
            data.from_items([{'item': 'a'}, {'item': 'b'}]).map_prompt(
                'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
                output='answer', max_requests=2).take_all()
        assert len(native_server) == 1
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
        assert not run.journal.execution_commits('action-1')


def test_prompt_partial_checkpoint_resume_does_not_resend(tmp_path, native_server, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    class Interrupted(Exception):
        pass
    def pipeline():
        return data.from_items([{'item': str(i)} for i in range(8)]).map_prompt_async(
            'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
            output='answer', concurrency=1, max_requests=8,
            options={'max_response_bytes': 65536, 'max_event_bytes': 65536})
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        result = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().take_all()
    assert 0 < len(native_server) < 8
    with RunContext.resume(ref) as run, data.local_execution():
        assert pipeline().take_all() == [{'item': str(i), 'answer': 'ok'} for i in range(8)]
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 8
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 8
    assert len(native_server) == 8


def test_unknown_model_request_blocks_resume_before_new_http(tmp_path, native_server):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    def pipeline():
        return data.from_items([{'item': 'a'}, {'item': '__drop_response__'}]).map_prompt_async(
            'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
            output='answer', concurrency=1, max_requests=2,
            options={'max_response_bytes': 65536, 'max_event_bytes': 65536})
    with pytest.raises(DataflowError):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 1}) as run, data.local_execution():
            ref = run.ref
            pipeline().take_all()
    assert len(native_server) == 2


    with run.journal._connection() as db:
        row = db.execute('SELECT error_json FROM calls WHERE response_json IS NULL').fetchone()
        assert row is not None
        failure = json.loads(row[0])
        assert failure['type'] == 'RemoteProtocolError'
        assert failure['call'].get('request_sent') is not False
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 2
    with pytest.raises(DataflowError):
        with RunContext.resume(ref), data.local_execution():
            pipeline().take_all()
    assert len(native_server) == 2


def test_shared_union_preserves_native_invocation_multiplicity(tmp_path, native_server):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        shared = data.from_items([{'item': 'a'}])
        result = shared.union(shared).map_prompt('enrich', config=parse_prompt_pack(PACK),
            inputs={'payload': 'item'}, output='answer', max_requests=2).take_all()
        assert result == [{'item': 'a', 'answer': 'ok'}, {'item': 'a', 'answer': 'ok'}]
        assert len(native_server) == 2
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
