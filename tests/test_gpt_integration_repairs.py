"""Public execution and real journal counterexamples for IR-01 through IR-07."""
from pathlib import Path

import pytest
import lance

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.seal.program_identity import plan_identity
from demiflow.execution.seal.business_sources import plan_code_pin
from demiflow.execution.seal.plan_facts import operation_facts
from test_embeddings import server as embedding_server


def _identity(row):
    return row


def test_run_stream_uses_the_sealed_worker_limits(tmp_path):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2}) as run:
        stats = data.from_items([{'id': 1}, {'id': 2}]).map_async(lambda row: row).run_stream()
        physical = load_sealed_plan(run.directory, 'action-1', version='v3')['physical']
        assert physical.policy.checkpoint_rows == 2
        assert stats.emitted == 2
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


@pytest.mark.parametrize('terminal', ['take', 'take_all', 'count', 'iter_rows', 'iter_batches',
                                    'materialize', 'schema', 'size_bytes', 'write_lance'])
def test_public_terminal_consumes_sealed_execution(tmp_path, monkeypatch, terminal):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    calls = []
    original = DataflowExecution.start
    def observed(execution):
        result = original(execution)
        calls.append((execution._action.action_id, execution._physical))
        return result
    monkeypatch.setattr(DataflowExecution, 'start', observed)
    with RunContext.start(tmp_path / 'run') as run:
        ds = data.from_items([{'id': 1}, {'id': 2}]).map(lambda row: row)
        if terminal == 'take':
            assert ds.take(1) == [{'id': 1}]
        elif terminal == 'write_lance':
            ds.write_lance(str(tmp_path / 'out.lance'), mode='create')
            assert lance.dataset(str(tmp_path / 'out.lance')).count_rows() == 2
        elif terminal in {'iter_rows', 'iter_batches'}:
            assert list(getattr(ds, terminal)())
        else:
            assert getattr(ds, terminal)() is not None
        assert len(calls) == 1
        action_id, physical = calls[0]
        assert load_sealed_plan(run.directory, action_id, version='v3')['physical'] == physical


@pytest.mark.parametrize('change', [dict(batch_size=3), dict(call_output='audit'),
                                   dict(error_output='error'), dict(max_requests=1),
                                   dict(flush_interval=1)])
def test_embedding_identity_binds_actual_options(change):
    from demiflow.embeddings import EmbeddingModel
    model = EmbeddingModel('fixture', 'r1', 3, 'http://127.0.0.1:1/v1')
    def build(**extra):
        return data.from_items([{'text': 'x'}]).map_embeddings(model=model,
            inputs={'text': 'text'}, **{'batch_size': 2, **extra})
    a, b = build(), build(**change)
    assert plan_identity('materialize', a._source, a._plan) != plan_identity('materialize', b._source, b._plan)
    assert plan_code_pin(a._plan) != plan_code_pin(b._plan)


def test_embedding_actor_tampering_fails_before_seal():
    from demiflow.embeddings import EmbeddingModel
    from demiflow.data.runtime_contract import RuntimeContractError
    ds = data.from_items([{'text': 'x'}]).map_embeddings(
        model=EmbeddingModel('fixture', 'r1', 3, 'http://127.0.0.1:1/v1'), inputs={'text': 'text'})
    ds._stages[-1].output = 'changed'
    for check in (lambda: operation_facts(ds._plan.operations[0]),
                  lambda: plan_code_pin(ds._plan),
                  lambda: plan_identity('materialize', ds._source, ds._plan)):
        with pytest.raises(RuntimeContractError, match='differs'):
            check()


def test_embedding_batch_workers_share_native_journal(embedding_server, tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2,
            'checkpoint_interval_ms': 60000, 'checkpoint_timeout_s': 5}) as run:
        ds = data.from_items([{'text': str(i)} for i in range(7)]).map_embeddings(
            model=embedding_server['model'], inputs={'text': 'text'}, batch_size=2,
            concurrency=2, max_requests=8)
        result = ds.materialize().take_all()
        assert sorted(row['text'] for row in result) == list(map(str, range(7)))
        assert run._native_prompt_journal.path == run.journal.path


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        self.count += 1
        return dict(row, count=self.count)


def test_plain_instance_auto_state_and_prepend_public_composition(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        ds = data.from_items([{'id': 1}, {'id': 2}]).prepend(
            data.from_items([{'id': 0}]), max_rows=1).map(Counter(), concurrency=1)
        assert ds.materialize().take_all() == [dict(id=i, count=i+1) for i in range(3)]


def test_embedding_known_http_failure_is_replayed_without_request(embedding_server, tmp_path):
    from httpx import HTTPStatusError
    embedding_server['status'] = 500
    def build():
        return data.from_items([{'text': 'x'}]).map_embeddings(
            model=embedding_server['model'], inputs={'text': 'text'}).materialize()
    with pytest.raises(HTTPStatusError, match='HTTP 500'):
        with RunContext.start(tmp_path / 'run') as run:
            ref = run.ref
            build()
    count = len(embedding_server['bodies'])
    assert count == 1
    embedding_server['status'] = 200
    with pytest.raises(HTTPStatusError):
        with RunContext.resume(ref):
            build()
    assert len(embedding_server['bodies']) == count


def test_auto_json_state_restores_with_committed_prepend_frontier(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint import commit
    from demiflow.execution.journal.run_journal import RunJournal
    def build():
        return data.from_items([{'id': i} for i in range(1, 9)]).prepend(
            data.from_items([{'id': 0}]), max_rows=1).map(Counter(), concurrency=1).materialize().take_all()
    def crash(name, commit_id):
        fact = RunJournal(tmp_path / 'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['intent']['sinks'] == ['materialize']:
            raise RuntimeError('injected loss')
    with monkeypatch.context() as m:
        m.setattr(commit, 'fault_point', crash)
        with pytest.raises(RuntimeError, match='injected loss'):
            with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 3,
                    'checkpoint_interval_ms': 60000}) as run:
                ref = run.ref
                build()
    with RunContext.resume(ref):
        rows = build()
    assert rows == [dict(id=i, count=i+1) for i in range(9)]


@pytest.mark.parametrize('window', ['after_prepared', 'after_acked'])
def test_embedding_actual_process_loss_resumes_without_duplicate_requests(embedding_server, tmp_path, window):
    import os
    import subprocess
    import sys
    import json
    script = r'''
import os, sys, json
from pathlib import Path
from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RunRef
import demiflow.execution.checkpoint.commit as commit
root, endpoint, window = sys.argv[1:]
if window != 'resume':
    def fault(name, commit_id):
        fact = RunJournal(root).execution_commit(commit_id)
        if name == window and fact['intent']['sinks'] == ['materialize'] and fact['sequence'] == 2:
            # Model calls keep flowing while a cut commits, so lose the process only once
            # all 8 requests are answered: recovery must then reuse every response.
            import sqlite3, time
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    with sqlite3.connect(Path(root, 'run.sqlite')) as db:
                        total, unanswered = db.execute(
                            'SELECT count(*), coalesce(sum(response_json IS NULL), 0) FROM calls').fetchone()
                except sqlite3.OperationalError:
                    total, unanswered = 0, 0
                if total == 8 and unanswered == 0:
                    os._exit(73)
                time.sleep(0.01)
            os._exit(74)
    commit.fault_point = fault
prior = RunJournal(root).recovery_identity() if Path(root, 'run.sqlite').exists() else None
scope = RunContext.resume(RunRef(root, prior['run_id'])) if prior else RunContext.start(root,
    limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000, 'checkpoint_timeout_s': 5})
with scope:
    rows = data.from_items([{'text':str(i)} for i in range(23)]).map_embeddings(
        model=EmbeddingModel('fixture', 'fixed-revision', 3, endpoint),
        inputs={'text':'text'}, batch_size=3, concurrency=2, max_requests=30).materialize().take_all()
Path(root, 'result.json').write_text(json.dumps(rows))
'''
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(root), str(root/'tests')]),
           'PYTHONDONTWRITEBYTECODE': '1', 'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1'}
    args = [sys.executable, '-B', '-c', script, str(tmp_path/'run'), embedding_server['model'].base_url]
    failed = subprocess.run([*args, window], env=env, capture_output=True, text=True, timeout=60)
    assert failed.returncode == 73, failed.stdout + failed.stderr
    resumed = subprocess.run([*args, 'resume'], env=env, capture_output=True, text=True, timeout=60)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    rows = json.loads((tmp_path/'run'/'result.json').read_text())
    assert sorted(int(row['text']) for row in rows) == list(range(23))
    calls = [int(text) for body in embedding_server['bodies'] for text in body['input']]
    assert sorted(calls) == list(range(23))


def test_materialized_inputs_survive_nested_relational_binding(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        held = data.from_items([{'id': 1, 'x': 2}]).materialize()
        combined = held.union(held).join(data.from_items([{'id': 1, 'y': 3}]), on='id')
        assert combined.take_all() == [{'id': 1, 'x': 2, 'y': 3}] * 2


def test_native_program_stage_swap_is_rejected_before_execution(tmp_path):
    from dataclasses import replace
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.seal.plan import verify_action_programs
    from test_runtime_v3_real_seams import prepared

    ds = data.range(2).map(_identity).map(_identity)
    with prepared(tmp_path, ds) as (execution, services):
        sealed = execution._sealed
        assert len(sealed.programs) >= 2
        swapped = replace(sealed, programs=tuple(reversed(sealed.programs)))
        with pytest.raises(DataflowError, match='membership differs'):
            verify_action_programs(services.action, execution._graph, execution._physical, swapped,
                                   store=services.store, bounds=services.bounds)
        assert not services.action.context.journal.execution_commits(services.action.action_id)


def test_timed_embedding_packed_requests_keep_distinct_membership(embedding_server, tmp_path):
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':4, 'checkpoint_interval_ms':60000}):
        rows = [{'text': str(i) + 'x'*160} for i in range(9)]
        result = data.from_items(rows).map_embeddings(model=embedding_server['model'],
            inputs={'text':'text'}, batch_size=3, concurrency=2, flush_interval=.01,
            options={'batch_request_bytes':300, 'max_request_bytes':1024}).materialize().take_all()
    assert sorted(r['text'] for r in result) == sorted(r['text'] for r in rows)
    assert sorted(t for body in embedding_server['bodies'] for t in body['input']) == sorted(r['text'] for r in rows)
    assert len(embedding_server['bodies']) == 9


def test_bound_pull_rejects_wide_input_before_calling_business_code(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    trace = tmp_path/'called'
    def operation(row):
        Path(trace).write_text('called')  # runs in a worker: record on disk
        return row
    ds = data.from_items([{'wide': 'x'*10000}]).map(operation)
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED: record string exceeds byte budget'):
        with RunContext.start(tmp_path/'run', limits={'max_row_bytes':128, 'max_batch_bytes':256}):
            ds.take(1)
    assert not trace.exists()
