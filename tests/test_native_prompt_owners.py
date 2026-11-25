"""Model predicates remain semantic owners across local barrier snapshots."""
import json

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.storage.state_artifacts import StateArtifacts
from demiflow.operator_llm import parse_prompt_pack
from image_http_fixture import fixture_http, json_definition


class FirstThree:
    """True for the first three rows it sees; the count is the state a resume must restore."""

    def __init__(self):
        self.count = 0

    def __call__(self, row):
        self.count += 1
        return self.count <= 3

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, value):
        self.count = value['count']


def _unanswered(run_dir):
    import sqlite3
    with sqlite3.connect(run_dir/'run.sqlite') as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='calls'").fetchone():
            return 0
        return db.execute('SELECT count(*) FROM calls WHERE response_json IS NULL').fetchone()[0]


def test_stateful_model_predicate_resumes_from_its_snapshot(tmp_path, fixture_http, monkeypatch):
    import time
    import pytest
    import demiflow.execution.checkpoint.commit as protocol
    monkeypatch.setenv('SYNTHETIC_JSON_KEY', 'fixture-only')
    config = parse_prompt_pack(json.dumps(json_definition(fixture_http['base'])))
    limits = {'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000,
              'max_row_bytes': 65536, 'max_batch_bytes': 262144}

    def graph():
        return data.from_items([{'instruction': str(i)} for i in range(6)]).map_prompt_async(
            'answer', config=config, inputs=['instruction'], output='answer', when=FirstThree(),
            max_requests=6, concurrency=1, options={'trust_env': False, 'max_request_bytes': 8192,
                                                    'max_response_bytes': 8192, 'max_event_bytes': 8192})
    interrupted = []

    def interrupt(name, commit_id):
        fact = RunJournal(tmp_path/'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['sequence'] == 1:
            # Model calls continue while a cut commits. Interrupt once none is unanswered,
            # so the resume tests the predicate's state rather than an uncertain call.
            deadline = time.monotonic() + 10
            while _unanswered(tmp_path/'run') and time.monotonic() < deadline:
                time.sleep(.01)
            interrupted.append(fact)
            raise RuntimeError('interrupted after the first cut')
    monkeypatch.setattr(protocol, 'fault_point', interrupt)
    with pytest.raises(RuntimeError, match='interrupted after the first cut'):
        with RunContext.start(tmp_path/'run', limits=limits) as run:
            reference = run.ref
            graph().materialize()
    assert interrupted and 0 < len(fixture_http['requests']) < 3
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference):
        rows = graph().materialize().take_all()
    assert sorted(row['instruction'] for row in rows if 'answer' in row) == ['0', '1', '2']
    assert len(rows) == 6 and len(fixture_http['requests']) == 3


def mutate_input(row):
    row['instruction'] += '!'
    return True


def test_native_pending_input_precedes_mutating_predicate(tmp_path, fixture_http, monkeypatch):
    import asyncio
    import os
    import threading
    import pytest
    import demiflow.execution.checkpoint.commit as protocol
    from demiflow.operator_llm.runtime import PromptActor
    reached = threading.Event()
    original = PromptActor.__call__
    async def delayed(actor, row):
        result = await original(actor, row)
        if os.getenv('NATIVE_MUTATING_DELAY') == '1':
            if row['id'] == 7:
                reached.set()
            if row['id'] == 0:
                for _ in range(500):
                    if reached.is_set():
                        break
                    await asyncio.sleep(.01)
                else:
                    raise RuntimeError('native free worker stopped at marker')
        return result
    monkeypatch.setattr(PromptActor, '__call__', delayed)
    monkeypatch.setenv('SYNTHETIC_JSON_KEY', 'fixture-only')
    monkeypatch.setenv('NATIVE_MUTATING_DELAY', '1')
    config = parse_prompt_pack(json.dumps(json_definition(fixture_http['base'])))
    def graph():
        return data.from_items([{'id': i, 'instruction': str(i)} for i in range(12)]).map_prompt_async(
            'answer', config=config, inputs=['instruction'], output='answer', when=mutate_input,
            max_requests=12, concurrency=2, options={'trust_env': False, 'max_request_bytes': 8192,
                                                    'max_response_bytes': 8192, 'max_event_bytes': 8192})
    def fail(name, commit_id):
        fact = RunJournal(tmp_path/'run').execution_commit(commit_id)
        if name == 'after_acked' and fact['sequence'] == 1 and fact['intent']['sinks'] == ['materialize']:
            assert reached.wait(5)
            raise RuntimeError('known mutated input cut')
    monkeypatch.setattr(protocol, 'fault_point', fail)
    with pytest.raises(RuntimeError, match='known mutated input cut'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000,
                                                    'max_row_bytes': 65536, 'max_batch_bytes': 262144}) as run:
            reference = run.ref
            # Thread workers run the patched actor in this process.
            with data.local_execution(worker_mode='thread'):
                graph().materialize()
    monkeypatch.delenv('NATIVE_MUTATING_DELAY')
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference), data.local_execution(worker_mode='thread'):
        rows = graph().materialize().take_all()
    assert sorted(row['instruction'] for row in rows) == sorted(str(i)+'!' for i in range(12))
    assert len(fixture_http['requests']) == 12
