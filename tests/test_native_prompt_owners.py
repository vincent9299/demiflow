"""Model predicates remain semantic owners across local barrier snapshots."""
import json

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.storage.state_artifacts import StateArtifacts
from demiflow.operator_llm import parse_prompt_pack
from image_http_fixture import fixture_http, json_definition


class CountingPredicate:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        self.count += 1
        return self.count % 2 == 1

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, value):
        self.count = value['count']


def test_stateful_model_predicate_is_locally_aligned_and_snapshotted(tmp_path, fixture_http, monkeypatch):
    monkeypatch.setenv('SYNTHETIC_JSON_KEY', 'fixture-only')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000,
                                                'max_row_bytes': 65536, 'max_batch_bytes': 262144}) as run:
        rows = data.from_items([{'instruction': str(i)} for i in range(6)]).map_prompt_async(
            'answer', config=parse_prompt_pack(json.dumps(json_definition(fixture_http['base']))),
            inputs=['instruction'], output='answer', when=CountingPredicate(), max_requests=3,
            concurrency=2, options={'trust_env': False, 'max_request_bytes': 8192,
                                    'max_response_bytes': 8192, 'max_event_bytes': 8192}
        ).materialize().take_all()
        facts = run.journal.execution_commits('action-1')
        assert all('node-0-inflight' not in fact['prepared']['state'] for fact in facts)
        assert [StateArtifacts(run).read(fact['prepared']['state']['node-0-native-owner-0'])['count']
                for fact in facts] == [fact['intent']['position']['next_ordinal'] for fact in facts]
    assert len(rows) == 6 and len(fixture_http['requests']) == 3


async def pass_row(row):
    return row


def test_exact_source_threshold_uses_one_final_cut(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}):
        rows = data.from_items([{'id': 0}, {'id': 1}]).map_async(pass_row).materialize().take_all()
    assert rows == [{'id': 0}, {'id': 1}]
    assert len(RunJournal(tmp_path/'run').execution_commits('action-1')) == 1


def mutate_input(row):
    row['instruction'] += '!'
    return True


def test_native_pending_input_precedes_mutating_predicate(tmp_path, fixture_http, monkeypatch):
    import asyncio
    import os
    import threading
    import pytest
    import demiflow.execution.commit as protocol
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
        if name == 'after_acked' and fact['sequence'] == 1 and fact['intent']['sinks'] == ['output']:
            assert reached.wait(5)
            raise RuntimeError('known mutated input cut')
    monkeypatch.setattr(protocol, 'fault_point', fail)
    with pytest.raises(RuntimeError, match='known mutated input cut'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000,
                                                    'max_row_bytes': 65536, 'max_batch_bytes': 262144}) as run:
            reference = run.ref
            graph().materialize()
    monkeypatch.delenv('NATIVE_MUTATING_DELAY')
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference):
        rows = graph().materialize().take_all()
    assert sorted(row['instruction'] for row in rows) == sorted(str(i)+'!' for i in range(12))
    assert len(fixture_http['requests']) == 12
