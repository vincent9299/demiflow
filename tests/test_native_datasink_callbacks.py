"""Public Datasink hooks on actual generic native sinks, not KV-specific hooks."""
import json
from pathlib import Path

import pytest

from demiflow import data
from demiflow.data.datasink import WriteResult
from demiflow.execution.run_context import RunContext, current_run
from demiflow.execution.run_journal import RunJournal
from test_execution_recovery_entrypoints import FormalSink


class CallbackSink(FormalSink):
    def __init__(self, events, behavior='normal'):
        self.events, self.behavior = Path(events), behavior
        self.receipt = None

    def recovery_identity(self):
        return {**super().recovery_identity(), 'events':str(self.events), 'behavior':self.behavior}

    def record(self, event):
        with self.events.open('a') as stream:
            stream.write(json.dumps(event)+'\n')

    def on_write_start(self, schema=None):
        self.record(['start'])

    def prepare(self, blocks, context):
        self.record(['prepare'])
        if self.behavior == 'prepare_failure':
            raise ValueError('original native preparation failure')
        return super().prepare(blocks, context)

    def commit(self, prepared, context):
        self.record(['commit'])
        return super().commit(prepared, context)

    def on_write_complete(self, result):
        assert isinstance(result, WriteResult)
        assert all(c['status']=='acked' for c in current_run().journal.execution_commits('action-1'))
        self.receipt = result
        self.record(['complete', result.written_rows])
        marker = self.events.with_suffix('.completed-once')
        if self.behavior == 'complete_failure' and not marker.exists():
            marker.write_text('already called after publication')
            raise RuntimeError('completion callback failure')

    def on_write_failed(self, error):
        self.record(['failed', str(error)])
        if self.behavior == 'prepare_failure':
            raise LookupError('secondary failure callback failure')


def _source(events):
    def observe(row):
        with events.open('a') as stream: stream.write(json.dumps(['source'])+'\n')
        return row
    return data.from_items([{'id':1}, {'id':2}]).map(observe)


def _events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_callbacks_surround_real_input_and_confirmed_aggregate(tmp_path):
    events = tmp_path/'events'
    sink = CallbackSink(events)
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1):
        _source(events).write_datasink(sink)
    assert sink.receipt.written_rows == 2
    calls = _events(events)
    assert calls[0] == ['start'] and calls[-1] == ['complete', 2]
    assert calls.count(['start']) == calls.count(['prepare']) == calls.count(['commit']) == 1
    assert calls.count(['source']) == 2
    assert not any(c[0]=='failed' for c in calls)


def test_failure_hook_runs_once_and_cannot_replace_original_error(tmp_path):
    events = tmp_path/'events'
    with pytest.raises(ValueError, match='original native preparation failure') as caught:
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1):
            _source(events).write_datasink(CallbackSink(events, 'prepare_failure'))
    calls = _events(events)
    assert calls[0] == ['start']
    assert len([c for c in calls if c[0]=='failed']) == 1
    assert not any(c[0] in {'complete', 'commit'} for c in calls)
    assert any('secondary failure callback failure' in note for note in caught.value.__notes__)


def test_completion_callback_failure_keeps_ack_and_resume_does_not_rewrite(tmp_path):
    events = tmp_path/'events'
    with pytest.raises(RuntimeError, match='completion callback failure') as caught:
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=1):
            reference = run.ref
            _source(events).write_datasink(CallbackSink(events, 'complete_failure'))
    assert any('after confirmed publication' in note for note in caught.value.__notes__)
    journal = RunJournal(tmp_path/'run')
    before = journal.execution_commits('action-1')
    assert before and all(c['status']=='acked' for c in before)
    sink = CallbackSink(events, 'complete_failure')
    with RunContext.resume(reference), data.local_execution(workers=1):
        _source(events).write_datasink(sink)
    assert journal.execution_commits('action-1') == before
    calls = _events(events)
    assert calls.count(['start']) == calls.count(['complete', 2]) == 2
    assert calls.count(['prepare']) == calls.count(['commit']) == 1
    assert calls.count(['source']) == 2
    assert not any(c[0]=='failed' for c in calls)
    assert sink.receipt.written_rows == 2
