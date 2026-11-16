"""Public Datasink hooks on actual generic native sinks, not KV-specific hooks."""
import json

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from native_datasink_support import CallbackSink


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
