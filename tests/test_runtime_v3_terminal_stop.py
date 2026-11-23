"""Terminal demand is authenticated independently from source exhaustion."""
import json

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('kind', ['string', 'json'])
@pytest.mark.parametrize('terminal', ['take', 'take_batch'])
def test_single_worker_declared_terminal_demand_keeps_typed_result(tmp_path, monkeypatch, kind, terminal):
    schema = pa.schema([('v', pa.json_() if kind == 'json' else pa.string())])
    table = pa.Table.from_pylist([{'v': '{"a":1}'}], schema=schema)
    events = []
    handle = DataflowExecution._handle_event
    def observe(execution, event):
        if event.kind == 'eoi' and event.payload.get('pruned_splits'):
            events.append({'stopping': execution._stopping,
                'limit': execution._limit, 'seen': execution._terminal_seen,
                'consumers': {k: sorted(v) for k, v in execution._exchange._satisfied_consumers.items()},
                'pruned': list(event.payload['pruned_splits'])})
        return handle(execution, event)
    monkeypatch.setattr(DataflowExecution, '_handle_event', observe)
    try:
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 20}) as run, data.local_execution(
                workers=1, worker_mode='thread'):
            source = data.from_arrow(table)
            if terminal == 'take':
                assert source.take(1) == table.to_pylist()
            else:
                assert source.take_batch(1, batch_format='pyarrow').equals(table, check_metadata=True)
            cuts = run.journal.execution_commits('action-1')
            assert cuts and all(c['status'] == 'acked' for c in cuts)
            if events:
                assert cuts[-1]['prepared']['sinks'][terminal]['manifest']['final'] is False
    finally:
        (tmp_path/'source-stop-events.json').write_text(json.dumps(events, indent=2))


def test_driver_stop_does_not_authorize_forged_or_stale_source_eoi(tmp_path, monkeypatch):
    from dataclasses import replace
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    original = DataflowExecution._handle_event
    checked = []
    def observe(execution, event):
        if event.kind == 'eoi' and event.payload.get('pruned_splits') and not checked:
            assert execution._stopping
            assert execution._terminal_stop_sources
            for changed in (
                    {'boot_id': 'wrong-boot'}, {'attempt': event.attempt + 1},
                    {'controller_epoch': event.controller_epoch + 1},
                    {'plan_revision': event.plan_revision + 1}, {'fence': 'f' * 64}):
                with pytest.raises(DataflowError, match='FENCED'):
                    execution._validate_event(replace(event, payload=_thaw_json(event.payload), **changed))
            payload = _thaw_json(event.payload)
            payload['pruned_splits'] = ['foreign-split']
            with pytest.raises(DataflowError, match='pruned input differs'):
                original(execution, replace(event, payload=payload))
            authorization = execution._terminal_stop_sources
            satisfied = execution._exchange._satisfied_consumers
            # A real downstream Limit may independently finish the input.
            # Remove both kinds of evidence to test that stopping alone grants
            # no authority; leave the actual event untouched for processing.
            assert satisfied
            assert all(execution._source_demand_satisfied(split.source_id)
                       for split in execution._endpoints[event.allocation_id]._assignment.source_splits)
            execution._exchange._satisfied_consumers = {}
            execution._terminal_stop_sources = {}
            try:
                # _stopping by itself grants no authority over a source split.
                with pytest.raises(DataflowError, match='source stop lacks'):
                    original(execution, event)
            finally:
                execution._terminal_stop_sources = authorization
            old = authorization[event.allocation_id]
            authorization[event.allocation_id] = ('old-boot', *old[1:])
            try:
                with pytest.raises(DataflowError, match='source stop lacks'):
                    original(execution, event)
            finally:
                authorization[event.allocation_id] = old
                execution._exchange._satisfied_consumers = satisfied
            assert not execution._checkpoint.inputs_complete()
            checked.append(event)
        return original(execution, event)
    monkeypatch.setattr(DataflowExecution, '_handle_event', observe)
    with RunContext.start(tmp_path/'run', limits={'timeout_s':20}) as run, data.local_execution(
            workers=1, worker_mode='thread'):
        assert data.from_items([{'v':1}]).take(1) == [{'v':1}]
        assert checked
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'

