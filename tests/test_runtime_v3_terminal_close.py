"""A user closing iteration retains a real non-final prefix receipt."""
import pytest
from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('workers', [1, 2])
def test_user_iterator_close_preserves_nonfinal_source_prefix(tmp_path, monkeypatch, workers):
    executions = []
    original = DataflowExecution.start
    def start(execution):
        executions.append(execution)
        return original(execution)
    monkeypatch.setattr(DataflowExecution, 'start', start)
    with RunContext.start(tmp_path/'run', limits={'timeout_s':20,
            'checkpoint_interval_ms':60000}) as run, data.local_execution(
            workers=workers, worker_mode='thread'):
        rows = data.range(1000).iter_rows()
        assert next(rows) == {'id':0}
        rows.close()
        execution = executions[0]
        image = execution._restore_image
        assert execution._stopping and not execution._checkpoint.inputs_complete()
        assert not image.cut.final
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert 0 < execution._terminal_result.rows < 1000
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        cursors = [cursor for part in image.contributions for cursor in part.sources]
        assert cursors and all(not cursor.end_of_input and cursor.next_sequence < 1000 for cursor in cursors)


@pytest.mark.parametrize('workers', [1, 2])
def test_closed_subscription_acknowledges_late_real_output(tmp_path, monkeypatch, workers):
    """Delay one genuine terminal envelope past close, without forging output."""
    import time
    from dataclasses import replace
    from demiflow.execution.dataflow import _OutputSubscription
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    held, released, acknowledgements = [], [], []
    handle_event = DataflowExecution._handle_event
    read = _OutputSubscription.read_admitted
    close = _OutputSubscription.close
    ack = DataflowExecution._ack
    def delayed(execution, event):
        if (event.kind == 'terminal' and execution._terminal_queue
                and not held and not released):
            held.append(event)
            return
        return handle_event(execution, event)
    def await_real_envelope(subscription, **kwargs):
        if not released and not held:
            deadline = time.monotonic() + 2
            while not held and time.monotonic() < deadline:
                subscription.execution.pump(deadline_ms=1)
            assert held, 'the real producer must emit its second bounded record'
        return read(subscription, **kwargs)
    def observe_ack(execution, delivery):
        acknowledgements.append(delivery)
        return ack(execution, delivery)
    def release_after_close(subscription):
        close(subscription)
        if not held:
            return
        execution = subscription.execution
        event = held.pop()
        delivery = event.payload['delivery_id']
        assert execution._terminal_subscription_closed and execution._stopping
        assert delivery not in execution._deliveries
        before = list(acknowledgements)
        with pytest.raises(DataflowError, match='FENCED'):
            execution._validate_event(replace(event, boot_id='stale-boot',
                payload=_thaw_json(event.payload)))
        assert acknowledgements == before
        execution._validate_event(event)
        handle_event(execution, event)
        assert acknowledgements.count(delivery) == 1
        assert delivery not in execution._deliveries and not execution._terminal_queue
        released.append((execution, event))

    monkeypatch.setattr(DataflowExecution, '_handle_event', delayed)
    monkeypatch.setattr(DataflowExecution, '_ack', observe_ack)
    monkeypatch.setattr(_OutputSubscription, 'read_admitted', await_real_envelope)
    monkeypatch.setattr(_OutputSubscription, 'close', release_after_close)
    with RunContext.start(tmp_path/'run', limits={'timeout_s':20,
            'checkpoint_interval_ms':60000}) as run, data.local_execution(
            workers=workers, worker_mode='thread'):
        rows = data.range(1000).iter_rows()
        assert next(rows) == {'id':0}
        rows.close()
        assert len(released) == 1
        execution, event = released[0]
        assert not execution._deliveries and not execution._terminal_queue
        assert acknowledgements.count(event.payload['delivery_id']) == 1
        image = execution._restore_image
        assert image is not None and not image.cut.final
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        assert 0 < execution._terminal_result.rows < 1000
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert not execution._endpoints and not execution._active_worker_allocations
