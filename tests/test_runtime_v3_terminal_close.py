"""A user closing iteration retains a real non-final prefix receipt."""
import pytest
from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
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
    from demiflow.execution.outputs.terminals import TerminalSubscription
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    held, released, acknowledgements = [], [], []
    handle_event = DataflowExecution._handle_event
    read = TerminalSubscription.read_admitted
    close = TerminalSubscription.close
    from demiflow.execution.exchange.deliveries import DeliveryExchange
    ack = DeliveryExchange.acknowledge
    active = []
    subscribe = DataflowExecution.subscribe_terminal
    def remember(execution):
        active.append(execution)
        return subscribe(execution)
    monkeypatch.setattr(DataflowExecution, 'subscribe_terminal', remember)
    def delayed(execution, event):
        if (event.kind == 'terminal' and execution._terminal._queue
                and not held and not released):
            held.append(event)
            return
        return handle_event(execution, event)
    def await_real_envelope(subscription, **kwargs):
        if not released and not held:
            deadline = time.monotonic() + 2
            while not held and time.monotonic() < deadline:
                active[-1].pump(deadline_ms=1)
            assert held, 'the real producer must emit its second bounded record'
        return read(subscription, **kwargs)
    def observe_ack(exchange, delivery, receiver):
        acknowledgements.append(delivery)
        return ack(exchange, delivery, receiver)
    def release_after_close(subscription):
        close(subscription)
        if not held:
            return
        execution = active[-1]
        event = held.pop()
        delivery = event.payload['delivery_id']
        assert execution._terminal.closed and execution._stopping
        assert delivery not in execution._exchange._inflight
        before = list(acknowledgements)
        with pytest.raises(DataflowError, match='FENCED'):
            execution._validate_event(replace(event, boot_id='stale-boot',
                payload=_thaw_json(event.payload)))
        assert acknowledgements == before
        execution._validate_event(event)
        handle_event(execution, event)
        assert acknowledgements.count(delivery) == 1
        assert delivery not in execution._exchange._inflight and not execution._terminal._queue
        released.append((execution, event))

    monkeypatch.setattr(DataflowExecution, '_handle_event', delayed)
    monkeypatch.setattr(DeliveryExchange, 'acknowledge', observe_ack)
    monkeypatch.setattr(TerminalSubscription, 'read_admitted', await_real_envelope)
    monkeypatch.setattr(TerminalSubscription, 'close', release_after_close)
    with RunContext.start(tmp_path/'run', limits={'timeout_s':20,
            'checkpoint_interval_ms':60000}) as run, data.local_execution(
            workers=workers, worker_mode='thread'):
        rows = data.range(1000).iter_rows()
        assert next(rows) == {'id':0}
        rows.close()
        assert len(released) == 1
        execution, event = released[0]
        assert not execution._exchange._inflight and not execution._terminal._queue
        assert acknowledgements.count(event.payload['delivery_id']) == 1
        image = execution._restore_image
        assert image is not None and not image.cut.final
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        assert 0 < execution._terminal_result.rows < 1000
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert not execution._endpoints and not execution._active_worker_allocations


@pytest.mark.parametrize('concurrency', [None, 1, 3])
@pytest.mark.parametrize('durability', ['ephemeral', 'durable'])
def test_native_input_prefetch_respects_concurrency_without_changing_durable_windows(concurrency, durability):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.executors.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages, plan_exchanges
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from test_map_prompt_async import PACK

    source = data.from_items([{'item': 'text'}])
    operation = source.map_prompt if concurrency is None else source.map_prompt_async
    options = {} if concurrency is None else {'concurrency': concurrency}
    dataset = operation('enrich', config=parse_prompt_pack(PACK),
        inputs={'payload': 'item'}, output='answer', max_requests=3, **options)
    request = ActionRequest(kind='iter_rows', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='local',
        action=RunAction(_CompileContext(), 'native-prefetch', {}))
    policy, bounds = resolve_runtime_policy({'max_active_workers': 4, 'durability': durability}, {}, provider='local')
    graph = lower_action(request, bounds=bounds).graph
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    exchanges = plan_exchanges(graph, stages, policy=policy, bounds=bounds)
    prompt, = [node for node in graph.nodes if node.kernel.kind == 'prompt']
    incoming = {edge.edge_id for edge in graph.edges if edge.consumer == prompt.occurrence_id}
    windows = [edge.credit_rows for edge in exchanges if edge.edge_id in incoming]
    assert windows
    if durability == 'ephemeral':
        assert windows == [concurrency or 1]
    else:
        assert all(rows > (concurrency or 1) for rows in windows)
