"""Parking drains live model requests without spending cut-coordination time."""
from types import SimpleNamespace as NS

import pytest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_runtime_v3_finite_checkpoint_wait import _driver, _wait, _runner
from test_codex_exec import PACK, fake_cli


@pytest.mark.parametrize('kind', ['prompt', 'embedding'])
def test_only_authorized_native_residency_drain_extends_business_wait(monkeypatch, kind):
    driver, clock = _driver(monkeypatch)
    driver._graph.nodes[0].kernel.kind = kind
    driver._durable_residency = None
    with pytest.raises(DataflowError, match='sealed business drain'):
        _wait(driver)
    driver._durable_residency = object()
    _wait(driver)
    clock[0] = 90
    driver._check_checkpoint_deadline()
    _wait(driver, sequence=2, occurrence=None)
    clock[0] = 151
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        driver._check_checkpoint_deadline()


def test_model_wait_requires_driver_parking_and_native_model_kind():
    runner = _runner()
    kernel = NS(pending_calls=True, _spec=NS(kind='prompt'))
    runner.e = NS(_kernels={('model', 'p0'): kernel})
    runner._checkpoint_wait_requested = True
    runner._activity_generation = 1
    runner._park_mode = True  # A worker's smaller local grant is not authorization.
    assert runner._checkpoint_wait_notification() is None
    runner._park_wait_authorized = True
    kernel._spec.kind = 'AsyncMapOp'
    assert runner._checkpoint_wait_notification() is None
    kernel._spec.kind = 'embedding'
    assert runner._checkpoint_wait_notification()['occurrence_id'] == 'model'
    assert runner._checkpoint_wait_notification() is None
    kernel.pending_calls = False
    assert runner._checkpoint_wait_notification()['occurrence_id'] is None


def test_serial_native_model_completes_under_smaller_checkpoint_budget(tmp_path, monkeypatch, fake_cli):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.operator_llm.parser import parse_prompt_pack
    cli, capture = fake_cli
    cli.write_text(cli.read_text().replace("output = pathlib.Path", "time.sleep(4)\noutput = pathlib.Path"))
    pack = parse_prompt_pack(PACK)
    options = {'codex_exec': {'bin': str(cli)}, 'timeout_s': 20}
    notices = []
    physical = []
    handle = DataflowExecution._handle_event
    def record(driver, event):
        if not physical:
            physical.append(([(s.resident_bytes, s.replica_target) for s in driver._physical.stages], driver._bounds.max_resident_bytes, driver._physical.residency.control_reserved_bytes, driver._durable_residency is not None))
        if event.kind == 'checkpoint_wait' and event.payload['occurrence_id']:
            notices.append(event.payload['occurrence_id'])
        return handle(driver, event)
    monkeypatch.setattr(DataflowExecution, '_handle_event', record)
    pump = DataflowExecution._pump_once
    requested = []
    def checkpoint_while_model_is_live(driver, **kwargs):
        # The real external process writes this before its delayed response.
        # Exercise the same driver's ordinary checkpoint admission explicitly.
        if not requested and capture.exists():
            requested.append(True)
            driver.request_checkpoint()
        return pump(driver, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_pump_once', checkpoint_while_model_is_live)
    with RunContext.start(tmp_path/'run', limits={
        'memory_bytes': 80*1024**2, 'max_state_bytes': 1024**2, 'max_row_bytes': 16*1024**2,
        'max_batch_bytes': 32*1024**2, 'checkpoint_timeout_s': 2,
        'checkpoint_interval_ms': 100, 'timeout_s': 60}), data.local_execution(worker_mode='thread'):
        rows = data.from_items([{'task': 'one', 'images': []}]).map_prompt_async(
            'author', config=pack, inputs=['task', 'images'], output='answer',
            max_requests=1, options=options, concurrency=1, queue_depth=1).map_prompt_async(
            'author', config=pack, inputs=['task', 'images'], output='second',
            max_requests=1, options=options, concurrency=1, queue_depth=1).take_all()
    assert len(rows) == 1 and rows[0]['task'] == 'one'
    assert rows[0]['answer'] == rows[0]['second'] == 7
    assert len(capture.read_text().splitlines()) == 2
    assert notices, physical
