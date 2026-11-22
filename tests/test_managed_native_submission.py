"""Native host admission exercises the real compiler, commit and observations."""
import pytest
import pyarrow as pa
from demiflow import data
from demiflow.execution.host.control import NativeExecution, RunContext, local_execution, DEFAULT_LIMITS
from demiflow.execution.host.inspection import capture_managed_pipeline, native_submission_view


def flow(*, target, stage):
    data.require(target != stage, 'Targets must differ')
    schema = pa.schema([('x', pa.int64())])
    receipt = data.from_items([{'x': 1}, {'x': 2}]).save_lance(
        stage, schema=schema, key='x', stage='rows', mode='append', max_rows=2,
        max_index_bytes=8 * 1024**2).write_lance(target, schema=schema, mode='create', return_receipt=True)
    data.require(receipt.input_rows == 2, 'Keep all rows')
    return receipt


def test_native_capture_and_execute_same_contract(tmp_path):
    args = dict(target=str(tmp_path / 'out.lance'), stage=str(tmp_path / 'stage.lance'))
    events, observed = [], []
    capture = capture_managed_pipeline(flow, args, limits=DEFAULT_LIMITS)
    view = native_submission_view(capture)
    with RunContext(tmp_path / 'run', limits=DEFAULT_LIMITS), local_execution(worker_mode='thread', batch_rows=DEFAULT_LIMITS['batch_rows'], block_size=DEFAULT_LIMITS['batch_rows']):
        assert not (tmp_path / 'out.lance').exists()
        context = NativeExecution(contract=capture.submission,
            **{k: view[k] for k in ('nodes', 'edges', 'actions', 'operators')},
            source_root=tmp_path, resource_root=tmp_path, limits=DEFAULT_LIMITS,
            record=lambda *v: events.append(v), check_stop=lambda: None,
            observe=lambda v: observed.extend(v), report_digest='a' * 64)
        with context.bind_context():
            receipt = flow(**args)
    assert receipt.status == 'committed' and receipt.written_rows == 2
    assert any(v[1] == 'stage_commit' for v in events)
    assert any(v[1] == 'receipt' for v in events)
    assert any(v['document'].get('dag_id') for v in observed)


def test_capture_rejects_unbounded_append_before_effect(tmp_path):
    with pytest.raises(Exception, match='requires create'):
        capture_managed_pipeline(lambda: data.from_items([{'x': 1}]).write_lance(
            str(tmp_path / 'append.lance'), schema=pa.schema([('x', pa.int64())])), {}, limits=DEFAULT_LIMITS)
    assert not (tmp_path / 'append.lance').exists()


@pytest.mark.parametrize('inherited', [True, False])
def test_native_forwarding_preserves_actual_sdk_declarations(monkeypatch, inherited):
    from demiflow.execution import dataflow_api
    from demiflow.execution.host.inspection import capture_pipeline
    calls = []
    original = dataflow_api.resolve_runtime_policy

    def observe(options, limits, *, provider, explicit_limits=()):
        calls.append(set(explicit_limits))
        return original(options, limits, provider=provider, explicit_limits=explicit_limits)

    monkeypatch.setattr(dataflow_api, 'resolve_runtime_policy', observe)
    native = {'max_row_bytes': DEFAULT_LIMITS['max_row_bytes'], 'batch_rows': 8}

    def construct():
        kwargs = {'inherited_native_resources' if inherited else 'native_resources': native}
        with local_execution(worker_mode='thread', **kwargs):
            return data.range(1).take_all()

    captured = capture_pipeline(construct, {}, limits=DEFAULT_LIMITS, explicit_limits=[])
    assert calls == [set() if inherited else set(native)]
    assert captured.policy.native_limits['batch_rows'] == 8
    assert captured.bounds.max_row_bytes <= native['max_row_bytes']
