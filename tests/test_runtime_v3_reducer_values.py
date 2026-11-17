"""Reducer values keep their original types through the one real dataflow path."""
import hashlib
import json
import math
from pathlib import Path
import tempfile

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext


ROOT = Path(__file__).resolve().parents[1]
LIMITS = {'memory_bytes': 32 * 1024**2, 'max_row_bytes': 4096,
          'max_batch_bytes': 8192, 'max_state_bytes': 128 * 1024,
          'max_scratch_bytes': 64 * 1024**2, 'timeout_s': 90}


def _check_tree(expected_tree):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(expected_tree)


def _fold_list(state, row, *, expected_tree):
    _check_tree(expected_tree)
    return state + [row['v']]


def _fold_scalar(state, row, *, expected_tree):
    _check_tree(expected_tree)
    return state * 10 + row['v']


def _fold_dict(state, row, *, expected_tree):
    _check_tree(expected_tree)
    return {'k': row['k'], 'values': state['values'] + [row['v']]}


def _box(value, *, expected_tree):
    _check_tree(expected_tree)
    return {'type': type(value).__name__, 'value': value}


def _fold_typed(state, row, *, calls):
    with Path(calls).open('a') as target:
        target.write(str(row['v']) + '\n')
    return state + [(row['v'], b'\x00\xff', -0.0)]


def _slow_keep(value):
    import time
    assert type(value) is list and type(value[0]) is tuple
    time.sleep(.02)
    return True


def _oversized_value(state, row, *, calls):
    with Path(calls).open('a') as target:
        target.write('called\n')
    return ['x' * 6000]


def _assert_rows(actual, expected):
    # Group output order is not an API promise; order inside each fold is.
    _assert_exact(sorted(actual, key=repr), sorted(expected, key=repr))


def _assert_exact(actual, expected):
    assert type(actual) is type(expected), (type(actual), type(expected))
    if type(expected) in (list, tuple):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected):
            _assert_exact(left, right)
    elif type(expected) is dict:
        assert list(actual) == list(expected)
        for key in expected:
            _assert_exact(actual[key], expected[key])
    else:
        assert actual == expected
        if type(expected) is float and expected == 0:
            assert math.copysign(1, actual) == math.copysign(1, expected)


@pytest.fixture(scope='module', params=['local', 'ray_core'])
def provider(request):
    from demiflow.data.api import _use_executor
    from runtime_v3_regression_inventory import assert_private
    from runtime_v3_support import _RayDataBlocker
    assert_private()
    name = request.param
    with _RayDataBlocker() as blocker:
        if name == 'local':
            from demiflow.execution.executors.local import LocalDatasetExecutor
            from demiflow.execution.local_kernel import LocalKernelOptions
            executor = LocalDatasetExecutor(workers=2, local_kernel=LocalKernelOptions(
                workers=2, worker_mode='thread', partitions=1, batch_rows=2))
            try:
                with _use_executor(executor):
                    yield name
            finally:
                executor.close()
        else:
            import ray
            from demiflow.execution.executors.ray import RayDatasetExecutor
            assert not ray.is_initialized(), 'test must own its Ray Core cluster'
            executor = RayDatasetExecutor(workers=2)
            with tempfile.TemporaryDirectory(prefix='demiflow-rvalue-ray-', dir='/tmp') as root:
                try:
                    ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
                             object_store_memory=128 * 1024**2, _temp_dir=root)
                    with _use_executor(executor):
                        yield name
                finally:
                    executor.close()
                    ray.shutdown()
        assert not blocker.attempts


def _flow(reducer, initial):
    return data.from_items([{'k': 1, 'v': 3}, {'k': 1, 'v': 5}, {'k': 2, 'v': 7}]).reduce_by_key(
        'k', reducer, initial=initial, fn_kwargs={'expected_tree': str(ROOT)})


def _typed_flow(calls, count=3):
    return data.from_items([{'k': i, 'v': i} for i in range(count)]).reduce_by_key(
        'k', _fold_typed, initial=[], fn_kwargs={'calls': str(calls)})


def test_original_list_scalar_dict_outputs_and_map_inputs(provider, tmp_path):
    from demiflow.execution.sealed_plans import load_sealed_plan
    cases = [('list', _fold_list, [], [[3, 5], [7]]),
             ('scalar', _fold_scalar, 0, [35, 7]),
             ('dict', _fold_dict, {'values': []},
              [{'k': 1, 'values': [3, 5]}, {'k': 2, 'values': [7]}])]
    for name, reducer, initial, expected in cases:
        for mapped in (False, True):
            with RunContext.start(tmp_path / f'{name}-{mapped}', limits=LIMITS) as run:
                flow = _flow(reducer, initial)
                if mapped:
                    flow = flow.map(_box, fn_kwargs={'expected_tree': str(ROOT)})
                want = ([{'type': type(v).__name__, 'value': v} for v in expected] if mapped else expected)
                _assert_rows(flow.take_all(), want)
                assert load_sealed_plan(run.directory, 'action-1')['physical'].policy.provider == provider
                assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        assert initial == ({'values': []} if name == 'dict' else [] if name == 'list' else 0)


def test_typed_values_materialize_read_and_map_without_aliases(provider, tmp_path):
    calls = tmp_path / 'calls'
    expected = [[(i, b'\x00\xff', -0.0)] for i in range(3)]
    with RunContext.start(tmp_path / 'run', limits=LIMITS):
        saved = _typed_flow(calls).materialize()
        try:
            first = saved.take_all()
            _assert_rows(first, expected)
            first[0].append('caller mutation')
            _assert_rows(saved.take_all(), expected)
            _assert_rows(saved.map(_box, fn_kwargs={'expected_tree': str(ROOT)}).take_all(),
                          [{'type': 'list', 'value': v} for v in expected])
        finally:
            saved.release()
    assert sorted(calls.read_text().splitlines()) == ['0', '1', '2']


def test_false_zero_empty_values_are_outputs_and_only_none_drops(provider, tmp_path):
    values = [False, 0, [], {}, (), b'', '', None]
    with RunContext.start(tmp_path / 'run', limits=LIMITS):
        flow = data.from_items([{'k': i, 'value': value} for i, value in enumerate(values)])
        _assert_rows(flow.reduce_by_key('k', lambda state, row: row['value']).take_all(), values[:-1])


@pytest.mark.parametrize('reverse', [False, True])
def test_mixed_values_do_not_invent_table_columns(provider, tmp_path, reverse):
    values = [{'first': 1}, ['a', 'b'], 7, {'different': 2}]
    if reverse:
        values.reverse()
    with RunContext.start(tmp_path / 'build', limits=LIMITS):
        flow = data.from_items([{'k': i, 'value': value} for i, value in enumerate(values)])
        saved = flow.reduce_by_key('k', lambda state, row: row['value']).materialize()
    try:
        assert saved._source.handle._schema is None
        assert saved._source.handle.declared_schema is None
        assert saved.schema(fetch_if_missing=False) is None
        assert saved.columns(fetch_if_missing=False) is None
        with RunContext.start(tmp_path / 'read', limits=LIMITS):
            _assert_rows(saved.take_all(), values)
            _assert_rows(saved.map(_box, fn_kwargs={'expected_tree': str(ROOT)}).take_all(),
                         [{'type': type(v).__name__, 'value': v} for v in values])
        for kind in ('schema', 'columns'):
            with pytest.raises(DataflowError, match='requires mapping rows'):
                with RunContext.start(tmp_path / kind, limits=LIMITS):
                    getattr(saved, kind)()
    finally:
        saved.release()


def test_relational_region_keeps_nonmapping_reducer_result(provider, tmp_path):
    from demiflow.execution.sealed_plans import load_sealed_plan
    with RunContext.start(tmp_path / 'run', limits=LIMITS) as run:
        left = data.from_items([{'k': 1, 'v': 3}, {'k': 1, 'v': 5}])
        joined = left.join(data.from_items([{'k': 1, 'r': 2}]), on='k').join(
            data.from_items([{'k': 1, 'z': 4}]), on='k')
        _assert_rows(joined.reduce_by_key('k', _fold_list, initial=[],
            fn_kwargs={'expected_tree': str(ROOT)}).take_all(), [[3, 5]])
        physical = load_sealed_plan(run.directory, 'action-1')['physical']
        assert any(stage.stage_id.startswith('relational-v1-') for stage in physical.stages)


class InterruptedAfterAck(Exception):
    pass


def test_final_original_ack_replays_values_without_workers_or_reducer(provider, tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    calls = tmp_path / 'calls'
    expected = [[(i, b'\x00\xff', -0.0)] for i in range(3)]
    with pytest.raises(InterruptedAfterAck):
        with RunContext.start(tmp_path / 'run', limits=LIMITS) as run:
            reference = run.ref
            _assert_rows(_typed_flow(calls).take_all(), expected)
            original = run.journal.execution_commits('action-1')
            call_log = calls.read_bytes()
            assert original and all(fact['status'] == 'acked' for fact in original)
            raise InterruptedAfterAck('after the real final action ACK')
    def forbidden(*args, **kwargs):
        raise AssertionError('final ACK replay must not launch a worker')
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', forbidden)
    with RunContext.resume(reference) as run:
        _assert_rows(_typed_flow(calls).take_all(), expected)
        assert run.journal.execution_commits('action-1') == original
    assert calls.read_bytes() == call_log


def test_materialized_reducer_values_resume_original_nonfinal_ack(provider, tmp_path, monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.state_artifacts import StateArtifacts
    calls = tmp_path / 'calls'
    expected = [[(i, b'\x00\xff', -0.0)] for i in range(12)]
    with RunContext.start(tmp_path / 'build', limits=LIMITS):
        saved = _typed_flow(calls, count=12).materialize()
    call_log = calls.read_bytes()
    commit = DataflowCheckpointCoordinator.commit
    prefixes = []
    def interrupted(coordinator, cut_id, *, sinks, native_queue_state=None):
        image = commit(coordinator, cut_id, sinks=sinks, native_queue_state=native_queue_state)
        counts = [json.loads(b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes)))
                  ['manifest']['total_rows'] for ref in image.confirmed_sinks.values()]
        if sum(counts) and not image.cut.final and not prefixes:
            prefixes.append(sum(counts))
            raise InterruptedAfterAck('after the real nonfinal prefix ACK')
        return image
    try:
        with pytest.raises(InterruptedAfterAck):
            with RunContext.start(tmp_path / 'run', limits={**LIMITS, 'checkpoint_rows': 1,
                    'checkpoint_interval_ms': 60000}) as run:
                reference = run.ref
                with monkeypatch.context() as patch:
                    patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupted)
                    saved.filter(_slow_keep).take_all()
        assert len(prefixes) == 1 and 0 < prefixes[0] < len(expected)
        with RunContext.resume(reference) as run:
            before = run.journal.execution_commits('action-1')
            assert before and all(fact['status'] == 'acked' for fact in before)
            original = StateArtifacts(run).read(before[-1]['prepared']['state']['dataflow-cut-v3'])
            assert not original['cut']['final']
            _assert_rows(saved.filter(_slow_keep).take_all(), expected)
            after = run.journal.execution_commits('action-1')
            assert after[:len(before)] == before
            assert StateArtifacts(run).read(after[-1]['prepared']['state']['dataflow-cut-v3'])['cut']['final']
        assert calls.read_bytes() == call_log
    finally:
        saved.release()


@pytest.mark.parametrize('bound', ['state', 'row'])
def test_reducer_value_limits_reject_without_committed_output(tmp_path, bound):
    limits = {**LIMITS, 'max_state_bytes': 1024 if bound == 'state' else 16384,
              'max_row_bytes': 8192 if bound == 'state' else 1024}
    with pytest.raises(DataflowError) as failure:
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=1):
            data.from_items([{'k': 1}]).reduce_by_key('k', _oversized_value,
                fn_kwargs={'calls': str(tmp_path / 'calls')}).take_all()
    assert failure.value.code == 'BUDGET_EXCEEDED'
    assert ('state' if bound == 'state' else 'row byte budget') in str(failure.value)
    assert (tmp_path / 'calls').read_text() == 'called\n'
    from demiflow.execution.run_journal import RunJournal
    assert RunJournal(tmp_path / 'run').execution_commits('action-1') == []


@pytest.mark.parametrize('value', [[(b'\xff', -0.0), [1, None]], 35, False, 0, [], {}, (), b'', ''])
def test_record_codec_clone_and_state_preserve_top_level_values(value):
    from demiflow.execution.contracts.dataflow import Record, thaw_record_value
    from demiflow.execution.dataflow_records import clone_record
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _decode_payload
    from demiflow.execution import dataflow_record_frames as frames
    from demiflow.execution.kernels import state_codec
    from test_runtime_v3_c import bounds
    raw = _encode_payload(value, max_bytes=4096)
    record = Record(hashlib.sha256(raw).hexdigest(), {'offset': [1, 2]}, None, value, len(raw), 4)
    frozen = clone_record(record, sequence=5)
    _assert_exact(thaw_record_value(frozen.inline), value)
    _assert_exact(_decode_payload(raw, max_bytes=4096), value)
    budget = bounds(max_row_bytes=4096, max_resident_bytes=256 * 1024)
    document = frames.encode_document(frozen, budget)
    restored = frames.decode_document(document, record_id=frozen.record_id, sequence=5, bounds=budget)
    _assert_exact(thaw_record_value(restored.inline), value)
    saved = state_codec.dumps([frozen], max_bytes=65536)
    _assert_exact(thaw_record_value(state_codec.loads(saved, max_bytes=65536)[0].inline), value)


def test_top_level_mapping_keys_and_closed_data_gate_remain_strict():
    from demiflow.execution.contracts.dataflow import Record
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _decode_payload
    with pytest.raises(DataflowError):
        Record('0' * 64, {}, None, {1: 'invalid key'}, 1, 0)
    class Code:
        def __reduce__(self):
            raise AssertionError('business transport must never invoke pickle')
    with pytest.raises(DataflowError):
        Record('0' * 64, {}, None, Code(), 1, 0)
    for value in (Code(), [Code()]):
        with pytest.raises(DataflowError):
            _encode_payload(value, max_bytes=4096)
    with pytest.raises(DataflowError):
        _decode_payload(_encode_payload(None, max_bytes=4096), max_bytes=4096)
