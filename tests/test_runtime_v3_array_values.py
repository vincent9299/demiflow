"""Real worker exchange preserves NumPy data; object arrays fail closed."""
import numpy as np
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _decode_payload


def test_array_codec_fidelity_detachment_and_bounds():
    values = {'array': np.array([[-0.0, 0.0, np.nan]], dtype='>f4'),
              'scalar': np.int64(12), 'empty': np.zeros((0, 3), dtype='int16'),
              'complex': np.complex128(2 + 3j), 'time': np.datetime64('2026-01-01')}
    encoded = _encode_payload(values, max_bytes=4096)
    decoded = _decode_payload(encoded, max_bytes=4096)
    for key, value in values.items():
        assert decoded[key].dtype == value.dtype
        assert decoded[key].shape == value.shape
        assert decoded[key].tobytes() == value.tobytes()
    record = Record('a' * 64, {}, None, values, len(encoded), 0)
    values['array'][0, 0] = 17
    assert np.signbit(record.inline['array'][0, 0])
    with pytest.raises(ValueError):
        record.inline['array'].setflags(write=True)
    business = thaw_record_value(record.inline)
    business['array'][0, 0] = 18
    assert np.signbit(record.inline['array'][0, 0])
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
        _encode_payload({'array': np.zeros(1024)}, max_bytes=64)
    with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
        _encode_payload({'array': np.array([object()], dtype=object)}, max_bytes=4096)


def test_public_numpy_batch_exchange_materialize_and_committed_replay(tmp_path):
    def batch(columns):
        import numpy as np
        return [{'n': n * 2, 'vector': np.array([-0.0, float(n)], dtype='float32')}
                for n in columns['n']]
    def dataset():
        return data.from_items([{'n': 1}, {'n': 2}]).map_batches(batch, batch_size=2)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            materialized = dataset().materialize()
            try:
                rows = materialized.take_all()
            finally:
                materialized.release()
            commits = run.journal.execution_commits('action-1')
            raise Interrupted()
    with RunContext.resume(ref) as run, data.local_execution():
        materialized = dataset().materialize()
        try:
            restored = materialized.take_all()
        finally:
            materialized.release()
        assert run.journal.execution_commits('action-1') == commits
    for expected, actual in zip(rows, restored, strict=True):
        assert type(actual['n']) is np.int64
        assert actual['n'] == expected['n']
        assert actual['vector'].dtype == np.dtype('float32')
        assert actual['vector'].tobytes() == expected['vector'].tobytes()
        assert np.signbit(actual['vector'][0])
