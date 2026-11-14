"""LNU06: apply_batch is real IPC write-out. It streams callback output to a task
spill under the sealed output staging byte ceiling, fails before publishing an
over-budget result, and closes the writer on the error path.
"""
import cloudpickle
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.arrow_batch_sink import apply_batch
from demiflow.execution.recovery_contract import RecoveryContractError


def encoded_op(callback, batch_size=2):
    operation = data.from_items([{'id': i} for i in range(4)]).map_batches(
        callback, batch_format='pyarrow', batch_size=batch_size)._plan.operations[0]
    return cloudpickle.dumps(operation)


def batch(n=4):
    return pa.table({'id': pa.array(list(range(n)), type=pa.int64())})


def test_unbound_writes_ipc_and_reports_rows(tmp_path):
    path = str(tmp_path / 'out.arrow')
    table = batch()
    result = apply_batch(table, encoded_op(lambda b: b), path, table.schema)
    assert result.value.rows == 4
    assert result.rows_input == 4
    with pa.OSFile(path, 'rb') as stream:
        readback = pa.ipc.open_stream(stream).read_all()
    assert readback.equals(table)


def test_fitting_output_budget_admits(tmp_path):
    path = str(tmp_path / 'out.arrow')
    table = batch()
    result = apply_batch(table, encoded_op(lambda b: b), path, table.schema, 4096, memory_bytes=table.nbytes)
    assert result.value.rows == 4


def test_oversized_output_fails_before_publish(tmp_path):
    path = str(tmp_path / 'out.arrow')
    table = batch()
    with pytest.raises(RecoveryContractError, match='output staging byte budget'):
        apply_batch(table, encoded_op(lambda b: b), path, table.schema, table.nbytes - 1)


def test_cumulative_multi_output_budget(tmp_path):
    path = str(tmp_path / 'out.arrow')
    table = batch()

    def twice(b):
        yield b
        yield b

    # Two emitted tables accumulate; a ceiling of one table fails on the second.
    with pytest.raises(RecoveryContractError, match='scratch byte budget'):
        apply_batch(table, encoded_op(twice), path, table.schema, 320, memory_bytes=table.nbytes)


def test_error_path_closes_writer(tmp_path):
    path = str(tmp_path / 'out.arrow')
    table = batch()

    def fail_after_first(b):
        yield b
        raise ValueError('deliberate output failure')

    with pytest.raises(ValueError, match='deliberate output failure'):
        apply_batch(table, encoded_op(fail_after_first), path, table.schema)
    # Failed task output is removed; no partial IPC file can be consumed.
    from pathlib import Path
    assert not Path(path).exists()
