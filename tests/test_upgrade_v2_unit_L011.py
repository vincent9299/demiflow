"""L011 end-to-end: a bound ExecutionPlacement drives the real Arrow batch sink.

These exercise the full sealed chain on real Lance data, not a parameter
passthrough: local options tightening -> TaskScheduler byte shares -> rechunk
input buffering -> apply_batch IPC write-out -> exception cancel/release. The
contract sample mirrors interfaces.pyi ExecutionPlacement (design stub, not a
runtime import); only profile/stage_limits/stage_ids are consumed here.
"""
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RecoveryContractError
from demiflow.execution.arrow_batch_sink import execute as arrow_execute


def _placement(*, cpu_millis=2000, memory_bytes=1 << 30, inflight_rows=None,
               inflight_bytes=None, output_bytes=None, stage_id='arrow_map_batches'):
    budget = {}
    if inflight_rows is not None:
        budget['inflight_rows'] = inflight_rows
    if inflight_bytes is not None:
        budget['inflight_bytes'] = inflight_bytes
    if output_bytes is not None:
        budget['output_bytes'] = output_bytes
    return SimpleNamespace(
        execution_id='exec-l011', placement='driver', group_id=None,
        backend='arrow_batch', stage_ids=(stage_id,),
        profile={'limits': {'cpu_millis': cpu_millis, 'memory_bytes': memory_bytes}},
        programs=(), stage_limits={stage_id: budget}, recovery='delivery_only')


def _source_table(tmp_path, rows=16):
    schema = pa.schema([('id', pa.int64())])
    table = pa.Table.from_pylist([{'id': i} for i in range(rows)], schema=schema)
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(table, uri, max_rows_per_file=4, max_rows_per_group=4)
    return uri, table, schema


def _append_n(batch):
    return batch.append_column('batch_n', pa.array([len(batch)] * len(batch), type=pa.int64()))


def _run(executor, ds, schema, execution):
    batches = list(arrow_execute(executor, ds._source, ds._plan, schema, execution=execution))
    return pa.Table.from_batches(batches, schema=schema) if batches else pa.table({}, schema=schema)


def test_bound_tightens_concurrency_preserves_declared_batch_and_writes_ipc(tmp_path):
    uri, table, schema = _source_table(tmp_path)
    out_schema = schema.append(pa.field('batch_n', pa.int64()))
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.read_lance(uri, version=1).map_batches(_append_n, batch_format='pyarrow', batch_size=4)
        execution = _placement(cpu_millis=2000, inflight_rows=4, inflight_bytes=1 << 20, output_bytes=1 << 20)
        result = _run(executor, ds, out_schema, execution)
        stats = session.stats
        # cpu_millis=2000 tightens real concurrency from 4 to 2; width=4.
        assert stats['workers'] == 2
        assert stats['partitions'] <= 2
        assert stats['peak_pending_tasks'] <= 4
        assert stats['arrow_batch_sink'] and stats['status'] == 'complete'
        # The shared executor configuration is borrowed, never mutated.
        assert executor._local_kernel.workers == 4
        assert executor._local_kernel.batch_rows == 8192
    expected = table.append_column('batch_n', pa.array([4] * 16, type=pa.int64()))
    assert result.equals(expected)
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_bound_inflight_rows_tightens_undeclared_reader_batch(tmp_path):
    uri, table, schema = _source_table(tmp_path)
    out_schema = schema.append(pa.field('batch_n', pa.int64()))
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        ds = data.read_lance(uri, version=1).map_batches(_append_n, batch_format='pyarrow')
        # No declared business batch_size: the physical reader batch borrows
        # inflight_rows=3, so callback groups are 3,3,3,3,3,1 over 16 rows.
        execution = _placement(inflight_rows=3, inflight_bytes=1 << 20, output_bytes=1 << 20)
        result = _run(session._executor, ds, out_schema, execution)
        assert session.stats['status'] == 'complete'
    expected = table.append_column('batch_n', pa.array([3] * 15 + [1], type=pa.int64()))
    assert result.equals(expected)
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_bound_inflight_bytes_rejects_oversized_callback_group(tmp_path):
    uri, table, schema = _source_table(tmp_path)
    out_schema = schema.append(pa.field('batch_n', pa.int64()))
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        ds = data.read_lance(uri, version=1).map_batches(_append_n, batch_format='pyarrow', batch_size=4)
        # width=4, inflight_bytes=64 -> per-task share 16 < a 4-row int64 group
        # (32 bytes). rechunk must fail the declared group, not shrink its scope.
        execution = _placement(cpu_millis=2000, inflight_rows=4, inflight_bytes=64, output_bytes=1 << 20)
        with pytest.raises(RecoveryContractError, match='inflight byte budget'):
            _run(session._executor, ds, out_schema, execution)
        assert session.stats['status'] == 'failed'
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_bound_output_bytes_rejects_oversized_callback_output(tmp_path):
    uri, table, schema = _source_table(tmp_path)
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        ds = data.read_lance(uri, version=1).map_batches(_append_n, batch_format='pyarrow', batch_size=4)
        out_schema = schema.append(pa.field('batch_n', pa.int64()))
        # Input fits (share 256) but output staging share 16 < 40-byte output
        # (id 32 + batch_n 8); apply_batch fails before publishing the IPC stream.
        execution = _placement(cpu_millis=2000, inflight_rows=4, inflight_bytes=1 << 20, output_bytes=64)
        with pytest.raises(RecoveryContractError, match='output staging byte budget'):
            _run(session._executor, ds, out_schema, execution)
        assert session.stats['status'] == 'failed'
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_bound_callback_failure_cancels_and_releases_temp(tmp_path):
    uri, table, schema = _source_table(tmp_path)

    def boom(batch):
        if batch['id'][0].as_py() >= 8:
            raise ValueError('deliberate bound callback failure')
        return batch.append_column('batch_n', pa.array([len(batch)] * len(batch), type=pa.int64()))

    out_schema = schema.append(pa.field('batch_n', pa.int64()))
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        ds = data.read_lance(uri, version=1).map_batches(boom, batch_format='pyarrow', batch_size=4)
        execution = _placement(cpu_millis=2000, inflight_rows=4, inflight_bytes=1 << 20, output_bytes=1 << 20)
        with pytest.raises(ValueError, match='deliberate bound callback failure'):
            _run(session._executor, ds, out_schema, execution)
        assert session.stats['status'] == 'failed'
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_unbound_execution_keeps_baseline_concurrency(tmp_path):
    uri, table, schema = _source_table(tmp_path)
    out_schema = schema.append(pa.field('batch_n', pa.int64()))
    with data.local_execution(workers=4, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        ds = data.read_lance(uri, version=1).map_batches(_append_n, batch_format='pyarrow', batch_size=4)
        result = _run(session._executor, ds, out_schema, None)
        stats = session.stats
        assert stats['workers'] == 4
        assert stats['status'] == 'complete'
    expected = table.append_column('batch_n', pa.array([4] * 16, type=pa.int64()))
    assert result.equals(expected)
    assert not list(tmp_path.glob('demiflow-arrow-*'))