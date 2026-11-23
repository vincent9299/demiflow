import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.run_journal import RunJournal


@pytest.mark.parametrize('mode', ['thread', 'process'])
@pytest.mark.parametrize('empty', [False, True])
def test_arrow_sink_preserves_exact_scan_global_batches_and_nested_values(tmp_path, monkeypatch, mode, empty):
    schema = pa.schema([('id', pa.int64()), ('items', pa.list_(pa.struct([('text', pa.large_string())])))])
    table = pa.Table.from_pylist([{'id': i, 'items': None if i == 0 else [] if i == 1 else [{'text': None if i == 2 else str(i)}]} for i in range(8)], schema=schema)
    source, target = str(tmp_path/'source.lance'), str(tmp_path/'target.lance')
    lance.write_dataset(table, source, max_rows_per_file=2, max_rows_per_group=2)
    lance.write_dataset(table, source, mode='append')
    output_schema = schema.append(pa.field('batch_n', pa.int64()))
    def callback(batch, factor):
        assert batch.schema.equals(schema)
        values = pa.array([len(batch) * factor] * len(batch), type=pa.int64())
        result = batch.append_column('batch_n', values)
        yield result.slice(0, 1).to_batches()[0]
        yield result.slice(1)
    with data.local_execution(workers=2, worker_mode=mode, batch_rows=3, temp_directory=str(tmp_path)) as session:
        data.read_lance(source, version=1, filter='false' if empty else 'id >= 1', limit=6, batch_size=2).map_batches(
            callback, batch_format='pyarrow', fn_kwargs={'factor': 2}).write_lance(target, mode='create', schema=output_schema)
        journal = RunJournal(session.run_ref.directory)
        assert journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert journal.execution_status()['status'] == 'succeeded'
    expected = table.slice(1, 6).append_column('batch_n', pa.array([6]*6, type=pa.int64()))
    if empty:
        expected = expected.slice(0, 0)
    assert lance.dataset(target).to_table().equals(expected)
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_arrow_callback_failure_does_not_publish_or_retry(tmp_path):
    source, target = str(tmp_path/'source.lance'), str(tmp_path/'target.lance')
    table = pa.table({'id': [1, 2, 3, 4]})
    lance.write_dataset(table, source)
    lance.write_dataset(table.slice(0, 1), target)
    calls = tmp_path/'calls.txt'
    def fail(batch):
        with calls.open('a') as stream:
            stream.write(str(batch['id'][0].as_py()) + '\n')
        if batch['id'][0].as_py() == 3:
            raise ValueError('deliberate callback failure')
        return batch
    from demiflow.execution import RecoveryContractError
    from demiflow.execution.run_journal import RunJournal
    # Catching the callback locally must not turn its incomplete action into a
    # successful enclosing native run. Preserve both failure boundaries.
    with pytest.raises(RecoveryContractError, match='unconfirmed actions'):
        with data.local_execution(workers=1, worker_mode='thread', temp_directory=str(tmp_path)) as session:
            with pytest.raises(Exception, match='deliberate callback failure'):
                data.read_lance(source, version=1).map_batches(fail, batch_size=2, batch_format='pyarrow').write_lance(
                    target, mode='overwrite', expected_version=1, schema=table.schema)
            journal = RunJournal(session.run_ref.directory)
            # A confirmed input prefix may be checkpointed without publishing
            # any Lance version. The failed callback cannot seal a final sink.
            for commit in journal.execution_commits('action-1'):
                sink = commit['prepared']['sinks']['write_lance']
                assert sink['manifest']['final'] is False
                assert sink['native'] is None
                assert sink['rows'] == 0
            assert journal.execution_action('action-1')['result'] is None
    assert RunJournal(session.run_ref.directory).execution_status()['status'] == 'failed'
    assert calls.read_text().splitlines() == ['1', '3']
    assert lance.dataset(target).version == 1
    assert lance.dataset(target).to_table().equals(table.slice(0, 1))
    assert not list(tmp_path.glob('demiflow-arrow-*'))


@pytest.mark.parametrize('declared', [True, False])
@pytest.mark.parametrize('zero_copy', [False, True])
def test_v3_public_arrow_batches_cross_fragments_without_row_conversions(tmp_path, monkeypatch, declared, zero_copy):
    """One business call per group, including groups spanning source fragments."""
    from _arrow_payload_fixtures import install_arrow_probe
    from _worker_event_fixtures import events
    from demiflow.execution.kernels import dataflow as kernels
    from demiflow.execution.storage import streams

    source, target = str(tmp_path / 'source'), str(tmp_path / 'target')
    observed = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=declared)
    old_spec = pa.schema([('id', pa.int64()), ('blob', pa.large_binary()), ('unused', pa.string())])
    table = pa.table({'id': list(range(17)), 'blob': [bytes([i]) * 1500 for i in range(17)],
                      'unused': ['unused'] * 17}, schema=old_spec)
    lance.write_dataset(table, source, max_rows_per_file=5, max_rows_per_group=5)
    lance.write_dataset(table, source, mode='append')
    projection = table.select(['id', 'blob'])
    calls = tmp_path / 'groups'
    rebuilt, carried = [], []
    original_build, original_store = kernels._rows_to_batch, streams.store_mapped_arrow

    def count_rebuild(rows, *args, **kwargs):
        rebuilt.append(len(rows))
        return original_build(rows, *args, **kwargs)

    def count_artifacts(batch, *args, **kwargs):
        result = original_store(batch, *args, **kwargs)
        if result is not None:
            carried.append(len(result))
        return result

    monkeypatch.setattr(kernels, '_rows_to_batch', count_rebuild)
    monkeypatch.setattr(streams, 'store_mapped_arrow', count_artifacts)

    def expand(batch):
        import pyarrow.compute as compute
        assert batch.schema.names == ['id', 'blob']
        with calls.open('a') as out:
            out.write(str((batch['id'][0].as_py(), len(batch))) + '\n')
        transformed = batch.set_column(0, 'id', compute.add(batch['id'], 1))
        yield transformed
        # Expanded rows inherit the last original position and a distinct ID.
        yield transformed.slice(len(batch) - 1, 1)

    with data.local_execution(workers=2, worker_mode='thread', partitions=2,
                              batch_rows=6, temp_directory=str(tmp_path)):
        receipt = data.read_lance(source, version=1, columns=['id', 'blob'], batch_size=3).map_batches(
            expand, batch_size=6, batch_format='pyarrow', zero_copy_batch=zero_copy).write_lance(
            target, mode='create', schema=projection.schema if declared else None, return_receipt=True)
    assert calls.read_text().splitlines() == ['(0, 6)', '(6, 6)', '(12, 5)']
    assert receipt.committed_version == 1 and receipt.written_rows == 20
    order = [0, 1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 16]
    out = lance.dataset(target).to_table()
    assert out['id'].to_pylist() == [i + 1 for i in order]
    assert out['blob'].to_pylist() == [bytes([i]) * 1500 for i in order]
    if declared:
        assert rebuilt == []
        assert sum(carried) == 20
        assert not any(e['event'] in {'python_fallback', 'python_decode'} for e in events(observed))
    else:
        assert rebuilt == [6, 6, 5] and not carried
        assert any(e['event'] == 'python_fallback' for e in events(observed))


def test_mapped_arrow_refuses_oversized_output_without_lance_publication(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.run_context import RunContext

    source, target = str(tmp_path / 'source'), str(tmp_path / 'target')
    table = pa.table({'id': [1, 2, 3]})
    lance.write_dataset(table, source)
    schema = pa.schema([('blob', pa.binary())])
    def too_large(batch):
        return pa.table({'blob': [b'x' * 16384] * len(batch)})

    with pytest.raises(DataflowError) as raised:
        with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 4096}), data.local_execution():
            data.read_lance(source, version=1).map_batches(
                too_large, batch_size=2, batch_format='pyarrow').write_lance(target, mode='create', schema=schema)
    assert raised.value.code == 'BUDGET_EXCEEDED'
    assert not (tmp_path / 'target' / '_versions').exists()


def test_arrow_output_accounting_matches_typed_record_bytes():
    from demiflow.execution.contracts.record_codec import mapped_row_bytes
    from demiflow.execution.contracts.record_codec import _encode_payload as encode_payload

    rows = [{'i': -728, 'binary': b'\x91' * 32, 'nested': [{'word': '汉"\t\x00', 'xs': [1.0, -0.0]}]},
            {'i': None, 'binary': b'', 'nested': []}, {'i': 91, 'binary': None, 'nested': None}]
    batch = pa.Table.from_pylist(rows).to_batches()[0]
    assert [mapped_row_bytes(batch, i, 65536) for i in range(len(rows))] == [
        len(encode_payload(row, max_bytes=65536)) for row in batch.to_pylist()]
