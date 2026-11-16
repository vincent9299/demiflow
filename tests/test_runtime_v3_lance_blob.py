"""Native blob storage keeps its original construction and finite admission."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import InvalidLanceRequest
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError

LIMITS = dict(memory_bytes=128*1024**2, max_rss_bytes=2*1024**3,
              max_row_bytes=65536, max_batch_bytes=2*1024**2, timeout_s=90)


def _schema():
    field = lance.blob_field('data').with_metadata({b'origin':b'original'})
    return pa.schema([('id', pa.int64()), field], metadata={b'table':b'kept'})


def test_real_dataset_blob_bytes_null_empty_and_metadata(tmp_path):
    schema, target = _schema(), str(tmp_path/'blobs.lance')
    binary = b'full-original\x00\xff'*500
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution(batch_rows=3):
        receipt = data.from_items([{'id':0,'data':binary}, {'id':1,'data':None},
                                  {'id':2,'data':b''}]).write_lance(target, mode='create', schema=schema,
                                                                  return_receipt=True)
        assert receipt.written_rows == 3 and run.journal.execution_commits('action-1')[-1]['status']=='acked'
    dataset = lance.dataset(target)
    assert dataset.schema.equals(schema, check_metadata=True)
    values = dataset.take_blobs('data', indices=[0,1,2])
    try:
        assert values[0].read() == binary and values[1] is None and values[2].read() == b''
    finally:
        for value in values:
            if value is not None:
                value.close()


def test_native_blob_uri_remains_external_reference_without_file_materialization(tmp_path):
    directory = tmp_path/'external'
    directory.mkdir()
    source = directory/'external.bin'
    source.write_bytes(b'old-external-bytes')
    target = str(tmp_path/'uri.lance')
    from lance.dataset import DatasetBasePath
    empty = pa.Table.from_arrays([pa.array([], type=pa.int64()), lance.blob_array([])], schema=_schema())
    lance.write_dataset(empty, target, initial_bases=[DatasetBasePath(str(directory), name='external')])
    with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution():
        data.from_items([{'id':0,'data':str(source)}]).write_lance(target, mode='overwrite', schema=_schema(), expected_version=1)
    # Same-size replacement proves the write stored the original URI semantics,
    # not a driver-created full-file bytes value or copied payload snapshot.
    source.write_bytes(b'new-external-bytes')
    value = lance.dataset(target).take_blobs('data', indices=[0])[0]
    try:
        assert value.read() == b'new-external-bytes'
    finally:
        value.close()


def test_blob_constructor_is_not_called_when_input_and_storage_do_not_fit(tmp_path, monkeypatch):
    calls = []
    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError('blob constructor before admission')
    monkeypatch.setattr(lance, 'blob_array', forbidden)
    # R/B describe one business payload, while Python+Arrow+writer copies
    # occupy the separately sealed publication workspace within original M.
    # Keep the original 14KB row and caps; make their actual coexistence fail.
    limits = {**LIMITS, 'memory_bytes':384*1024, 'max_batch_bytes':32768, 'max_row_bytes':32768}
    target = tmp_path/'rejected.lance'
    with pytest.raises(DataflowError, match='batch conversion admission') as error:
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(batch_rows=1):
            data.from_items([{'id':0,'data':b'x'*14000}]).write_lance(str(target), mode='create', schema=_schema())
    assert error.value.code=='BUDGET_EXCEEDED' and not calls
    assert not (target/'_versions').exists()
    assert run.journal.execution_action('action-1')['result'] is None
    for fact in run.journal.execution_commits('action-1'):
        assert fact['status']=='pending' and fact['prepared'] is None and not fact['receipts']


def test_same_blob_payload_fits_original_large_run_and_individual_batch_caps(tmp_path):
    limits = {**LIMITS, 'max_batch_bytes':32768, 'max_row_bytes':32768}
    target = tmp_path/'admitted.lance'
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(batch_rows=1):
        data.from_items([{'id':0,'data':b'x'*14000}]).write_lance(str(target), mode='create', schema=_schema())
    dataset = lance.dataset(str(target))
    assert dataset.version == 1
    value = dataset.take_blobs('data', indices=[0])[0]
    try:
        assert value.read() == b'x'*14000
    finally:
        value.close()


