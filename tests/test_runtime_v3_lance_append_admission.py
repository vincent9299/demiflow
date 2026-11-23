"""Append mismatches fail before native preparation or publication."""
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.errors import InvalidLanceRequest
from demiflow.execution.pipeline.run_context import RunContext
LIMITS = dict(memory_bytes=128*1024**2, max_rss_bytes=2*1024**3,
              max_row_bytes=65536, max_batch_bytes=2*1024**2, timeout_s=90)


@pytest.mark.parametrize('explicit', [False, True])
def test_native_append_schema_refused_before_staging_and_publication(tmp_path, monkeypatch, explicit):
    target = str(tmp_path/'target.lance')
    original = pa.table({'id':pa.array([1], type=pa.int64())})
    lance.write_dataset(original, target)
    calls=[]
    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError('fragment allocation before append schema admission')
    monkeypatch.setattr(lance.fragment.LanceFragment, 'create', forbidden)
    with pytest.raises(InvalidLanceRequest):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
            data.from_items([{'id':'wrong'}]).write_lance(target, mode='append',
                schema=pa.schema([('id',pa.string())]) if explicit else None)
    assert not calls
    assert lance.dataset(target).version == 1
    assert lance.dataset(target).to_table().equals(original)
    assert run.journal.execution_action('action-1')['result'] is None
    for fact in run.journal.execution_commits('action-1'):
        assert fact['status']=='pending' and fact['prepared'] is None and not fact['receipts']


def test_append_keeps_existing_explicit_schema_cast(tmp_path):
    target = str(tmp_path/'target.lance')
    schema = pa.schema([('id',pa.int64())])
    lance.write_dataset(pa.table({'id':[1]}, schema=schema), target)
    source = pa.table({'id':pa.array([2], type=pa.int32())})
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution():
        data.from_arrow(source).write_lance(target, mode='append', expected_version=1, schema=schema)
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
    assert lance.dataset(target).version==2
    assert lance.dataset(target).to_table().to_pylist()==[{'id':1},{'id':2}]
