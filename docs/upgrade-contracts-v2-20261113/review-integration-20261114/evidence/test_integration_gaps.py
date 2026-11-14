"""Independent positive requirements; failures are review evidence, not repaired here."""
from pathlib import Path
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext
from test_embeddings import server as embedding_server


def test_public_lance_copy_seals_executed_plan(tmp_path):
    source=tmp_path/'in.lance'
    lance.write_dataset(pa.table({'id':[1,2]}), str(source))
    with RunContext.start(tmp_path/'run') as run:
        data.read_lance(str(source),version=1).write_lance(str(tmp_path/'out.lance'),mode='create')
        assert run.journal.source_binding('action-1-sealed-plan') is not None


def test_public_grouped_materialization_is_usable(tmp_path):
    with RunContext.start(tmp_path/'run'):
        groups=data.from_items([{'key':'a','id':i} for i in range(3)]).group_batches(
            'key',max_rows=2,flush_interval=1,chunk_bytes=4096,buffer_bytes=8192).materialize().take_all()
    assert sum(len(group['items']) for group in groups)==3


def test_public_prepend_materialization_is_usable(tmp_path):
    with RunContext.start(tmp_path/'run'):
        result=data.from_items([{'id':1}]).prepend(data.from_items([{'id':0}]),max_rows=1).materialize().take_all()
    assert result==[{'id':0},{'id':1}]


def test_public_embedding_materialization_aligns_all_workers(embedding_server,tmp_path):
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':2,'checkpoint_interval_ms':60000,'checkpoint_timeout_s':5}):
        result=data.from_items([{'text':str(i)} for i in range(4)]).map_embeddings(
            model=embedding_server['model'],inputs={'text':'text'}, batch_size=2,concurrency=2,
            max_requests=8,options={'trust_env':False}).materialize().take_all()
    assert len(result)==4


def test_arrow_rechunk_accounts_retained_parent_buffers():
    from demiflow.execution.arrow_batch_sink import rechunk
    parent=pa.record_batch({'x':list(range(1024))})
    iterator=rechunk([parent], 1, max_bytes=16)
    try:
        result=next(iterator)
        assert result.get_total_buffer_size()<=16, (result.nbytes,result.get_total_buffer_size())
    finally: iterator.close()
