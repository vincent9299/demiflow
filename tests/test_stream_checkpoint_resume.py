import asyncio

import lance
import pyarrow as pa
import pytest

from demiflow import data


SCHEMA = pa.schema([('id', pa.string()), ('value', pa.string())])


def test_append_checkpoint_preserves_head_and_verifies_repeated_keys(tmp_path):
    uri = tmp_path/'out.lance'
    def save(rows):
        return data.from_items(rows).save_lance(uri, schema=SCHEMA, key='id',
            stage='out', mode='append', max_batch=1, output_ref='ref').run_stream()
    first = save([{'id': 'a', 'value': 'one'}]).outputs['out']
    replay = save([{'id': 'a', 'value': 'one'}]).outputs['out']
    assert replay == first
    second = save([{'id': 'b', 'value': 'two'}]).outputs['out']
    assert lance.dataset(**first).count_rows() == 1
    assert lance.dataset(**second).count_rows() == 2
    with pytest.raises(ValueError, match='conflicting'):
        save([{'id': 'a', 'value': 'changed'}])
    assert lance.dataset(str(uri)).version == second['version']


def test_prepend_injects_pending_at_boundary_even_when_upstream_empty(tmp_path):
    seen = []
    upstream = []
    (data.from_items([]).map_async(lambda r: upstream.append(r) or r)
        .prepend(data.from_items([{'id': 'pending', 'value': 'saved'}]), max_rows=1)
        .map(lambda r: seen.append(r['id']) or r)
        .save_lance(tmp_path/'out.lance', schema=SCHEMA, key='id', stage='out').run_stream())
    assert upstream == [] and seen == ['pending']


def test_prepend_streams_without_waiting_for_upstream(tmp_path):
    released = asyncio.Event()
    seen = []
    async def wait(row):
        await asyncio.wait_for(released.wait(), 3)
        return row
    def consume(row):
        seen.append(row['id'])
        released.set()
        return row
    (data.from_items([{'id': 'new', 'value': 'x'}]).map_async(wait)
        .prepend(data.from_items([{'id': 'old', 'value': 'y'}]), max_rows=1)
        .map(consume).save_lance(tmp_path/'out.lance', schema=SCHEMA, key='id', stage='out')
        .run_stream())
    assert seen == ['old', 'new']


@pytest.mark.parametrize('rows,bounds,error', [
    ([{'id': 'a'}], {'max_rows': 0}, 'max_rows'),
    ([{'id': 'a', 'payload': 'x'*1000}], {'max_rows': 1, 'max_row_bytes': 512}, 'chunk_bytes'),
])
def test_prepend_bounds(rows, bounds, error):
    with pytest.raises(ValueError, match=error):
        data.from_items([]).prepend(data.from_items(rows), **bounds).run_stream()


def test_save_when_passes_unsaved_rows_and_append_bound(tmp_path):
    seen = []
    uri = tmp_path/'out.lance'
    (data.from_items([{'id': 'a', 'value': 'one'}, {'id': 'skip'}])
        .save_lance(uri, schema=SCHEMA, key='id', stage='out', mode='append',
            when=lambda r: r['id'] != 'skip', max_rows=1)
        .map(lambda r: seen.append(r['id']) or r).run_stream())
    assert seen == ['a', 'skip'] and lance.dataset(str(uri)).count_rows() == 1
    with pytest.raises(ValueError, match='max_rows'):
        (data.from_items([{'id': 'b', 'value': 'two'}])
            .save_lance(uri, schema=SCHEMA, key='id', stage='out', mode='append', max_rows=1).run_stream())


def test_global_checkpoint_recovers_commit_before_manifest_publication(tmp_path, monkeypatch):
    from demiflow.execution.stream_checkpoint import StreamCheckpoint
    path = tmp_path/'checkpoint.json'
    checkpoint = StreamCheckpoint(path, identity={'input': 1})
    original = checkpoint.committed
    def interrupt(stage, ref):
        raise OSError('crash after table commit')
    monkeypatch.setattr(checkpoint, 'committed', interrupt)
    with pytest.raises(OSError, match='crash'):
        (data.from_items([{'id': 'a', 'value': 'one'}])
            .save_lance(tmp_path/'out.lance', schema=SCHEMA, key='id', stage='out', mode='append')
            .run_stream(checkpoint=checkpoint))
    recovered = StreamCheckpoint(path, identity={'input': 1})
    assert recovered.state['pending'] is None
    assert lance.dataset(**recovered.state['outputs']['out']).count_rows() == 1
    with pytest.raises(ValueError, match='identity'):
        StreamCheckpoint(path, identity={'input': 2})


def test_global_checkpoint_never_publishes_downstream_without_upstream(tmp_path):
    from demiflow.execution.stream_checkpoint import StreamCheckpoint
    checkpoint = StreamCheckpoint(tmp_path/'checkpoint.json', identity='graph-v1')
    vectors = []
    original = checkpoint.publish
    def publish():
        original()
        if 'down' in checkpoint.state['outputs']:
            vectors.append(dict(checkpoint.state['outputs']))
    checkpoint.publish = publish
    stats = (data.from_items([{'id': 'a', 'value': 'one'}])
        .save_lance(tmp_path/'up.lance', schema=SCHEMA, key='id', stage='up', mode='append')
        .save_lance(tmp_path/'down.lance', schema=SCHEMA, key='id', stage='down', mode='append')
        .run_stream(checkpoint=checkpoint))
    assert stats.outputs == checkpoint.state['outputs']
    for vector in vectors:
        assert lance.dataset(**vector['down']).count_rows() <= lance.dataset(**vector['up']).count_rows()


def test_checkpoint_rejects_missing_stage_and_foreign_table_head(tmp_path):
    from demiflow import StreamCheckpoint
    path = tmp_path/'checkpoint.json'
    checkpoint = StreamCheckpoint(path, identity='graph')
    uri = tmp_path/'out.lance'
    (data.from_items([{'id': 'a', 'value': 'one'}])
        .save_lance(uri, schema=SCHEMA, key='id', stage='out', mode='append')
        .run_stream(checkpoint=checkpoint))
    with pytest.raises(ValueError, match='missing'):
        data.from_items([]).map_async(lambda r: r).run_stream(checkpoint=checkpoint)
    data.from_items([]).write_lance(str(uri), schema=SCHEMA, mode='overwrite')
    with pytest.raises(ValueError, match='outside'):
        StreamCheckpoint(path, identity='graph')
