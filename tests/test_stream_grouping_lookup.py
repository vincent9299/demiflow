import asyncio
import lance
import pyarrow as pa
import pytest
from demiflow import data
from pathlib import Path
from _worker_event_fixtures import emit, events


def test_keyed_timeout_releases_downstream_before_upstream_finishes(tmp_path):
    # The timer must deliver a batch while the third upstream callback waits.
    # A bounded filesystem gate observes this across actual managed workers;
    # it does not assume their asyncio loops share a driver Event object.
    gate = str(tmp_path / 'downstream-ready')
    path = str(tmp_path / 'seen.jsonl')
    async def source(row):
        if row['id'] == 3:
            async def wait_for_downstream():
                while not Path(gate).exists():
                    await asyncio.sleep(.001)
            await asyncio.wait_for(wait_for_downstream(), 2)
        return row
    async def sink(group):
        assert all(r['key'] == group['key'] for r in group['items'])
        for row in group['items']:
            emit(path, 'seen', id=row['id'])
        Path(gate).touch()
        return group
    (data.from_items([{'key': 'a', 'id': 1}, {'key': 'b', 'id': 2}, {'key': 'a', 'id': 3}])
        .map_async(source, concurrency=3, queue_depth=1)
        .group_batches('key', max_rows=4, flush_interval=.03, chunk_bytes=4096, buffer_bytes=8192)
        .map_async(sink, concurrency=1, queue_depth=1).run_stream())
    assert sorted(event['id'] for event in events(path)) == [1, 2, 3]


def test_keyed_pressure_flush_and_oversize_do_not_drop_rows(tmp_path):
    path = str(tmp_path / 'pressure.jsonl')
    def collect(group):
        for row in group['items']:
            emit(path, 'seen', row=row)
        return group
    (data.from_items([{'key': str(i), 'text': 'x'*600} for i in range(8)])
        .group_batches('key', max_rows=4, flush_interval=10, chunk_bytes=2048, buffer_bytes=2048, max_groups=2)
        .map(collect).run_stream())
    seen = [event['row'] for event in events(path)]
    assert len(seen) == 8
    assert sorted(seen, key=lambda r: r['key']) == [{'key': str(i), 'text': 'x'*600} for i in range(8)]
    with pytest.raises(ValueError, match='chunk_bytes'):
        data.from_items([{'key': 'a', 'text': 'x'*10000}]).group_batches(
            'key', flush_interval=.01, chunk_bytes=2048, buffer_bytes=2048).run_stream()


def test_lookup_is_fixed_bounded_and_does_not_share_mutable_cached_rows(tmp_path):
    uri = str(tmp_path/'cache.lance')
    schema = pa.schema([('id', pa.string()), ('value', pa.string())])
    lance.write_dataset(pa.Table.from_pylist([{'id': 'a', 'value': 'old'}], schema=schema), uri)
    lance.write_dataset(pa.Table.from_pylist([{'id': 'a', 'value': 'new'}], schema=schema), uri, mode='overwrite')
    path = str(tmp_path / 'cache-observations.jsonl')
    def collect(row):
        emit(path, 'seen', value=row['matches'][0]['value'] if row['matches'] else None)
        if row['matches']:
            row['matches'][0]['value'] = 'mutated'
        return row
    (data.from_items([{'id': 'a'}, {'id': 'a'}, {'id': 'missing'}])
        .lookup_lance({'uri': uri, 'version': 1}, on='id').map(collect).run_stream())
    assert [event['value'] for event in events(path)] == ['old', 'old', None]
    lance.write_dataset(pa.Table.from_pylist([{'id': 'a', 'value': '1'}, {'id': 'a', 'value': '2'}], schema=schema), uri, mode='overwrite')
    with pytest.raises(ValueError, match='match/byte'):
        data.from_items([{'id': 'a'}]).lookup_lance({'uri': uri, 'version': 3}, on='id').run_stream()


def test_composite_stream_keys_preserve_same_sha_for_two_concepts(tmp_path):
    schema = pa.schema([('concept', pa.string()), ('sha', pa.string())])
    stats = (data.from_items([{'concept': 'a', 'sha': 'same'}, {'concept': 'b', 'sha': 'same'}])
        .save_lance(tmp_path/'results.lance', schema=schema, key=['concept', 'sha'], stage='out', max_batch=1)
        .run_stream())
    assert lance.dataset(**stats.outputs['out']).count_rows() == 2


def test_lookup_nested_list_schema_and_quoted_key(tmp_path):
    uri = str(tmp_path/'nested.lance')
    schema = pa.schema([('id', pa.string()), ('tags', pa.list_(pa.string())),
                        ('ref', pa.struct([('uri', pa.string()), ('version', pa.int64())]))])
    rows = [{'id': "x' OR TRUE --", 'tags': ['a'], 'ref': {'uri': 'test', 'version': 1}},
            {'id': 'different', 'tags': [], 'ref': None}]
    lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), uri)
    path = str(tmp_path / 'nested-observations.jsonl')
    def collect(row):
        for match in row['matches']:
            emit(path, 'seen', row=match)
        return row
    (data.from_items([{'id': rows[0]['id']}])
        .lookup_lance({'uri': uri, 'version': 1}, on='id')
        .map(collect).run_stream())
    assert [event['row'] for event in events(path)] == rows[:1]


def test_composite_nested_commit_reconciliation_does_not_resend(tmp_path, monkeypatch):
    from demiflow.execution.native_queue import stream_lance
    schema = pa.schema([('concept', pa.string()), ('sha', pa.string()), ('tags', pa.list_(pa.string()))])
    writer = stream_lance.StreamLanceWriter(tmp_path/'ambiguous.lance', schema, key=['concept', 'sha'])
    writer.initialize()
    original = stream_lance.write_lance
    calls = []
    def committed_then_disconnected(*args, **kwargs):
        calls.append(True)
        original(*args, **kwargs)
        raise OSError('connection lost after commit')
    monkeypatch.setattr(stream_lance, 'write_lance', committed_then_disconnected)
    rows = [{'concept': "a'", 'sha': 'same', 'tags': ['nested']},
            {'concept': 'b', 'sha': 'same', 'tags': []}]
    assert writer._write(rows) == rows
    assert calls == [True]
    assert lance.dataset(**writer.reference()).count_rows() == 2
