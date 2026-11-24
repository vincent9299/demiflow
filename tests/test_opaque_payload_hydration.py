"""Hydration preserves row envelopes while bounding repeated native reads."""
import gc
import pickle
import weakref

import lance
import pyarrow as pa
import pytest

from demiflow.lance import read as opaque_payloads


def _stats():
    return dict(rows=0, batches=0, max_lookup_bytes=0, max_batch_bytes=0)


def _source(tmp_path, columns):
    uri = tmp_path / 'payloads.lance'
    schema = pa.schema([(name, pa.large_binary()) for name in columns])
    lance.write_dataset(pa.table(columns, schema=schema), uri)
    return {name: dict(field=name, size_field=name + '_n',
                       source=dict(uri=str(uri), version=1)) for name in columns}


def _batch(columns, offsets):
    result = {'ordinal': pa.array(range(len(next(iter(offsets.values())))), type=pa.uint64())}
    for name, indices in offsets.items():
        result[name] = pa.array(indices, type=pa.uint64())
        result[name + '_n'] = pa.array(
            [None if i is None else len(columns[name][i]) for i in indices], type=pa.uint64())
    return pa.record_batch(result)


@pytest.mark.parametrize('indices', [
    [2, 0, 2, None, 1, 2, 0], [0] * 31, list(range(3)), [None] * 7,
])
def test_real_lookup_preserves_positions_and_only_reads_distinct_offsets(tmp_path, monkeypatch, indices):
    columns = {'p': [pickle.dumps({'id': i, 'nested': [None, {'items': {i}}]}) for i in range(3)]}
    references = _source(tmp_path, columns)
    original = lance.dataset
    calls = []

    class Snapshot:
        def __init__(self, **source):
            self.dataset = original(**source)

        def take(self, offsets, *, columns):
            calls.append(list(offsets))
            return self.dataset.take(offsets, columns=columns)

    monkeypatch.setattr(lance, 'dataset', lambda **source: Snapshot(**source))
    report = _stats()
    result = list(opaque_payloads.hydrate_payloads(
        [_batch(columns, {'p': indices})], list(references.values()),
        max_row_bytes=4096, stats=report))
    assert len(result) == 1
    assert result[0].column('ordinal').to_pylist() == list(range(len(indices)))
    assert result[0].column('p').to_pylist() == [None if i is None else columns['p'][i] for i in indices]
    expected = list(dict.fromkeys(i for i in indices if i is not None))
    assert calls == ([expected] if expected else [])
    assert report['rows'] == len(indices)


def test_multiple_references_do_not_share_offset_identity(tmp_path):
    columns = {'p': [b'p-zero', b'p-one'], 'q': [b'q-zero', b'q-one']}
    references = _source(tmp_path, columns)
    indices = {'p': [0, 0, None, 1, 0], 'q': [1, 0, 1, None, 1]}
    result = list(opaque_payloads.hydrate_payloads(
        [_batch(columns, indices)], list(references.values()),
        max_row_bytes=4096, stats=_stats()))
    for name, offsets in indices.items():
        assert result[0].column(name).to_pylist() == [None if i is None else columns[name][i] for i in offsets]


@pytest.mark.parametrize('lengths', [[3, 2], [2, 3], [2, 2]])
def test_duplicate_length_mismatch_is_never_hidden(tmp_path, lengths):
    references = _source(tmp_path, {'p': [b'one']})
    batch = pa.record_batch({'p': [0, 0], 'p_n': lengths})
    with pytest.raises(RuntimeError, match='Pinned opaque payload lookup changed size or lost rows'):
        list(opaque_payloads.hydrate_payloads(
            [batch], list(references.values()), max_row_bytes=4096, stats=_stats()))


@pytest.mark.parametrize('offset,length,error', [
    (None, 3, 'Unmatched payload has a length'),
    (0, None, 'Matched payload has no length'),
    (0, -1, 'Invalid private payload length'),
])
def test_invalid_lengths_keep_existing_errors(tmp_path, offset, length, error):
    references = _source(tmp_path, {'p': [b'one']})
    batch = pa.record_batch({'p': pa.array([offset], type=pa.uint64()),
                            'p_n': pa.array([length], type=pa.int64())})
    with pytest.raises(ValueError, match=error):
        list(opaque_payloads.hydrate_payloads(
            [batch], list(references.values()), max_row_bytes=4096, stats=_stats()))


def test_repeated_payload_still_splits_by_expanded_bytes(tmp_path, monkeypatch):
    columns = {'p': [b'x' * 400]}
    references = _source(tmp_path, columns)
    monkeypatch.setattr(opaque_payloads, 'ARROW_BATCH_BYTES', 1024)
    report = _stats()
    result = list(opaque_payloads.hydrate_payloads(
        [_batch(columns, {'p': [0] * 11})], list(references.values()),
        max_row_bytes=1024, stats=report))
    assert [part.num_rows for part in result] == [2, 2, 2, 2, 2, 1]
    assert all(part.nbytes <= 1024 for part in result)
    assert report['rows'] == 11
    assert sum(part.column('p').nbytes for part in result) > 11 * 400


def test_combined_oversized_repeated_payload_rejects_before_take(monkeypatch):
    calls = []

    class Snapshot:
        def take(self, *args, **kwargs):
            calls.append(True)
            raise AssertionError('Oversized lookup must not allocate')

    monkeypatch.setattr(lance, 'dataset', lambda **source: Snapshot())
    references = [dict(field=n, size_field=n + '_n', source=dict(uri='private', version=1))
                  for n in ('p', 'q')]
    batch = pa.record_batch({'p': [0, 0], 'p_n': [600, 600], 'q': [0, 0], 'q_n': [600, 600]})
    with pytest.raises(MemoryError, match='max_row_bytes'):
        list(opaque_payloads.hydrate_payloads([batch], references, max_row_bytes=1024, stats=_stats()))
    assert calls == []


def test_multichunk_native_result_is_lossless(monkeypatch):
    class Snapshot:
        def take(self, indices, *, columns):
            values = [b'zero', b'one']
            return pa.table({'p': pa.chunked_array(
                [pa.array([values[i]], type=pa.large_binary()) for i in indices])})

    monkeypatch.setattr(lance, 'dataset', lambda **source: Snapshot())
    references = [dict(field='p', size_field='p_n', source=dict(uri='private', version=1))]
    batch = pa.record_batch({'p': [1, 0, 1, None], 'p_n': [3, 4, 3, None]})
    result = list(opaque_payloads.hydrate_payloads([batch], references, max_row_bytes=1024, stats=_stats()))
    assert result[0].column('p').to_pylist() == [b'one', b'zero', b'one', None]


def test_native_slice_does_not_retain_an_oversized_parent_buffer(monkeypatch):
    backing = pa.array([b'x' * 2**20, b'one'], type=pa.large_binary())

    class Snapshot:
        def take(self, indices, *, columns):
            assert indices == [1]
            return pa.table({'p': backing.slice(1, 1)})

    monkeypatch.setattr(lance, 'dataset', lambda **source: Snapshot())
    references = [dict(field='p', size_field='p_n', source=dict(uri='private', version=1))]
    batch = pa.record_batch({'p': [1], 'p_n': [3]})
    result = list(opaque_payloads.hydrate_payloads([batch], references, max_row_bytes=1024, stats=_stats()))
    value = result[0].column('p')
    assert value.to_pylist() == [b'one']
    assert value.get_total_buffer_size() <= value.nbytes + 8


def test_close_releases_snapshot_and_does_not_schedule_more_reads(monkeypatch):
    snapshots, calls = [], []

    class Snapshot:
        def take(self, indices, *, columns):
            calls.append(list(indices))
            return pa.table({'p': pa.array([b'x' * 400] * len(indices), type=pa.large_binary())})

    def open_snapshot(**source):
        value = Snapshot()
        snapshots.append(weakref.ref(value))
        return value

    monkeypatch.setattr(lance, 'dataset', open_snapshot)
    monkeypatch.setattr(opaque_payloads, 'ARROW_BATCH_BYTES', 1024)
    references = [dict(field='p', size_field='p_n', source=dict(uri='private', version=1))]
    batch = pa.record_batch({'p': [0] * 20, 'p_n': [400] * 20})
    rows = opaque_payloads.hydrate_payloads([batch], references, max_row_bytes=1024, stats=_stats())
    assert next(rows).num_rows == 2
    assert len(calls) == 1
    rows.close()
    gc.collect()
    assert len(calls) == 1
    assert all(ref() is None for ref in snapshots)


def test_public_join_decodes_repeated_mutable_envelopes_independently(tmp_path):
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    def annotate(row):
        before = row['payload']['items'][0]
        row['payload']['items'][0] = 99
        return {'tag': row['tag'], 'before_mutation': before}

    rows = (source.from_items([{'k': 1, 'payload': {'items': [0]}}])
            .join(source.from_items([{'k': 1, 'tag': 0}, {'k': 1, 'tag': 1}]), on='k')
            .map(annotate).take_all())
    assert sorted(rows, key=lambda row: row['tag']) == [
        {'tag': 0, 'before_mutation': 0}, {'tag': 1, 'before_mutation': 0}]
