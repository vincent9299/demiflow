"""Private row-offset hydration enforces its budget before native take."""
import lance
import pyarrow as pa
import pytest

from demiflow.execution.opaque_payloads import hydrate_payloads


def stats():
    return dict(rows=0, batches=0, max_lookup_bytes=0, max_batch_bytes=0)


def test_combined_payload_budget_rejects_before_lookup(monkeypatch):
    calls = []
    class Snapshot:
        def take(self, *args, **kwargs):
            calls.append(args)
            raise AssertionError('Oversized lookup must not allocate')
    monkeypatch.setattr(lance, 'dataset', lambda **kwargs: Snapshot())
    refs = [dict(field=f'p{i}', size_field=f'n{i}', source=dict(uri='private', version=1)) for i in range(2)]
    batch = pa.record_batch({'p0': [0], 'n0': [5 * 2**20], 'p1': [0], 'n1': [5 * 2**20]})
    with pytest.raises(MemoryError, match='max_row_bytes'):
        list(hydrate_payloads([batch], refs, max_row_bytes=8 * 2**20, stats=stats()))
    assert calls == []


def test_offsets_preserve_duplicates_order_nulls_and_detect_changed_length(tmp_path):
    path = tmp_path / 'private.lance'
    lance.write_dataset(pa.table({'p': pa.array([b'one'], type=pa.large_binary())}), path)
    saved = lance.write_dataset(pa.table({'p': pa.array([b'second'], type=pa.large_binary())}), path, mode='append')
    assert len(saved.get_fragments()) == 2
    refs = [dict(field='p', size_field='n', source=dict(uri=str(path), version=2))]
    lance.write_dataset(pa.table({'p': pa.array([b'new head'], type=pa.large_binary())}), path, mode='overwrite')
    batch = pa.record_batch({'p': [1, None, 0, 1], 'n': [6, None, 3, 6], 'key': ['b', 'x', 'a', 'b']})
    report = stats()
    values = list(hydrate_payloads([batch], refs, max_row_bytes=8 * 2**20, stats=report))
    assert len(values) == 1
    assert values[0].column('p').to_pylist() == [b'second', None, b'one', b'second']
    assert values[0].column('key').to_pylist() == ['b', 'x', 'a', 'b']
    assert report['rows'] == 4
    with pytest.raises(RuntimeError, match='changed size'):
        list(hydrate_payloads([pa.record_batch({'p': [0], 'n': [2]})], refs,
                             max_row_bytes=8 * 2**20, stats=stats()))
