"""The public preview seam preserves budgets, value representation and ownership."""
import datetime
import json
import resource
from types import SimpleNamespace

import pyarrow as pa
import pytest

from demiflow.lance import preview


def test_preview_value_preserves_special_values_without_changing_input():
    value = {'bytes': b'\x00\xff', 'day': datetime.date(2026, 11, 19),
             'time': datetime.time(1, 2, 3), 'values': (float('inf'), float('nan'), None)}
    actual = preview.preview_value(value)
    assert actual == {'bytes': {'type': 'bytes', 'base64': 'AP8='},
        'day': '2026-11-19', 'time': '01:02:03',
        'values': [{'type': 'float', 'value': 'inf'}, {'type': 'float', 'value': 'nan'}, None]}
    json.dumps(actual, allow_nan=False)
    assert value['bytes'] == b'\x00\xff' and isinstance(value['values'], tuple)


class Reader:
    def __init__(self, batch):
        self.batch, self.closed = batch, False
    def __iter__(self):
        yield self.batch
    def close(self):
        self.closed = True


def table(monkeypatch, text='small'):
    # Unit seam only; real confined decoding is covered by development inputs.
    monkeypatch.setattr(preview.resource, 'getrlimit', lambda _: (4 * 1024**3, 4 * 1024**3))
    batch = pa.record_batch({'text': [text], '_rowid': [17]})
    reader, calls = Reader(batch), []
    def scanner(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(to_batches=lambda: reader)
    dataset = SimpleNamespace(schema=batch.schema, scanner=scanner)
    monkeypatch.setattr(preview, '_open', lambda *a, **k: pytest.fail('opened another snapshot'))
    return dataset, reader, calls


def test_sample_keeps_open_snapshot_scanner_limits_rowids_and_close(monkeypatch):
    dataset, reader, calls = table(monkeypatch)
    rows = preview.sample_open_table(dataset, columns=['text'], limit=1, maximum=1024)
    assert rows == [{'text': 'small', '_rowid': '17'}]
    assert reader.closed
    assert calls == [dict(columns={'text': '`text`'}, offset=0, limit=1, filter=None,
        batch_size=1, batch_size_bytes=1024, batch_readahead=1, fragment_readahead=1,
        io_buffer_size=1024**2, with_row_id=True, scan_in_order=True, use_scalar_index=False)]


def test_native_buffer_budget_failure_still_closes_reader(monkeypatch):
    dataset, reader, _ = table(monkeypatch, text='x' * 100)
    with pytest.raises(MemoryError, match='native buffers'):
        preview.sample_open_table(dataset, columns=['text'], limit=1, maximum=16)
    assert reader.closed


@pytest.mark.parametrize('maximum', [0, -1, True, 262145, 1.5])
def test_invalid_byte_budget_rejected_without_native_read(maximum):
    with pytest.raises(ValueError, match='byte budget'):
        preview.sample_open_table(object(), columns=['text'], maximum=maximum)


def test_sample_requires_existing_worker_allocation_guard(monkeypatch):
    monkeypatch.setattr(preview.resource, 'getrlimit', lambda _: (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
    with pytest.raises(RuntimeError, match='isolated worker'):
        preview.sample_open_table(object(), columns=['text'])


def test_sample_does_not_accept_a_query_expression():
    with pytest.raises(TypeError, match='filter'):
        preview.sample_open_table(object(), columns=['text'], filter='arbitrary query')
