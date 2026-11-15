"""Pre-constructor rejection and unchanged real Local/Ray columnar terminals."""
from contextlib import nullcontext
from dataclasses import replace
from decimal import Decimal
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_terminals import _convert_batch
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def _handle(maximum):
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    return SimpleNamespace(bounds=replace(bounds, max_batch_bytes=maximum,
                                          max_resident_bytes=max(maximum, 1 << 20)))


def _forbid_constructor(monkeypatch, fmt, calls):
    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError('third-party payload constructor called before admission')
    if fmt == 'pyarrow':
        class Table:
            from_pylist = staticmethod(forbidden)
        monkeypatch.setattr(pa, 'Table', Table)
    elif fmt == 'pandas':
        monkeypatch.setattr(pd.DataFrame, '__init__', forbidden)
    else:
        monkeypatch.setattr(np, 'asarray', forbidden)


@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas', 'numpy'])
def test_wide_input_and_output_rejected_before_constructor(monkeypatch, fmt):
    rows = [{'value': 'a' * 14000}, {'value': 'b' * 14000}]
    calls = []
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, fmt, calls)
        with pytest.raises(DataflowError, match='conversion admission') as error:
            _convert_batch(rows, fmt, _handle(32768))
        assert error.value.code == 'BUDGET_EXCEEDED'
    assert not calls
    result = _convert_batch(rows, fmt, _handle(512 * 1024))
    actual = (result.to_pylist() if fmt == 'pyarrow' else
              result.to_dict('records') if fmt == 'pandas' else
              [{'value': value} for value in result['value'].tolist()])
    assert actual == rows


@pytest.mark.parametrize('dtype', [pa.binary(4096), pa.list_(pa.int64(), 1024),
    pa.struct([('payload', pa.list_(pa.int64(), 1024))])])
@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas'])
def test_null_schema_layout_amplification_is_admitted_before_arrow(monkeypatch, dtype, fmt):
    rows = [{'value': None}]
    schema = pa.schema([pa.field('value', dtype, nullable=True)], metadata={b'intent': b'exact'})
    calls = []
    # Typed pandas calls Arrow first. Neither constructor may allocate the
    # intermediate even when the final frame would fit after that copy dies.
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'pyarrow', calls)
        with pytest.raises(DataflowError, match='conversion admission'):
            _convert_batch(rows, fmt, _handle(1024), schema=schema)
    assert not calls
    result = _convert_batch(rows, fmt, _handle(256 * 1024), schema=schema)
    if fmt == 'pyarrow':
        assert result.schema.equals(schema, check_metadata=True)
        assert result.to_pylist() == rows
    else:
        assert type(result) is pd.DataFrame and result.to_dict('records') == rows


def test_numpy_unicode_promotion_uses_widest_column_not_average(monkeypatch):
    rows = [{'value': 'z' * 2048}] + [{'value': 'a'} for _ in range(9)]
    calls = []
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'numpy', calls)
        with pytest.raises(DataflowError, match='conversion admission'):
            _convert_batch(rows, 'numpy', _handle(16384))
    assert not calls
    result = _convert_batch(rows, 'numpy', _handle(1 << 20))
    assert result['value'].dtype == np.dtype('U2048')
    assert result['value'].tolist() == [row['value'] for row in rows]


def test_numpy_schema_cannot_hide_an_allocating_row_column(monkeypatch):
    rows = [{'id': i, 'value': 'z' * 2048 if i == 0 else 'a'} for i in range(10)]
    calls = []
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'numpy', calls)
        with pytest.raises(DataflowError, match='conversion admission'):
            _convert_batch(rows, 'numpy', _handle(16384), schema=pa.schema([('id', pa.int64())]))
    assert not calls


@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas'])
def test_dictionary_map_and_nested_text_coercions_keep_native_values(fmt):
    schema = pa.schema([('category', pa.dictionary(pa.int8(), pa.string())),
        ('values', pa.map_(pa.string(), pa.int32())),
        ('nested', pa.struct([('text', pa.string())]))])
    rows = [{'category': 'a', 'values': [('k', 1)], 'nested': {'text': b'abc\xf0\x9f\x99\x82'}},
            {'category': None, 'values': None, 'nested': None}]
    expected = pa.Table.from_pylist(rows, schema=schema)
    actual = _convert_batch(rows, fmt, _handle(1 << 20), schema=schema)
    if fmt == 'pyarrow':
        assert actual.equals(expected, check_metadata=True)
    else:
        pd.testing.assert_frame_equal(actual, expected.to_pandas())


@pytest.mark.parametrize('rows', [
    [{'value': [1, 2]}, {'value': [3, 4]}],
    [{'value': None}, {'value': 'b'}],
    [{'value': 1}, {'value': 'b'}],
    [{'value': b'\xff'}, {'value': b'hello'}],
    [{'value': -0.0}, {'value': float('inf')}],
])
def test_numpy_shape_dtype_and_values_are_not_rewritten(rows):
    expected = np.asarray([row['value'] for row in rows])
    actual = _convert_batch(rows, 'numpy', _handle(1 << 20))['value']
    assert actual.dtype == expected.dtype and actual.shape == expected.shape
    np.testing.assert_array_equal(actual, expected)
    if actual.dtype.kind == 'f':
        assert np.signbit(actual[0])


@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas'])
def test_nested_types_metadata_and_nulls_remain_native(fmt):
    schema = pa.schema([
        pa.field('n', pa.int32(), nullable=False),
        pa.field('amount', pa.decimal256(40, 2)),
        pa.field('items', pa.large_list(pa.struct([('text', pa.large_string())]))),
        pa.field('pair', pa.list_(pa.float64(), 2)),
    ], metadata={b'original': b'unchanged'})
    rows = [{'n': 1, 'amount': Decimal('1.25'), 'items': [{'text': '中🙂'}], 'pair': [-0.0, 2.0]},
            {'n': 2, 'amount': None, 'items': [], 'pair': None}]
    expected = pa.Table.from_pylist(rows, schema=schema)
    actual = _convert_batch(rows, fmt, _handle(1 << 20), schema=schema)
    if fmt == 'pyarrow':
        assert actual.equals(expected, check_metadata=True)
    else:
        pd.testing.assert_frame_equal(actual, expected.to_pandas())


def test_unsupported_extension_is_explicit_before_constructor(monkeypatch):
    class Custom(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), 'demiflow-test-unbounded-custom')
        def __arrow_ext_serialize__(self):
            return b''
        @classmethod
        def __arrow_ext_deserialize__(cls, storage, serialized):
            return cls()
    calls = []
    schema = pa.schema([('value', Custom())])
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'pyarrow', calls)
        with pytest.raises(DataflowError, match='extension type') as error:
            _convert_batch([{'value': 1}], 'pyarrow', _handle(1 << 20), schema=schema)
    assert error.value.code == 'UNSUPPORTED_CAPABILITY' and not calls


@pytest.fixture(scope='module')
def provider():
    name = os.environ.get('DF_CONVERSION_PROVIDER', 'local')
    assert name in ('local', 'ray_core')
    if name == 'ray_core':
        from runtime_v3_recovery_scenarios import _provider
        from runtime_v3_support import _RayDataBlocker
        with _RayDataBlocker() as blocked, _provider(name):
            yield name
            assert not blocked.attempts
    else:
        yield name


def _local(provider, memory):
    return (data.local_execution(workers=2, worker_mode='process', memory_bytes=memory)
            if provider == 'local' else nullcontext())


def _consume(ds, terminal, fmt, count):
    if terminal == 'take_batch':
        return ds.take_batch(count, batch_format=fmt)
    batches = list(ds.iter_batches(batch_size=count, batch_format=fmt))
    assert len(batches) == 1
    return batches[0]


def _wide_rows():
    yield {'value': 'a' * 14000}
    yield {'value': 'b' * 14000}


@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas', 'numpy'])
@pytest.mark.parametrize('terminal', ['take_batch', 'iter_batches'])
def test_public_conversion_preflight_and_success(tmp_path, monkeypatch, provider, fmt, terminal):
    rows = [{'value': 'a' * 14000}, {'value': 'b' * 14000}]
    limits = {'timeout_s': 90, 'memory_bytes': 256 * 1024, 'max_batch_bytes': 32768,
              'max_row_bytes': 16384, 'max_state_bytes': 16384}
    calls = []
    with pytest.raises(DataflowError, match='conversion admission'):
        with RunContext.start(tmp_path / 'negative', limits=limits), _local(provider, limits['memory_bytes']):
            ds = data.from_iter(_wide_rows)
            with monkeypatch.context() as scoped:
                _forbid_constructor(scoped, fmt, calls)
                _consume(ds, terminal, fmt, 2)
    assert not calls
    limits.update(memory_bytes=4 * 1024 * 1024, max_batch_bytes=512 * 1024)
    with RunContext.start(tmp_path / 'positive', limits=limits) as run, _local(provider, limits['memory_bytes']):
        ds = data.from_iter(_wide_rows)
        result = _consume(ds, terminal, fmt, 2)
        actual = (result.to_pylist() if fmt == 'pyarrow' else result.to_dict('records')
                  if fmt == 'pandas' else [{'value': value} for value in result['value'].tolist()])
        assert actual == rows
        assert ds.execution_metadata().diagnostics['engine'] == 'dataflow_v3'
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_public_typed_null_schema_preserved(tmp_path, provider):
    schema = pa.schema([('blob', pa.binary(4096)), ('items', pa.list_(pa.int64(), 1024))],
                       metadata={b'original': b'yes'})
    table = pa.Table.from_pylist([{'blob': None, 'items': None}], schema=schema)
    limits = {'timeout_s': 90, 'memory_bytes': 4 * 1024 * 1024,
              'max_batch_bytes': 256 * 1024, 'max_row_bytes': 32768, 'max_state_bytes': 32768}
    with RunContext.start(tmp_path / 'run', limits=limits), _local(provider, limits['memory_bytes']):
        ds = data.from_arrow(table)
        assert ds.take_batch(1, batch_format='pyarrow').equals(table, check_metadata=True)
        pd.testing.assert_frame_equal(ds.take_batch(1, batch_format='pandas'), table.to_pandas())


_STANDARD_LAYOUTS = [
    (pa.string_view(), 'a' * 13 + '中🙂'),
    (pa.binary_view(), b'\xff' * 1000),
    (pa.list_view(pa.int64()), [1, 2, 3]),
    (pa.large_list_view(pa.int64()), [1, 2, 3]),
    (pa.run_end_encoded(pa.int16(), pa.int64()), 7),
    (pa.json_(), '{"n":1}'),
]


@pytest.mark.parametrize('dtype,value', _STANDARD_LAYOUTS)
@pytest.mark.parametrize('fmt', ['pyarrow', 'pandas'])
def test_standard_views_and_run_end_layouts_preflight_and_native_return(monkeypatch, dtype, value, fmt):
    rows = [{'value': value}, {'value': None}, {'value': value}]
    schema = pa.schema([('value', dtype)], metadata={b'exact': b'original'})
    calls = []
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'pyarrow', calls)
        with pytest.raises(DataflowError, match='conversion admission'):
            _convert_batch(rows, fmt, _handle(1024), schema=schema)
    assert not calls
    expected = pa.Table.from_pylist(rows, schema=schema)
    actual = _convert_batch(rows, fmt, _handle(1 << 20), schema=schema)
    if fmt == 'pyarrow':
        assert actual.equals(expected, check_metadata=True)
    else:
        pd.testing.assert_frame_equal(actual, expected.to_pandas())


def test_inline_view_and_noninline_heap_have_distinct_allocation_bounds(monkeypatch):
    schema = pa.schema([('value', pa.string_view())])
    # 12 bytes fit the view descriptor. The 13th needs a real 32KiB heap block,
    # even though final compaction leaves a tiny 13-byte payload.
    actual = _convert_batch([{'value': 'a' * 12}], 'pyarrow', _handle(4096), schema=schema)
    assert actual.to_pylist() == [{'value': 'a' * 12}]
    calls = []
    with monkeypatch.context() as scoped:
        _forbid_constructor(scoped, 'pyarrow', calls)
        with pytest.raises(DataflowError, match='conversion admission'):
            _convert_batch([{'value': 'a' * 13}], 'pyarrow', _handle(4096), schema=schema)
    assert not calls


@pytest.mark.parametrize('dtype,value', _STANDARD_LAYOUTS)
def test_public_standard_layout_schema_and_pandas(tmp_path, provider, dtype, value):
    schema = pa.schema([('value', dtype)], metadata={b'source': b'keep'})
    table = pa.Table.from_pylist([{'value': value}, {'value': None}, {'value': value}], schema=schema)
    limits = {'timeout_s': 90, 'memory_bytes': 4 * 1024 * 1024,
              'max_batch_bytes': 256 * 1024, 'max_row_bytes': 32768, 'max_state_bytes': 32768}
    with RunContext.start(tmp_path / 'run', limits=limits), _local(provider, limits['memory_bytes']):
        ds = data.from_arrow(table)
        actual = ds.take_batch(3, batch_format='pyarrow')
        assert actual.equals(table, check_metadata=True)
        batches = list(ds.iter_batches(batch_size=3, batch_format='pandas'))
        assert len(batches) == 1
        pd.testing.assert_frame_equal(batches[0], table.to_pandas())
