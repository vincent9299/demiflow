"""Native CSV declarations retain all parser semantics and original history."""
from enum import Enum
import functools
import hashlib

import cloudpickle
import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from demiflow.execution.contracts.recovery_contract import document, identity, RecoveryContractError
from demiflow.execution.seal.program_identity import describe


@pytest.mark.parametrize('factory,field,value', [
    (pacsv.ParseOptions, 'delimiter', '\t'),
    (pacsv.ParseOptions, 'quote_char', False),
    (pacsv.ParseOptions, 'double_quote', False),
    (pacsv.ParseOptions, 'escape_char', '\\'),
    (pacsv.ParseOptions, 'newlines_in_values', True),
    (pacsv.ParseOptions, 'ignore_empty_lines', False),
    (pacsv.ReadOptions, 'use_threads', False),
    (pacsv.ReadOptions, 'block_size', 4096),
    (pacsv.ReadOptions, 'skip_rows', 1),
    (pacsv.ReadOptions, 'skip_rows_after_names', 1),
    (pacsv.ReadOptions, 'column_names', ['id', 'value']),
    (pacsv.ReadOptions, 'autogenerate_column_names', True),
    (pacsv.ReadOptions, 'encoding', 'latin1'),
    (pacsv.ConvertOptions, 'check_utf8', False),
    (pacsv.ConvertOptions, 'column_types', {'id': pa.int32()}),
    (pacsv.ConvertOptions, 'default_column_type', pa.string()),
    (pacsv.ConvertOptions, 'null_values', ['missing']),
    (pacsv.ConvertOptions, 'true_values', ['yes']),
    (pacsv.ConvertOptions, 'false_values', ['no']),
    (pacsv.ConvertOptions, 'decimal_point', ','),
    (pacsv.ConvertOptions, 'timestamp_parsers', [pacsv.ISO8601, '%Y/%m/%d']),
    (pacsv.ConvertOptions, 'strings_can_be_null', True),
    (pacsv.ConvertOptions, 'quoted_strings_can_be_null', False),
    (pacsv.ConvertOptions, 'auto_dict_encode', True),
    (pacsv.ConvertOptions, 'auto_dict_max_cardinality', 2),
    (pacsv.ConvertOptions, 'include_columns', ['id']),
    (pacsv.ConvertOptions, 'include_missing_columns', True),
])
def test_each_native_option_is_bound_and_survives_actual_serialization(factory, field, value):
    original = factory()
    changed = factory(**{field: value})
    assert identity(describe(original)) != identity(describe(changed))
    assert describe(cloudpickle.loads(cloudpickle.dumps(changed))) == describe(changed)
    assert describe(factory(**{field: value})) == describe(changed)
    data_fields = {n for n in dir(original) if not n.startswith('_') and not callable(getattr(original, n))}
    assert set(describe(original)['fields']) == data_fields


def _handler(row, *, decision='skip'):
    return decision


def test_handler_code_closure_defaults_and_partial_bindings_are_distinct_without_calls():
    def factory(decision):
        def handler(row):
            raise AssertionError('description must not execute callback: ' + decision)
        return handler
    handlers = [factory('skip'), factory('error'), _handler,
                functools.partial(_handler, decision='skip'), functools.partial(_handler, decision='error')]
    identities = [identity(describe(pacsv.ParseOptions(invalid_row_handler=h))) for h in handlers]
    assert len(set(identities)) == len(handlers)
    for handler in handlers:
        option = pacsv.ParseOptions(invalid_row_handler=handler)
        assert describe(cloudpickle.loads(cloudpickle.dumps(option))) == describe(option)


def test_native_column_types_and_iso_parser_order_do_not_collapse():
    first = pacsv.ConvertOptions(column_types={'value': pa.timestamp('ms', 'UTC'), 'id': pa.int32()})
    reordered = pacsv.ConvertOptions(column_types={'id': pa.int32(), 'value': pa.timestamp('ms', 'UTC')})
    changed = pacsv.ConvertOptions(column_types={'id': pa.int32(), 'value': pa.timestamp('us', 'UTC')})
    assert identity(describe(first)) == identity(describe(reordered))
    assert identity(describe(first)) != identity(describe(changed))
    assert describe(pacsv.ConvertOptions(timestamp_parsers=[pacsv.ISO8601, '%Y'])) != describe(
        pacsv.ConvertOptions(timestamp_parsers=['%Y', pacsv.ISO8601]))


def test_csv_extension_does_not_rewrite_historical_primitive_or_long_list_bytes():
    old = {'legacy': [None, True, 1, -0.0, 'é'], 'binary': b'abc'}
    expected = {'legacy': [None, True, 1, -0.0, 'é'],
                'binary': {'bytes_sha256': hashlib.sha256(b'abc').hexdigest(), 'bytes': 3}}
    assert document(describe(old)) == document(expected)
    values = [str(i) for i in range(1500)]
    hashed = hashlib.sha256(b''.join(document(v)+b'\n' for v in values)).hexdigest()
    assert describe(values) == {'items': 1500, 'sha256': hashed}
    class Choice(Enum):
        ONE = 'one'
    assert describe(Choice.ONE) == {'enum': Choice.__module__+'.'+Choice.__qualname__, 'value': 'one'}


def test_source_snapshot_options_keep_old_digest_and_native_complete_semantics():
    import pickle
    from demiflow.execution.sources.snapshots import _options_digest
    original = {'columns': ['id'], 'include_paths': True, 'format': 'jsonl'}
    assert _options_digest(original) == hashlib.sha256(pickle.dumps(original, protocol=4)).hexdigest()
    for options in ({'parse_options': pacsv.ParseOptions(delimiter=';')},
                    {'read_options': pacsv.ReadOptions(block_size=4096)},
                    {'convert_options': pacsv.ConvertOptions(column_types={'id': pa.int32()})}):
        assert _options_digest(options) == _options_digest(cloudpickle.loads(cloudpickle.dumps(options)))
    assert _options_digest({'parse_options': pacsv.ParseOptions(delimiter=';')}) != _options_digest(
        {'parse_options': pacsv.ParseOptions(delimiter='\t')})
