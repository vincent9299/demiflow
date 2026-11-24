"""Pure standard operations retain typed ports and borrowed-value boundaries."""
import importlib.util
import json

import pytest

from demiflow.data.runtime_contract import check_value
from demiflow.forge.operator_check import OperatorChecker
from demiflow.forge.source import SourceTree


def inspect(tmp_path, source):
    (tmp_path / 'operators').mkdir(exist_ok=True)
    path = tmp_path / 'operators/pure.py'
    path.write_text(source)
    tree = SourceTree(tmp_path)
    checker = OperatorChecker(tree)
    description = checker.describe(tree.load('operators.pure')['convert'])
    checker.check(description)
    spec = importlib.util.spec_from_file_location('operators.pure', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.convert, description


def test_json_requires_builtin_guards_and_preserves_invalid_input(tmp_path):
    convert, port = inspect(tmp_path, '''
import json
from typing import TypedDict
class Result(TypedDict):
    values: list[str]
    error: str | None
def convert(raw: str) -> Result:
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return {'values': [], 'error': 'not a mapping'}
        items = payload.get('names')
        if not isinstance(items, list):
            return {'values': [], 'error': 'missing names'}
        values: list[str] = []
        for item in items:
            if not isinstance(item, str):
                return {'values': [], 'error': 'not text'}
            values.append(item)
        return {'values': sorted(set(values)), 'error': None}
    except ValueError:
        return {'values': [], 'error': 'invalid JSON'}
''')
    for raw, expected in [('broken', 'invalid JSON'), ('null', 'not a mapping'),
                           ('{}', 'missing names'), ('{"names":[false]}', 'not text')]:
        value = convert(raw)
        assert value == {'values': [], 'error': expected}
        check_value(value, port['returns'], 'result')
    assert convert('{"names":["b","a","b"]}') == {'values': ['a', 'b'], 'error': None}


def test_explicit_json_digest_format_and_pure_text_operations(tmp_path):
    convert, port = inspect(tmp_path, '''
import hashlib
import html
import json
import re
from typing import TypedDict
class Row(TypedDict):
    name: str
    count: int
class Result(TypedDict):
    text: str
    digest: str
def convert(row: Row) -> Result:
    text = re.sub(r'\\s+', ' ', html.unescape(row['name'])).strip()
    encoded = json.dumps([text, row['count'], None], ensure_ascii=False, sort_keys=True, indent=2)
    return {'text': text, 'digest': hashlib.sha256((encoded + '\\n').encode('utf-8')).hexdigest()}
''')
    import hashlib
    result = convert({'name': ' A &amp;  B\n', 'count': 7})
    assert result == {'text': 'A & B', 'digest': hashlib.sha256((json.dumps(['A & B', 7, None], ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()).hexdigest()}
    check_value(result, port['returns'], 'result')
    # The ordinary callable also passes the real plan boundary and native sink.
    from demiflow import data
    from demiflow.execution.pipeline.checked import CheckedExecution
    from demiflow.forge.flow_check import FlowChecker
    from demiflow.forge.platform import inspect_modules, LIMITS
    from demiflow.workbench.runs.manager import DEFAULT_LIMITS
    from pathlib import Path
    from test_forge_native_semantics import native
    import lance
    (tmp_path / 'flows').mkdir()
    path = tmp_path / 'flows/pure.py'
    path.write_text('from demiflow import data\nfrom operators.pure import convert\n'
                    'def run(*, target):\n'
                    '    rows = data.from_items([{"name": " A &amp;  B\\n", "count": 7}])\n'
                    '    return rows.map(convert).write_lance(target, mode="create", return_receipt=True)\n')
    target = str(tmp_path / 'output.lance')
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(tmp_path, catalog, {}, [target]).inspect('flows/pure.py:run', {'target': target})
    assert report['status'] == 'checked', report['diagnostics']
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, limits=DEFAULT_LIMITS, record=lambda *args: None, check_stop=lambda: None)
    with native(tmp_path), checked.bind_context():
        receipt = data.from_items([{'name': ' A &amp;  B\n', 'count': 7}]).map(convert).write_lance(target, mode='create', return_receipt=True)
    assert lance.dataset(target, version=receipt.committed_version).to_table().to_pylist() == [result]


def test_shallow_copy_can_append_without_mutating_borrowed_children(tmp_path):
    convert, _ = inspect(tmp_path, '''
from typing import TypedDict
class Row(TypedDict):
    value: int
def convert(rows: list[Row]) -> list[Row]:
    result = list(rows)
    result.append({'value': 3})
    return sorted(result, key=lambda row: row['value'])
''')
    original = [{'value': 2}, {'value': 1}]
    assert convert(original) == [{'value': 1}, {'value': 2}, {'value': 3}]
    assert original == [{'value': 2}, {'value': 1}]


@pytest.mark.parametrize('source', [
    "import json\ndef convert(raw: str) -> str:\n    return json.loads(raw)['name']\n",
    "import json\ndef convert(raw: str) -> str:\n    value: str = json.loads(raw)\n    return value\n",
    "import json\ndef convert(raw: str) -> str:\n    return str(json.loads(raw, object_hook=str))\n",
    "import json\ndef convert(raw: str) -> str:\n    return json.dumps(raw, default=str)\n",
    "import hashlib\ndef convert(raw: bytes) -> str:\n    return hashlib.file_digest(raw, 'sha256').hexdigest()\n",
    "import re\ndef convert(raw: str) -> str:\n    return re.sub(raw, '', raw)\n",
    "def convert(rows: list[int]) -> list[int]:\n    rows.append(1)\n    return rows\n",
    "def convert(rows: list[list[int]]) -> list[list[int]]:\n    copied = list(rows)\n    copied[0].append(1)\n    return copied\n",
    "def convert(rows: list[int]) -> list[int]:\n    copied = list(rows)\n    copied.append('wrong')\n    return copied\n",
    "import json\ndef convert(raw: str) -> str:\n    return json.dumps({raw})\n",
])
def test_unknown_effects_casts_and_borrowed_mutations_stay_rejected(tmp_path, source):
    with pytest.raises(ValueError):
        inspect(tmp_path, source)


def test_trusted_module_name_cannot_be_shadowed(tmp_path):
    (tmp_path / 'json.py').write_text('raise AssertionError("must not import")')
    with pytest.raises(ValueError, match='shadows'):
        inspect(tmp_path, 'import json\ndef convert(raw: str) -> str:\n    return json.dumps(raw)\n')


@pytest.mark.parametrize('body', [
    "return value if value is not None else ''",
    "return value or ''",
    "if value is None:\n        value = ''\n    return value",
])
def test_nullable_scalar_branches_have_a_proven_nonnull_result(tmp_path, body):
    convert, port = inspect(tmp_path, 'def convert(value: str | None) -> str:\n    ' + body + '\n')
    for value in (None, '', 'kept'):
        result = convert(value)
        assert result == (value or '')
        check_value(result, port['returns'], 'result')


def test_branch_merge_retains_record_fields_and_nullable_members(tmp_path):
    convert, port = inspect(tmp_path, '''
from typing import TypedDict
class Row(TypedDict):
    text: str | None
    count: int
def convert(value: Row | None) -> Row:
    if value is None:
        value = {'text': None, 'count': 0}
    return value
''')
    for value in (None, {'text': 'kept', 'count': 3}):
        result = convert(value)
        check_value(result, port['returns'], 'result')
        assert result == (value if value is not None else {'text': None, 'count': 0})


@pytest.mark.parametrize('source', [
    "def convert(value: str | None) -> str:\n    return value and 'present'\n",
    "def convert(value: str | None) -> str:\n    return value if value is None else ''\n",
    "def convert(value: str | None) -> str:\n    if value is not None:\n        value = ''\n    return value\n",
    "def convert(rows: list[int] | None) -> list[int]:\n    values = rows or []\n    values.append(3)\n    return values\n",
    "def convert(flag: bool, rows: list[int]) -> list[int]:\n    values = []\n    if flag:\n        values = rows\n    values.append(3)\n    return values\n",
])
def test_branch_join_cannot_erase_null_or_borrowed_mutability(tmp_path, source):
    with pytest.raises(ValueError):
        inspect(tmp_path, source)


def test_primitive_format_keeps_declared_precision(tmp_path):
    convert, port = inspect(tmp_path, '''
def convert(value: float) -> str:
    return f'probability={value:.6f}'
''')
    assert convert(.8) == 'probability=0.800000'
    check_value(convert(.8), port['returns'], 'result')


@pytest.mark.parametrize('source', [
    "def convert(value: float | None) -> str:\n    return f'{value:.6f}'\n",
    "def convert(value: float, width: int) -> str:\n    return f'{value:{width}f}'\n",
    "def convert(value: float) -> str:\n    return f'{value:9999f}'\n",
    "def convert(value: str) -> str:\n    return f'{value:.6f}'\n",
    "def convert(value: list[str]) -> str:\n    return f'{value}'\n",
])
def test_unbounded_or_unknown_string_format_remains_rejected(tmp_path, source):
    with pytest.raises(ValueError):
        inspect(tmp_path, source)


def test_dictionary_comprehension_keeps_typed_values(tmp_path):
    convert, port = inspect(tmp_path, '''
import json
from typing import TypedDict
class Count(TypedDict):
    status: str
    count: int
def convert(rows: list[Count]) -> str:
    counts = {row['status']: row['count'] for row in rows}
    return json.dumps(counts, sort_keys=True)
''')
    result = convert([{'status': 'failed', 'count': 2}, {'status': 'done', 'count': 5}])
    assert json.loads(result) == {'done': 5, 'failed': 2}
    check_value(result, port['returns'], 'result')


@pytest.mark.parametrize('source', [
    "def convert(rows: list[list[int]]) -> dict[str, list[int]]:\n    result = {'key': row for row in rows}\n    result['key'].append(2)\n    return result\n",
    "def convert(rows: list[str | None]) -> dict[str, int]:\n    return {row: 1 for row in rows}\n",
    "def convert(rows: list[list[int]]) -> dict[str, int]:\n    return {row: 1 for row in rows}\n",
])
def test_dictionary_comprehension_cannot_hide_borrowing_or_unknown_keys(tmp_path, source):
    with pytest.raises(ValueError):
        inspect(tmp_path, source)
