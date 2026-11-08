"""Cross-layer contract regressions; expected values are independent of reports."""
import inspect

import pytest

from demiflow.data.callable_contract import bind_native_parameters, default_value, signature_parameters
from demiflow.data.flow_contract import compatible, field, scalar, union_type
from demiflow.data.runtime_contract import RuntimeContractError, check_value


def test_machine_defaults_never_evaluate_repr_and_keep_explicit_none():
    class Opaque:
        def __repr__(self):
            raise AssertionError("repr must not run")

    opaque = Opaque()

    def native(a=opaque, *, b=(b"x", None), c=None):
        pass

    signature = inspect.signature(native)
    parameters = signature_parameters(signature)
    for parameter in parameters:
        parameter["default"] = "untrusted display text"
        parameter["default_value"] = default_value(signature.parameters[parameter["name"]].default)
    assert bind_native_parameters(parameters, [], {"a": None}) == {"a": None, "b": (b"x", None), "c": None}
    with pytest.raises(ValueError, match="a: Opaque default"):
        bind_native_parameters(parameters, [], {})
    assert default_value([None] * 4097)["status"] == "opaque"
    assert default_value(float("inf"))["status"] == "opaque"


def test_union_row_list_null_checks_actual_values_and_missing_keys():
    row = {**scalar("record"), "fields": {"x": field(scalar("int"))}}
    output = union_type(row, {**scalar("list"), "item": row}, scalar("none"))
    assert compatible(row, output)
    assert not compatible(output, row)
    assert compatible(union_type(row, scalar("none")), output)
    for value in ({"x": 1}, [{"x": 2}], None):
        check_value(value, output, "result")
    for value in ({}, {"x": None}, [{"x": True}], 1):
        with pytest.raises(RuntimeContractError):
            check_value(value, output, "result")
    assert union_type(output, row) == output


def test_union_static_operator_annotations(tmp_path):
    from demiflow.forge.operator_check import OperatorChecker
    from demiflow.forge.source import SourceTree, Definition

    (tmp_path / "operators").mkdir()
    (tmp_path / "operators" / "sample.py").write_text(
        "from typing import TypedDict, Union\n"
        "class Row(TypedDict):\n    x: int\n"
        "async def expand(row: Row) -> Union[Row, list[Row], None]:\n"
        "    if row['x'] < 0:\n        return None\n"
        "    if row['x'] == 0:\n        return row\n"
        "    return [row]\n"
    )
    tree = SourceTree(tmp_path)
    definition = tree.load("operators.sample")["expand"]
    assert isinstance(definition, Definition)
    checker = OperatorChecker(tree)
    description = checker.describe(definition)
    checker.check(description)
    assert description["returns"]["kind"] == "union"
    assert description["returns"]["nullable"]


def test_iterator_values_are_checked_incrementally_and_closed():
    from demiflow.data.runtime_contract import CallValidation

    closed = []

    def expand(row):
        try:
            yield {'x': 1}
            yield {'x': 'invalid'}
        finally:
            closed.append(True)

    item = {**scalar('record'), 'fields': {'x': field(scalar('int'))}}
    check = CallValidation(expand, {'label': 'expand', 'parameters': [], 'returns': {**scalar('iterator'), 'item': item},
                                    'max_row_bytes': 1024, 'max_output_rows': 3})
    iterator = check.output(expand({}))
    assert not closed
    assert next(iterator) == {'x': 1}
    with pytest.raises(RuntimeContractError, match='expected int'):
        next(iterator)
    assert closed == [True]


def test_generator_static_contract_and_explicit_stop(tmp_path):
    from demiflow.forge.operator_check import OperatorChecker
    from demiflow.forge.source import SourceTree

    (tmp_path / 'operators').mkdir()
    (tmp_path / 'operators/expand.py').write_text(
        'from typing import TypedDict, Iterator\n'
        'class Row(TypedDict):\n    x: int\n'
        'def expand(row: Row) -> Iterator[Row]:\n'
        "    if row['x'] == 0:\n        return\n"
        '    yield row\n    yield row\n'
    )
    tree = SourceTree(tmp_path)
    checker = OperatorChecker(tree)
    description = checker.describe(tree.load('operators.expand')['expand'])
    checker.check(description)
    assert description['generator']
