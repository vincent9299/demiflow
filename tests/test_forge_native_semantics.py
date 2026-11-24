"""Independent native fixtures protect declared transfer rules against drift."""

from contextlib import contextmanager
import inspect

import pytest
from demiflow import data
from demiflow.data.callable_contract import signature_parameters, validate_field_binding
from demiflow.data.flow_contract import (
    compatible,
    field,
    scalar,
    transfer_fields,
    validate_semantics,
)
from demiflow.execution.pipeline.local_kernel import local_execution


@contextmanager
def native(tmp_path):
    with local_execution(
        workers=1,
        worker_mode="thread",
        partitions=1,
        memory_bytes=128 * 2**20,
        temp_directory=str(tmp_path),
        native_resources={
            "memory_bytes": 128 * 2**20,
            "max_rss_bytes": 2 * 2**30,
            "threads": 1,
            "partitions": 1,
            "timeout_s": 30,
            "admission_timeout_s": 30,
        },
    ):
        yield


def test_native_field_map_raw_map_outputs_and_binding(tmp_path):
    schema = {"x": field(scalar("int")), "keep": field(scalar("str"))}
    with native(tmp_path):
        source = data.from_items([{"x": 1, "keep": "k"}])

        def increment(value: int, *, amount: int = 1) -> int:
            return value + amount

        rows = source.map(increment, inputs={"value": "x"}, output="x").take_all()
        predicted = transfer_fields(
            "map",
            schema,
            {"inputs": {"value": "x"}, "output": "x"},
            returns=scalar("int"),
        )
        assert rows == [{"x": 2, "keep": "k"}] and set(rows[0]) == set(predicted)
        rows = source.map(lambda row: {"only": row["x"]}).take_all()
        predicted = transfer_fields(
            "map",
            schema,
            {},
            returns={**scalar("record"), "fields": {"only": field(scalar("int"))}},
        )
        assert rows == [{"only": 1}] and set(rows[0]) == set(predicted)
        rows = source.map(
            lambda value: {"computed": value + 1},
            inputs={"value": "x"},
            outputs={"computed": "result"},
        ).take_all()
        predicted = transfer_fields(
            "map",
            schema,
            {"inputs": {"value": "x"}, "outputs": {"computed": "result"}},
            returns={**scalar("record"), "fields": {"computed": field(scalar("int"))}},
        )
        assert rows == [{"x": 1, "keep": "k", "result": 2}] and set(rows[0]) == set(
            predicted
        )
        for inputs, kwargs in [
            ({"missing": "x"}, {}),
            ({"value": "x"}, {"value": 2}),
            ({}, {}),
        ]:
            with pytest.raises((ValueError, TypeError)):
                validate_field_binding(
                    signature_parameters(inspect.signature(increment)),
                    inputs,
                    kwargs,
                    label="increment",
                )
            with pytest.raises((ValueError, TypeError)):
                source.map(increment, inputs=inputs, fn_kwargs=kwargs, output="result")


@pytest.mark.parametrize(
    "how,expected_count", [("inner", 2), ("left", 4), ("semi", 1), ("anti", 2)]
)
def test_native_join_multiplicity_missing_null_and_materialize(
    tmp_path, how, expected_count
):
    left = {"id": field(scalar("str", nullable=True)), "left": field(scalar("int"))}
    right = {"id": field(scalar("str", nullable=True)), "right": field(scalar("int"))}
    predicted = transfer_fields("join", left, {"on": "id", "how": how}, other=right)
    with native(tmp_path):
        a = data.from_items(
            [
                {"id": "a", "left": 1},
                {"id": "missing", "left": 2},
                {"id": None, "left": 3},
            ]
        )
        b = data.from_items(
            [
                {"id": "a", "right": 10},
                {"id": "a", "right": 20},
                {"id": None, "right": 30},
            ]
        )
        result = a.join(b, on="id", how=how).materialize().take_all()
        assert len(result) == expected_count
        for row in result:
            assert all(name in row for name, f in predicted.items() if f["required"])
            assert set(row) <= set(predicted)
        if how == "left":
            missing = [r for r in result if r["id"] != "a"]
            assert all("right" not in row for row in missing)
            assert (
                predicted["right"]["required"] is False
                and predicted["right"]["type"]["nullable"] is False
            )
        elif how == "inner":
            assert {r["right"] for r in result} == {10, 20}


def test_native_collision_and_empty_write_contract(tmp_path):
    schema = {
        "id": field(scalar("str")),
        "x": field(scalar("int")),
        "x_right": field(scalar("int")),
    }
    with pytest.raises(ValueError, match="collision"):
        transfer_fields(
            "join",
            schema,
            {"on": "id"},
            other={"id": field(scalar("str")), "x": field(scalar("int"))},
        )
    from demiflow.execution import RecoveryContractError
    with pytest.raises(RecoveryContractError, match='unconfirmed actions'):
        with native(tmp_path):
            with pytest.raises(ValueError, match="collision"):
                data.from_items([{"id": "a", "x": 1, "x_right": 2}]).join(
                    data.from_items([{"id": "a", "x": 3}]), on="id"
                ).take_all()
            target = tmp_path / "empty.lance"
            with pytest.raises(ValueError):
                data.from_items([]).write_lance(
                    str(target), mode="create", return_receipt=True
                )
            assert not target.exists()


@pytest.mark.parametrize(
    "required,nullable,target_required,target_nullable,expected",
    [
        (True, False, True, False, True),
        (True, True, True, False, False),
        (False, False, True, True, False),
        (False, False, False, False, True),
        (True, True, False, True, True),
        (False, True, False, False, False),
    ],
)
def test_required_and_null_matrix(
    required, nullable, target_required, target_nullable, expected
):
    source = {
        **scalar("record"),
        "fields": {"value": field(scalar("int", nullable=nullable), required=required)},
    }
    target = {
        **scalar("record"),
        "fields": {
            "value": field(
                scalar("int", nullable=target_nullable), required=target_required
            )
        },
    }
    assert compatible(source, target) is expected


def test_native_declarations_reference_real_signature_and_no_wrapping():
    for function in [
        data.read_lance,
        data.Dataset.map,
        data.Dataset.filter,
        data.Dataset.join,
        data.Dataset.write_lance,
    ]:
        params = signature_parameters(inspect.signature(function))
        validate_semantics(function.__demiflow_flow__, params)
        assert not hasattr(function, "__wrapped__")
        bad = {**function.__demiflow_flow__, "datasets": ["absent_parameter"]}
        with pytest.raises(ValueError, match="absent"):
            validate_semantics(bad, params)


@pytest.mark.parametrize(
    "kind,arguments,returns",
    [
        ("map", {"output": "value"}, {**scalar("record"), "fields": {}}),
        ("project", {"cols": []}, None),
        ("drop", {"cols": ["x", "x"]}, None),
        ("rename", {"names": {}}, None),
        ("join", {"on": "x", "chunk_bytes": 0}, None),
    ],
)
def test_native_invalid_structural_options(kind, arguments, returns):
    schema = {"x": field(scalar("int"))}
    with pytest.raises(ValueError):
        transfer_fields(kind, schema, arguments, returns=returns, other=schema)
