"""F001 verify_preserve: operation_capability is the sole native kind authority.

The function body is byte-identical to baseline bbd93ff (SHA
ec987ee7e1659d09dd7ea8d7fb416319b7b08036f3456201e33e8b9b756beeda). These tests lock
its preserved behavior: C02 base key set, the kind table, native specializations,
third-party rejection, and the native/stream/bound flags. Real producers only.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import demiflow
from demiflow.data import plan as ops
from demiflow.data.native_options import parse_native_options
from demiflow.data.plan import CallableSpec
from demiflow.execution.plan_facts import operation_capability

_BASELINE_SHA = "ec987ee7e1659d09dd7ea8d7fb416319b7b08036f3456201e33e8b9b756beeda"
_C02_KEYS = {"kind", "driver", "native_options", "stream_options", "bound"}


def _fn(row):
    return row


def _spec():
    return CallableSpec.create(_fn)


def test_module_source_is_worktree():
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def test_function_body_matches_baseline_sha():
    path = Path(demiflow.__file__).parent / "execution" / "plan_facts.py"
    source = path.read_text()
    node = next(n for n in ast.walk(ast.parse(source))
                if isinstance(n, ast.FunctionDef) and n.name == "operation_capability")
    digest = hashlib.sha256(ast.get_source_segment(source, node).encode()).hexdigest()
    assert digest == _BASELINE_SHA


def test_c02_key_set_has_no_admitted_field():
    fact = operation_capability(ops.MapOp(_spec()))
    assert set(fact) == _C02_KEYS
    assert "admitted" not in fact


def test_kind_table_preserved():
    cases = [
        (ops.MapOp(_spec()), "map"),
        (ops.BoundMapOp(_spec(), inputs={"x": "x"}), "map"),
        (ops.FilterOp(_spec()), "filter"),
        (ops.FlatMapOp(_spec()), "flat_map"),
        (ops.MapBatchesOp(_spec()), "map_batches"),
        (ops.AsyncMapOp(_spec()), "async_map"),
        (ops.BatchMapOp(_spec()), "batch_map"),
        (ops.LimitOp(3), "limit"),
        (ops.SelectColumnsOp(("a",)), "project"),
        (ops.DropColumnsOp(("a",)), "drop"),
        (ops.RenameColumnsOp({"a": "b"}), "rename"),
        (ops.AddColumnOp("c", _spec()), "add_column"),
        (ops.RandomSampleOp(0.5), "sample"),
    ]
    for operation, kind in cases:
        assert operation_capability(operation)["kind"] == kind, type(operation).__name__


def test_native_specializations_preserved():
    # Real data.plan subclasses of AsyncMapOp/BatchMapOp keep their stream kind.
    assert operation_capability(ops.EmbeddingMapOp(_spec()))["kind"] == "batch_map"
    assert operation_capability(ops.DeduplicateOp(_spec()))["kind"] == "async_map"
    assert operation_capability(ops.StreamGroupBatchesOp(_spec()))["kind"] == "batch_map"


def test_third_party_subclass_rejected():
    class ForeignAsync(ops.AsyncMapOp):
        pass

    class ForeignBatch(ops.BatchMapOp):
        pass

    assert operation_capability(ForeignAsync(_spec()))["kind"] == "unknown"
    assert operation_capability(ForeignBatch(_spec()))["kind"] == "unknown"


def test_non_specialized_plan_op_is_unknown():
    assert operation_capability(ops.SortOp(("id",)))["kind"] == "unknown"
    assert operation_capability(ops.RandomizeBlockOrderOp())["kind"] == "unknown"
    assert operation_capability(ops.RepartitionOp(num_blocks=2))["kind"] == "unknown"


def test_native_options_flag():
    assert operation_capability(ops.MapOp(_spec()))["native_options"] is False
    nat = parse_native_options({"ray": {}}, family="row_transform")
    assert operation_capability(ops.MapOp(_spec(), native_options=nat))["native_options"] is True
    # native_options only recognized for exact kinds, not specializations
    assert operation_capability(ops.EmbeddingMapOp(_spec()))["native_options"] is False


def test_stream_options_flag_and_driver():
    plain = operation_capability(ops.MapOp(_spec()))
    assert plain["stream_options"] is False and plain["driver"] == "pull"
    streamed = operation_capability(ops.MapOp(_spec(), stream_options=ops.MapStreamOptions()))
    assert streamed["stream_options"] is True and streamed["driver"] == "stream"
    # stream_options only inspected for MapOp/BoundMapOp
    assert operation_capability(ops.FilterOp(_spec()))["stream_options"] is False


def test_bound_flag():
    assert operation_capability(ops.BoundMapOp(_spec(), inputs={"x": "x"}))["bound"] is True
    assert operation_capability(ops.MapOp(_spec()))["bound"] is False


def test_async_and_batch_driver_is_stream():
    assert operation_capability(ops.AsyncMapOp(_spec()))["driver"] == "stream"
    assert operation_capability(ops.BatchMapOp(_spec()))["driver"] == "stream"
    assert operation_capability(ops.EmbeddingMapOp(_spec()))["driver"] == "stream"
    assert operation_capability(ops.LimitOp(1))["driver"] == "pull"


def test_model_map_kind_via_real_producer():
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    pack = parse_prompt_pack("""
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: enrich-v1
    model: {name: m, transport: openai_compatible, base_url_env: B, api_key_env: K}
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [summary]
      properties: {summary: {type: string}}
    template: "Describe {{ entity | json }}"
""")
    ds = data.from_items([{"entity": {"n": 1}}]).map_prompt(
        "enrich", config=pack, inputs=["entity"], outputs={"summary": "summary"})
    fact = operation_capability(ds._plan.operations[-1])
    assert fact["kind"] == "model_map"
    assert set(fact) == _C02_KEYS