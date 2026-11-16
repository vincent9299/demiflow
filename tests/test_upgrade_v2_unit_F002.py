"""F002 operation_facts: single admission authority, kind-dispatched arguments,
bounded specialization domains, and non-secret model_map projection.

Locks the absorbed plan-facts single-authority candidate (C package #38) plus
the synthetic-secret negatives required by the F002 card. Already-supported
types must stay byte-identical (HR1/T11); newly admitted native specializations
must carry a bounded native domain so plan_identity never falls back to the
32-level describe() budget.
"""
import hashlib

import pytest

from demiflow import data
from demiflow.data import plan as ops
from demiflow.data.plan import CallableSpec
from demiflow.execution.plan_facts import operation_capability, operation_facts
from demiflow.execution.recovery_contract import document, identity
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.planning.dag import _operation_semantics, compile_logical_dag


def _fn(row):
    return row


def _spec():
    return CallableSpec(target=_fn)


def _vector_op(**options):
    spec = dict(uri='file:///t', version=1, vector_column='vec',
                columns=['id'], top_k=5, storage_options={})
    spec.update(options)
    return data.from_items([{'q': [1., 2.]}]).search_vectors(
        query='q', output='out', **spec)._plan.operations[-1]


def _dag_id(ds):
    return compile_logical_dag(
        action_id="act", source=ds._source, operations=ds._plan.operations,
        terminal_kind="materialize", sink_ids=("out",))["dag_id"]


# --- HR1 / T11: already-supported facts byte-identical to the sealed baseline ---

def test_bound_map_facts_match_sealed_baseline():
    op = ops.BoundMapOp(_spec(), inputs={"text": "question"}, output="normalized_question")
    fact = operation_facts(op)
    assert hashlib.sha256(document(fact)).hexdigest() == (
        "dab72ca5d7651e5bac302467a7b4c0df4fe1a26ab02362fe2b5a33bd0b020412")
    assert identity([_operation_semantics(op)[0], 0]) == (
        "ec6f96ec07e0632896399ad952ca11ef82bc47d017c0b77074143a45a5823cfc")


@pytest.mark.parametrize("op,kind,argkeys", [
    (ops.MapOp(_spec()), "map", {"inputs", "output", "outputs"}),
    (ops.BoundMapOp(_spec(), inputs={"a": "a"}, output="b"), "map", {"inputs", "output", "outputs"}),
    (ops.FilterOp(_spec()), "filter", set()),
    (ops.FlatMapOp(_spec()), "flat_map", set()),
    (ops.AsyncMapOp(_spec()), "async_map",
     {"concurrency", "queue_depth", "catch", "label", "hard_timeout", "execution"}),
    (ops.BatchMapOp(_spec()), "batch_map",
     {"concurrency", "queue_depth", "catch", "label", "hard_timeout", "max_batch", "flush_interval"}),
    (ops.LimitOp(5), "limit", {"limit"}),
    (ops.SelectColumnsOp(("a",)), "project", {"cols"}),
    (ops.DropColumnsOp(("a",)), "drop", {"cols"}),
    (ops.RenameColumnsOp({"a": "b"}), "rename", {"names"}),
])
def test_supported_kind_arguments_shape(op, kind, argkeys):
    fact = operation_facts(op)
    document(fact)
    assert fact["kind"] == kind
    assert set(fact["arguments"]) == argkeys
    assert set(fact) >= {"kind", "driver", "native_options", "stream_options", "bound", "arguments"}


def test_supported_facts_deterministic_across_constructions():
    a = ops.AsyncMapOp(_spec(), concurrency=3, label="x")
    b = ops.AsyncMapOp(_spec(), concurrency=3, label="x")
    assert document(operation_facts(a)) == document(operation_facts(b))


# --- T3: admission stays bounded; no arbitrary third-party subclass ---

def test_third_party_subclass_rejected():
    class Evil(ops.AsyncMapOp):
        pass

    class Evil2(ops.BatchMapOp):
        pass

    for cls in (Evil, Evil2):
        with pytest.raises(RuntimeContractError, match="Unknown actual operation contract"):
            operation_facts(cls(_spec()))


@pytest.mark.parametrize("op", [
    ops.SortOp(("a",)), ops.RepartitionOp(num_blocks=2),
    ops.RandomShuffleOp(), ops.RandomizeBlockOrderOp(),
])
def test_supported_global_ops_have_matching_facts(op):
    expected = {ops.SortOp: 'sort', ops.RepartitionOp: 'repartition',
                ops.RandomShuffleOp: 'shuffle', ops.RandomizeBlockOrderOp: 'block_order'}
    assert operation_capability(op)['kind'] == expected[type(op)]
    fact = operation_facts(op)
    assert fact['kind'] == expected[type(op)] and fact['arguments']
    document(fact)


def test_native_options_guard_still_raises():
    from demiflow.data.native_options import parse_native_options
    native = parse_native_options({"ray": {"compute": {"kind": "task_pool", "size": 1}}}, family="row_transform")
    op = ops.MapOp(_spec(), native_options=native)
    with pytest.raises(RuntimeContractError, match="Backend option facts"):
        operation_facts(op)


# --- T1: native specializations admitted with arguments + bounded native domain ---

def test_deduplicate_admitted_with_domain(tmp_path):
    ds = data.from_items([{"id": "a", "g": "x"}]).deduplicate(
        "g", path=str(tmp_path / "k"), key="id", keep_duplicates=True)
    op = ds._plan.operations[-1]
    assert type(op) is ops.DeduplicateOp
    fact = operation_facts(op)
    document(fact)
    assert fact["kind"] == "async_map"
    assert fact["native"]["contract"] == "demiflow.deduplicate"
    assert fact["arguments"]["queue_depth"] == fact["arguments"]["concurrency"]


def test_group_batches_admitted_with_domain():
    ds = data.from_items([{"id": "a", "g": "x"}]).group_batches("g", flush_interval=1.0)
    op = ds._plan.operations[-1]
    assert type(op) is ops.StreamGroupBatchesOp
    fact = operation_facts(op)
    document(fact)
    assert fact["kind"] == "batch_map"
    assert fact["native"]["contract"] == "demiflow.stream_group_batches"
    assert {"on", "output", "chunk_bytes", "max_groups", "buffer_bytes"} <= set(fact["native"]["arguments"])


def test_prepend_admitted_with_bounded_source_identity():
    ds = data.from_items([{"id": "a"}]).prepend(data.from_items([{"id": "z"}]), max_rows=10)
    op = ds._plan.operations[-1]
    assert type(op) is ops.StreamPrependOp
    fact = operation_facts(op)
    document(fact)
    assert fact["native"]["contract"] == "demiflow.stream_prepend"
    assert {"max_rows", "max_row_bytes", "source"} <= set(fact["native"]["arguments"])


def test_embedding_admitted_with_domain():
    from demiflow.embeddings import EmbeddingModel
    ds = data.from_items([{'t': 'x'}]).map_embeddings(
        model=EmbeddingModel('m', 'r1', 4, 'http://127.0.0.1:1/v1'), inputs={'text': 't'}, output='emb')
    fact = operation_facts(ds._plan.operations[-1])
    document(fact)
    assert fact['kind'] == 'batch_map'
    assert fact['native']['contract'] == 'demiflow.embeddings.v2'


def test_vector_search_admitted_with_domain():
    op = _vector_op()
    fact = operation_facts(op)
    document(fact)
    assert fact["kind"] == "async_map"
    assert fact["native"]["contract"] == "demiflow.search_vectors"


# --- T4: equal specialization plans compile to the same dag_id ---

def test_equal_specialization_plans_stable_dag_id(tmp_path):
    rows = [{"id": "a", "g": "x"}, {"id": "b", "g": "y"}]
    d1 = data.from_items(rows).deduplicate("g", path=str(tmp_path / "k1"), key="id", keep_duplicates=True)
    d2 = data.from_items(rows).deduplicate("g", path=str(tmp_path / "k2"), key="id", keep_duplicates=True)
    assert _dag_id(d1) == _dag_id(d2)


# --- T10: relational MapBatchesOp keeps describe-based declaration identity ---

def test_map_batches_has_no_native_domain():
    op = ops.MapBatchesOp(_spec(), batch_size=8, batch_format="pyarrow")
    fact = operation_facts(op)
    document(fact)
    assert fact["kind"] == "map_batches"
    assert fact.get("native") is None
    assert fact["arguments"] == {"batch_size": 8, "batch_format": "pyarrow", "zero_copy_batch": False}


# --- model_map: bounded semantic identity, no secrets, no random node_id ---

_PACK_YAML = """
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: enrich-v1
    model: {name: mock-model, transport: openai_compatible, base_url_env: B, api_key_env: K}
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [summary]
      properties: {summary: {type: string}}
    template: "Describe {{ entity | json }}"
"""


def _model_op(options=None):
    from demiflow.operator_llm.parser import parse_prompt_pack
    pack = parse_prompt_pack(_PACK_YAML)
    ds = data.from_items([{"entity": {"n": 1}}]).map_prompt(
        "enrich", config=pack, inputs=["entity"], outputs={"summary": "summary"}, options=options)
    return ds, ds._plan.operations[-1]


def test_model_map_excludes_random_node_id():
    ds1, op1 = _model_op()
    ds2, op2 = _model_op()
    assert op1.node_id != op2.node_id
    fact = operation_facts(op1)
    assert "node_id" not in fact["arguments"]
    assert _dag_id(ds1) == _dag_id(ds2)


def test_model_map_config_is_bounded_semantic_identity():
    _, op = _model_op()
    fact = operation_facts(op)
    document(fact)
    assert set(fact["arguments"]["config"]) == {"schema_version", "content_hash"}
    assert "prompts" not in fact["arguments"]["config"]


def test_model_map_options_exclude_credentials_and_environment_paths():
    options = {
        "request_options": {"api_key": "SUPERSECRET-KEY", "temperature": 0.5},
        "journal_dir": "/secret/journal/path",
        "stream": True,
        "stream_log_dir": "/secret/logs",
    }
    with pytest.raises((ValueError, RuntimeError), match='request_options|journal_dir|Unsupported|Unknown|offline'):
        _model_op(options)
    from dataclasses import replace
    _, original = _model_op()
    op = replace(original, options=options)
    fact = operation_facts(op)
    blob = document(fact).decode()
    assert "SUPERSECRET-KEY" not in blob
    assert "/secret/journal/path" not in blob
    assert "/secret/logs" not in blob
    projected = fact["arguments"]["options"]
    assert projected["request_options"] == {"temperature": 0.5}
    assert projected["stream"] is True
    assert "journal_dir" not in projected
    assert "stream_log_dir" not in projected


def test_model_map_none_options_stay_none():
    _, op = _model_op(None)
    assert operation_facts(op)["arguments"]["options"] is None


# --- T9: VectorSearch storage_options referenced by bounded identity, not copied ---

def test_vector_search_storage_options_secret_negative():
    with pytest.raises(ValueError, match='unsupported Lance storage option'):
        _vector_op(storage_options={"secret_access_key": "SUPERSECRET-VALUE"})
    op = _vector_op(storage_options={"endpoint": "http://SUPERSECRET-VALUE"})
    fact = operation_facts(op)
    blob = document(fact).decode()
    assert "SUPERSECRET-VALUE" not in blob
    ref = fact["native"]["arguments"]["storage_options"]
    assert set(ref) == {"sha256", "keys"}
    assert ref["keys"] == ["endpoint"]


def test_vector_search_storage_options_identity_stable():
    spec = {"uri": "file:///t", "version": 1, "vector_column": "vec", "columns": ["id"],
            "top_k": 5, "storage_options": {"endpoint": "http://x", "region": "r"}}
    a = _vector_op(**spec)
    b = _vector_op(**spec)
    assert document(operation_facts(a)) == document(operation_facts(b))



def test_resource_only_native_facts_preserve_the_actual_claim():
    from demiflow.data.native_options import parse_native_options
    native = parse_native_options({'ray': {'worker_resources': {'cpu': 1, 'memory_bytes': 1048576}}}, family='row_transform')
    fact = operation_facts(ops.MapOp(_spec(), native_options=native))
    assert fact['resource_declaration'] == {'cpu': 1., 'memory_bytes': 1048576}
    document(fact)
