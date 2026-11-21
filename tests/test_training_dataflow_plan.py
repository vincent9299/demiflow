from dataclasses import replace
import json

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import ItemsSource, LanceSource
from demiflow.data.training import PredictorSpec, TrainingRecipe
from demiflow.execution.contracts.dataflow import (
    ActionRequest, ArtifactRef, HostDescriptor, SourceSnapshot,
)
from demiflow.execution.contracts.dataflow_codec import decode_wire, encode_wire
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.lance.model import LanceScanSpec
from demiflow.planning.dataflow_compile import capture_graph, compile_kernel_spec
from demiflow.planning.dataflow_place import compile_physical_dataflow, validate_physical_dataflow
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def trainer(request, context):
    raise AssertionError("planning ran the trainer")


class Predictor:
    def __init__(self, *, model, inputs, parameters):
        raise AssertionError("planning loaded a model")

    def __call__(self, batch):
        raise AssertionError("planning ran a batch")


def recipe(**kwargs):
    return TrainingRecipe(trainer=trainer, predictor=PredictorSpec(factory=Predictor, inputs={"text": "x"}, input_schema={"text":"string"},
        output_schema={"score": "float"}), train_schema={"x": "string"}, base_model_digest="a" * 64,
        environment_digest="b" * 64, model_format="test", budget={"optimizer_updates": 4},
        topology={"nodes": 1}, save_steps=(2, 4), **kwargs)


def capture(ds):
    request = ActionRequest("count", ds._source, ds._plan, {}, None, (), "local",
                            type("Action", (), {"action_id": "test"})())
    return capture_graph(request, bounds=resolve_runtime_policy({}, {}, provider="local")[1])


def place(graph):
    policy, bounds = resolve_runtime_policy({}, {}, provider="local")
    host = HostDescriptor("node", "local", "test", (policy.store_id,),
                          {"cpu": 64, "memory_bytes": 1 << 46}, ())
    snapshots = tuple(SourceSnapshot(n, "source", {}, True, "seek", None) for n in graph.sources)
    ref = ArtifactRef("a" * 64, 1, "local", "a", "application/json")
    return compile_physical_dataflow(graph, snapshots, (), policy=policy,
                                     hosts=(host,), bounds=bounds, constants_ref=ref)


@pytest.mark.parametrize("direct", [False, True])
def test_training_ports_and_finite_predict_branches(direct):
    owner = object()
    source = LanceSource(LanceScanSpec("/sample", version=2)) if direct else ItemsSource(({"x": "s"},))
    training = Dataset(source, LogicalPlan(), owner)
    holdout = Dataset(ItemsSource(({"x": "test"},)), LogicalPlan(), owner)
    scores = training.fit(recipe=recipe()).checkpoints.select(steps=[4, 2]).predict(on=holdout)
    # A model dependency and its fit job belong to graph capture. The ordinary
    # kernel compiler cannot represent that dependency in a standalone KernelSpec.
    with pytest.raises(DataflowError, match='prediction requires graph capture'):
        compile_kernel_spec(scores._plan.operations[-1], program_ref=None,
                            bounds=resolve_runtime_policy({}, {}, provider="local")[1])
    compiled = capture(scores)
    graph = compiled.graph
    assert len(graph.jobs) == 1
    job = graph.jobs[0]
    assert {d.occurrence_id for d in compiled.declarations} >= {job.occurrence_id}
    selected = [edge for edge in graph.value_edges if edge.input_port == "model"]
    assert [edge.step for edge in selected] == [4, 2]
    assert all(edge.producer == job.occurrence_id for edge in selected)
    train = next(e for e in graph.value_edges if e.input_port == "train")
    assert (train.producer in graph.sources) is direct
    assert len(set(e.producer for e in graph.edges if e.consumer in {m.consumer for m in selected})) == 1
    physical = place(graph)
    assert len(physical.jobs) == 1
    occs = {occ for stage in physical.stages for occ in stage.occurrences}
    assert job.occurrence_id not in occs
    assert (train.producer in occs) is (not direct)
    predictions = {e.consumer for e in selected}
    for edge in graph.edges:
        if edge.consumer in predictions or edge.producer in predictions:
            assert next(e for e in physical.exchanges if e.edge_id == edge.edge_id).mode == "finite_sealed"
    for value, name in [(graph, "DataflowGraph"), (physical, "PhysicalDataflow")]:
        raw = encode_wire(value, type_name=name, bounds=physical.bounds)
        assert json.loads(raw)["schema"].endswith(".v4")
        assert decode_wire(raw, type_name=name, bounds=physical.bounds) == value
    with pytest.raises(DataflowError):
        validate_physical_dataflow(graph, replace(physical, value_edges=physical.value_edges[:-1]))


def test_plain_graph_keeps_v3_bytes_and_field_set():
    graph = capture(Dataset(ItemsSource(({"x": 1},)), LogicalPlan(), object())).graph
    physical = place(graph)
    for value, name in [(graph, "DataflowGraph"), (physical, "PhysicalDataflow")]:
        raw = encode_wire(value, type_name=name, bounds=physical.bounds)
        frame = json.loads(raw)
        assert frame["schema"].endswith(".v3")
        assert "jobs" not in frame["payload"] and "value_edges" not in frame["payload"]
        assert encode_wire(decode_wire(raw, type_name=name, bounds=physical.bounds),
                           type_name=name, bounds=physical.bounds) == raw


def test_static_flow_resolves_training_and_selection_without_executing(tmp_path):
    from demiflow.forge.flow_check import FlowChecker
    from demiflow.forge.platform import discover_catalog
    root = tmp_path
    (root / "flow.py").write_text(
        'from demiflow import data\n'
        'def trainer(request, context):\n    raise RuntimeError("training ran")\n'
        'class Predictor:\n'
        '    def __init__(self, *, model, inputs, parameters):\n        raise RuntimeError("factory ran")\n'
        '    def __call__(self, batch):\n        return batch\n'
        'def evaluate(*, digest):\n'
        '    ds = data.range(5)\n'
        '    predictor = data.PredictorSpec(factory=Predictor,inputs={"x":"id"},input_schema={"x":"int64"},output_schema={"score":"int"},native_options={"ray":{"worker_resources":{"gpu":1}}})\n'
        '    recipe = data.TrainingRecipe(trainer=trainer,predictor=predictor,train_schema={"id":"int"},'
        'base_model_digest=digest,environment_digest=digest,model_format="m",budget={"optimizer_updates":4},'
        'save_steps=[2,4],topology={"nodes":1})\n'
        '    fitted = ds.fit(recipe=recipe)\n'
        '    return fitted.checkpoints.select(steps=[4,2]).predict(on=ds).count()\n')
    catalog = discover_catalog(["demiflow.data"])
    report = FlowChecker(root, catalog, {}, []).inspect("flow.py:evaluate", {"digest": "a" * 64})
    assert report["status"] == "checked", report["diagnostics"]
    assert [n["kind"] for n in report["nodes"]] == ["range", "fit", "predict", "count"]
    assert report["value_edges"] == [{"from": report["nodes"][1]["id"], "to": report["nodes"][2]["id"]}]
    assert report["nodes"][1]["analysis"]["topology"] == {"nodes":1}
    assert report["nodes"][2]["capability_fact"]["resource_declaration"]["gpu"] == 1
    assert len(report["actions"]) == 1


def test_multiple_consumers_and_continuation_share_immutable_fit_occurrences():
    from demiflow.execution.plan_capture import build_logical_plan_view
    port = object()
    ds = Dataset(ItemsSource(({"x": "s"},)), LogicalPlan(), port)
    first = ds.fit(recipe=recipe()).checkpoints.select(steps=[2])
    second = ds.fit(recipe=recipe(), resume_from=first).checkpoints.select(steps=[4])
    result = first.predict(on=ds).union(second.predict(on=ds))
    compiled = capture(result)
    graph = compiled.graph
    assert len(graph.jobs) == 2
    parent, = [e for e in graph.value_edges if e.input_port == "parent"]
    assert (parent.producer, parent.consumer, parent.step) == (graph.jobs[0].occurrence_id, graph.jobs[1].occurrence_id, 2)
    assert len({e.producer for e in graph.value_edges if e.kind == "fixed_dataset"}) == 1
    view = build_logical_plan_view(graph, declarations=compiled.declarations)
    assert len([n for n in view["nodes"] if n["op_kind"] == "fit"]) == 2
    assert {e["to"]["port"] for e in view["edges"] if e.get("kind") == "model_manifest"} == {"model", "parent"}


def test_known_schema_rejects_missing_predict_input_and_mismatched_struct():
    import pyarrow as pa
    from demiflow.data.training_schema import validate_columns
    schema = pa.schema([("media", pa.list_(pa.struct([("digest", pa.string()), ("uri", pa.string())])))])
    validate_columns(schema, {"media": "list<item: struct<uri: string, digest: string>>"})
    with pytest.raises(ValueError, match="media"):
        validate_columns(schema, {"media": "list<item: struct<uri: int64, digest: string>>"})


def test_v4_seal_rejects_a_forged_fit_identity():
    from demiflow.execution.seal.plan import verify_fit_membership, SEAL_SCHEMA, TRAINING_SEAL_SCHEMA
    ds = Dataset(ItemsSource(({"x": "s"},)), LogicalPlan(), object())
    graph = capture(ds.fit(recipe=recipe()).select(steps=[2]).predict(on=ds)).graph
    for marker in ({"schema": SEAL_SCHEMA}, {"schema": TRAINING_SEAL_SCHEMA, "fit_occurrences": [graph.sources[0]]}):
        with pytest.raises(DataflowError, match="seal"):
            verify_fit_membership(marker, graph)
    verify_fit_membership({"schema": TRAINING_SEAL_SCHEMA, "fit_occurrences": [graph.jobs[0].occurrence_id]}, graph)


def test_predictor_input_aliases_keep_all_type_constraints():
    from dataclasses import replace
    spec = recipe().predictor
    duplicate = {'left':'x', 'right':'x'}
    compatible = replace(spec, inputs=duplicate, input_schema={'left':'str', 'right':'string'})
    assert dict(compatible.inputs) == duplicate
    with pytest.raises(ValueError, match='Conflicting predictor types for column x'):
        replace(compatible, input_schema={'left':'int64', 'right':'string'})
    valid = replace(compatible, inputs={'left':'id', 'right':'x'},
                    input_schema={'left':'int64', 'right':'string'})
    owner = object()
    rows = Dataset(ItemsSource(({'x':'yes','id':1},)), LogicalPlan(), owner)
    selection = rows.fit(recipe=replace(recipe(), predictor=valid)).checkpoints.select(steps=[2])
    with pytest.raises(ValueError, match='Conflicting predictor types for column x'):
        selection.predict(on=rows, inputs={'left':'x'})

def test_native_view_projects_fit_topology_and_predictor_gpu_without_running(tmp_path):
    from dataclasses import replace
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution.host.control import DEFAULT_LIMITS
    from demiflow.execution.host.inspection import capture_managed_pipeline, native_submission_view
    config = recipe()
    config = replace(config, topology={'nodes': 2, 'gpus_per_node': 4}, predictor=replace(
        config.predictor, native_options={'ray': {'worker_resources': {'gpu': 1}}}))
    def entry():
        rows = data.from_items([{'x':'text'}])
        fitted = rows.fit(recipe=config)
        return fitted.checkpoints.select(steps=[2,4]).predict(on=rows).write_lance(
            str(tmp_path / 'scores.lance'), schema=pa.schema([('score',pa.float64())]), mode='create')
    captured = capture_managed_pipeline(entry, {}, limits=DEFAULT_LIMITS)
    nodes = native_submission_view(captured)['nodes']
    job, = (n for n in nodes if n['kind'] == 'fit')
    assert job['analysis']['topology'] == {'nodes':2, 'gpus_per_node':4}
    branches = [n for n in nodes if n['parameters'].get('predict_step')]
    assert [n['parameters']['predict_step'] for n in branches] == [2,4]
    assert all(n['capability_fact']['resource_declaration'] == {'gpu':1} for n in branches)
    assert not (tmp_path / 'scores.lance').exists()
