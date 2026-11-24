"""Lower a training declaration into job value ports and finite ordinary row branches."""
from dataclasses import replace

from ..data.native_options import parse_native_options
from ..data.plan import CallableSpec, MapBatchesOp
from ..data.sources import LanceSource
from ..execution.contracts.dataflow import FitJobNode, FitJobStage, InputPort, KernelSpec, ProgramDeclaration, ValueEdge
from ..execution.contracts.dataflow_errors import DataflowError
from ..lance.model import LanceScanSpec
from .dataflow_budget import default_profile


FIT_CONFIGURATION_FIELDS = frozenset({'semantics', 'train_schema', 'monitor_schemas', 'media_fields',
                                      'retrain_id', 'parent'})


def _fixed_input(capture, source, plan, path):
    child = capture.capture_chain(source, plan, path)
    if (not plan.operations and isinstance(source, LanceSource) and isinstance(source.query, LanceScanSpec)
            and not (source.query.columns or source.query.projection or source.query.filter or source.query.limit)):
        return capture.nodes[child].occurrence_id
    if child not in capture.fixed_inputs:
        kernel = KernelSpec("fixed_dataset", None, {"kind": "fixed_dataset"}, (InputPort("main", "incremental", "none", None),),
                            "none", "journaled", "io", default_profile(None, bounds=capture.bounds),
                            capture.bounds.max_row_bytes, False)
        index = capture._add_node("terminal", kernel, {"kind": "fixed_dataset"}, {}, {}, None)
        node = capture.nodes[index]
        capture.nodes[index] = replace(node, kernel=replace(node.kernel, configuration={
            **node.kernel.configuration, "sink_id": node.occurrence_id}))
        capture._add_edge(child, "main", index, "main")
        capture.fixed_inputs[child] = node.occurrence_id
    return capture.fixed_inputs[child]


def _fit(capture, decl, path):
    from ..data.training import ModelSelection
    from .dataflow_compile import _callable_code_identity, _hash, _plain
    if decl in capture.fit_ids:
        return capture.fit_ids[decl]
    if decl in capture.active_fits or len(path) > capture.bounds.max_depth:
        raise DataflowError("INVALID_CONTRACT", "recursive or excessively deep training dependency")
    capture.active_fits.add(decl)
    try:
        fixed = [(name, _fixed_input(capture, source, plan, path + (name,))) for name, source, plan in decl.inputs]
        parent = decl.parent
        parent_job = _fit(capture, parent.declaration, path + ("parent",)) if isinstance(parent, ModelSelection) else None
        recipe = decl.recipe
        spec = CallableSpec.create(recipe.trainer)
        semantics = {k: _plain(getattr(recipe, k)) for k in ("parameters", "base_model_digest",
                     "environment_digest", "budget", "save_steps", "model_format", "topology")}
        configuration = {"semantics": semantics, "train_schema": _plain(recipe.train_schema),
                         "monitor_schemas": _plain(recipe.monitor_schemas), "media_fields": _plain(recipe.media_fields),
                         "retrain_id": decl.retrain_id,
                         "parent": None if parent_job else _plain(parent)}
        identity = _callable_code_identity(spec)
        capture._check_node_budget()
        occurrence = _hash(["dataflow-fit-v4", capture.request.action.action_id, len(capture.jobs)])
        job = FitJobNode(occurrence, _hash(["fit-semantic-v4", configuration, identity]), configuration, None)
        capture.jobs.append(job)
        if capture.report is not None:
            capture._bind_report(job, capture.report.operation(decl))
        capture.declarations.append((None, ProgramDeclaration(occurrence, spec, None, None, identity)))
        for role, producer in fixed:
            capture.value_edges.append(ValueEdge(producer, "dataset", occurrence, role, "fixed_dataset"))
        if parent_job:
            capture.value_edges.append(ValueEdge(parent_job, "manifest", occurrence, "parent", "model_manifest", parent.steps[0]))
        capture.fit_ids[decl] = occurrence
        return occurrence
    finally:
        capture.active_fits.remove(decl)


def capture_predict(capture, operation, holdout, path):
    from .dataflow_compile import compile_kernel_spec, _plain, _callable_code_identity
    from ..data.training_schema import prediction_columns
    job = _fit(capture, operation.fit, path + ("fit",))
    recipe = operation.fit.recipe
    predictor = recipe.predictor
    spec = CallableSpec.create(predictor.factory)
    identity = _callable_code_identity(spec)
    batch = MapBatchesOp(spec, predictor.batch_size, predictor.batch_format, False,
                         parse_native_options(_plain(predictor.native_options), family="batch_transform"))
    kernel = compile_kernel_spec(batch, program_ref=None, bounds=capture.bounds)
    if len(operation.steps) > capture.bounds.max_partitions:
        raise DataflowError("BUDGET_EXCEEDED", "selected model count exceeds partition bound")
    branches = []
    for step in operation.steps:
        facts = {"predict_step": step, "predict_inputs": _plain(operation.inputs),
                 "predict_parameters": _plain(operation.parameters),
                 "predict_input_schema": prediction_columns(operation.inputs, predictor.input_schema),
                 "predict_model_format": recipe.model_format,
                 "predict_output_schema": _plain(predictor.output_schema)}
        member = replace(kernel, configuration={**kernel.configuration, **facts}, mergeable=False)
        index = capture._add_node("transform", member, _plain(member.configuration), {}, identity, None)
        capture._remember_codec_window(index, batch)
        occurrence = capture.nodes[index].occurrence_id
        capture.declarations.append((index, ProgramDeclaration(occurrence, spec, None, None, identity)))
        capture._add_edge(holdout, "main", index, "main")
        capture.value_edges.append(ValueEdge(job, "manifest", occurrence, "model", "model_manifest", step))
        branches.append(index)
    if len(branches) == 1:
        return branches[0]
    ports = tuple(InputPort(str(i), "finite_complete", "none", None) for i in range(len(branches)))
    semantic = {"kind": "union", "inputs": len(branches)}
    union = KernelSpec("union", None, semantic, ports, "none", "pure", "cpu",
                       default_profile(None, bounds=capture.bounds), capture.bounds.max_row_bytes, False)
    index = capture._add_node("relational", union, semantic, {}, {}, None)
    capture._remember_codec_window(index)
    for i, branch in enumerate(branches):
        capture._add_edge(branch, "main", index, str(i))
    return index


def validate_values(graph, nodes):
    jobs = {job.occurrence_id: job for job in graph.jobs}
    if len(jobs) != len(graph.jobs) or jobs.keys() & nodes.keys():
        raise DataflowError("INVALID_CONTRACT", "duplicate training occurrence")
    for job in graph.jobs:
        configuration = job.configuration
        if set(configuration) != FIT_CONFIGURATION_FIELDS:
            raise DataflowError("INVALID_CONTRACT", "fit configuration has missing or unknown fields")
        required = {"base_model_digest", "environment_digest", "parameters", "model_format",
                    "budget", "save_steps", "topology"}
        if set(configuration["semantics"]) != required:
            raise DataflowError("INVALID_CONTRACT", "fit semantics has missing or unknown fields")
    connected = {}
    for edge in graph.value_edges:
        target = edge.consumer, edge.input_port
        if target in connected:
            raise DataflowError("INVALID_CONTRACT", "duplicate value input port")
        connected[target] = edge
        if edge.kind == "fixed_dataset":
            node = nodes.get(edge.producer)
            fixed = node and (node.kind == "source" and node.kernel.kind == "lance" or
                               node.kind == "terminal" and node.kernel.kind == "fixed_dataset"
                               and node.kernel.configuration.get("sink_id") == node.occurrence_id)
            if not fixed or edge.consumer not in jobs:
                raise DataflowError("INVALID_CONTRACT", "fixed dataset edge requires an input publisher and fit")
        elif edge.producer not in jobs:
            raise DataflowError("INVALID_CONTRACT", "manifest producer must be a fit job")
    expected = {(job.occurrence_id, name) for job in graph.jobs
                for name in ("train", *job.configuration["monitor_schemas"])}
    if expected != {target for target, edge in connected.items() if edge.kind == "fixed_dataset"}:
        raise DataflowError("INVALID_CONTRACT", "fixed training ports differ from recipe")
    for node in graph.nodes:
        edge = connected.get((node.occurrence_id, "model"))
        step = node.kernel.configuration.get("predict_step")
        if step is not None and (node.kernel.kind != "MapBatchesOp" or edge is None or edge.step != step):
            raise DataflowError("INVALID_CONTRACT", "prediction branch lacks exact model selection")
    for edge in graph.value_edges:
        if edge.kind != "model_manifest":
            continue
        if edge.consumer in jobs and edge.input_port == "parent" and jobs[edge.consumer].configuration["parent"] is None:
            continue
        node = nodes.get(edge.consumer)
        if (not node or node.kernel.kind != "MapBatchesOp" or edge.input_port != "model"
                or edge.step != node.kernel.configuration.get("predict_step")):
            raise DataflowError("INVALID_CONTRACT", "model value targets an incompatible consumer")


def reference_only_sources(graph):
    """Such Lance sources supply a version, and own zero row participants."""
    fixed = {edge.producer for edge in graph.value_edges if edge.kind == "fixed_dataset"}
    row_sources = {edge.producer for edge in graph.edges}
    return (fixed & set(graph.sources)) - row_sources


def physical_values(graph, stages):
    jobs = tuple(FitJobStage("fit-" + job.occurrence_id, job.occurrence_id) for job in graph.jobs)
    owner = {occ: stage.stage_id for stage in stages for occ in stage.occurrences}
    owner.update((job.occurrence_id, job.stage_id) for job in jobs)
    owner.update((src, src) for src in reference_only_sources(graph))
    return jobs, tuple(replace(edge, producer=owner[edge.producer], consumer=owner[edge.consumer])
                       for edge in graph.value_edges)


def validate_training_inputs(graph, physical, *, bounds):
    """Use sealed metadata only. The owner verifies actual materializations before fit."""
    from ..data.training_schema import validate_columns
    from demiflow.execution.compiler_support.dataflow_schema import node_schemas
    snapshots = {s.source_id: s for s in physical.sources} if physical is not None else {}
    by_id = {node.occurrence_id: node for node in graph.nodes}
    schemas = node_schemas(graph, physical, bounds=bounds)
    jobs = {job.occurrence_id: job for job in graph.jobs}
    inputs = {}
    for edge in graph.edges:
        inputs.setdefault(edge.consumer, []).append(edge.producer)
    roots = set()
    for edge in graph.value_edges:
        if edge.kind == "fixed_dataset":
            roots.add(edge.producer)
            job = jobs[edge.consumer]
            required = (job.configuration["train_schema"] if edge.input_port == "train"
                        else job.configuration["monitor_schemas"][edge.input_port])
            schema = schemas.get(edge.producer)
            if schema is not None:
                validate_columns(schema, required)
        elif edge.input_port == "model":
            holdout, = inputs[edge.consumer]
            roots.add(holdout)
            schema = schemas.get(holdout)
            if schema is not None:
                validate_columns(schema, by_id[edge.consumer].kernel.configuration["predict_input_schema"])
    if physical is not None:
        visited = set()
        while roots:
            key = roots.pop()
            if key in visited:
                continue
            visited.add(key)
            snapshot = snapshots.get(key)
            if snapshot and (not snapshot.finite or snapshot.replay in {"attempt_only", "lease_queue"}):
                raise DataflowError("UNSUPPORTED_CAPABILITY", "training inputs require a fixed finite dataset",
                                    context={"source_id": key})
            roots.update(inputs.get(key, ()))
