"""Select original execution facts and project history without running business code."""
from __future__ import annotations

from demiflow.execution.contracts.pipeline import NATIVE_SUBMISSION_FORMAT as FORMAT

from pathlib import Path
from dataclasses import replace
from demiflow.execution.contracts.dataflow import DataflowGraph, HistoricalBinding, NodeObservation, PhysicalDataflow, SealedExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError as _err
from demiflow.execution.recovery_contract import RecoveryContractError, identifier
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.contracts.dataflow_codec import artifact_reference_fields as _ref_json
from demiflow.execution.seal.history import load_v3_binding, load_native_binding
from demiflow.execution.checkpoint.history import latest_checkpoint_image
PLAN_VIEW_SCHEMA = "demiflow.plan_view.v3"
_SEALED_PLAN_KEY = "-sealed-plan"
_DATAFLOW_SEALED_KEY = "-dataflow-sealed"


def _check_action_id(action_id):
    try:
        identifier(action_id)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "action_id invalid: " + str(exc))

def _check_max_bytes(max_bytes):
    if type(max_bytes) is not int or max_bytes < 0:
        raise _err("INVALID_CONTRACT", "max_bytes must be a nonnegative int")

def load_execution_version(directory: Path, action_id: str, *, max_bytes: int) -> HistoricalBinding:
    """Read the original sealed facts for one action and bind them by version.

    Reads the journal-pointed documents directly (no ``load_sealed_plan`` /
    ``load_dataflow`` callback). v1/v2 chain facts and the v3 native seal marker
    are validated by their own original rules; a historical identity is never
    recomputed. Unknown/future schemas and absent facts raise
    ``INCOMPATIBLE_HISTORY`` rather than fabricating a current projection.
    """
    _check_action_id(action_id)
    _check_max_bytes(max_bytes)
    directory = Path(directory)
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise _err("INCOMPATIBLE_HISTORY", "run journal is absent; no sealed execution facts")
    v3 = journal.source_binding(action_id + _DATAFLOW_SEALED_KEY)
    v1 = journal.source_binding(action_id + _SEALED_PLAN_KEY)
    if v3 is not None and v1 is not None:
        raise _err("INVALID_CONTRACT", "action carries both v3 and v1/v2 sealed facts")
    if v3 is not None:
        return load_v3_binding(_engine_run_id(journal), directory, action_id, v3)
    if v1 is not None:
        binding = load_native_binding(_engine_run_id(journal), directory, action_id, v1, max_bytes=max_bytes)
        image = latest_checkpoint_image(journal, directory, action_id, max_bytes=max_bytes)
        return replace(binding, restore_image=image)
    raise _err("INCOMPATIBLE_HISTORY",
               "action has no sealed plan facts (pre-seal run); cannot bind a historical execution")

def _engine_run_id(journal):
    binding = journal.recovery_identity()
    return binding.get("run_id") if type(binding) is dict else None

def build_plan_view(graph: DataflowGraph, sealed: SealedExecution, physical: PhysicalDataflow,
                    *, observations: tuple[NodeObservation, ...]) -> dict:
    """Project the real logical/physical plan, resources, exchange reasons and
    recovery limitations into the workbench's sealed-plan view shape.

    Pure: reads only its arguments, performs no I/O and no recompilation. Each
    occurrence is preserved (the same function at two call sites is two nodes, never
    merged). A node without an observation is explicitly unobserved; success is
    never inferred from the run state. Bridge fields are kept for the identity join.
    """
    if not isinstance(graph, DataflowGraph):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a DataflowGraph")
    if not isinstance(sealed, SealedExecution):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a SealedExecution")
    if not isinstance(physical, PhysicalDataflow):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a PhysicalDataflow")
    if type(observations) is not tuple:
        raise _err("INVALID_CONTRACT", "observations must be a tuple of NodeObservation")
    if graph.dag_id != physical.dag_id:
        raise _err("INVALID_CONTRACT", "graph and physical dag_id differ")
    revision = physical.plan_revision
    if type(revision) is not int or not 0 <= revision <= 2**53 - 1:
        raise _err("INVALID_CONTRACT", "plan_revision must be a non-negative safe integer")
    occurrences = {node.occurrence_id for node in graph.nodes}
    for endpoint in (*graph.sources, *graph.terminals):
        if endpoint not in occurrences:
            raise _err("INVALID_CONTRACT", "source/terminal occurrence is outside the node set")
    obs_by_occurrence = {}
    for obs in observations:
        if not isinstance(obs, NodeObservation):
            raise _err("INVALID_CONTRACT", "observations must contain NodeObservation values")
        if obs.plan_id != physical.plan_id:
            raise _err("INVALID_CONTRACT", "observation plan_id differs from the sealed plan")
        obs_by_occurrence[obs.occurrence_id] = obs
    nodes = []
    output_ports = {}
    for edge in graph.edges:
        output_ports.setdefault(edge.producer, [])
        if edge.output_port not in output_ports[edge.producer]:
            output_ports[edge.producer].append(edge.output_port)
    for node in graph.nodes:
        nodes.append({
            "logical_node_id": node.semantic_id,
            "occurrence_id": node.occurrence_id,
            "op_kind": node.kernel.kind,
            "operator_spec_ref": (_ref_json(node.program_ref) if node.program_ref is not None else None),
            "input_ports": [port.name for port in node.kernel.inputs],
            "output_ports": output_ports.get(node.occurrence_id, []),
            "shared_token": node.shared_token,
            "traits": {"state": node.kernel.state, "effect": node.kernel.effect,
                       "resource_role": node.kernel.resource_role, "mergeable": node.kernel.mergeable},
        })
    edges = [{"edge_id": edge.edge_id,
              "from": {"logical_node_id": _semantic(graph, edge.producer),
                       "occurrence_id": edge.producer, "port": edge.output_port},
              "to": {"logical_node_id": _semantic(graph, edge.consumer),
                     "occurrence_id": edge.consumer, "port": edge.input_port}}
             for edge in graph.edges]
    shared = _shared_ancestors(graph)
    dag = {"schema_version": graph.schema, "dag_id": graph.dag_id, "action_id": graph.action_id,
           "nodes": nodes, "edges": edges, "shared_ancestors": shared,
           "sources": list(graph.sources), "terminals": list(graph.terminals)}
    if graph.jobs:
        from demiflow.execution.plan_capture import project_training_ports
        project_training_ports(graph, dag)
    stage_by_occurrence = {job.occurrence_id: {"stage_id": job.stage_id, "kind": "fit"} for job in physical.jobs}
    for stage in physical.stages:
        for occ in stage.occurrences:
            stage_by_occurrence[occ] = {
                "stage_id": stage.stage_id, "partition_count": len(stage.partitions),
                "profile": _profile_json(stage.profile),
                "replica_min": stage.replica_min, "replica_target": stage.replica_target,
                "replica_max": stage.replica_max, "resident_bytes": stage.resident_bytes,
                "output_bytes": stage.output_bytes, "state_bytes": stage.state_bytes,
            }
    exchanges = [{"channel_id": ex.channel_id, "edge_id": ex.edge_id, "mode": ex.mode,
                  "routing": ex.routing, "keys": list(ex.keys), "reason": ex.reason,
                  "read_domain": ex.read_domain, "route_epoch": ex.route_epoch,
                  "credit_rows": ex.credit_rows, "credit_bytes": ex.credit_bytes,
                  "storage_bytes": ex.storage_bytes,
                  "producer_count": len(ex.producer_partitions),
                  "consumer_count": len(ex.consumer_partitions)}
                 for ex in physical.exchanges]
    physical_view = {
        "plan_id": physical.plan_id, "plan_revision": physical.plan_revision,
        "recovery": physical.recovery, "limitations": list(physical.limitations),
        "stages": stage_by_occurrence, "exchanges": exchanges,
        "policy": {"provider": physical.policy.provider, "durability": physical.policy.durability,
                   "fusion": physical.policy.fusion, "default_partitions": physical.policy.default_partitions,
                   "max_replicas": physical.policy.max_replicas,
                   "required_recovery": physical.policy.required_recovery},
        "residency": {"simultaneous_groups": [list(g) for g in physical.residency.simultaneous_groups],
                      "control_reserved_bytes": physical.residency.control_reserved_bytes,
                      "consumer_reserved_slots": physical.residency.consumer_reserved_slots,
                      "required_spill_bytes": physical.residency.required_spill_bytes},
        "scopes": [{"scope_id": s.scope_id, "generation": s.generation,
                    "membership": list(s.membership), "completion_condition": s.completion_condition}
                   for s in physical.scopes],
    }
    if physical.jobs:
        physical_view["value_edges"] = [vars(edge).copy() for edge in physical.value_edges]
    node_observations = []
    for node in (*graph.nodes, *graph.jobs):
        obs = obs_by_occurrence.get(node.occurrence_id)
        if obs is None:
            node_observations.append({"occurrence_id": node.occurrence_id, "observed": False,
                                      "availability": "not_observed", "metrics": None})
        else:
            node_observations.append({"occurrence_id": node.occurrence_id, "attempt": obs.attempt,
                                      "plan_id": obs.plan_id, "observed": bool(obs.observed),
                                      "availability": "observed" if obs.observed else "not_observed",
                                      "metrics": dict(obs.metrics)})
    return {"schema": PLAN_VIEW_SCHEMA, "action_id": graph.action_id, "dag_id": graph.dag_id,
            "plan_id": physical.plan_id, "plan_revision": physical.plan_revision,
            "binding_level": "sealed", "status": "sealed", "dag": dag, "physical": physical_view,
            "node_map": {}, "identity_bridge": {"available": False, "reason": "identity_bridge_missing",
                                                "detail": "build_plan_view keeps the bridge ref; joining is done by the caller"},
            "identity_bridge_ref": _ref_json(sealed.identity_bridge_ref),
            "code_pin_ref": _ref_json(sealed.code_pin_ref),
            "node_observations": node_observations}

def _semantic(graph, occurrence_id):
    for node in graph.nodes:
        if node.occurrence_id == occurrence_id:
            return node.semantic_id
    raise _err("INVALID_CONTRACT", "edge references an unknown occurrence")

def _shared_ancestors(graph):
    consumers = {}
    for edge in graph.edges:
        consumers.setdefault(edge.producer, set()).add(edge.consumer)
    shared = []
    seen = set()
    for node in graph.nodes:
        if len(consumers.get(node.occurrence_id, ())) > 1:
            shared.append({"logical_node_id": node.semantic_id, "occurrence_id": node.occurrence_id})
    return shared

def _profile_json(profile):
    to_dict = getattr(profile, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    requests = getattr(profile, "requests", None)
    if isinstance(requests, dict):
        return {"requests": dict(requests)}
    return {"requests": {}}


def native_submission_view(captured):
    """Product-neutral occurrence/edge/target projection of an admitted plan."""
    from demiflow.execution.plan_capture import build_logical_plan_view
    from demiflow.execution.contracts.dataflow import thaw_record_value
    contract = captured.submission
    if not contract or contract['format'] != FORMAT:
        raise ValueError('An admitted native capture is required')
    graph = captured.compiled.graph
    action = contract['action_id']
    keys = {n.occurrence_id: action if n.occurrence_id in graph.terminals else n.occurrence_id for n in (*graph.nodes, *graph.jobs)}
    targets = {o['node_id']: o for o in contract['outputs']}
    nodes = []
    for n in graph.nodes:
        key = keys[n.occurrence_id]
        output = targets.get(key)
        kind = 'write_table' if n.occurrence_id in graph.terminals else 'save_lance' if output else n.kernel.kind
        parameters = thaw_record_value(n.kernel.configuration)
        if output:
            parameters.update(uri=output['uri'], mode=output.get('writer_mode', 'create'))
            if 'stage' in output:
                parameters.update(stage=output['stage'], max_index_bytes=output['max_index_bytes'])
        gpu = sum(device['count'] for device in n.kernel.profile.requests.get('accelerators', ())
                  if device['type'] == 'gpu')
        capabilities = {'capability_fact': {'resource_declaration': {'gpu': gpu}}} if gpu else {}
        nodes.append(dict(id=key, kind=kind, role='action' if n.occurrence_id in graph.terminals else 'source' if n.kind == 'source' else 'transform',
            parameters=parameters, normalized_parameters={'schema': {'identity': output['schema_identity']}} if output else {},
            analysis={}, contract={}, source=None, operator=None, api=n.kernel.kind, **capabilities))
    edges = [dict(**{'from': keys[e.producer], 'to': keys[e.consumer]}, port=i) for i, e in enumerate(graph.edges)]
    for job in graph.jobs:
        nodes.append(dict(id=job.occurrence_id, kind="fit", role="value", parameters=thaw_record_value(job.configuration),
                          normalized_parameters={}, analysis={"topology": thaw_record_value(job.configuration["semantics"]["topology"])},
                          contract={}, source=None, operator=None, api="fit"))
    values = [dict(**{'from': keys[e.producer], 'to': keys[e.consumer]}, port=e.input_port,
                   kind=e.kind, step=e.step) for e in graph.value_edges]
    return dict(nodes=nodes, edges=edges, actions=[action], operators=[], **({'value_edges': values} if values else {}),
        observations=[dict(action_id=action, node_id=n['id'], occurrence=0) for n in nodes],
        dag=build_logical_plan_view(graph, declarations=captured.compiled.declarations,
                                    observation_labels=captured.compiled.observation_labels))
