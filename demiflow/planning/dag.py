"""Logical DAG / partition / channel compilation (U1, §5 of the E2 design).

Additive planning artifacts only: the public Dataset/Pipeline API, plan_action
and existing executors are untouched; driver integration belongs to E3.
Stable logical identities derive from semantic declarations, never from worker
counts, chunk sizes, attempts or physical locations.
"""
from __future__ import annotations

from dataclasses import asdict

from ..data.runtime_contract import RuntimeContractError
from ..execution.recovery_contract import document, identifier, identity
from ..execution.recovery_identity import describe, source_identity
from .traits import (
    unified_operation_traits, unified_source_traits, unified_terminal_traits,
)

LOGICAL_DAG_SCHEMA = "demiflow.logical_dag.v1"
PHYSICAL_PLAN_SCHEMA = "demiflow.physical_execution_plan.v1"

_SCHEME_KINDS = frozenset({"single", "forward", "hash", "target", "rebalance", "broadcast"})
_EXCHANGE_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_ROUTINGS = frozenset({"forward", "target", "rebalance", "hash", "broadcast", "fan_out", "fan_in"})
_GROUP_ROLES = frozenset({"cpu_prepare", "gpu_model", "io", "control"})
_DEFAULT_TRAITS = {
    "category": "transform", "worker_model": "ephemeral", "lifecycle": "transient",
    "policy_class": "ephemeral_worker", "elastic": True, "input_mode": "incremental",
    "cardinality": "one", "ordering": "unordered", "state_ownership": "none",
    "effect_kind": "pure", "batching": "record",
}


def _traits_dict(traits):
    value = asdict(traits)
    document(value)
    return value


def _operation_semantics(operation):
    from ..execution.plan_facts import operation_capability, operation_facts
    try:
        facts = operation_facts(operation)
    except (RuntimeContractError, RuntimeError, ValueError):
        facts = None
    semantic = {"operation": facts if facts is not None else describe(operation)}
    kind = operation_capability(operation)["kind"]
    if kind == "unknown":
        kind = type(operation).__name__
    return semantic, kind


def occurrence_id_for(action_id, logical_node_id, occurrence):
    return identity([action_id, logical_node_id, occurrence])


def edge_id_for(from_node, from_port, to_node, to_port):
    return identity([from_node, from_port, to_node, to_port])


def partition_id_for(scope_id, scheme_version, index):
    return identity([scope_id, scheme_version, index])


def channel_id_for(edge_id, scheme_version):
    return identity([edge_id, scheme_version])


def partition_scheme(*, kind, partition_count, key_fields=None, scheme_version=1):
    if kind not in _SCHEME_KINDS:
        raise ValueError("unknown partition scheme kind")
    if type(partition_count) is not int or partition_count < 1:
        raise ValueError("partition_count must be positive")
    if type(scheme_version) is not int or scheme_version < 1:
        raise ValueError("scheme_version must be positive")
    if kind == "hash" and not key_fields:
        raise ValueError("hash partitioning requires key fields")
    value = {"kind": kind, "partition_count": partition_count, "scheme_version": scheme_version}
    if key_fields is not None:
        if type(key_fields) not in (list, tuple) or any(type(field) is not str for field in key_fields):
            raise ValueError("key_fields must be strings")
        value["key_fields"] = list(key_fields)
    document(value)
    return value


def compile_logical_dag(*, action_id, source, operations, terminal_kind,
                        sink_ids=(), operator_spec_refs=None):
    """Compile the existing linear-chain LogicalPlan + SourcePlan + terminal.

    The legacy single chain is a compatibility input, not a replaced object.
    Repeated compilation of the same semantic plan yields the same dag_id.
    """
    identifier(action_id)
    source_semantic = source_identity(source)
    source_node_id = identity({"source": source_semantic})
    source_id = identity(source_semantic)
    scheme = partition_scheme(kind="single", partition_count=1)
    nodes = [{
        "logical_node_id": source_node_id,
        "occurrence_id": occurrence_id_for(action_id, source_node_id, 0),
        "op_kind": type(source).__name__, "operator_spec_ref": None,
        "input_ports": [], "output_ports": ["out"],
        "traits": _traits_dict(unified_source_traits(source)),
    }]
    chain = [source_node_id]
    for index, operation in enumerate(operations):
        semantic, kind = _operation_semantics(operation)
        node_id = identity([semantic, index])
        spec_ref = None
        if operator_spec_refs is not None:
            spec_ref = operator_spec_refs[index]
            identifier(spec_ref)
        nodes.append({
            "logical_node_id": node_id,
            "occurrence_id": occurrence_id_for(action_id, node_id, index),
            "op_kind": kind, "operator_spec_ref": spec_ref,
            "input_ports": ["in"], "output_ports": ["out"],
            "traits": _traits_dict(unified_operation_traits(operation)),
        })
        chain.append(node_id)
    terminal_semantic = {"terminal": terminal_kind, "sink_ids": sorted(sink_ids)}
    document(terminal_semantic)
    terminal_id = identity(terminal_semantic)
    nodes.append({
        "logical_node_id": terminal_id,
        "occurrence_id": occurrence_id_for(action_id, terminal_id, len(chain)),
        "op_kind": terminal_kind, "operator_spec_ref": None,
        "input_ports": ["in"], "output_ports": [],
        "traits": _traits_dict(unified_terminal_traits()),
    })
    chain.append(terminal_id)
    edges = []
    for producer, consumer in zip(chain, chain[1:]):
        edges.append({
            "edge_id": edge_id_for(producer, "out", consumer, "in"),
            "from": {"logical_node_id": producer, "port": "out"},
            "to": {"logical_node_id": consumer, "port": "in"},
        })
    value = {
        "schema_version": LOGICAL_DAG_SCHEMA,
        "action_id": action_id,
        "nodes": nodes, "edges": edges,
        "sources": [{"source_id": source_id, "source_identity_ref": source_semantic,
                     "partition_scheme": scheme}],
        "terminals": [{"logical_node_id": terminal_id, "kind": terminal_kind,
                       "sink_ids": sorted(sink_ids)}],
        "shared_ancestors": [],
    }
    value["dag_id"] = dag_id_for(value)
    return validate_logical_dag(value)


def compile_declared_dag(*, action_id, nodes, edges, descriptors):
    """Compatibility input: checked/plan_facts declared-flow nodes and edges.

    ``descriptors`` maps each declared node id to its bounded semantic
    declaration (source_facts/operation_facts output or equivalent). Occurrence
    structure reuses plan_facts.execution_instances, so shared bridges keep the
    existing reuse semantics and every ancestor executes once per action.
    """
    from ..execution.plan_facts import execution_instances
    identifier(action_id)
    by_id = {node["id"]: node for node in nodes}
    instances = execution_instances(nodes, edges, [action_id])
    logical_by_node = {}
    for node_id in by_id:
        if node_id not in descriptors:
            raise ValueError("declared node lacks a semantic descriptor: " + str(node_id))
        descriptor = descriptors[node_id]
        document(descriptor)
        logical_by_node[node_id] = identity(descriptor)
    node_entries = []
    for instance in instances:
        node_id = instance["node_id"]
        logical = logical_by_node[node_id]
        declared = by_id[node_id]
        traits = dict(_DEFAULT_TRAITS)
        traits.update(descriptors[node_id].get("traits", {}))
        document(traits)
        node_entries.append({
            "logical_node_id": logical,
            "occurrence_id": occurrence_id_for(action_id, logical, instance["occurrence"]),
            "op_kind": declared["kind"], "operator_spec_ref": None,
            "input_ports": ["in"] if declared.get("role") != "source" else [],
            "output_ports": ["out"] if declared.get("role") != "action" else [],
            "traits": traits,
        })
    edge_entries = []
    for edge in sorted(edges, key=lambda item: (item["from"], item["to"], item["port"])):
        if edge["from"] not in logical_by_node or edge["to"] not in logical_by_node:
            continue
        edge_entries.append({
            "edge_id": edge_id_for(logical_by_node[edge["from"]], str(edge["port"]),
                                   logical_by_node[edge["to"]], "in"),
            "from": {"logical_node_id": logical_by_node[edge["from"]], "port": str(edge["port"])},
            "to": {"logical_node_id": logical_by_node[edge["to"]], "port": "in"},
        })
    consumers = {}
    for edge in edge_entries:
        consumers.setdefault(edge["to"]["logical_node_id"], []).append(edge["edge_id"])
    shared = []
    seen = set()
    for instance in instances:
        logical = logical_by_node[instance["node_id"]]
        incoming = consumers.get(logical, [])
        if (instance["mode"] == "reuse" or len(incoming) > 1) and logical not in seen:
            seen.add(logical)
            shared.append({"logical_node_id": logical,
                           "occurrence_id": occurrence_id_for(action_id, logical, 0),
                           "consumers": sorted(incoming)})
    sources = [{"source_id": logical_by_node[node["id"]],
                "source_identity_ref": descriptors[node["id"]],
                "partition_scheme": partition_scheme(kind="single", partition_count=1)}
               for node in nodes if node.get("role") == "source"]
    terminals = [{"logical_node_id": logical_by_node[node["id"]], "kind": node["kind"],
                  "sink_ids": sorted(node.get("sink_ids", []))}
                 for node in nodes if node.get("role") == "action"]
    value = {
        "schema_version": LOGICAL_DAG_SCHEMA, "action_id": action_id,
        "nodes": node_entries, "edges": edge_entries,
        "sources": sources, "terminals": terminals, "shared_ancestors": shared,
    }
    value["dag_id"] = dag_id_for(value)
    return validate_logical_dag(value)


def dag_id_for(value):
    return identity({key: item for key, item in value.items() if key != "dag_id"})


def _reject_logical_cycle(adjacency):
    """Iterative three-color DFS over unique logical nodes; a back-edge is a cycle.

    Vertices are unique logical_node_ids, so a shared ancestor's reuse occurrences
    collapse to one vertex and legitimate fan-out stays acyclic.
    """
    color = dict.fromkeys(adjacency, 0)
    for start in adjacency:
        if color[start]:
            continue
        color[start] = 1
        stack = [(start, iter(adjacency[start]))]
        while stack:
            node, children = stack[-1]
            for child in children:
                if color[child] == 1:
                    raise ValueError("cycle in logical dag")
                if color[child] == 0:
                    color[child] = 1
                    stack.append((child, iter(adjacency[child])))
                    break
            else:
                color[node] = 2
                stack.pop()


def validate_logical_dag(value):
    keys = {"schema_version", "dag_id", "action_id", "nodes", "edges", "sources",
            "terminals", "shared_ancestors"}
    if type(value) is not dict or set(value) != keys:
        raise ValueError("logical dag key set is unsupported")
    if value["schema_version"] != LOGICAL_DAG_SCHEMA:
        raise ValueError("unsupported logical dag schema")
    identifier(value["action_id"])
    identifier(value["dag_id"])
    if value["dag_id"] != dag_id_for(value):
        raise ValueError("dag_id does not bind the content")
    node_keys = set()
    node_ids = set()
    for node in value["nodes"]:
        if type(node) is not dict or set(node) != {
                "logical_node_id", "occurrence_id", "op_kind", "operator_spec_ref",
                "input_ports", "output_ports", "traits"}:
            raise ValueError("logical node key set is unsupported")
        identifier(node["logical_node_id"])
        identifier(node["occurrence_id"])
        if node["occurrence_id"] in node_keys:
            raise ValueError("duplicate occurrence in logical dag")
        node_keys.add(node["occurrence_id"])
        node_ids.add(node["logical_node_id"])
        document(node["traits"])
    # Declared vertices include sources/terminals: a source behind a reused bridge
    # has no occurrence entry yet its data edges are legitimate, so referential
    # integrity spans the whole declared vertex set, not only occurrences.
    for source in value["sources"]:
        node_ids.add(source["source_id"])
    for terminal in value["terminals"]:
        node_ids.add(terminal["logical_node_id"])
    adjacency = {node_id: [] for node_id in node_ids}
    for edge in value["edges"]:
        if type(edge) is not dict or set(edge) != {"edge_id", "from", "to"}:
            raise ValueError("logical edge key set is unsupported")
        identifier(edge["edge_id"])
        for endpoint in (edge["from"], edge["to"]):
            if type(endpoint) is not dict or set(endpoint) != {"logical_node_id", "port"}:
                raise ValueError("edge endpoint key set is unsupported")
            identifier(endpoint["logical_node_id"])
        producer = edge["from"]["logical_node_id"]
        consumer = edge["to"]["logical_node_id"]
        if producer not in node_ids or consumer not in node_ids:
            raise ValueError("edge references unknown logical node")
        if producer == consumer:
            raise ValueError("self-loop edge in logical dag")
        adjacency[producer].append(consumer)
    _reject_logical_cycle(adjacency)
    document(value)
    return value


def select_exchange(consumer_traits, *, durability_declared="ephemeral"):
    """Input requirements choose the scope; policy chooses durability.

    No guessing 'needs everything' from arbitrary user functions, and required
    capabilities never silently degrade.
    """
    if durability_declared not in ("ephemeral", "durable"):
        raise ValueError("durability must be declared")
    if consumer_traits.get("input_mode") == "finite_scope":
        return "finite_sealed", "consumer requires the complete input scope"
    if durability_declared == "durable":
        return "durable_incremental", "policy declares durable incremental exchange"
    return "memory_incremental", "incremental consumer with ephemeral policy default"


def _routing_for(scheme_kind, producer_count, consumer_count):
    if scheme_kind in ("hash", "target", "rebalance", "broadcast"):
        return scheme_kind
    if producer_count == 1 and consumer_count > 1:
        return "fan_out"
    if producer_count > 1 and consumer_count == 1:
        return "fan_in"
    return "forward"


def build_channel_plan(*, edge_id, scheme_version, producer_partitions, consumer_partitions,
                       exchange_mode, routing, credit_budget=None, durability_declared="ephemeral",
                       read_domain, seal_scope=None):
    if exchange_mode not in _EXCHANGE_MODES or routing not in _ROUTINGS:
        raise ValueError("unsupported exchange mode/routing")
    if durability_declared not in ("ephemeral", "durable"):
        raise ValueError("durability must be declared")
    if exchange_mode == "finite_sealed" and seal_scope is None:
        raise ValueError("finite sealed exchange requires a seal scope")
    if exchange_mode != "finite_sealed" and seal_scope is not None:
        raise ValueError("incremental exchange cannot carry a seal scope")
    budget = credit_budget or {"rows": 1024, "bytes": 8 * 1024**2}
    if type(budget) is not dict or set(budget) != {"rows", "bytes"} \
            or any(type(budget[name]) is not int or budget[name] < 1 for name in ("rows", "bytes")):
        raise ValueError("credit budget needs positive rows/bytes")
    value = {
        "channel_id": channel_id_for(edge_id, scheme_version),
        "producer_partitions": list(producer_partitions),
        "consumer_partitions": list(consumer_partitions),
        "exchange_mode": exchange_mode, "routing": routing,
        "credit_budget": budget,
        "durability": {"declared": durability_declared, "read_domain": read_domain},
    }
    if seal_scope is not None:
        if type(seal_scope) is not dict or set(seal_scope) != {"scope_id", "membership", "generation"}:
            raise ValueError("seal scope key set is unsupported")
        value["seal_scope"] = seal_scope
    return validate_channel_plan(value)


def validate_channel_plan(value):
    keys = {"channel_id", "producer_partitions", "consumer_partitions", "exchange_mode",
            "routing", "credit_budget", "durability"}
    if type(value) is not dict or not keys <= set(value) or set(value) - keys - {"seal_scope"}:
        raise ValueError("channel plan key set is unsupported")
    identifier(value["channel_id"])
    if value["exchange_mode"] not in _EXCHANGE_MODES or value["routing"] not in _ROUTINGS:
        raise ValueError("unsupported exchange mode/routing")
    for name in ("producer_partitions", "consumer_partitions"):
        if type(value[name]) is not list or not value[name]:
            raise ValueError(name + " must be a nonempty list")
        for item in value[name]:
            identifier(item)
    durability = value["durability"]
    if type(durability) is not dict or set(durability) != {"declared", "read_domain"} \
            or durability["declared"] not in ("ephemeral", "durable") \
            or type(durability["read_domain"]) is not str or not durability["read_domain"]:
        raise ValueError("durability declaration is unsupported")
    if value["exchange_mode"] == "durable_incremental" and durability["declared"] != "durable":
        raise ValueError("durable exchange requires a durable declaration")
    budget = value["credit_budget"]
    if type(budget) is not dict or set(budget) != {"rows", "bytes"} \
            or any(type(budget[name]) is not int or budget[name] < 1 for name in ("rows", "bytes")):
        raise ValueError("credit budget needs positive rows/bytes")
    if "seal_scope" in value:
        scope = value["seal_scope"]
        if type(scope) is not dict or set(scope) != {"scope_id", "membership", "generation"}:
            raise ValueError("seal scope key set is unsupported")
        identifier(scope["scope_id"])
        if type(scope["generation"]) is not int or scope["generation"] < 0:
            raise ValueError("seal generation must be nonnegative")
    document(value)
    return value


def compile_physical_execution_plan(*, logical_dag, plan_revision, artifact_manifest_ref,
                                    worker_groups, recovery, feasibility_snapshot_ref,
                                    partition_count=1, scheme_kind="single", key_fields=None,
                                    scheme_version=1, credit_budget=None,
                                    durability_declared="ephemeral", read_domain,
                                    stage_budgets=None):
    """Derive stages/partitions/channels from a LogicalDag deterministically.

    Worker counts live only in worker_groups/stage budgets (physical facts);
    they never feed logical identities. Exchange selections carry reasons.
    """
    validate_logical_dag(logical_dag)
    if type(plan_revision) is not int or plan_revision < 0:
        raise ValueError("plan_revision must be nonnegative")
    if type(artifact_manifest_ref) is not dict or set(artifact_manifest_ref) != {"manifest_id", "sha256"}:
        raise ValueError("artifact_manifest_ref needs manifest_id/sha256")
    scheme = partition_scheme(kind=scheme_kind, partition_count=partition_count,
                              key_fields=key_fields, scheme_version=scheme_version)
    partitions_by_node = {}
    stages = []
    for node in logical_dag["nodes"]:
        partitions = [partition_id_for(node["logical_node_id"], scheme_version, index)
                      for index in range(partition_count)]
        partitions_by_node[node["logical_node_id"]] = partitions
        budgets = (stage_budgets or {}).get(node["occurrence_id"], {
            "inflight_rows": 1024, "inflight_bytes": 8 * 1024**2, "output_bytes": 64 * 1024**2,
        })
        if type(budgets) is not dict or set(budgets) != {"inflight_rows", "inflight_bytes", "output_bytes"} \
                or any(type(value) is not int or value < 1 for value in budgets.values()):
            raise ValueError("stage budgets need positive row/byte bounds")
        stages.append({
            "stage_id": identity([logical_dag["dag_id"], node["occurrence_id"]]),
            "logical_node_id": node["logical_node_id"], "occurrence_id": node["occurrence_id"],
            "partitions": partitions, "traits": node["traits"], "budgets": budgets,
        })
    channels, selections = [], []
    for edge in logical_dag["edges"]:
        producer = edge["from"]["logical_node_id"]
        consumer = edge["to"]["logical_node_id"]
        consumer_node = next(node for node in logical_dag["nodes"]
                             if node["logical_node_id"] == consumer)
        mode, reason = select_exchange(consumer_node["traits"], durability_declared=durability_declared)
        producer_partitions = partitions_by_node[producer]
        consumer_partitions = partitions_by_node[consumer]
        routing = _routing_for(scheme_kind, len(producer_partitions), len(consumer_partitions))
        seal_scope = None
        if mode == "finite_sealed":
            seal_scope = {"scope_id": identity([edge["edge_id"], scheme_version]),
                          "membership": producer_partitions, "generation": scheme_version}
        channel = build_channel_plan(
            edge_id=edge["edge_id"], scheme_version=scheme_version,
            producer_partitions=producer_partitions, consumer_partitions=consumer_partitions,
            exchange_mode=mode, routing=routing, credit_budget=credit_budget,
            durability_declared=durability_declared, read_domain=read_domain, seal_scope=seal_scope,
        )
        channels.append(channel)
        selections.append({"channel_id": channel["channel_id"], "mode": mode, "reason": reason})
    stage_ids = {stage["stage_id"] for stage in stages}
    groups = []
    for group in worker_groups:
        if type(group) is not dict or set(group) != {
                "group_id", "role", "profile_ref", "stage_refs",
                "min_workers", "initial_workers", "max_workers"}:
            raise ValueError("worker group key set is unsupported")
        identifier(group["group_id"])
        if group["role"] not in _GROUP_ROLES:
            raise ValueError("unknown worker group role")
        if type(group["stage_refs"]) is not list or not set(group["stage_refs"]) <= stage_ids:
            raise ValueError("worker group references unknown stages")
        counts = [group[name] for name in ("min_workers", "initial_workers", "max_workers")]
        if any(type(count) is not int or count < 1 for count in counts) or not counts[0] <= counts[1] <= counts[2]:
            raise ValueError("worker counts must satisfy min <= initial <= max")
        groups.append(dict(group))
    if type(recovery) is not dict or set(recovery) != {
            "checkpoint_interval_ms", "unconfirmed_rows", "unconfirmed_bytes", "terminals"}:
        raise ValueError("recovery key set is unsupported")
    for name in ("checkpoint_interval_ms", "unconfirmed_rows", "unconfirmed_bytes"):
        if type(recovery[name]) is not int or recovery[name] < 1:
            raise ValueError("recovery bounds must be positive")
    value = {
        "schema_version": PHYSICAL_PLAN_SCHEMA,
        "plan_revision": plan_revision,
        "logical_dag_ref": {"dag_id": logical_dag["dag_id"],
                            "sha256": identity(logical_dag)},
        "artifact_manifest_ref": dict(artifact_manifest_ref),
        "worker_groups": groups, "stages": stages, "channels": channels,
        "exchange_selections": selections, "recovery": dict(recovery),
        "feasibility_snapshot_ref": feasibility_snapshot_ref,
    }
    value["plan_id"] = identity({key: item for key, item in value.items() if key != "plan_id"})
    return validate_physical_execution_plan(value)


def validate_physical_execution_plan(value):
    keys = {"schema_version", "plan_id", "plan_revision", "logical_dag_ref",
            "artifact_manifest_ref", "worker_groups", "stages", "channels",
            "exchange_selections", "recovery", "feasibility_snapshot_ref"}
    if type(value) is not dict or set(value) != keys:
        raise ValueError("physical execution plan key set is unsupported")
    if value["schema_version"] != PHYSICAL_PLAN_SCHEMA:
        raise ValueError("unsupported physical execution plan schema")
    identifier(value["plan_id"])
    expected = identity({key: item for key, item in value.items() if key != "plan_id"})
    if value["plan_id"] != expected:
        raise ValueError("plan_id does not bind the content")
    for channel in value["channels"]:
        validate_channel_plan(channel)
    document(value)
    return value