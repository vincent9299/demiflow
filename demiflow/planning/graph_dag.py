"""Native static-graph compilation adapter (C package, architecture upgrade).

Composes per-program linear DAG fragments (from the frozen ``dag.py`` public
``compile_logical_dag``) into one full-graph logical DAG.  Same-function
operations in different targets keep distinct execution instances; a shared
ancestor is kept once.  Only ``dag.py`` public API and ``recovery_contract.identity``
are used — no private semantics helpers, no Forge descriptors, no runtime state.
"""
from __future__ import annotations

from ..execution.recovery_contract import identity
from .dag import (
    compile_logical_dag,
    dag_id_for,
    edge_id_for,
    occurrence_id_for,
    validate_logical_dag,
)


def compile_graph_logical_dag(*, action_id, source, ancestor, targets):
    """Compile a static ancestor→targets graph into one logical DAG.

    Returns ``(logical_dag, programs)`` where *programs* is a list of
    ``{group_id, role, reader_id, operators}`` dicts.  Each operator entry
    carries ``program_index``, ``logical_node_id`` and ``occurrence_id`` so the
    caller can build ``operator_bindings`` against the physical plan.

    The ancestor's operations are shared (executed once); each target's
    operations are re-derived with a stable structural position so identical
    functions at the same local index do not collapse into one node.
    """
    ancestor_plan = ancestor["plan"]
    ancestor_dag = compile_logical_dag(
        action_id=action_id, source=source,
        operations=ancestor_plan.operations,
        terminal_kind="materialize", sink_ids=("output",))
    source_node = ancestor_dag["nodes"][0]
    ancestor_ops = ancestor_dag["nodes"][1:-1]

    programs = [{
        "group_id": "ancestor",
        "role": "ancestor",
        "reader_id": None,
        "operators": [
            {"program_index": j,
             "logical_node_id": node["logical_node_id"],
             "occurrence_id": node["occurrence_id"]}
            for j, node in enumerate(ancestor_ops)],
    }]

    all_nodes = [source_node] + list(ancestor_ops)
    edges = []
    terminals = []
    target_first_nodes = []
    target_last_nodes = []

    for i, target in enumerate(targets):
        reader_id = target["reader_id"]
        target_dag = compile_logical_dag(
            action_id=action_id, source=source,
            operations=target["plan"].operations,
            terminal_kind="materialize", sink_ids=(reader_id,))
        raw_ops = target_dag["nodes"][1:-1]
        terminal_node = target_dag["nodes"][-1]
        rederived = []
        for j, node in enumerate(raw_ops):
            new_id = identity([node["logical_node_id"], "target", i])
            new_occ = occurrence_id_for(action_id, new_id, j)
            rederived.append({
                "logical_node_id": new_id,
                "occurrence_id": new_occ,
                "op_kind": node["op_kind"],
                "operator_spec_ref": node["operator_spec_ref"],
                "input_ports": list(node["input_ports"]),
                "output_ports": list(node["output_ports"]),
                "traits": dict(node["traits"]),
            })
        all_nodes.extend(rederived)
        all_nodes.append(terminal_node)
        terminals.append(terminal_node)
        programs.append({
            "group_id": "target-" + reader_id,
            "role": "target",
            "reader_id": reader_id,
            "operators": [
                {"program_index": j,
                 "logical_node_id": node["logical_node_id"],
                 "occurrence_id": node["occurrence_id"]}
                for j, node in enumerate(rederived)],
        })
        target_first_nodes.append(rederived[0] if rederived else terminal_node)
        target_last_nodes.append(rederived[-1] if rederived else terminal_node)

        for j in range(len(rederived) - 1):
            edges.append(_edge(rederived[j], rederived[j + 1]))
        if rederived:
            edges.append(_edge(rederived[-1], terminal_node))

    ancestor_tail = ancestor_ops[-1] if ancestor_ops else source_node
    for first in target_first_nodes:
        edges.append(_edge(ancestor_tail, first))
    for j in range(len(ancestor_ops) - 1):
        edges.append(_edge(ancestor_ops[j], ancestor_ops[j + 1]))
    if ancestor_ops:
        edges.append(_edge(source_node, ancestor_ops[0]))

    shared = []
    if len(targets) > 1:
        consumer_edges = [
            e["edge_id"] for e in edges
            if e["from"]["logical_node_id"] == ancestor_tail["logical_node_id"]]
        shared.append({
            "logical_node_id": ancestor_tail["logical_node_id"],
            "occurrence_id": ancestor_tail["occurrence_id"],
            "consumers": sorted(consumer_edges),
        })

    value = {
        "schema_version": "demiflow.logical_dag.v1",
        "action_id": action_id,
        "nodes": all_nodes,
        "edges": edges,
        "sources": ancestor_dag["sources"],
        "terminals": [
            {"logical_node_id": t["logical_node_id"], "kind": "materialize",
             "sink_ids": [target["reader_id"]]}
            for t, target in zip(terminals, targets)],
        "shared_ancestors": shared,
    }
    value["dag_id"] = dag_id_for(value)
    return validate_logical_dag(value), programs


def _edge(producer, consumer):
    return {
        "edge_id": edge_id_for(
            producer["logical_node_id"], "out",
            consumer["logical_node_id"], "in"),
        "from": {"logical_node_id": producer["logical_node_id"], "port": "out"},
        "to": {"logical_node_id": consumer["logical_node_id"], "port": "in"},
    }