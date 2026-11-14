"""Dataset logical-DAG compilation with recursive PlanInput expansion (F004, C08).

The real PlanInput/KeyedSource/UnionSource tree is composed into one full-graph
logical DAG. Each linear sub-program is compiled with the frozen ``dag.py`` public
API so its LOCAL operator identities stay intact for ``verify_loaded_plan``; the
full graph re-derives every node per stable ``program_id`` so the same object at
two positions never collapses into one execution. Only ``dag.py`` public API and
``recovery_contract``/``recovery_identity`` identities are used — no business
import, no source read, no runtime state.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..data.plan import LogicalPlan
from ..data.sources import SourcePlan
from ..execution.local_kernel import KeyedSource, UnionSource
from ..execution.plan_interpreter import PlanContractError
from ..execution.recovery_contract import identifier, identity
from ..execution.recovery_identity import source_identity
from .dag import (
    compile_logical_dag,
    dag_id_for,
    edge_id_for,
    occurrence_id_for,
    partition_scheme,
    validate_logical_dag,
)

LOGICAL_DAG_SCHEMA = "demiflow.logical_dag.v1"
_MAX_PROGRAMS = 4096
_MAX_DEPTH = 32


@dataclass(frozen=True)
class ProgramSlice:
    program_id: str
    input_path: tuple[str | int, ...]
    source: SourcePlan
    plan: LogicalPlan
    sealed_operators: tuple[Mapping[str, str], ...]
    operator_bindings: tuple[Mapping[str, int | str], ...]
    source_occurrence_id: str | None = None


def compile_dataset_logical_dag(*, action_id, source, plan, terminal_kind, sink_ids):
    """Compile the real Dataset source/plan tree into (LogicalDag, ProgramSlices).

    ``input_path`` locates each PlanInput from the root with real fields/indices
    (root ``[]``); ``program_id = identity([action_id, input_path])``. A slice's
    ``sealed_operators`` keep the local linear compile order/identities, while
    ``operator_bindings`` map each local ``program_index`` to its full-graph
    occurrence. Cycles and program/depth budgets are rejected before execution.
    """
    identifier(action_id)
    if not isinstance(source, SourcePlan):
        raise PlanContractError("invalid_contract", "source must be a SourcePlan")
    if not isinstance(plan, LogicalPlan):
        raise PlanContractError("invalid_contract", "plan must be a LogicalPlan")
    sink_ids = tuple(sink_ids)

    nodes: list[dict] = []
    edges: list[dict] = []
    slices: list[ProgramSlice] = []
    source_entries: list[dict] = []
    terminals: list[dict] = []
    program_count = [0]

    def precheck(src, stack, depth):
        # Structural walk only: cycles/budgets are rejected before any fragment
        # is compiled, so a cyclic source never reaches describe/source_identity.
        if id(src) in stack:
            raise PlanContractError("invalid_contract", "cycle in dataset source graph")
        if depth >= _MAX_DEPTH:
            raise PlanContractError("invalid_contract", "dataset source graph exceeds depth budget")
        program_count[0] += 1
        if program_count[0] > _MAX_PROGRAMS:
            raise PlanContractError("invalid_contract", "dataset graph exceeds program budget")
        child_stack = stack | {id(src)}
        if isinstance(src, KeyedSource):
            precheck(src.left.source, child_stack, depth + 1)
            if src.right is not None:
                precheck(src.right.source, child_stack, depth + 1)
        elif isinstance(src, UnionSource):
            for item in src.inputs:
                precheck(item.source, child_stack, depth + 1)

    def rederive(node, program_id, occurrence):
        new_id = identity([node["logical_node_id"], program_id])
        return dict(node, logical_node_id=new_id,
                    occurrence_id=occurrence_id_for(action_id, new_id, occurrence))

    def edge(producer, consumer):
        return {
            "edge_id": edge_id_for(producer["logical_node_id"], "out",
                                   consumer["logical_node_id"], "in"),
            "from": {"logical_node_id": producer["logical_node_id"], "port": "out"},
            "to": {"logical_node_id": consumer["logical_node_id"], "port": "in"},
        }

    def expand(src, pl, input_path, is_root):
        program_id = identity([action_id, list(input_path)])
        if is_root:
            fragment = compile_logical_dag(
                action_id=action_id, source=src, operations=pl.operations,
                terminal_kind=terminal_kind, sink_ids=sink_ids)
        else:
            fragment = compile_logical_dag(
                action_id=action_id, source=src, operations=pl.operations,
                terminal_kind="materialize", sink_ids=(program_id,))
        local_source = fragment["nodes"][0]
        local_ops = fragment["nodes"][1:-1]
        composite = isinstance(src, (KeyedSource, UnionSource))
        full_source = rederive(local_source, program_id, 0)
        if composite:
            full_source = dict(full_source, input_ports=["in"])
        full_ops = [rederive(op, program_id, j) for j, op in enumerate(local_ops)]
        nodes.append(full_source)
        nodes.extend(full_ops)
        chain = [full_source] + full_ops
        for producer, consumer in zip(chain, chain[1:]):
            edges.append(edge(producer, consumer))
        tail = full_ops[-1] if full_ops else full_source

        sealed = tuple({"logical_node_id": op["logical_node_id"],
                        "occurrence_id": op["occurrence_id"]} for op in local_ops)
        bindings = tuple({"program_index": j, "occurrence_id": full_ops[j]["occurrence_id"]}
                         for j in range(len(full_ops)))
        slices.append(ProgramSlice(program_id, tuple(input_path), src, pl, sealed, bindings,
                                   full_source['occurrence_id']))
        if not composite:
            semantic = source_identity(src)
            source_entries.append({
                "source_id": identity(semantic), "source_identity_ref": semantic,
                "partition_scheme": partition_scheme(kind="single", partition_count=1)})

        if isinstance(src, KeyedSource):
            children = [("left", src.left)]
            if src.right is not None:
                children.append(("right", src.right))
            for field, item in children:
                child_tail = expand(item.source, item.plan,
                                    input_path + ("source", field), False)
                edges.append(edge(child_tail, full_source))
        elif isinstance(src, UnionSource):
            for index, item in enumerate(src.inputs):
                child_tail = expand(item.source, item.plan,
                                    input_path + ("source", "inputs", index), False)
                edges.append(edge(child_tail, full_source))

        if is_root:
            full_terminal = rederive(fragment["nodes"][-1], program_id, len(local_ops) + 1)
            nodes.append(full_terminal)
            edges.append(edge(tail, full_terminal))
            terminals.append({"logical_node_id": full_terminal["logical_node_id"],
                              "kind": terminal_kind, "sink_ids": sorted(sink_ids)})
        return tail

    precheck(source, frozenset(), 0)
    expand(source, plan, (), True)
    value = {
        "schema_version": LOGICAL_DAG_SCHEMA, "action_id": action_id,
        "nodes": nodes, "edges": edges, "sources": source_entries,
        "terminals": terminals, "shared_ancestors": [],
    }
    value["dag_id"] = dag_id_for(value)
    return validate_logical_dag(value), tuple(slices)
