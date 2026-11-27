"""Physical placement and resource feasibility (lane P, C03).

The single admission point for physical resource feasibility: CPU/GPU/API/IO
roles are scheduled independently, replicas stay bounded, and a plan is only
emitted when work, control and the minimal consumer set can all run inside the
admitted budgets. ``resources/contracts.py`` is byte-frozen and only consumed.
"""
from __future__ import annotations

from demiflow.execution.contracts.dataflow_kinds import MANAGED_HTTP_DEVICES_FIELD

import json
import logging
from dataclasses import replace
from typing import Mapping

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    Bounds,
    CapabilityReport,
    DataflowGraph,
    ExchangeSpec,
    FiniteScope,
    HostDescriptor,
    PhysicalDataflow,
    ResidencyPlan,
    RuntimePolicy,
    SourceSnapshot,
    SourceSplit,
    StageSpec,
)
from demiflow.execution.contracts.dataflow_codec import compute_physical_digest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.resources.contracts import ResourceProfile
from .dataflow_policy import gpu_count

_LOG = logging.getLogger('demiflow.planning.memory')
_RECOVERY_STRENGTH = {"attempt_only": 0, "delivery_only": 1, "final_commit": 2, "checkpoint": 3}
_RECOVERY_NAME = {value: key for key, value in _RECOVERY_STRENGTH.items()}
_MEMORY_MODE = "memory_incremental"
_DURABLE_MODES = frozenset({"durable_incremental", "finite_sealed"})


def _error(code: str, message: str, **context: object) -> DataflowError:
    # Managed runs persist the exception message. Include the same measured
    # quantities as the SDK diagnostic without serializing arbitrary context.
    if 'required' in context and 'available' in context and 'resource' in context:
        detail = f"{context['resource']} required={context['required']}, available={context['available']}"
        stage = context.get('group_first_stage', context.get('stage_id'))
        if stage is not None:
            detail += f", stage={stage}"
        if 'group_size' in context:
            detail += f", stages={context['group_size']}"
        message = f"{message} ({detail})"
    return DataflowError(code, message, context=context)


def _node_map(graph: DataflowGraph) -> dict[str, object]:
    nodes = {node.occurrence_id: node for node in graph.nodes}
    if len(nodes) != len(graph.nodes):
        raise _error("INVALID_CONTRACT", "duplicate occurrence in graph")
    return nodes


def _adjacency(graph: DataflowGraph):
    consumers: dict[str, list] = {node.occurrence_id: [] for node in graph.nodes}
    producers: dict[str, list] = {node.occurrence_id: [] for node in graph.nodes}
    indegree = {node.occurrence_id: 0 for node in graph.nodes}
    for edge in graph.edges:
        if edge.producer not in consumers or edge.consumer not in producers:
            raise _error("INVALID_CONTRACT", "edge references an unknown occurrence", edge_id=edge.edge_id)
        consumers[edge.producer].append(edge)
        producers[edge.consumer].append(edge)
        indegree[edge.consumer] += 1
    return consumers, producers, indegree


def _topo_order(graph: DataflowGraph) -> list[str]:
    _, _, indegree = _adjacency(graph)
    order_index = {node.occurrence_id: i for i, node in enumerate(graph.nodes)}
    ready = sorted((occ for occ, deg in indegree.items() if deg == 0), key=order_index.__getitem__)
    consumers, _, _ = _adjacency(graph)
    result: list[str] = []
    while ready:
        occ = ready.pop(0)
        result.append(occ)
        newly = []
        for edge in consumers[occ]:
            indegree[edge.consumer] -= 1
            if indegree[edge.consumer] == 0:
                newly.append(edge.consumer)
        if newly:
            ready = sorted(ready + newly, key=order_index.__getitem__)
    if len(result) != len(graph.nodes):
        raise _error("INVALID_CONTRACT", "graph contains a cycle")
    return result


def _isolation_tokens(profile: ResourceProfile) -> tuple[str, ...]:
    requirements = profile.requirements or {}
    token = requirements.get("isolation") if isinstance(requirements, Mapping) else None
    if token is None:
        return ()
    if isinstance(token, str):
        return (token,)
    if isinstance(token, (list, tuple)):
        return tuple(str(item) for item in token)
    return ()


def _partition_ids(stage_id: str, count: int) -> tuple[str, ...]:
    # Native K3/K4 identifiers exclude ':'; the physical plan must be usable
    # by both frozen protocols without rewriting its partition identities.
    return tuple(f"{stage_id}-p{index}" for index in range(count))


def _kernel_refs(node) -> tuple[ArtifactRef, ...]:
    refs = []
    for ref in (node.kernel.implementation_ref, node.program_ref):
        if ref is not None and ref not in refs:
            refs.append(ref)
    return tuple(refs)


def _is_multi_input(node) -> bool:
    return len(node.kernel.inputs) > 1


def _replica_bounds(node, partitions: int, policy: RuntimePolicy) -> tuple[int, int, int]:
    kernel = node.kernel
    pinned = (
        kernel.state != "none"
        or kernel.effect != "pure"
        or node.kind == "terminal"
        or kernel.kind in {"LimitOp", "RandomizeBlockOrderOp", "SortOp", "RandomShuffleOp", "RepartitionOp"}
        or _is_multi_input(node)
    )
    if pinned:
        # Stateful/effect/multi-input instances are never auto-scaled into a
        # shared pool; model initialization must not be replicated.
        return 1, 1, 1
    ceiling = max(1, min(partitions, policy.max_replicas,
                         policy.max_active_workers or policy.max_replicas))
    return 1, ceiling, ceiling


def partition_stages(
    graph: DataflowGraph,
    *,
    policy: RuntimePolicy,
    bounds: Bounds,
) -> tuple[StageSpec, ...]:
    nodes = _node_map(graph)
    partitions = max(1, min(policy.default_partitions, bounds.max_partitions))
    stages = []
    for occ in _topo_order(graph):
        node = nodes[occ]
        kernel = node.kernel
        stage_id = "stage-" + occ
        count = partitions
        # A sealed thread_attempt borrows the authenticated driver's live
        # declaration, not a second independent resident state owner. Keep the
        # original profile (including its function image allowance), input,
        # output and snapshot budgets; only the extra residency floor is zero.
        # Staging sets the marker. execution='thread' alone cannot authorize
        # this: an explicitly process-hosted map still owns its state copy.
        state_bytes = (bounds.max_state_bytes if kernel.state != "none"
                       and not kernel.configuration.get('thread_attempt') else 0)
        # Per-replica working set: one input batch, the declared output bound
        # and any state ceiling co-reside inside the provisioned worker.
        resident = bounds.max_batch_bytes + kernel.output_bound_bytes + state_bytes
        # A selected one-row worker may carry a measured codec workspace
        # below the full pair of maximum business rows.
        if 'codec_min_bytes' in kernel.configuration and kernel.state == 'none':
            resident = min(resident, kernel.profile.requests['memory_bytes'])
        from demiflow.execution.contracts.dataflow import REF_CARRIER
        from demiflow.execution.contracts.dataflow import TERMINAL_WORKSPACE
        memory = kernel.configuration.get('terminal_memory', {})
        if node.kind == 'terminal' and (kernel.configuration.get('input_carrier') == REF_CARRIER
                or memory.get('schema') == TERMINAL_WORKSPACE):
            resident = kernel.profile.requests['memory_bytes']
        if kernel.kind == 'reduce':
            from demiflow.execution.contracts.relational_region import reducer_resident_bytes as resident_bytes
            from demiflow.execution.contracts.relational_region import reducer_contract as contract
            reducer_memory = contract(kernel)
            if reducer_memory is not None:
                resident = resident_bytes(reducer_memory,bounds)
                if 'codec_min_bytes' in kernel.configuration:
                    # A layout tried with the measured floor holds that floor.
                    resident = min(resident, kernel.profile.requests['memory_bytes'])
                state_bytes = bounds.max_state_bytes
        replica_min, replica_target, replica_max = _replica_bounds(node, count, policy)

        profile = service_profile(kernel, policy=policy, bounds=bounds)
        # Task-scoped opaque declarations are reloaded synchronously once per
        # worker. The sealed payload size accounts its decode buffer; Python
        # object heap is not inferred from a pickle's length.
        startup = kernel.configuration.get('task_load_bytes', 0)
        measured_state = (kernel.state != 'none' and 'task_batch_rows' in kernel.configuration
            and kernel.uses_measured_workspace(bounds)
            and not kernel.configuration.get('shared_actor'))
        if measured_state:
            # The private reload and the measured state/payload use one grant.
            # Keep the decoder's known peak aside from the working allowance.
            resident = profile.requests['memory_bytes']
            if resident <= startup:
                raise _error('RESOURCE_INFEASIBLE', 'private task load exhausts its worker memory',
                             stage_id=stage_id, resource='memory_bytes', required=startup, available=resident)
            state_bytes = min(state_bytes, resident - startup)
        elif startup:
            fixed = kernel.configuration.get('fixed_worker_min_bytes')
            resident = max(resident, fixed or 0) + startup
            if fixed is not None:
                granted = profile.requests['memory_bytes']
                if resident > granted:
                    raise _error('RESOURCE_INFEASIBLE', 'private task load exceeds its declared worker memory',
                                 stage_id=stage_id, resource='memory_bytes', required=resident, available=granted)
            else:
                profile = replace(profile, requests={**profile.requests,
                    'memory_bytes': profile.requests['memory_bytes'] + startup})
        stages.append(StageSpec(
            stage_id=stage_id,
            occurrences=(occ,),
            partitions=_partition_ids(stage_id, replica_target),
            profile=profile,
            replica_min=replica_min,
            replica_target=replica_target,
            replica_max=replica_max,
            resident_bytes=resident,
            output_bytes=kernel.output_bound_bytes,
            state_bytes=state_bytes,
            kernel_refs=_kernel_refs(node),
        ))
    return tuple(stages)


def _stage_of_occurrence(stages: tuple[StageSpec, ...]) -> dict[str, StageSpec]:
    owner: dict[str, StageSpec] = {}
    for stage in stages:
        for occ in stage.occurrences:
            if occ in owner:
                raise _error("INVALID_CONTRACT", "occurrence has two stage owners", occurrence_id=occ)
            owner[occ] = stage
    return owner


def _fusible(a: StageSpec, b: StageSpec, node_a, node_b) -> bool:
    if len(a.occurrences) != 1 or len(b.occurrences) != 1:
        return False
    if len(a.partitions) != len(b.partitions):
        return False
    if a.profile != b.profile:
        return False
    for node in (node_a, node_b):
        kernel = node.kernel
        if kernel.effect != "pure" or kernel.state != "none" or not kernel.mergeable:
            return False
        if node.kind in {"source", "terminal"} or _is_multi_input(node):
            return False
    return True


def select_fusion_groups(
    graph: DataflowGraph,
    stages: tuple[StageSpec, ...],
    *,
    policy: RuntimePolicy,
) -> tuple[StageSpec, ...]:
    if not policy.fusion:
        return stages
    nodes = _node_map(graph)
    owner = _stage_of_occurrence(stages)
    consumers, producers, _ = _adjacency(graph)
    stage_order = {stage.stage_id: i for i, stage in enumerate(stages)}

    def single_linear_edge(stage: StageSpec) -> object | None:
        occ = stage.occurrences[0]
        out_edges = consumers[occ]
        if len(out_edges) != 1:
            return None
        edge = out_edges[0]
        downstream = owner[edge.consumer]
        if len(producers[edge.consumer]) != 1:
            return None
        return downstream

    merged: dict[str, StageSpec] = {}
    consumed: set[str] = set()
    for stage in stages:
        if stage.stage_id in consumed:
            continue
        group = [stage]
        consumed.add(stage.stage_id)
        current = stage
        while True:
            downstream = single_linear_edge(current)
            if downstream is None or downstream.stage_id in consumed:
                break
            tail_node = nodes[current.occurrences[-1]]
            head_node = nodes[downstream.occurrences[0]]
            if not _fusible(current, downstream, tail_node, head_node):
                break
            # Cumulative co-residency must still fit one provisioned replica;
            # two full-budget stages may not be fused into a double allowance.
            ceiling = current.profile.requests["memory_bytes"]
            combined = sum(item.resident_bytes for item in group) + downstream.resident_bytes
            if combined > ceiling:
                break
            group.append(downstream)
            consumed.add(downstream.stage_id)
            current = downstream
        if len(group) == 1:
            merged[stage.stage_id] = stage
            continue
        head = group[0]
        occurrences = tuple(occ for item in group for occ in item.occurrences)
        fused_id = "stage-" + occurrences[0]
        refs: list[ArtifactRef] = []
        for item in group:
            for ref in item.kernel_refs:
                if ref not in refs:
                    refs.append(ref)
        replica_min, replica_target, replica_max = _replica_bounds(
            nodes[occurrences[0]], len(head.partitions), policy
        )
        merged[fused_id] = StageSpec(
            stage_id=fused_id,
            occurrences=occurrences,
            partitions=head.partitions,
            profile=head.profile,
            replica_min=replica_min,
            replica_target=replica_target,
            replica_max=replica_max,
            resident_bytes=sum(item.resident_bytes for item in group),
            output_bytes=sum(item.output_bytes for item in group),
            state_bytes=sum(item.state_bytes for item in group),
            kernel_refs=tuple(refs),
        )
    return tuple(sorted(merged.values(), key=lambda s: stage_order[s.stage_id]))


def _input_port(node, port_name: str):
    for port in node.kernel.inputs:
        if port.name == port_name:
            return port
    return None


def _routing(node, input_port="main"):
    kind = node.kernel.kind
    config = node.kernel.configuration
    if kind == "broadcast":
        return "broadcast", ()
    if kind in {"join", "reduce", "groups", "repartition", "repartition_hash"}:
        keys = (config.get("right_on") if kind == "join" and input_port == "right" else None)
        keys = keys or config.get("on") or config.get("keys")
        if not keys:
            raise _error("INVALID_CONTRACT", "hash exchange requires the declared business keys")
        return "hash", tuple(str(key) for key in keys)
    if kind in {"random_shuffle", "rebalance", "repartition_rebalance"}:
        return "rebalance", ()
    return "forward", ()


def plan_exchanges(
    graph: DataflowGraph,
    stages: tuple[StageSpec, ...],
    *,
    policy: RuntimePolicy,
    bounds: Bounds,
) -> tuple[ExchangeSpec, ...]:
    nodes = _node_map(graph)
    owner = _stage_of_occurrence(stages)
    credit_rows = max(1, min(256, bounds.max_pending_events))
    credit_bytes = max(1, min(bounds.max_resident_bytes, bounds.max_batch_bytes * 4))
    from demiflow.execution.contracts.dataflow import sealed_execution_ceilings
    ceilings = sealed_execution_ceilings(graph)
    credit_rows = min(credit_rows, ceilings.get('inflight_rows', credit_rows))
    credit_bytes = min(credit_bytes, ceilings.get('inflight_bytes', credit_bytes))
    exchanges = []
    for edge in graph.edges:
        producer_stage = owner[edge.producer]
        consumer_stage = owner[edge.consumer]
        consumer_node = nodes[edge.consumer]
        port = _input_port(consumer_node, edge.input_port)
        if port is None:
            raise _error("INVALID_CONTRACT", "edge targets an unknown input port", edge_id=edge.edge_id)
        # The model phase consumes ordinary batches incrementally from a sealed
        # exchange. The transport boundary does not change its kernel's finish().
        model_boundary = (consumer_node.kernel.configuration.get("predict_step") is not None
                          or nodes[edge.producer].kernel.configuration.get("predict_step") is not None)
        if port.requirement == "finite_complete" or model_boundary:
            mode = "finite_sealed"
        elif policy.durability == "durable":
            mode = "durable_incremental"
        else:
            mode = _MEMORY_MODE
        edge_credit_rows = credit_rows
        if mode == _MEMORY_MODE and producer_stage.stage_id != consumer_stage.stage_id:
            # Plain tasks declare their input batch size. Bound live transport
            # prefetch by it so early-close need not drain a generic 64/256-row
            # window first. Finite scopes still spool their complete input;
            # task boundaries and historical sealed admission remain unchanged.
            # A durable window owns disk-backed references, not the consumer's
            # in-memory task queue. It must fit one bounded producer invocation
            # while that consumer is absent; its bytes and pending deliveries
            # remain charged by the original store/channel authorities.
            task_rows = [nodes[occ].kernel.configuration['task_batch_rows']
                         for occ in consumer_stage.occurrences
                         if nodes[occ].kernel.configuration.get('task_batch_rows')]
            edge_credit_rows = min([credit_rows, *task_rows])
            config = consumer_node.kernel.configuration
            if (config.get('streaming_map') or 'concurrency' in config
                    or consumer_node.kernel.kind in ('AsyncMapOp', 'BatchMapOp')):
                # The receiving worker owns its active calls separately. The
                # cross-worker window is the declared input queue, not a second
                # generic 256-row prefetch allowance ahead of that queue.
                # Native consumers declare the same concurrency contract;
                # their admitted external work must also drain on early close.
                depth = config.get('queue_depth') or config.get('concurrency', 1)
                edge_credit_rows = min(edge_credit_rows, depth)
        routing, keys = _routing(consumer_node, edge.input_port)
        if routing == "forward" and producer_stage.partitions != consumer_stage.partitions:
            routing = "rebalance"
        scope_id = "scope-" + edge.edge_id if mode == "finite_sealed" else None
        # A finite input keeps its complete bounded spool, independently of
        # the live transport window. The common splitter below assigns shares
        # of the original run disk ceiling; the store still charges all actual
        # staging/native/output bytes against that one authority.
        storage_bytes = (0 if mode == _MEMORY_MODE else bounds.max_spill_bytes
                         if mode == 'finite_sealed' else min(bounds.max_spill_bytes, credit_bytes * 8))
        exchanges.append(ExchangeSpec(
            channel_id="chan-" + edge.edge_id,
            edge_id=edge.edge_id,
            producer_partitions=producer_stage.partitions,
            consumer_partitions=consumer_stage.partitions,
            mode=mode,
            routing=routing,
            keys=keys,
            scope_id=scope_id,
            route_epoch=0,
            credit_rows=edge_credit_rows,
            credit_bytes=credit_bytes,
            storage_bytes=storage_bytes,
            read_domain=policy.store_id,
            reason=f"{mode}/{routing}:requirement={port.requirement},ordering={port.ordering}",
        ))
    return _share_exchange_storage(tuple(exchanges), bounds)


def _share_exchange_storage(exchanges, bounds):
    """Divide the one run ceiling; each edge cannot reserve the whole pool."""
    durable = [edge for edge in exchanges if edge.mode in _DURABLE_MODES]
    if not durable or sum(edge.storage_bytes for edge in durable) <= bounds.max_spill_bytes:
        return exchanges
    share = bounds.max_spill_bytes // len(durable)
    if share < 1:
        raise _error('BUDGET_EXCEEDED', 'spill budget cannot admit every durable edge',
                     resource='max_spill_bytes', required=len(durable), available=bounds.max_spill_bytes)
    return tuple(replace(edge, storage_bytes=min(edge.storage_bytes, share),
                         credit_bytes=min(edge.credit_bytes, share))
                 if edge.mode in _DURABLE_MODES else edge for edge in exchanges)


def _partition_owner(stages: tuple[StageSpec, ...]) -> dict[str, StageSpec]:
    owner: dict[str, StageSpec] = {}
    for stage in stages:
        for partition in stage.partitions:
            owner[partition] = stage
    return owner


def solve_residency(
    stages: tuple[StageSpec, ...],
    exchanges: tuple[ExchangeSpec, ...],
    *,
    hosts: tuple[HostDescriptor, ...],
    bounds: Bounds,
    max_active_workers: int | None = None,
    max_cpu_millis: int | None = None,
    publication_workspace_bytes: int = 0,
    publication_reservation=None,
) -> ResidencyPlan:
    if not stages:
        raise _error("INVALID_CONTRACT", "physical plan has no stages")
    if not hosts:
        raise _error("RESOURCE_INFEASIBLE", "no host can run the plan",
                     resource="hosts", required=1, available=0)
    control_reserved = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
    if type(publication_workspace_bytes) is not int or publication_workspace_bytes < 0:
        raise _error('INVALID_CONTRACT', 'publication workspace must be nonnegative')
    control_reserved += publication_workspace_bytes
    if publication_reservation is not None:
        control_reserved = publication_reservation.maximum
    available = bounds.max_resident_bytes - control_reserved
    if available <= 0:
        raise _error("RESOURCE_INFEASIBLE", "control reservation leaves no working memory",
                     resource="memory_bytes", required=control_reserved + min(s.resident_bytes for s in stages),
                     available=bounds.max_resident_bytes)

    partition_owner = _partition_owner(stages)
    producer_ids: set[str] = set()
    consumer_ids: set[str] = set()
    parent = {stage.stage_id: stage.stage_id for stage in stages}

    def find(item: str) -> str:
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for exchange in exchanges:
        if not exchange.producer_partitions or not exchange.consumer_partitions:
            raise _error("INVALID_CONTRACT", "exchange has an empty partition set", channel_id=exchange.channel_id)
        prod = partition_owner[exchange.producer_partitions[0]].stage_id
        cons = partition_owner[exchange.consumer_partitions[0]].stage_id
        producer_ids.add(prod)
        consumer_ids.add(cons)
        if exchange.mode == _MEMORY_MODE:
            # A memory edge requires simultaneous residency. Runtime additionally
            # proves whether durable boundaries can be retired safely.
            union(prod, cons)

    order = {stage.stage_id: i for i, stage in enumerate(stages)}
    components: dict[str, list[StageSpec]] = {}
    for stage in stages:
        components.setdefault(find(stage.stage_id), []).append(stage)
    ordered_components = sorted(components.values(), key=lambda group: min(order[s.stage_id] for s in group))

    simultaneous = []
    for group in ordered_components:
        # Only an isolated stage can cycle partitions. A memory-connected
        # cohort still needs every live producer and consumer simultaneously.
        count = lambda s: min(len(s.partitions), s.replica_target) if len(group) == 1 else len(s.partitions)
        if max_active_workers is not None and sum(count(s) for s in group) > max_active_workers:
            raise _error("RESOURCE_INFEASIBLE", "memory-connected group exceeds active worker budget",
                         resource="max_active_workers", required=sum(count(s) for s in group),
                         available=max_active_workers, group_first_stage=group[0].stage_id, group_size=len(group),
                         max_active_workers=max_active_workers)
        footprint = sum(max(stage.resident_bytes, stage.profile.requests['memory_bytes'])
                        * count(stage) for stage in group)
        group_available = (bounds.max_resident_bytes - publication_reservation.reserved(s.stage_id for s in group)
                           if publication_reservation is not None else available)
        if footprint > group_available:
            raise _error(
                "RESOURCE_INFEASIBLE",
                "a memory-connected stage group exceeds the resident budget and cannot be staged",
                resource="memory_bytes", required=footprint, resident_bytes=footprint, available=group_available,
                group_first_stage=group[0].stage_id, group_size=len(group),
            )
        simultaneous.append(tuple(stage.stage_id for stage in group))

    leaf_stages = consumer_ids - producer_ids
    consumer_slots = max(1, len(leaf_stages))
    smallest = min(stage.resident_bytes for stage in stages)
    if smallest > (bounds.max_resident_bytes - publication_reservation.base
                   if publication_reservation is not None else available):
        raise _error("RESOURCE_INFEASIBLE", "even the smallest stage cannot start within the resident budget",
                     resource="memory_bytes", required=smallest, available=(
                         bounds.max_resident_bytes - publication_reservation.base if publication_reservation else available))

    required_spill = sum(ex.storage_bytes for ex in exchanges if ex.mode in _DURABLE_MODES)
    if required_spill > bounds.max_spill_bytes:
        raise _error("BUDGET_EXCEEDED", "durable channel storage exceeds the spill budget",
                     resource="max_spill_bytes", required=required_spill, spill_bytes=required_spill, available=bounds.max_spill_bytes)

    cpu_capacity = sum(host.logical_resources.get(
        "cpu_millis", host.logical_resources.get("cpu", 0.0) * 1000) for host in hosts)
    if max_cpu_millis is not None:
        if type(max_cpu_millis) is not int or max_cpu_millis <= 0:
            raise _error("INVALID_CONTRACT", "CPU ceiling must be a positive integer")
        cpu_capacity = min(cpu_capacity, max_cpu_millis)
    memory_capacity = sum(host.logical_resources.get("memory_bytes", 0.0) for host in hosts)
    gpu_capacity = sum(host.logical_resources.get("gpu", 0.0) for host in hosts)
    isolation: set[str] = set()
    for group in ordered_components:
        count = lambda s: min(len(s.partitions), s.replica_target) if len(group) == 1 else len(s.partitions)
        cpu_needed = sum(s.profile.requests["cpu_millis"] * count(s) for s in group)
        memory_needed = sum(s.profile.requests["memory_bytes"] * count(s) for s in group)
        gpu_needed = sum(gpu_count(s.profile) * count(s) for s in group)
        for stage in group:
            isolation.update(_isolation_tokens(stage.profile))
        if cpu_needed > cpu_capacity:
            raise _error("RESOURCE_INFEASIBLE", "insufficient CPU capacity for resident group", resource="cpu_millis", required=cpu_needed, cpu_millis=cpu_needed,
                         available=cpu_capacity, group_first_stage=group[0].stage_id, group_size=len(group))
        if memory_needed > memory_capacity:
            raise _error("RESOURCE_INFEASIBLE", "insufficient host memory for resident group", resource="host_memory_bytes", required=memory_needed, memory_bytes=memory_needed,
                         available=memory_capacity, group_first_stage=group[0].stage_id, group_size=len(group))
        if gpu_needed > gpu_capacity:
            raise _error("RESOURCE_INFEASIBLE", "insufficient GPU capacity", resource="gpu", required=gpu_needed, gpu=gpu_needed,
                         available=gpu_capacity, group_first_stage=group[0].stage_id, group_size=len(group))
    supported_isolation: set[str] = set()
    for host in hosts:
        supported_isolation.update(host.isolation_capabilities)
    missing = isolation - supported_isolation
    if missing:
        raise _error("RESOURCE_INFEASIBLE", "provider lacks required isolation", isolation=sorted(missing)[0])

    return ResidencyPlan(
        simultaneous_groups=tuple(simultaneous),
        control_reserved_bytes=control_reserved,
        consumer_reserved_slots=consumer_slots,
        required_spill_bytes=required_spill,
    )


def infer_recovery(
    graph: DataflowGraph,
    snapshots: tuple[SourceSnapshot, ...],
    *,
    policy: RuntimePolicy,
) -> CapabilityReport:
    cap = _RECOVERY_STRENGTH["checkpoint"]
    reasons: list[str] = []
    for snapshot in snapshots:
        if snapshot.replay == "attempt_only":
            cap = min(cap, _RECOVERY_STRENGTH["attempt_only"])
            reasons.append(f"source {snapshot.source_id} replay=attempt_only")
        elif snapshot.replay == "lease_queue":
            cap = min(cap, _RECOVERY_STRENGTH["final_commit"])
            reasons.append(f"source {snapshot.source_id} replay=lease_queue")
        elif snapshot.replay == "durable_spool" and not snapshot.finite:
            cap = min(cap, _RECOVERY_STRENGTH["final_commit"])
            reasons.append(f"source {snapshot.source_id} spool is not finite")
    for node in graph.nodes:
        kernel = node.kernel
        if kernel.state == "nonrecoverable":
            cap = min(cap, _RECOVERY_STRENGTH["attempt_only"])
            reasons.append(f"node {node.occurrence_id} state=nonrecoverable")
        if kernel.effect == "unknown_replay_forbidden":
            # Unknown paid results must never be judged automatically retryable.
            cap = min(cap, _RECOVERY_STRENGTH["delivery_only"])
            reasons.append(f"node {node.occurrence_id} effect=unknown_replay_forbidden")
    recovery = _RECOVERY_NAME[cap]
    supported = cap >= _RECOVERY_STRENGTH[policy.required_recovery]
    if not supported:
        reasons.append(f"required_recovery={policy.required_recovery} exceeds provable={recovery}")
    profiles: list[ResourceProfile] = []
    seen: set[str] = set()
    for node in graph.nodes:
        key = json.dumps(node.kernel.profile.to_dict(), sort_keys=True)
        if key not in seen:
            seen.add(key)
            profiles.append(node.kernel.profile)
    return CapabilityReport(
        supported=supported,
        reasons=tuple(reasons),
        recovery=recovery,
        required_domains=(policy.store_id,),
        required_profiles=tuple(profiles),
    )


def _check_bounds(bounds: Bounds) -> None:
    for name in (
        "max_nodes", "max_edges", "max_partitions", "max_depth", "max_control_bytes",
        "max_row_bytes", "max_batch_bytes", "max_resident_bytes", "max_spill_bytes",
        "max_state_bytes", "max_pending_events", "max_open_files", "max_output_rows", "timeout_ms",
    ):
        value = getattr(bounds, name)
        if type(value) is not int or value <= 0:
            raise _error("BUDGET_EXCEEDED", "bounds field must be a positive int (0 never means unlimited)", field=name)


def validate_physical_dataflow(graph: DataflowGraph, physical: PhysicalDataflow) -> None:
    if physical.schema != ("demiflow.physical_dataflow.v4" if graph.jobs else "demiflow.physical_dataflow.v3"):
        raise _error("INVALID_CONTRACT", "unexpected physical schema")
    if physical.dag_id != graph.dag_id:
        raise _error("INVALID_CONTRACT", "physical dag_id does not match the graph")
    _check_bounds(physical.bounds)
    nodes = _node_map(graph)

    seen_occ: dict[str, str] = {}
    stage_ids: set[str] = set()
    for stage in physical.stages:
        if stage.stage_id in stage_ids:
            raise _error("INVALID_CONTRACT", "duplicate stage_id", stage_id=stage.stage_id)
        stage_ids.add(stage.stage_id)
        if not stage.occurrences:
            raise _error("INVALID_CONTRACT", "dangling stage without occurrences", stage_id=stage.stage_id)
        if not stage.partitions or len(set(stage.partitions)) != len(stage.partitions):
            raise _error("INVALID_CONTRACT", "stage partitions must be non-empty and unique", stage_id=stage.stage_id)
        if len(stage.partitions) > physical.bounds.max_partitions:
            raise _error("BUDGET_EXCEEDED", "stage exceeds max_partitions", stage_id=stage.stage_id)
        if not 1 <= stage.replica_min <= stage.replica_target <= stage.replica_max <= physical.policy.max_replicas:
            raise _error("INVALID_CONTRACT", "invalid replica bounds", stage_id=stage.stage_id)
        for occ in stage.occurrences:
            if occ not in nodes:
                raise _error("INVALID_CONTRACT", "stage owns an unknown occurrence", occurrence_id=occ)
            if occ in seen_occ:
                raise _error("INVALID_CONTRACT", "occurrence has two stage owners", occurrence_id=occ)
            seen_occ[occ] = stage.stage_id
            window = (stage.kernel_memory_bytes or {}).get(occ)
            if window is not None:
                nodes[occ].kernel.with_memory_bytes(window, sealed_bounds=physical.bounds)
    from .dataflow_training import physical_values, reference_only_sources
    refs = reference_only_sources(graph)
    if set(seen_occ) != set(nodes) - refs:
        raise _error("INVALID_CONTRACT", "stages do not cover every occurrence exactly once")

    if (physical.jobs, physical.value_edges) != physical_values(graph, physical.stages):
        raise _error("INVALID_CONTRACT", "physical job stages or value bindings differ from their logical ports")
    if any(split.source_id in refs for split in physical.splits):
        raise _error("INVALID_CONTRACT", "version-only input has executable splits")
    owner = _stage_of_occurrence(physical.stages)
    edge_ids = {edge.edge_id for edge in graph.edges}
    seen_edges: set[str] = set()
    channels: set[str] = set()
    partition_owner = _partition_owner(physical.stages)
    valid_domains = {physical.policy.store_id} | {split.required_domain for split in physical.splits}
    sealed_scopes: dict[str, ExchangeSpec] = {}
    for exchange in physical.exchanges:
        if exchange.edge_id not in edge_ids:
            raise _error("INVALID_CONTRACT", "exchange references an unknown edge", edge_id=exchange.edge_id)
        if exchange.edge_id in seen_edges:
            raise _error("INVALID_CONTRACT", "edge has more than one exchange", edge_id=exchange.edge_id)
        seen_edges.add(exchange.edge_id)
        if exchange.channel_id in channels:
            raise _error("INVALID_CONTRACT", "duplicate channel_id", channel_id=exchange.channel_id)
        channels.add(exchange.channel_id)
        edge = next(e for e in graph.edges if e.edge_id == exchange.edge_id)
        prod_stage = owner[edge.producer]
        cons_stage = owner[edge.consumer]
        if not set(exchange.producer_partitions) <= set(prod_stage.partitions):
            raise _error("INVALID_CONTRACT", "exchange producer partitions foreign to stage", edge_id=exchange.edge_id)
        if not set(exchange.consumer_partitions) <= set(cons_stage.partitions):
            raise _error("INVALID_CONTRACT", "exchange consumer partitions foreign to stage", edge_id=exchange.edge_id)
        if exchange.read_domain not in valid_domains:
            raise _error("INVALID_CONTRACT", "unknown read_domain", read_domain=exchange.read_domain)
        if exchange.mode == "finite_sealed":
            if exchange.scope_id is None:
                raise _error("INVALID_CONTRACT", "sealed exchange lacks a scope", edge_id=exchange.edge_id)
            sealed_scopes[exchange.scope_id] = exchange
    if seen_edges != edge_ids:
        raise _error("INVALID_CONTRACT", "every edge must have exactly one exchange")

    incoming: dict[str, set[str]] = {occ: set() for occ in nodes}
    for edge in graph.edges:
        node = nodes[edge.consumer]
        if _input_port(node, edge.input_port) is None:
            raise _error("INVALID_CONTRACT", "edge targets an unknown input port", edge_id=edge.edge_id)
        incoming[edge.consumer].add(edge.input_port)
    for occ, node in nodes.items():
        declared = {port.name for port in node.kernel.inputs}
        if declared and incoming[occ] != declared:
            raise _error("INVALID_CONTRACT", "input ports must each have a unique connection", occurrence_id=occ)

    consumers, _, _ = _adjacency(graph)
    reached: set[str] = set()
    stack = list(graph.sources)
    while stack:
        occ = stack.pop()
        if occ in reached:
            continue
        reached.add(occ)
        stack.extend(edge.consumer for edge in consumers[occ])
    for terminal in graph.terminals:
        if terminal not in reached:
            raise _error("INVALID_CONTRACT", "terminal is unreachable from any source", occurrence_id=terminal)
    if len(set(graph.terminals)) != len(graph.terminals):
        raise _error("INVALID_CONTRACT", "duplicate sink terminal")
    for stage in physical.stages:
        if not any(occ in reached for occ in stage.occurrences):
            raise _error("INVALID_CONTRACT", "dangling stage unreachable from sources", stage_id=stage.stage_id)

    snapshot_ids = {snapshot.source_id for snapshot in physical.sources}
    if len(snapshot_ids) != len(physical.sources):
        raise _error("INVALID_CONTRACT", "duplicate source snapshot")
    if snapshot_ids != set(graph.sources):
        raise _error("SOURCE_CHANGED", "source snapshots drift from the graph sources")
    split_ids: set[str] = set()
    for split in physical.splits:
        if split.source_id not in snapshot_ids:
            raise _error("SOURCE_CHANGED", "split references an unknown source snapshot", split_id=split.split_id)
        if split.split_id in split_ids:
            raise _error("INVALID_CONTRACT", "duplicate split_id", split_id=split.split_id)
        split_ids.add(split.split_id)

    if _RECOVERY_STRENGTH[physical.recovery] < _RECOVERY_STRENGTH[physical.policy.required_recovery]:
        raise _error("INVALID_CONTRACT", "recovery guarantee weaker than required_recovery")

    scope_ids: set[str] = set()
    for scope in physical.scopes:
        if scope.scope_id in scope_ids:
            raise _error("INVALID_CONTRACT", "duplicate scope_id", scope_id=scope.scope_id)
        scope_ids.add(scope.scope_id)
        exchange = sealed_scopes.get(scope.scope_id)
        if exchange is None:
            raise _error("INVALID_CONTRACT", "scope has no sealed exchange", scope_id=scope.scope_id)
        if tuple(scope.membership) != exchange.producer_partitions:
            raise _error("INVALID_CONTRACT", "scope membership must equal the finite producer set", scope_id=scope.scope_id)
    if scope_ids != set(sealed_scopes):
        raise _error("INVALID_CONTRACT", "every sealed exchange needs a complete finite scope")

    if physical.constants_ref is None:
        raise _error("INVALID_CONTRACT", "physical plan lacks constants_ref")
    if compute_physical_digest(physical) != physical.plan_id:
        raise _error("INVALID_CONTRACT", "plan_id does not match the canonical physical projection")


def _refine_source_partitions(
    graph: DataflowGraph,
    stages: tuple[StageSpec, ...],
    splits: tuple[SourceSplit, ...],
    policy: RuntimePolicy,
    bounds: Bounds,
) -> tuple[StageSpec, ...]:
    sources = set(graph.sources)
    by_source: dict[str, int] = {}
    for split in splits:
        by_source[split.source_id] = by_source.get(split.source_id, 0) + 1
    refined = []
    for stage in stages:
        source_occ = next((occ for occ in stage.occurrences if occ in sources), None)
        count = by_source.get(source_occ, 0) if source_occ else 0
        if source_occ is None or count == 0:
            refined.append(stage)
            continue
        if count > bounds.max_partitions:
            raise _error("BUDGET_EXCEEDED", "source splits exceed max_partitions", source_id=source_occ)
        node = next(n for n in graph.nodes if n.occurrence_id == source_occ)
        replica_min, replica_target, replica_max = _replica_bounds(node, count, policy)
        refined.append(replace(
            stage,
            partitions=_partition_ids(stage.stage_id, replica_target),
            replica_min=replica_min,
            replica_target=replica_target,
            replica_max=replica_max,
        ))
    return tuple(refined)


def _select_budgeted_residency(graph, snapshots, stages, exchanges, *, hosts, bounds, policy):
    """Choose new-plan transport; never reinterpret an already sealed exchange.

    Memory connectivity is a placement preference for ordinary Dataset plans.
    If it cannot fit, checkpointable boundaries may park a bounded durable
    window. The fixed-plan solver remains strict about memory co-residency.
    """
    from demiflow.execution.contracts.dataflow import sealed_execution_ceilings
    ceilings = sealed_execution_ceilings(graph)
    cpu_ceiling = ceilings.get('cpu_millis')
    from demiflow.execution.scheduler.dataflow_publication_budget import graph_workspace_bytes
    publication = graph_workspace_bytes(graph)
    from demiflow.execution.scheduler.dataflow_publication_budget import PublicationReservation
    reservation = PublicationReservation(graph, stages, bounds)
    try:
        return exchanges, solve_residency(stages, exchanges, hosts=hosts, bounds=bounds,
                                         max_active_workers=policy.max_active_workers,
                                         max_cpu_millis=cpu_ceiling, publication_workspace_bytes=publication,
                                         publication_reservation=reservation)
    except DataflowError as original:
        if original.code != "RESOURCE_INFEASIBLE":
            raise
        # Temporal scheduling cannot manufacture a missing device or make one
        # indivisible stage fit. Preserve that real admission error rather than
        # masking it with the disk cost of an irrelevant durable fallback.
        try:
            solve_residency(stages, (), hosts=hosts, bounds=bounds,
                            max_active_workers=policy.max_active_workers, max_cpu_millis=cpu_ceiling,
                            publication_workspace_bytes=publication, publication_reservation=reservation)
        except DataflowError:
            raise
        eligible = (all(s.replay != "attempt_only" for s in snapshots)
                    and all(n.kernel.state != "nonrecoverable" for n in graph.nodes))
        owners = _partition_owner(stages)
        candidates = [e for e in exchanges if e.mode == _MEMORY_MODE
                      and owners[e.producer_partitions[0]].stage_id != owners[e.consumer_partitions[0]].stage_id]
        if not eligible or not candidates or bounds.max_spill_bytes <= 0:
            raise _error("RESOURCE_INFEASIBLE",
                         "memory group cannot fit and its source/state cannot safely park", **original.context) from original
        nodes = _node_map(graph)
        edges = {edge.edge_id: edge for edge in graph.edges}
        # For a finite, fully materialized action a shared actor can finish
        # one lifetime between two bounded spools. Parking its worker per
        # window would repeatedly close and recreate the same declared owner.
        shared_stages = {stage.stage_id for stage in stages if any(
            nodes[occ].kernel.configuration.get('shared_actor') for occ in stage.occurrences)}
        terminals = (nodes[occ].kernel for occ in graph.terminals)
        complete_action = all(s.finite for s in snapshots) and all(
            kernel.kind == 'materialize' or (
                kernel.kind == 'take_all' and kernel.configuration['arguments'].get('limit') is None)
            for kernel in terminals)
        def park(exchange, reason):
            producer = nodes[edges[exchange.edge_id].producer].kernel
            # A finite kernel emits its complete result inside finish(). An
            # absent consumer cannot ACK a per-row durable window, and parking
            # halfway through that invocation cannot snapshot its generator.
            # Use the existing bounded finite spool/EOI proof for this natural
            # boundary. Incremental and effectful kernels keep their original
            # checkpoint/ACK protocol. Only newly compiled plans select here.
            finite = (producer.effect == "pure" and bool(producer.inputs)
                      and all(port.requirement == "finite_complete" for port in producer.inputs))
            if complete_action and shared_stages.intersection((
                    owners[exchange.producer_partitions[0]].stage_id,
                    owners[exchange.consumer_partitions[0]].stage_id)):
                finite = True
            mode = "finite_sealed" if finite else "durable_incremental"
            return replace(exchange, mode=mode,
                scope_id="scope-" + exchange.edge_id if finite else None,
                credit_rows=min(256, bounds.max_pending_events, ceilings.get('inflight_rows', 256)),
                storage_bytes=(bounds.max_spill_bytes if finite else
                               min(bounds.max_spill_bytes, exchange.credit_bytes * 8)),
                reason=f"{mode}:{reason}_requires_" +
                       ("finite_completion;" if finite else "checkpointed_parking;") + exchange.reason)

        # Park only the memory edges inside the group the solver could not
        # place, then ask it again. A group that fits keeps its live memory
        # transport: parking it would only add checkpointed handoffs of a
        # bounded window between stages that could have run together.
        current, failure = tuple(exchanges), original
        while True:
            reason = ("active_worker_budget" if failure.context.get('resource') == 'max_active_workers'
                      else "resident_budget")
            pending = [e for e in current if e in candidates]
            local = [e for e in pending if {owners[e.producer_partitions[0]].stage_id,
                     owners[e.consumer_partitions[0]].stage_id} <= _memory_group(
                         stages, current, failure.context.get('group_first_stage'))]
            parked = set(local or pending)
            if not parked:
                raise failure
            current = _share_exchange_storage(tuple(park(e, reason) if e in parked else e
                                                    for e in current), bounds)
            try:
                return current, solve_residency(stages, current, hosts=hosts, bounds=bounds,
                                                max_active_workers=policy.max_active_workers,
                                                max_cpu_millis=cpu_ceiling, publication_workspace_bytes=publication,
                                                publication_reservation=reservation)
            except DataflowError as exc:
                if exc.code != "RESOURCE_INFEASIBLE":
                    raise
                failure = exc


def _memory_group(stages, exchanges, first_stage):
    """The stages joined to ``first_stage`` by memory edges (empty if unnamed)."""
    if first_stage is None:
        return set()
    owners = _partition_owner(stages)
    neighbours = {}
    for exchange in exchanges:
        if exchange.mode == _MEMORY_MODE:
            a = owners[exchange.producer_partitions[0]].stage_id
            b = owners[exchange.consumer_partitions[0]].stage_id
            neighbours.setdefault(a, set()).add(b)
            neighbours.setdefault(b, set()).add(a)
    group, pending = set(), [first_stage]
    while pending:
        stage = pending.pop()
        if stage not in group:
            group.add(stage)
            pending.extend(neighbours.get(stage, ()))
    return group


def _join_arrow_snapshots(graph, snapshots, splits, stages):
    """Keep a fixed Lance probe columnar up to a co-located finite hash join."""
    from .dataflow_compile import _plain
    from ..execution.contracts.relational_region import TYPED_JOIN_SUFFIX
    if len(stages) == 1 and stages[0].stage_id.endswith(TYPED_JOIN_SUFFIX):
        return tuple(replace(source, immutable_identity={**_plain(source.immutable_identity),
            'record_carrier_v1': 'mapped_arrow'}) for source in snapshots)
    # The probe reaches this one worker directly, in source order. A hash
    # exchange, a UDF, a fork or another consumer may need the original row.
    if (graph.jobs or len(graph.nodes) != 4 or len(graph.edges) != 3 or len(snapshots) != 2
            or len(splits) != 2 or len(stages) != 1 or len(stages[0].partitions) != 1):
        return snapshots
    nodes = _node_map(graph)
    for edge in graph.edges:
        join = nodes[edge.consumer].kernel
        if (edge.input_port == 'left' and nodes[edge.producer].kernel.kind == 'lance'
                and join.kind == 'join' and join.configuration.get('how', 'inner') in ('inner', 'semi')
                and len(join.configuration.get('on', ())) == 1):
            return tuple(replace(source, immutable_identity={**_plain(source.immutable_identity),
                'record_carrier_v1': 'arrow_batch'}) if source.source_id == edge.producer else source
                         for source in snapshots)
    return snapshots


def compile_physical_dataflow(
    graph: DataflowGraph,
    snapshots: tuple[SourceSnapshot, ...],
    splits: tuple[SourceSplit, ...],
    *,
    policy: RuntimePolicy,
    hosts: tuple[HostDescriptor, ...],
    bounds: Bounds,
    constants_ref: ArtifactRef,
) -> PhysicalDataflow:
    """Select and seal one physical layout.

    Join and reducer kernels first get their maximum-row windows. Only when no
    layout fits those does a second selection give the kernels that declared a
    floor that floor (and then any slack); their grant then counts actual bytes.
    If that also cannot fit, the original refusal stands.
    """
    arguments = dict(policy=policy, hosts=hosts, bounds=bounds, constants_ref=constants_ref)
    try:
        return _compile_physical(graph, snapshots, splits, **arguments, _measured_relational=False)
    except DataflowError as exc:
        from demiflow.execution.contracts.relational_region import MEASURED_KINDS
        if exc.code != 'RESOURCE_INFEASIBLE' or not any(
                node.kernel.kind in MEASURED_KINDS and 'codec_min_bytes' in node.kernel.configuration
                for node in graph.nodes):
            raise
        refused = exc
    try:
        physical = _compile_physical(graph, snapshots, splits, **arguments, _measured_relational=True)
    except DataflowError as exc:
        if exc.code == 'RESOURCE_INFEASIBLE':
            raise refused
        raise
    from demiflow.execution.contracts.relational_region import measured_relational_windows
    for occurrence, kind, granted, preferred in measured_relational_windows(graph, physical):
        _LOG.warning('%s %s runs with measured memory: %d of its preferred %d bytes, because the plan does not '
                     'fit with maximum-size rows; a row above about %d bytes may fail with BUDGET_EXCEEDED during '
                     'the run. Raise memory_bytes, or declare max_row_bytes to have the plan checked before it starts.',
                     kind, occurrence[:12], granted, preferred, bounds.max_row_bytes * granted // preferred)
    return physical


def _compile_physical(graph, snapshots, splits, *, policy, hosts, bounds, constants_ref,
                      _allow_offset_sql=True, _measured_relational=False) -> PhysicalDataflow:
    _check_bounds(bounds)
    snapshot_ids = {snapshot.source_id for snapshot in snapshots}
    if snapshot_ids != set(graph.sources):
        raise _error("SOURCE_CHANGED", "snapshots must cover exactly the graph sources")
    for split in splits:
        if split.source_id not in snapshot_ids:
            raise _error("SOURCE_CHANGED", "split references an unknown source", split_id=split.split_id)

    from .dataflow_budget import minimum_workspace_graph, allocate_workspace_slack
    declared_graph = graph
    graph = minimum_workspace_graph(graph, bounds, allow_measured_terminal=any(
        s.replay == "attempt_only" for s in snapshots), measured_relational=_measured_relational)
    from .dataflow_training import physical_values, reference_only_sources
    refs = reference_only_sources(graph)
    splits = tuple(split for split in splits if split.source_id not in refs)
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    stages = _refine_source_partitions(graph, stages, splits, policy, bounds)
    # Optional co-location must fit the same caller ceiling as the original
    # allocation scope. Project capacity only for planning; real providers keep
    # their original descriptors and admission authority.
    from demiflow.execution.contracts.dataflow import sealed_execution_ceilings
    cpu_ceiling = sealed_execution_ceilings(graph).get('cpu_millis')
    if cpu_ceiling is not None:
        hosts = tuple(replace(host, logical_resources={**host.logical_resources,
            'cpu_millis': min(host.logical_resources.get('cpu_millis',
                host.logical_resources.get('cpu', 0) * 1000), cpu_ceiling)}) for host in hosts)
    from .dataflow_shared import colocate_shared_actors
    stages = colocate_shared_actors(graph, stages, bounds=bounds, hosts=hosts)
    # Whole-row-region shortcuts assume a single action sink. A v4 job or
    # materialization boundary may not be swallowed by one such region.
    if not graph.jobs:
        from .dataflow_single_worker import fuse_single_worker_dag
        stages = fuse_single_worker_dag(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
        from .dataflow_fusion import fuse_single_worker_pipeline
        stages = fuse_single_worker_pipeline(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
        from .dataflow_thread_fusion import fuse_thread_suffix
        stages = fuse_thread_suffix(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
        from .dataflow_relational_regions import fuse_relational_regions
        stages = fuse_relational_regions(graph, stages, policy=policy, bounds=bounds, hosts=hosts,
                                        snapshots=snapshots if _allow_offset_sql else ())
        from .dataflow_finite_fusion import fuse_finite_suffix
        stages = fuse_finite_suffix(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
    stages = select_fusion_groups(graph, stages, policy=policy)
    from .dataflow_graph_adapter import specialize_graph_stages, specialize_graph_exchanges
    stages = specialize_graph_stages(graph, stages, policy=policy, bounds=bounds, hosts=hosts)
    stages = tuple(stage for stage in stages if not refs.intersection(stage.occurrences))
    if _allow_offset_sql:
        from .dataflow_relational_regions import place_key_exclusion, place_join_probe, place_typed_join
        stages = place_join_probe(graph, snapshots, splits, stages, policy=policy, bounds=bounds, hosts=hosts)
        stages = place_key_exclusion(graph, snapshots, splits, stages, policy=policy, bounds=bounds, hosts=hosts)
        stages = place_typed_join(graph, snapshots, splits, stages, policy=policy, bounds=bounds, hosts=hosts)
    jobs, values = physical_values(graph, stages)
    exchanges = plan_exchanges(graph, stages, policy=policy, bounds=bounds)
    exchanges = specialize_graph_exchanges(graph, stages, exchanges, bounds=bounds)
    exchanges = _share_exchange_storage(exchanges, bounds)
    graph_requested_exchanges = exchanges
    try:
        exchanges, residency = _select_budgeted_residency(
            graph, snapshots, stages, exchanges, hosts=hosts, bounds=bounds, policy=policy)
    except DataflowError as exc:
        from ..execution.contracts.relational_region import JOIN_PROBE_SUFFIX, KEY_BATCH_ARROW_SUFFIX, needs_native_offsets
        if exc.code != 'RESOURCE_INFEASIBLE' or not any(
                needs_native_offsets(s) or s.stage_id.endswith((KEY_BATCH_ARROW_SUFFIX, JOIN_PROBE_SUFFIX)) for s in stages):
            raise
        # A larger optional stage may not coexist with other mandatory
        # reservations. Retry only this fresh physical choice, before sealing.
        return _compile_physical(graph, snapshots, splits, policy=policy, hosts=hosts, bounds=bounds,
                                 constants_ref=constants_ref, _allow_offset_sql=False,
                                 _measured_relational=_measured_relational)
    from .dataflow_graph_adapter import pack_graph_residency
    exchanges, residency = pack_graph_residency(graph, stages, exchanges, residency,
        requested=graph_requested_exchanges, hosts=hosts, bounds=bounds, policy=policy)
    from .dataflow_graph_adapter import tighten_graph_credits
    exchanges = tighten_graph_credits(graph, exchanges, bounds=bounds)
    from .dataflow_graph_adapter import constrain_graph_storage
    constrained = constrain_graph_storage(graph, exchanges)
    if constrained != exchanges:
        # Automatic durable parking must not discard an explicit graph limit.
        # Keep the same sole solver's sealed spill/accounting facts consistent.
        exchanges = _share_exchange_storage(constrained, bounds)
        from demiflow.execution.scheduler.dataflow_publication_budget import PublicationReservation
        residency = solve_residency(stages, exchanges, hosts=hosts, bounds=bounds,
            max_active_workers=policy.max_active_workers, max_cpu_millis=cpu_ceiling,
            publication_reservation=PublicationReservation(graph, stages, bounds))
    stages, reserved = allocate_workspace_slack(declared_graph, graph, stages, residency,
                                                hosts=hosts, bounds=bounds)
    # Validate the exact grants and the original (not floor) publication contract.
    from demiflow.execution.scheduler.dataflow_publication_budget import PublicationReservation
    residency = solve_residency(stages, exchanges, hosts=hosts, bounds=bounds,
        max_active_workers=policy.max_active_workers, max_cpu_millis=cpu_ceiling,
        publication_reservation=PublicationReservation(declared_graph, stages, bounds, declared=reserved))
    graph = declared_graph
    capability = infer_recovery(graph, snapshots, policy=policy)
    if not capability.supported:
        raise _error(
            "UNSUPPORTED_CAPABILITY",
            "required recovery is not provable for this graph",
            reason=capability.reasons[0] if capability.reasons else "unknown",
        )
    scopes = tuple(
        FiniteScope(
            scope_id=exchange.scope_id,
            generation=0,
            membership=exchange.producer_partitions,
            completion_condition="input_eoi",
        )
        for exchange in exchanges if exchange.mode == "finite_sealed"
    )
    physical = PhysicalDataflow(
        schema="demiflow.physical_dataflow.v4" if jobs else "demiflow.physical_dataflow.v3",
        jobs=jobs, value_edges=values,
        dag_id=graph.dag_id,
        plan_id="0" * 64,
        plan_revision=0,
        stages=stages,
        exchanges=exchanges,
        residency=residency,
        scopes=scopes,
        sources=_join_arrow_snapshots(graph, snapshots, splits, stages),
        splits=splits,
        policy=policy,
        bounds=bounds,
        recovery=capability.recovery,
        limitations=capability.reasons,
        constants_ref=constants_ref,
    )
    physical = replace(physical, plan_id=compute_physical_digest(physical))
    validate_physical_dataflow(graph, physical)
    if graph.jobs:
        from .dataflow_training import validate_training_inputs
        validate_training_inputs(graph, physical, bounds=bounds)
    return physical


def batch_carrier(graph, physical):
    """Optimize only a sealed, sequential source-to-terminal slice.

    Reordering, forks and mixed sources keep the original row codec. Otherwise
    one-cache consumers could reread an entire batch once per interleaved row.
    This selects a payload representation, never another execution route.
    """
    if len(physical.splits) != 1:
        return None
    if sum(node.kind == 'source' for node in graph.nodes) != 1:
        return None
    if sum(node.kind == 'terminal' for node in graph.nodes) != 1:
        return None
    terminal = next(node for node in graph.nodes if node.kind == 'terminal')
    if terminal.kernel.kind not in ('write_lance', 'materialize', 'iter_rows', 'count'):
        # size_bytes/summary and bounded collectors retain their original exact
        # row accounting. The carrier estimate is a resource bound, not a new
        # public measurement of a row's serialized size.
        return None
    mapped_arrow = terminal.kernel.configuration.get('input_carrier') == 'mapped_arrow.v1'
    if any(node.kind not in ('source', 'terminal') and node.kernel.kind != 'LimitOp'
           and not (mapped_arrow and node.kernel.kind == 'MapBatchesOp')
           for node in graph.nodes):
        return None
    if len(graph.edges) != len(graph.nodes) - 1:
        return None
    producers = [edge.producer for edge in graph.edges]
    consumers = [edge.consumer for edge in graph.edges]
    if (len(set(producers)) != len(producers) or len(set(consumers)) != len(consumers)
            or any(len(edge.producer_partitions) != 1 or len(edge.consumer_partitions) != 1
                   for edge in physical.exchanges)):
        return None
    if mapped_arrow:
        return 'mapped_arrow.v1'
    # Existing seals did not request contiguous native batches. Keep their
    # original scan boundaries and reference representation on recovery.
    return 'original_scan_batches'


def service_profile(kernel, *, policy, bounds):
    if not kernel.configuration.get(MANAGED_HTTP_DEVICES_FIELD):
        return kernel.profile
    if policy.provider != 'local' or policy.worker_mode == 'thread':
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Legacy managed service GPU slots require a Local process host')
    rss = (policy.native_limits or {}).get('max_rss_bytes')
    if type(rss) is not int or rss <= 0:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service requires explicit native_resources.max_rss_bytes')
    if rss < kernel.profile.requests['memory_bytes'] or rss > bounds.max_resident_bytes:
        raise DataflowError('RESOURCE_INFEASIBLE', 'Managed GPU service RSS does not fit the original run envelope')
    return replace(kernel.profile, requests={**kernel.profile.requests, 'memory_bytes': rss})
