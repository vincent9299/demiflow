"""Versioned, optional same-key finite compute regions in the original graph.

No source, UDF, exchange authority or logical occurrence is replaced. A region
owns one synchronous loop and one bounded payload/reference arena. Old sealed
stages have no region marker and can never opt into this interpretation.
"""
from dataclasses import dataclass, replace

from ..execution.contracts.dataflow_errors import DataflowError
from ..execution.recovery_contract import identity
from ..resources.contracts import ResourceProfile

PREFIX = 'relational-v1-'
MAX_JOIN_DEPTH = 2


@dataclass(frozen=True)
class RelationalRegionInput:
    edge_id: str
    consumer: str
    port: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class RelationalRegionPlan:
    region_id: str
    occurrences: tuple[str, ...]
    exit_occurrence: str
    inputs: tuple[RelationalRegionInput, ...]
    internal_edges: tuple[str, ...]
    join_depth: int
    ordered_groups: bool


def _fields(node, port):
    config = node.kernel.configuration
    return tuple(config.get('right_on') or config['on']) if port == 'right' else tuple(config['on'])


def _describe(graph, members):
    nodes = {n.occurrence_id: n for n in graph.nodes}
    selected = set(members)
    incoming = {occ: [] for occ in members}
    outgoing = {occ: [] for occ in members}
    for edge in graph.edges:
        if edge.consumer in selected:
            incoming[edge.consumer].append(edge)
        if edge.producer in selected:
            outgoing[edge.producer].append(edge)
    exits = [occ for occ in members if not any(e.consumer in selected for e in outgoing[occ])]
    if len(exits) != 1 or len(members) < 2:
        return None
    terminal = exits[0]
    if any(nodes[o].kernel.kind not in ('join', 'reduce') for o in members):
        return None
    if any(nodes[o].kernel.kind == 'reduce' and o != terminal for o in members):
        return None
    depth, external, inner = {}, [], []
    for occ in members:
        node = nodes[occ]
        if (node.kernel.effect != 'pure' or node.kernel.resource_role != 'cpu'
                or not node.kernel.inputs or any(p.requirement != 'finite_complete' for p in node.kernel.inputs)
                or node.kernel.profile.limits or node.kernel.profile.requirements
                or set(node.kernel.profile.requests) - {'cpu_millis', 'memory_bytes'}):
            return None
        if occ != terminal and (len(outgoing[occ]) != 1 or outgoing[occ][0].consumer not in selected):
            return None
        parents = []
        for edge in incoming[occ]:
            fields = _fields(node, edge.input_port)
            if edge.producer in selected:
                producer = nodes[edge.producer]
                if producer.kernel.kind != 'join' or tuple(producer.kernel.configuration['on']) != fields:
                    return None
                if edge.producer not in depth:
                    return None
                parents.append(depth[edge.producer])
                inner.append(edge.edge_id)
            else:
                external.append(RelationalRegionInput(edge.edge_id, occ, edge.input_port, fields))
        depth[occ] = max(parents, default=0) + (node.kernel.kind == 'join')
    maximum = max(depth.values())
    if not 1 <= maximum <= MAX_JOIN_DEPTH:
        return None
    payload = ['relational-region-v1', members, terminal,
               [(x.edge_id, x.consumer, x.port, x.fields) for x in external], inner, maximum]
    return RelationalRegionPlan(PREFIX + identity(payload), tuple(members), terminal,
                                tuple(external), tuple(inner), maximum,
                                nodes[terminal].kernel.kind == 'reduce')


def interpret_region(graph, stage):
    if not stage.stage_id.startswith(PREFIX):
        return None
    plan = _describe(graph, stage.occurrences)
    if plan is None or plan.region_id != stage.stage_id or len(stage.partitions) != 1:
        raise DataflowError('INCOMPATIBLE_HISTORY', 'sealed relational region differs from original graph')
    return plan


def region_resident_bytes(plan, bounds):
    # Only references (not hydrated parent rows) are retained between joins.
    # Hydration has at most depth+1 input/result rows, two codec/copy rows and
    # one output handoff. Input preparation reuses that window sequentially.
    # One bounded index and traversal metadata use the remaining fixed window.
    metadata = min(4 * 1024**2, max(4096, bounds.max_control_bytes // 16))
    return (bounds.max_batch_bytes + (plan.join_depth + 4) * bounds.max_row_bytes
            + (bounds.max_state_bytes if plan.ordered_groups else 0) + metadata)


def fuse_relational_regions(graph, stages, *, policy, bounds, hosts):
    if not policy.fusion:
        return stages
    nodes = {n.occurrence_id: n for n in graph.nodes}
    order = {n.occurrence_id: i for i, n in enumerate(graph.nodes)}
    owners = {o: s for s in stages for o in s.occurrences}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    used, replacements = set(), {}
    for node in reversed(graph.nodes):
        root = node.occurrence_id
        if root in used or node.kernel.kind not in ('join', 'reduce'):
            continue
        chosen = {root}
        def visit(occ, depth):
            for edge in incoming.get(occ, ()):
                parent = nodes[edge.producer]
                next_depth = depth + (parent.kernel.kind == 'join')
                if (parent.occurrence_id in used or parent.kernel.kind != 'join'
                        or next_depth > MAX_JOIN_DEPTH or len(outgoing.get(edge.producer, ())) != 1
                        or tuple(parent.kernel.configuration['on']) != _fields(nodes[occ], edge.input_port)):
                    continue
                chosen.add(edge.producer)
                visit(edge.producer, next_depth)
        visit(root, int(node.kernel.kind == 'join'))
        members = tuple(sorted(chosen, key=order.__getitem__))
        plan = _describe(graph, members)
        if plan is None:
            continue
        selected = [owners[o] for o in members]
        if any(len(s.occurrences) != 1 or len(s.partitions) != 1
               or s.profile.limits or s.profile.requirements
               or set(s.profile.requests) - {'cpu_millis', 'memory_bytes'} for s in selected):
            continue
        resident = region_resident_bytes(plan, bounds)
        control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
        cpu = max(s.profile.requests['cpu_millis'] for s in selected)
        if resident + control > bounds.max_resident_bytes or not any(
                resident <= h.logical_resources.get('memory_bytes', 0)
                and cpu <= h.logical_resources.get('cpu_millis', h.logical_resources.get('cpu', 0) * 1000)
                for h in hosts):
            continue
        first = selected[0]
        replacements[first.stage_id] = replace(first, stage_id=plan.region_id,
            occurrences=members, partitions=(plan.region_id + '-p0',),
            profile=ResourceProfile(requests={'cpu_millis': cpu, 'memory_bytes': resident}),
            resident_bytes=resident, output_bytes=sum(s.output_bytes for s in selected),
            state_bytes=max(sum(s.state_bytes for s in selected), bounds.max_state_bytes if plan.ordered_groups else 0),
            kernel_refs=tuple(dict.fromkeys(k for s in selected for k in s.kernel_refs)))
        used.update(members)
    return tuple(replacements[s.stage_id] if s.stage_id in replacements else s
                 for s in stages if s.stage_id in replacements or not any(o in used for o in s.occurrences))
