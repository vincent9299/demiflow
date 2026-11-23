"""Choose finite relational fusion and place its sealed stages within the admitted grant."""
from dataclasses import replace

from ..execution.contracts.relational_region import (
    MAX_JOIN_DEPTH, SQL_CHILD_RSS, describe_region, region_key_fields, region_resident_bytes,
)
from ..resources.contracts import ResourceProfile


def fuse_relational_regions(graph, stages, *, policy, bounds, hosts, snapshots=()):
    if not policy.fusion:
        return stages
    nodes = {n.occurrence_id: n for n in graph.nodes}
    order = {n.occurrence_id: i for i, n in enumerate(graph.nodes)}
    owners = {o: s for s in stages for o in s.occurrences}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    fixed_lance = {snap.source_id for snap in snapshots if snap.kind == 'lance'
                   and type(snap.immutable_identity.get('version')) is int}
    def fixed_source(occurrence):
        while nodes[occurrence].kernel.kind == 'SelectColumnsOp':
            upstream = incoming.get(occurrence, ())
            if len(upstream) != 1:
                return False
            occurrence = upstream[0].producer
        return occurrence in fixed_lance

    used, replacements = set(), {}
    for node in reversed(graph.nodes):
        root = node.occurrence_id
        if root in used or node.kernel.kind not in ('join', 'reduce', 'groups'):
            continue
        chosen = {root}
        def visit(occ, depth):
            for edge in incoming.get(occ, ()):
                parent = nodes[edge.producer]
                next_depth = depth + (parent.kernel.kind == 'join')
                if (parent.occurrence_id in used or parent.kernel.kind != 'join'
                        or next_depth > MAX_JOIN_DEPTH or len(outgoing.get(edge.producer, ())) != 1
                        or tuple(parent.kernel.configuration['on']) != region_key_fields(nodes[occ], edge.input_port)):
                    continue
                chosen.add(edge.producer)
                visit(edge.producer, next_depth)
        visit(root, int(node.kernel.kind == 'join'))
        members = tuple(sorted(chosen, key=order.__getitem__))
        plan = describe_region(graph, members)
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
        # Choose the native variant only for provably fixed typed leaves with
        # no callable between them and the region. A sealed legacy region or
        # a smaller provider grant keeps its original bounded hash algorithm.
        edges = {e.edge_id:e for e in graph.edges}
        if (fixed_lance and all(fixed_source(edges[i.edge_id].producer) for i in plan.inputs)
                and (policy.native_limits or {}).get('max_rss_bytes', SQL_CHILD_RSS) >= SQL_CHILD_RSS):
            native = describe_region(graph, members, sql_offsets=True)
            if native is not None:
                requested = region_resident_bytes(native, bounds)
                cpu_needed = max(cpu, 1000)
                if requested + control <= bounds.max_resident_bytes and any(
                        requested <= h.logical_resources.get('memory_bytes', 0)
                        and cpu_needed <= h.logical_resources.get('cpu_millis', h.logical_resources.get('cpu', 0)*1000)
                        for h in hosts):
                    plan, resident, cpu = native, requested, cpu_needed
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


def place_key_exclusion(graph, snapshots, stages, *, policy, bounds, hosts):
    """Admit the SQL process inside the stage that already owns a pinned left scan's anti join."""
    from ..execution.contracts.relational_region import KEY_SQL_SUFFIX, SQL_CHILD_RSS, SQL_PARENT_WINDOW

    fixed = {s.source_id for s in snapshots if s.kind == 'lance' and
             type(s.immutable_identity.get('version')) is int}
    exclusion = {n.occurrence_id for n in graph.nodes if n.kernel.kind == 'exclude_keys'
                 and n.kernel.configuration.get('lance_key_source') in fixed}
    if not exclusion or (policy.native_limits or {}).get('max_rss_bytes', SQL_CHILD_RSS) < SQL_CHILD_RSS:
        return stages
    result = []
    for stage in stages:
        if exclusion.isdisjoint(stage.occurrences):
            result.append(stage)
            continue
        resident = stage.resident_bytes + SQL_CHILD_RSS + SQL_PARENT_WINDOW
        cpu = max(1000, stage.profile.requests['cpu_millis'])
        limits = stage.profile.limits or {}
        if (resident > min(bounds.max_resident_bytes, limits.get('memory_bytes', resident))
                or cpu > limits.get('cpu_millis', cpu)
                or not any(resident <= h.logical_resources.get('memory_bytes', 0) and
                           cpu <= h.logical_resources.get('cpu_millis', h.logical_resources.get('cpu', 0)*1000)
                           for h in hosts)):
            result.append(stage)
            continue
        profile = replace(stage.profile, requests={**stage.profile.requests,
            'cpu_millis': cpu, 'memory_bytes': resident})
        result.append(replace(stage, stage_id=stage.stage_id + KEY_SQL_SUFFIX,
                              profile=profile, resident_bytes=resident))
    return tuple(result)
