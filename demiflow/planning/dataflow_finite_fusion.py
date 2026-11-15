"""Keep a finite CPU result and its synchronous consumers in one worker.

Logical ports, kernel state and the original resource grants remain separate
facts. This removes transport of every wide join row to an adjacent pure map;
it never moves sources, asynchronous actors, devices or user isolation grants.
"""
from dataclasses import replace

from ..resources.contracts import ResourceProfile

_FINITE = frozenset({'join', 'exclude_keys', 'SortOp', 'RandomShuffleOp'})
_TERMINALS = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches',
    'count', 'columns', 'schema', 'size_bytes', 'materialize', 'collect_summary'})


def fuse_finite_suffix(graph, stages, *, policy, bounds):
    if not policy.fusion:
        return stages
    nodes = {node.occurrence_id: node for node in graph.nodes}
    owners = {occ: stage for stage in stages for occ in stage.occurrences}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    used, replacements = set(), {}
    for terminal in graph.terminals:
        if nodes[terminal].kernel.kind not in _TERMINALS:
            continue
        selected, current, anchored = [terminal], terminal, False
        while len(incoming.get(current, ())) == 1:
            candidate = incoming[current][0].producer
            node, kernel = nodes[candidate], nodes[candidate].kernel
            if (candidate in used or len(outgoing.get(candidate, ())) != 1
                    or node.kind == 'source' or kernel.effect != 'pure'
                    or kernel.resource_role != 'cpu'):
                break
            finite = (kernel.kind in _FINITE and kernel.inputs
                      and all(port.requirement == 'finite_complete' for port in kernel.inputs))
            synchronous = (node.kind == 'transform' and kernel.state == 'none' and kernel.mergeable
                and len(kernel.inputs) == 1 and kernel.inputs[0].requirement == 'incremental'
                and kernel.configuration.get('concurrency', 1) == 1
                and not kernel.configuration.get('streaming_map')
                and not kernel.configuration.get('flush_interval'))
            if not (finite or synchronous):
                break
            selected.append(candidate)
            current = candidate
            if finite:
                anchored = True
                break  # Preserve the finite producer's independent input owners.
        if not anchored:
            continue
        selected.reverse()
        members = [owners[occ] for occ in selected]
        if any(len(stage.occurrences) != 1 or len(stage.partitions) != 1
               or stage.profile.limits or stage.profile.requirements
               or set(stage.profile.requests) - {'cpu_millis', 'memory_bytes'} for stage in members):
            continue
        resident = sum(max(stage.resident_bytes, stage.profile.requests['memory_bytes']) for stage in members)
        control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
        if resident > bounds.max_resident_bytes - control:
            continue
        first = members[0]
        replacements[first.stage_id] = replace(first, occurrences=tuple(selected),
            profile=ResourceProfile(requests={
                'cpu_millis': sum(stage.profile.requests['cpu_millis'] for stage in members),
                'memory_bytes': resident}),
            resident_bytes=resident, output_bytes=sum(stage.output_bytes for stage in members),
            state_bytes=sum(stage.state_bytes for stage in members),
            kernel_refs=tuple(dict.fromkeys(ref for stage in members for ref in stage.kernel_refs)))
        used.update(selected)
    return tuple(replacements[stage.stage_id] if stage.stage_id in replacements else stage
                 for stage in stages if stage.stage_id in replacements
                 or not any(occ in used for occ in stage.occurrences))
