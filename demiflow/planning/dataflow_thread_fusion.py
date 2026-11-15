"""Co-locate a linear streaming suffix in one admitted thread host.

Sources stay independent: a blocking source must never starve async completions
or timed batch flushes. Logical state/recovery/effect declarations remain intact.
"""
from dataclasses import replace
from ..resources.contracts import ResourceProfile

_TERMINALS = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches',
    'count', 'columns', 'schema', 'size_bytes', 'materialize', 'collect_summary', 'run_stream',
    'write_lance', 'write_file'})


def fuse_thread_suffix(graph, stages, *, policy, bounds):
    if not policy.fusion:
        return stages
    nodes = {n.occurrence_id: n for n in graph.nodes}
    # Legacy explicit inline streaming over attempt-local iterators needs only
    # source + streaming suffix. Do not change other sealed/resumable layouts.
    inline_iterator = (policy.provider == 'local' and policy.max_active_workers == 2
        and any(nodes[occ].kernel.kind == 'iterable' for occ in graph.sources)
        and any(n.kernel.configuration.get('streaming_map') for n in graph.nodes))
    if policy.worker_mode != 'thread' and not inline_iterator:
        return stages
    owner = {occ: stage for stage in stages for occ in stage.occurrences}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    used, replacements = set(), {}
    for terminal in graph.terminals:
        if nodes[terminal].kernel.kind not in _TERMINALS:
            continue
        suffix, current = [terminal], terminal
        while len(incoming.get(current, ())) == 1:
            candidate = incoming[current][0].producer
            node, stage = nodes[candidate], owner[candidate]
            kernel = node.kernel
            if (candidate in used or node.kind != 'transform' or kernel.effect != 'pure'
                    or kernel.resource_role != 'cpu' or len(kernel.inputs) != 1
                    or kernel.inputs[0].requirement != 'incremental'
                    or len(outgoing.get(candidate, ())) != 1
                    or len(stage.occurrences) != 1 or len(stage.partitions) != 1
                    or stage.profile.limits or stage.profile.requirements
                    or set(stage.profile.requests) - {'cpu_millis', 'memory_bytes'}):
                break
            suffix.insert(0, candidate)
            current = candidate
        members = [owner[occ] for occ in suffix]
        if (len(suffix) < 2 or any(len(s.partitions) != 1 or len(s.occurrences) != 1 for s in members)
                or not any(nodes[o].kernel.configuration.get('streaming_map')
                           or nodes[o].kernel.kind == 'AsyncMapOp' for o in suffix)):
            continue
        resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in members)
        if resident > bounds.max_resident_bytes - max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8)):
            continue
        merged = replace(members[0], occurrences=tuple(suffix),
            profile=ResourceProfile(requests={'cpu_millis': sum(s.profile.requests['cpu_millis'] for s in members),
                                              'memory_bytes': resident}),
            replica_min=1, replica_target=1, replica_max=1, resident_bytes=resident,
            output_bytes=sum(s.output_bytes for s in members), state_bytes=sum(s.state_bytes for s in members),
            kernel_refs=tuple(dict.fromkeys(ref for s in members for ref in s.kernel_refs)))
        replacements[members[0].stage_id] = merged
        used.update(suffix)
    return tuple(replacements[s.stage_id] if s.stage_id in replacements else s
                 for s in stages if s.stage_id in replacements or not any(o in used for o in s.occurrences))
