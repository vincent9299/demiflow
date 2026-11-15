"""Co-locate a bounded incremental suffix DAG in one admitted worker.

Sources stay independent: a blocking source must never starve async completions
or timed batch flushes. Logical state/recovery/effect declarations remain intact.
"""
from dataclasses import replace
from ..resources.contracts import ResourceProfile

_TERMINALS = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches',
    'count', 'columns', 'schema', 'size_bytes', 'materialize', 'collect_summary', 'run_stream',
    'write_lance', 'write_file'})


def fuse_thread_suffix(graph, stages, *, policy, bounds, hosts):
    if not policy.fusion:
        return stages
    nodes = {n.occurrence_id: n for n in graph.nodes}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    over_workers = (policy.max_active_workers is not None
        and sum(len(stage.partitions) for stage in stages) > policy.max_active_workers)
    # A default four-worker linear pipeline must not park every pure map just
    # because it has more than four logical stages. Reuse the already supported
    # source + suffix ownership, preserving every original logical kernel.
    # Broader/shared DAGs retain their existing small-worker/prepend rules.
    linear_suffix = (policy.provider == 'local' and policy.max_active_workers is not None
        and policy.max_active_workers >= 2 and over_workers
        and len(graph.sources) == len(graph.terminals) == 1
        and all(len(incoming.get(n.occurrence_id, ())) <= 1
                and len(outgoing.get(n.occurrence_id, ())) <= 1 for n in graph.nodes)
        and all(n.kernel.effect == 'pure' and n.kernel.resource_role == 'cpu'
                for n in graph.nodes if n.kind not in {'source', 'terminal'}))
    # Small-worker pipelines need the same source + suffix layout for ordinary
    # per-task transforms. Co-location keeps each original kernel's task/state
    # boundaries, including atomic expansion into a downstream batch consumer.
    bounded_suffix = linear_suffix or (policy.provider == 'local' and policy.max_active_workers == 2
        and over_workers)
    # A shared upstream feeding both prepend ports must run once. Preserve all
    # input owners/ports, but put the prepend and its result consumer together
    # when that removes an otherwise unnecessary worker-budget phase boundary.
    prepend_tail = (policy.provider == 'local' and policy.max_active_workers is not None
        and sum(len(s.partitions) for s in stages) > policy.max_active_workers
        and any(n.kernel.kind == 'StreamPrependOp' for n in graph.nodes))
    async_suffix = (policy.provider == 'local' and policy.max_active_workers is not None
        and sum(len(s.partitions) for s in stages) > policy.max_active_workers
        and any(n.kernel.kind == 'AsyncMapOp' for n in graph.nodes))
    if policy.worker_mode != 'thread' and not bounded_suffix and not prepend_tail and not async_suffix:
        return stages
    owner = {occ: stage for stage in stages for occ in stage.occurrences}
    used, replacements = set(), {}
    for terminal in graph.terminals:
        if nodes[terminal].kernel.kind not in _TERMINALS:
            continue
        # Follow every declared input port. A shared branch remains one node
        # owned by one worker; GraphRunner still calls the original kernels and
        # propagates each edge/EOI separately. Source owners are never included.
        selected, pending = {terminal}, [terminal]
        while pending:
            current = pending.pop()
            inputs = incoming.get(current, ())
            if not bounded_suffix and len(inputs) != 1:
                continue
            for edge in inputs:
                candidate = edge.producer
                if candidate in selected or candidate in used:
                    continue
                node, stage = nodes[candidate], owner[candidate]
                kernel = node.kernel
                native_save = (kernel.kind == 'SaveLanceOp' and kernel.effect == 'journaled'
                               and kernel.state in {'adapter', 'nonrecoverable'}
                               and kernel.resource_role == 'io')
                prepend = (prepend_tail and kernel.kind == 'StreamPrependOp'
                           and {port.name: port.requirement for port in kernel.inputs}
                           == {'main': 'incremental', 'prefix': 'finite_complete'})
                if (not (node.kind == 'transform' or node.kind == 'relational' and kernel.kind == 'union')
                        or not (kernel.effect == 'pure' and kernel.resource_role == 'cpu' or native_save)
                        or not (prepend or kernel.inputs
                                and all(port.requirement == 'incremental' for port in kernel.inputs))
                        or not bounded_suffix and len(outgoing.get(candidate, ())) != 1
                        or len(stage.occurrences) != 1 or len(stage.partitions) != 1
                        or stage.profile.limits or stage.profile.requirements
                        or set(stage.profile.requests) - {'cpu_millis', 'memory_bytes'}):
                    continue
                selected.add(candidate)
                pending.append(candidate)
        # partition_stages already established topological order. Preserve it
        # so mixed fan-in and per-task callbacks retain original ownership.
        suffix = [o for stage in stages for o in stage.occurrences if o in selected]
        members = [owner[occ] for occ in suffix]
        if (len(suffix) < 2 or any(len(s.partitions) != 1 or len(s.occurrences) != 1
                    or s.profile.limits or s.profile.requirements
                    or set(s.profile.requests) - {'cpu_millis', 'memory_bytes'} for s in members)
                or not (bounded_suffix or any(nodes[o].kernel.configuration.get('streaming_map')
                           or nodes[o].kernel.kind == 'AsyncMapOp'
                           or prepend_tail and nodes[o].kernel.kind == 'StreamPrependOp' for o in suffix))):
            continue
        resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in members)
        if resident > bounds.max_resident_bytes - max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8)):
            continue
        cpu = sum(s.profile.requests['cpu_millis'] for s in members)
        # Cluster totals do not prove that a single fused owner fits one host.
        # Keep independently placeable stages when no verified host fits.
        if not any(cpu <= host.logical_resources.get('cpu_millis',
                       host.logical_resources.get('cpu', 0) * 1000)
                   and resident <= host.logical_resources.get('memory_bytes', 0)
                   for host in hosts):
            continue
        merged = replace(members[0], occurrences=tuple(suffix),
            profile=ResourceProfile(requests={'cpu_millis': cpu,
                                              'memory_bytes': resident}),
            replica_min=1, replica_target=1, replica_max=1, resident_bytes=resident,
            output_bytes=sum(s.output_bytes for s in members), state_bytes=sum(s.state_bytes for s in members),
            kernel_refs=tuple(dict.fromkeys(ref for s in members for ref in s.kernel_refs)))
        replacements[members[0].stage_id] = merged
        used.update(suffix)
    return tuple(replacements[s.stage_id] if s.stage_id in replacements else s
                 for s in stages if s.stage_id in replacements or not any(o in used for o in s.occurrences))
