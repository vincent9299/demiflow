"""One-worker placement of a bounded DAG with original kernel ownership.

This does not park or replay an opaque source/state. Each kernel remains live
in the original GraphRunner, with its original state and finite spool protocol.
"""
from dataclasses import replace
from ..resources.contracts import ResourceProfile


def fuse_single_worker_dag(graph, stages, *, policy, bounds, hosts):
    if not policy.fusion or policy.max_active_workers != 1 or len(stages) < 2:
        return stages
    if any('graph_route' in node.kernel.configuration for node in graph.nodes):
        return stages  # Declared reader partitions have independent identities.
    concurrent = any(node.kernel.kind in {'AsyncMapOp','BatchMapOp','StreamGroupBatchesOp'}
        or node.kernel.configuration.get('streaming_map')
        or node.kernel.configuration.get('concurrency',1) != 1
        or node.kernel.configuration.get('flush_interval') for node in graph.nodes)
    # Blocking source pulls must not starve timers/async callbacks. Only native
    # platform sources with bounded pulls are allowed in the same business pump;
    # no user iterator, datasource/SQL callback or external scan is admitted.
    if concurrent and any(n.kind == 'source' and n.kernel.kind not in
            {'items','range','literal_empty','materialized'} for n in graph.nodes):
        return stages
    for node in graph.nodes:
        kernel = node.kernel
        if (kernel.effect != 'pure' or kernel.resource_role not in {'cpu','io'}
                or node.kind not in {'source','terminal','transform','relational'}):
            return stages
    # Original per-occurrence state is retained, including nonrecoverable class
    # instances. One sequential host does not require pretending they can park.
    if any(len(stage.partitions) != 1 or stage.replica_min != 1
            or stage.profile.limits or stage.profile.requirements
            or set(stage.profile.requests) - {'cpu_millis','memory_bytes'} for stage in stages):
        return stages
    resident = sum(max(s.resident_bytes,s.profile.requests['memory_bytes']) for s in stages)
    control = max(1,min(bounds.max_control_bytes,bounds.max_resident_bytes//8))
    if resident > bounds.max_resident_bytes-control:
        return stages
    # Stateful actors can retain lifecycle work. Preserve summed original CPU
    # declarations; pure stateless synchronous kernels retain serial CPU max.
    cpu_fn = sum if concurrent or any(n.kernel.state != 'none' for n in graph.nodes) else max
    cpu = cpu_fn(s.profile.requests['cpu_millis'] for s in stages)
    if hosts is None or not any(cpu <= host.logical_resources.get('cpu_millis',
            host.logical_resources.get('cpu',0)*1000)
            and resident <= host.logical_resources.get('memory_bytes',0) for host in hosts):
        return stages
    return (replace(stages[0],occurrences=tuple(o for s in stages for o in s.occurrences),
        profile=ResourceProfile(requests={'cpu_millis':cpu,'memory_bytes':resident}),
        replica_min=1,replica_target=1,replica_max=1,resident_bytes=resident,
        output_bytes=sum(s.output_bytes for s in stages),state_bytes=sum(s.state_bytes for s in stages),
        kernel_refs=tuple(dict.fromkeys(ref for s in stages for ref in s.kernel_refs))),)
