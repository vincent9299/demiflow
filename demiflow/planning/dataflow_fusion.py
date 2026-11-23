"""One admitted worker can run a bounded pure synchronous linear pipeline.

Logical occurrences and their observations remain intact. This only chooses
physical co-location in the existing GraphRunner; it creates no executor or
recovery authority. Resource grants cover the sum of all live kernel windows.
"""
from dataclasses import replace

from ..resources.contracts import ResourceProfile
from ..execution.contracts.dataflow import FIXED_SCAN_BATCHES

_SOURCES = frozenset({'items', 'range', 'file', 'lance', 'materialized', 'iterable', 'literal_empty'})
_TERMINALS = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches',
    'count', 'columns', 'schema', 'size_bytes', 'materialize', 'collect_summary'})


def fuse_single_worker_pipeline(graph, stages, *, policy, bounds, hosts=None):
    if not policy.fusion:
        return stages
    # Preserve independently runnable CPU pipelines when the requested ceiling
    # can already host them. Automatic co-location only extends the layout that
    # worker pressure would otherwise force into a shared suffix.
    fixed_scan = (len(graph.nodes) == 2 and any(
        n.kind == 'terminal' and n.kernel.configuration.get('input_carrier') == FIXED_SCAN_BATCHES
        for n in graph.nodes))
    if not fixed_scan and policy.max_active_workers != 1 and (policy.max_active_workers is None
            or sum(len(stage.partitions) for stage in stages) <= policy.max_active_workers):
        return stages
    if len(graph.sources) != 1 or len(graph.terminals) != 1 or len(stages) < 2:
        return stages
    nodes = {node.occurrence_id: node for node in graph.nodes}
    owners = {occ: stage for stage in stages for occ in stage.occurrences}
    inputs, outputs = {}, {}
    for edge in graph.edges:
        inputs.setdefault(edge.consumer, []).append(edge)
        outputs.setdefault(edge.producer, []).append(edge)
    chain, current = [], graph.sources[0]
    while current not in chain:
        chain.append(current)
        if current == graph.terminals[0]:
            break
        edges = outputs.get(current, ())
        if len(edges) != 1 or len(inputs.get(edges[0].consumer, ())) != 1:
            return stages
        current = edges[0].consumer
    if len(chain) != len(nodes) or chain[-1] != graph.terminals[0]:
        return stages
    members = [owners[occ] for occ in chain]
    for index, (occ, stage) in enumerate(zip(chain, members)):
        node, kernel = nodes[occ], nodes[occ].kernel
        if len(stage.occurrences) != 1 or len(stage.partitions) != 1:
            return stages
        if kernel.state != 'none' or kernel.effect != 'pure':
            return stages
        if stage.profile.limits or stage.profile.requirements:
            return stages
        if set(stage.profile.requests) - {'cpu_millis', 'memory_bytes'}:
            return stages
        if index == 0:
            if node.kind != 'source' or kernel.kind not in _SOURCES:
                return stages
        elif index == len(chain) - 1:
            if node.kind != 'terminal' or (kernel.kind not in _TERMINALS and not fixed_scan):
                return stages
        elif (node.kind != 'transform' or kernel.resource_role != 'cpu' or not kernel.mergeable
              or len(kernel.inputs) != 1 or kernel.inputs[0].requirement != 'incremental'
              or kernel.configuration.get('concurrency', 1) != 1):
            return stages
        if kernel.configuration.get('streaming_map') or kernel.configuration.get('flush_interval'):
            # A blocking source pull shares this business thread. Keeping a
            # timer-driven operator here would delay its flush until the next
            # source row/EOI, violating the declared incremental deadline.
            return stages
    resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in members)
    control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
    if resident > bounds.max_resident_bytes - control:
        return stages
    requests = {'cpu_millis': max(s.profile.requests['cpu_millis'] for s in members),
                'memory_bytes': resident}
    # max_active_workers is a ceiling, not a demand to create transport
    # between synchronous kernels. Optional co-location must fit one real
    # host; aggregate cluster capacity cannot justify a merged allocation.
    if hosts is None:
        if policy.max_active_workers != 1:
            return stages
    elif not any(requests['cpu_millis'] <= host.logical_resources.get(
            'cpu_millis', host.logical_resources.get('cpu', 0) * 1000)
            and resident <= host.logical_resources.get('memory_bytes', 0) for host in hosts):
        return stages
    refs = tuple(dict.fromkeys(ref for s in members for ref in s.kernel_refs))
    return (replace(members[0], occurrences=tuple(chain), profile=ResourceProfile(requests=requests),
        replica_min=1, replica_target=1, replica_max=1, resident_bytes=resident,
        output_bytes=sum(s.output_bytes for s in members), state_bytes=0, kernel_refs=refs),)
