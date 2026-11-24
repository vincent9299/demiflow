"""Place sealed resident groups using current completion and channel readiness facts."""
from __future__ import annotations

from demiflow.execution.contracts.dataflow_errors import DataflowError


def value_predecessors(physical):
    """Row stage prerequisites across job edges; jobs never become cut participants."""
    row_stages = {stage.stage_id for stage in physical.stages}
    jobs = {job.stage_id for job in physical.jobs}
    inputs = {}
    for edge in physical.value_edges:
        inputs.setdefault(edge.consumer, []).append(edge.producer)
    resolved = {}
    def row_producers(job, visiting):
        if job in visiting:
            raise DataflowError('INVALID_CONTRACT', 'cycle in physical model dependencies')
        if job not in resolved:
            result = set()
            for owner in inputs.get(job, ()):
                if owner in jobs:
                    result.update(row_producers(owner, visiting | {job}))
                elif owner in row_stages:
                    result.add(owner)
            resolved[job] = result
        return resolved[job]
    return {stage: set().union(*(row_producers(job, set()) for job in inputs[stage]))
            for stage in row_stages & inputs.keys()}

def finite_phases(physical, *, split_durable=False):
    stages = {s.stage_id: s for s in physical.stages}
    owners = {p: s.stage_id for s in physical.stages for p in s.partitions}
    parent = {sid: sid for sid in stages}
    def find(sid):
        while parent[sid] != sid:
            sid = parent[sid]
        return sid
    for edge in physical.exchanges:
        if edge.mode == 'memory_incremental' or not split_durable and edge.mode != 'finite_sealed':
            parent[find(owners[edge.producer_partitions[0]])] = find(owners[edge.consumer_partitions[0]])
    groups = {}
    for sid in stages:
        groups.setdefault(find(sid), []).append(sid)
    dependencies = {key: set() for key in groups}
    for edge in physical.exchanges:
        a, b = find(owners[edge.producer_partitions[0]]), find(owners[edge.consumer_partitions[0]])
        if a != b:
            dependencies[b].add(a)
    for consumer, producers in value_predecessors(physical).items():
        dependencies[find(consumer)].update(find(p) for p in producers)
    result = []
    while dependencies:
        ready = next((key for key, deps in dependencies.items() if not deps), None)
        if ready is None:
            raise DataflowError('RESOURCE_INFEASIBLE', 'streaming residency groups require cyclic simultaneous placement')
        result.append(tuple(groups[ready]))
        del dependencies[ready]
        for deps in dependencies.values():
            deps.discard(ready)
    return tuple(result)

def pack_finite_phases(physical, *, fits, prior=None):
    """Admit independent ready cohorts together under the existing envelope.

    Cohorts/ports come only from the sealed physical plan. This is placement of
    that plan, not a new graph or persistent authority. On recovery, keep the
    original unfinished active cohort exactly; completed workers never restart.
    """
    groups = list(finite_phases(physical))
    owners = {p: s.stage_id for s in physical.stages for p in s.partitions}
    dependencies = {s.stage_id: set() for s in physical.stages}
    for edge in physical.exchanges:
        producer, consumer = owners[edge.producer_partitions[0]], owners[edge.consumer_partitions[0]]
        if producer != consumer:
            dependencies[consumer].add(producer)
    model_dependencies = value_predecessors(physical)
    for consumer, producers in model_dependencies.items():
        dependencies[consumer].update(producers)
    completed = set(prior['completed_stages']) if prior is not None else set()
    active = tuple(prior['active_stages']) if prior is not None else ()
    if prior is not None and prior['phase_complete']:
        completed.update(active)
        active = ()
    result = []
    accounted = set(completed) | set(active)
    for group in groups:
        overlap = set(group) & accounted
        if overlap and overlap != set(group):
            raise DataflowError('CORRUPT_ARTIFACT', 'history splits an inseparable resident cohort')
    groups = [group for group in groups if not set(group) <= accounted]
    if active:
        if any(dependencies[s] - set(active) - completed for s in active) or not fits(active):
            raise DataflowError('RESOURCE_INFEASIBLE', 'original active cohort cannot resume within admitted capacity')
        result.append(active)
        completed.update(active)
    while groups:
        ready = [g for g in groups if all(not dependencies[s] - set(g) - completed for s in g)]
        if not ready:
            raise DataflowError('RESOURCE_INFEASIBLE', 'finite placement has no ready cohort')
        selected = ()
        for group in ready:
            if selected and (model_dependencies.keys() & set(group) or model_dependencies.keys() & set(selected)):
                continue
            if fits(selected + group):
                selected += group
        if not selected:
            raise DataflowError('RESOURCE_INFEASIBLE', 'ready cohort exceeds admitted host or resident capacity')
        result.append(selected)
        completed.update(selected)
        groups = [g for g in groups if not set(g) <= completed]
    return tuple(result)

def partition_phases(physical):
    """Cycle isolated stage partitions; memory peers remain one live cohort."""
    stages = {s.stage_id: s for s in physical.stages}
    result = []
    for group in finite_phases(physical, split_durable=True):
        members = tuple(p for sid in group for p in stages[sid].partitions)
        if len(group) == 1:
            width = stages[group[0]].replica_target
            result.extend((group, members[i:i + width]) for i in range(0, len(members), width))
        else:
            result.append((group, members))
    return tuple(result)


def choose_durable_phase(physical, groups, partition_groups, *, completed, value_inputs,
                         sources, finite_seals, edge_eoi, waiting_for):
    # A ready consumer runs promptly, before admitting another source window.
    for index in reversed(range(len(groups))):
        group = set(partition_groups[index])
        if any(value_inputs.get(s, set()) - completed for s in groups[index]):
            continue
        if group <= completed:
            continue
        inputs = [edge for edge in physical.exchanges
                  if set(edge.consumer_partitions) & group and not set(edge.producer_partitions) <= group]
        if any(edge.mode == 'finite_sealed' and edge.channel_id not in finite_seals for edge in inputs):
            continue
        pending = waiting_for(group)
        eof = bool(inputs) and all(edge.mode == 'finite_sealed' or
                all((edge.edge_id, p) in edge_eoi for p in edge.producer_partitions) for edge in inputs)
        if pending or eof:
            return index
    for index, group in enumerate(groups):
        if set(partition_groups[index]) <= completed:
            continue
        if any(set(s.occurrences) & sources for s in physical.stages if s.stage_id in group):
            return index
    raise DataflowError('RESOURCE_INFEASIBLE', 'no runnable parked stage has input or an authenticated EOI')
