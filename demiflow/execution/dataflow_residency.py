"""Finite boundary residency; all durable evidence remains in native commit cuts.

Streaming channels retain a live ACK owner and therefore form one resident group.
A phase may retire only after all its workers finished and its cut was ACKed.
"""
from .contracts.dataflow_errors import DataflowError
from .contracts.dataflow_codec import compute_participant_id

SCHEMA = 'demiflow.finite_residency.v1'


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


def participants_for(physical, stages):
    return tuple(compute_participant_id(s.stage_id, p) for s in physical.stages
                 if s.stage_id in stages for p in s.partitions)


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
            if fits(selected + group):
                selected += group
        if not selected:
            raise DataflowError('RESOURCE_INFEASIBLE', 'ready cohort exceeds admitted host or resident capacity')
        result.append(selected)
        completed.update(selected)
        groups = [g for g in groups if not set(g) <= completed]
    return tuple(result)


def validate_recovered_residency(physical, cut, contributions, residency, *, facts, artifacts, current_commit):
    """Every inherited contribution must be an unchanged earlier ACKed fact.

    Its origin cut/boot is retained. No synthetic initial worker is admitted.
    Old v3 cuts without residency continue to require the full participant table.
    """
    from .checkpoint.coordinator import _contribution_json, _artifact_from_json
    def reject(message):
        raise DataflowError('CORRUPT_ARTIFACT', message)
    if (type(residency) is not dict or set(residency) - {'satisfied_source_splits'} != {
            'schema', 'active_stages', 'completed_stages', 'phase_complete', 'finite_seals'}
            or residency['schema'] != SCHEMA or type(residency['phase_complete']) is not bool):
        reject('invalid residency cut schema')
    satisfied = residency.get('satisfied_source_splits', [])
    if (not isinstance(satisfied, list) or any(type(s) is not str for s in satisfied)
            or len(set(satisfied)) != len(satisfied)
            or not set(satisfied) <= {s.split_id for s in physical.splits}):
        reject('residency input completion names undeclared source splits')
    active, completed = residency['active_stages'], residency['completed_stages']
    all_stages = {s.stage_id for s in physical.stages}
    if (not isinstance(active, list) or not active or not isinstance(completed, list)
            or len(set(active)) != len(active) or len(set(completed)) != len(completed)
            or set(active) & set(completed) or not set(active + completed) <= all_stages):
        reject('invalid active/completed residency stages')
    active_ids = set(participants_for(physical, active))
    carried_ids = set(participants_for(physical, completed))
    if (set(cut.participants) != active_ids
            or {c.participant_id for c in contributions} != active_ids | carried_ids
            or len(contributions) != len(active_ids | carried_ids)):
        reject('residency participant set differs from actual stages')
    if cut.final and (not residency['phase_complete'] or set(active + completed) != all_stages):
        reject('final residency cut precedes completion of every stage')
    origins = {}
    completed_proofs = set()
    for fact in facts:
        if fact['commit_id'] == current_commit or fact['sequence'] >= cut.sequence:
            continue
        ref = ((fact.get('prepared') or {}).get('state') or {}).get('dataflow-cut-v3')
        if ref is None:
            continue
        previous = artifacts.read(ref)
        old_cut = previous.get('cut', {})
        for c in previous.get('contributions', ()):
            if c['cut_id'] == old_cut.get('cut_id') and c['participant_id'] in old_cut.get('participants', ()):
                origins[(c['cut_id'], c['participant_id'])] = c
        old = previous.get('residency')
        if old and old.get('phase_complete'):
            completed_proofs.update(old['active_stages'])
    if not set(completed) <= completed_proofs:
        reject('retired stages lack an ACKed completion cut')
    for contribution in contributions:
        if contribution.participant_id in active_ids:
            if contribution.cut_id != cut.cut_id:
                reject('active contribution belongs to a different cut')
        elif origins.get((contribution.cut_id, contribution.participant_id)) != _contribution_json(contribution):
            reject('inherited contribution differs from its original ACKed worker fact')
    finite_ids = {e.channel_id for e in physical.exchanges if e.mode == 'finite_sealed'}
    seals = residency['finite_seals']
    if type(seals) is not dict or not set(seals) <= finite_ids:
        reject('residency carries an undeclared finite channel')
    for ref in seals.values():
        _artifact_from_json(ref)
