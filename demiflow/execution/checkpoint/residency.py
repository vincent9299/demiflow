"""Validate and encode original residency cut proofs without owning live phase state."""
from __future__ import annotations

from dataclasses import asdict
from demiflow.execution.contracts.recovery_contract import document

import json
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import compute_participant_id
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.scheduler.residency import partition_phases

FINITE_SCHEMA = 'demiflow.finite_residency.v1'
DURABLE_SCHEMA = 'demiflow.durable_residency.v1'
PARTITION_SCHEMA = 'demiflow.durable_residency.v2'
SCHEMAS = (DURABLE_SCHEMA, PARTITION_SCHEMA)


def participants_for(physical, stages):
    return tuple(compute_participant_id(s.stage_id, p) for s in physical.stages
                 if s.stage_id in stages for p in s.partitions)

def validate_recovered_residency(physical, cut, contributions, residency, *, facts, artifacts, current_commit):
    """Every inherited contribution must be an unchanged earlier ACKed fact.

    Its origin cut/boot is retained. No synthetic initial worker is admitted.
    Old v3 cuts without residency continue to require the full participant table.
    """
    from demiflow.execution.contracts.checkpoint import contribution_json as _contribution_json, artifact_from_json as _artifact_from_json
    def reject(message):
        raise DataflowError('CORRUPT_ARTIFACT', message)
    if (type(residency) is not dict or set(residency) - {'satisfied_source_splits'} != {
            'schema', 'active_stages', 'completed_stages', 'phase_complete', 'finite_seals'}
            or residency['schema'] != FINITE_SCHEMA or type(residency['phase_complete']) is not bool):
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

def member_partitions(physical, value, name):
    if value.get('schema') == PARTITION_SCHEMA:
        return set(value[name + '_partitions'])
    return {p for stage in physical.stages if stage.stage_id in value[name + '_stages']
            for p in stage.partitions}

def participant_ids(physical, partitions):
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    return tuple(compute_participant_id(s.stage_id, p) for s in physical.stages
                 for p in s.partitions if p in partitions)

def frontier(contribution, partition, store, bounds):
    ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
    if ref is None:
        raise DataflowError('CORRUPT_ARTIFACT', 'parked worker lacks its actual frontier')
    return json.loads(b''.join(store.read(ref, max_bytes=bounds.max_state_bytes)))

def validate_durable_residency(physical, cut, contributions, residency, *, facts, artifacts, current_commit, store):
    from demiflow.execution.contracts.checkpoint import contribution_json as _contribution_json, contribution_from_json as _contribution_from_json, artifact_from_json as _artifact_from_json
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    def reject(reason):
        raise DataflowError('CORRUPT_ARTIFACT', reason)
    keys = {'schema', 'epoch', 'active_stages', 'visited_stages', 'completed_stages',
            'phase_complete', 'finite_seals', 'satisfied_source_splits', 'deliveries', 'edge_eoi', 'satisfied_inputs'}
    if type(residency) is not dict or residency.get('schema') not in SCHEMAS:
        reject('invalid durable residency schema')
    partitioned = residency['schema'] == PARTITION_SCHEMA
    if partitioned:
        keys |= {'active_partitions', 'visited_partitions', 'completed_partitions'}
    if set(residency) != keys:
        reject('invalid durable residency fields')
    stages = {s.stage_id for s in physical.stages}
    partitions = {p for s in physical.stages for p in s.partitions}
    for name in ('active', 'visited', 'completed'):
        values = residency[name + '_stages']
        if (type(values) is not list or any(type(x) is not str for x in values)
                or len(values) != len(set(values)) or not set(values) <= stages):
            reject('invalid durable residency stage membership')
        if partitioned:
            parts = residency[name + '_partitions']
            if (type(parts) is not list or any(type(x) is not str for x in parts)
                    or len(parts) != len(set(parts)) or not set(parts) <= partitions):
                reject('invalid durable residency partition membership')
            projected = {s.stage_id for s in physical.stages
                         if (set(s.partitions) <= set(parts) if name == 'completed'
                             else bool(set(s.partitions) & set(parts)))}
            if set(values) != projected:
                reject('partition residency disagrees with its stage projection')
    active = member_partitions(physical, residency, 'active')
    visited = member_partitions(physical, residency, 'visited')
    completed = member_partitions(physical, residency, 'completed')
    if (not active or completed & active or not completed <= visited
            or type(residency['phase_complete']) is not bool
            or type(residency['epoch']) is not int or not 0 <= residency['epoch'] < cut.sequence):
        reject('invalid parked generation/completion frontier')
    if partitioned and active not in [set(parts) for _, parts in partition_phases(physical)]:
        reject('active partition wave differs from sealed replica concurrency')
    if set(cut.participants) != set(participant_ids(physical, active)):
        reject('active parked cut participants differ')
    expected = set(participant_ids(physical, active | visited))
    if {c.participant_id for c in contributions} != expected or len(contributions) != len(expected):
        reject('parked recovery invented or omitted a visited worker')
    if cut.final and (not residency['phase_complete'] or completed | active != partitions):
        reject('final parked cut precedes all partition completion')
    origins = {}
    completed_proofs = set()
    for fact in facts:
        if fact['sequence'] > cut.sequence:
            continue
        ref = ((fact.get('prepared') or {}).get('state') or {}).get('dataflow-cut-v3')
        if ref is None:
            continue
        carrier = artifacts.read(ref)
        old_cut = carrier['cut']
        for value in carrier['contributions']:
            if value['cut_id'] == old_cut['cut_id'] and value['participant_id'] in old_cut['participants']:
                origins[(value['cut_id'], value['participant_id'])] = value
        old = carrier.get('residency')
        if fact['commit_id'] != current_commit and old and old.get('phase_complete'):
            completed_proofs.update(member_partitions(physical, old, 'active'))
    if not completed <= completed_proofs:
        reject('completed parked partitions lack their actual ACKed completion cut')
    for contribution in contributions:
        if origins.get((contribution.cut_id, contribution.participant_id)) != _contribution_json(contribution):
            reject('parked state differs from its original ACKed contribution')
    owners = {p: s for s in physical.stages for p in s.partitions}
    exchanges = {e.edge_id: e for e in physical.exchanges}
    entries = residency['deliveries']
    if type(entries) is not dict or len(entries) > physical.bounds.max_pending_events:
        reject('invalid parked delivery window')
    for key, entry in entries.items():
        if type(entry) is not dict or set(entry) != {'payload', 'consumed', 'producer_cut', 'consumer_cut'}:
            reject('invalid parked delivery fact')
        payload = entry['payload']
        edge = exchanges.get(payload.get('edge_id'))
        if (edge is None or edge.mode != 'durable_incremental' or payload.get('delivery_id') != key
                or payload.get('producer_partition') not in edge.producer_partitions
                or payload.get('target_partition') not in edge.consumer_partitions
                or payload.get('route_epoch') != edge.route_epoch
                or type(payload.get('route_sequence')) is not int or payload['route_sequence'] < 0
                or 'peer' in payload or type(entry['consumed']) is not bool):
            reject('parked delivery differs from sealed route')
        _artifact_from_json(payload['ref'])
        producer = payload['producer_partition']
        pid = compute_participant_id(owners[producer].stage_id, producer)
        origin = origins.get((entry['producer_cut'], pid))
        if origin is None:
            reject('parked delivery lacks original ACKed producer cut')
        image = frontier(_contribution_from_json(origin), producer, store, physical.bounds)
        if image.get('durable_deliveries', {}).get(key) != payload:
            reject('parked ref differs from actual producer frontier')
        if entry['consumed']:
            consumer = payload['target_partition']
            cid = compute_participant_id(owners[consumer].stage_id, consumer)
            proof = origins.get((entry['consumer_cut'], cid))
            if proof is None or key not in frontier(_contribution_from_json(proof), consumer, store,
                                                    physical.bounds).get('consumed_deliveries', ()):
                reject('parked consumption ACK lacks actual consumer frontier')
    valid_eoi = {(edge.edge_id, p) for edge in physical.exchanges for p in edge.producer_partitions}
    if any(type(x) is not list or len(x) != 2 or tuple(x) not in valid_eoi for x in residency['edge_eoi']):
        reject('parked EOI differs from sealed channel')
    latest = {c.participant_id: c for c in contributions}
    for edge_id, producer in residency['edge_eoi']:
        cid = compute_participant_id(owners[producer].stage_id, producer)
        if cid not in latest or edge_id not in frontier(latest[cid], producer, store, physical.bounds).get('emitted_eoi', ()):
            reject('parked EOI lacks actual producer frontier')
    for value in residency['satisfied_inputs']:
        if type(value) is not list or len(value) != 2:
            reject('invalid parked input satisfaction')
        edge_id, consumer = value
        if edge_id not in exchanges or consumer not in exchanges[edge_id].consumer_partitions:
            reject('parked input satisfaction differs from sealed edge')
        cid = compute_participant_id(owners[consumer].stage_id, consumer)
        if cid not in latest or edge_id not in frontier(latest[cid], consumer, store, physical.bounds).get('closed_inputs', ()):
            reject('parked input satisfaction lacks actual consumer frontier')
    if not set(residency['satisfied_source_splits']) <= {s.split_id for s in physical.splits}:
        reject('parked source satisfaction differs from declared splits')
    finite = {e.channel_id for e in physical.exchanges if e.mode == 'finite_sealed'}
    if not set(residency['finite_seals']) <= finite:
        reject('parked finite scope is undeclared')
    for value in residency['finite_seals'].values():
        _artifact_from_json(value)


def durable_metadata(physical, *, schema, epoch, active_stages, active_partitions,
                     visited, completed, phase_done, finite_seals, satisfied_splits,
                     deliveries, edge_eoi, satisfied_inputs, bounds):
    stages = physical.stages
    value = {'schema': schema, 'epoch': epoch,
            'active_stages': list(active_stages),
            'visited_stages': sorted(s.stage_id for s in stages if set(s.partitions) & visited),
            'completed_stages': sorted(s.stage_id for s in stages if set(s.partitions) <= completed),
            'phase_complete': phase_done,
            'finite_seals': {key: asdict(ref) for key, ref in finite_seals.items()},
            'satisfied_source_splits': list(satisfied_splits),
            'deliveries': deliveries,
            'edge_eoi': [list(x) for x in sorted(edge_eoi)],
            'satisfied_inputs': [list(x) for x in sorted(satisfied_inputs)]}
    if schema == PARTITION_SCHEMA:
        value.update(active_partitions=list(active_partitions),
            visited_partitions=sorted(visited), completed_partitions=sorted(completed))
    # producer/consumer cut proofs enlarge entries after begin; enforce the
    # same bound again before replacing the pending carrier.
    document(value, bounds.max_state_bytes)
    return value

def prove_durable_contributions(physical, cut, contributions, *, store, bounds,
                                transform_occurrences, prove_frontier):
    owned = {p: s for s in physical.stages for p in s.partitions}
    by_id = {c.participant_id: c for c in contributions.values()}
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    for partition, stage in owned.items():
        cid = compute_participant_id(stage.stage_id, partition)
        contribution = by_id.get(cid)
        if contribution is None:
            continue
        state = frontier(contribution, partition, store, bounds)
        prove_frontier(cut.cut_id, partition, state)
        # Nonrecoverable state cannot disappear merely because it has a cut.
        from demiflow.execution.contracts import state_codec
        from demiflow.execution.storage.streams import read_kernel_image
        for occurrence, ref in contribution.operator_states.items():
            if occurrence not in transform_occurrences:
                continue
            image = state_codec.loads(read_kernel_image(store, ref, bounds,
                occurrence=occurrence, partition=partition), max_bytes=bounds.max_state_bytes)
            for value in [image.get('state'), *(image.get('worker_states') or [])]:
                if value and value.get('mode') == 'nonrecoverable':
                    raise DataflowError('RESOURCE_INFEASIBLE', 'parked operator state has no recoverable adapter')


def finite_metadata(*, active_stages, completed_stages, phase_done, satisfied_splits, finite_seals):
    from demiflow.execution.contracts.checkpoint import artifact_json
    return {'schema': FINITE_SCHEMA,
        'active_stages': list(active_stages),
        'completed_stages': sorted(completed_stages),
        'phase_complete': phase_done,
        'satisfied_source_splits': list(satisfied_splits),
        'finite_seals': {key: artifact_json(ref) for key, ref in finite_seals.items()}}
