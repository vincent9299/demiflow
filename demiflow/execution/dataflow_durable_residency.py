"""Journal-backed transfer of a bounded durable window between resident groups.

Parking is not consumption ACK. The producer's credit remains in its actual
snapshot; only an actual consumer frontier can mark a delivery consumed.
"""
import json
from dataclasses import asdict

from .contracts.dataflow_errors import DataflowError
from .dataflow_residency import finite_phases, participants_for
from .recovery_contract import identity, document

SCHEMA = 'demiflow.durable_residency.v1'
PARTITION_SCHEMA = 'demiflow.durable_residency.v2'
SCHEMAS = (SCHEMA, PARTITION_SCHEMA)


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


def member_partitions(physical, value, name):
    if value.get('schema') == PARTITION_SCHEMA:
        return set(value[name + '_partitions'])
    return {p for stage in physical.stages if stage.stage_id in value[name + '_stages']
            for p in stage.partitions}


def participant_ids(physical, partitions):
    from .contracts.dataflow_codec import compute_participant_id
    return tuple(compute_participant_id(s.stage_id, p) for s in physical.stages
                 for p in s.partitions if p in partitions)



def frontier(contribution, partition, store, bounds):
    ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
    if ref is None:
        raise DataflowError('CORRUPT_ARTIFACT', 'parked worker lacks its actual frontier')
    return json.loads(b''.join(store.read(ref, max_bytes=bounds.max_state_bytes)))


class DurableResidency:
    def __init__(self, physical, graph, exchange, *, bounds, finite_seals, prior, image):
        self._bounds = bounds
        self._physical = physical
        self._sources = frozenset(graph.sources)
        self._transforms = frozenset(n.occurrence_id for n in graph.nodes if n.kind == 'transform')
        self._finite_seals = finite_seals
        self._exchange = exchange
        self.edge_eoi = set()
        self.satisfied_inputs = set()
        self.visited = set()
        self.completed = set()
        self.epoch = 0
        self.active_index = 0
        from .dataflow_residency import value_predecessors
        members = {s.stage_id: s.partitions for s in physical.stages}
        self._value_inputs = {s: {p for predecessor in predecessors for p in members[predecessor]}
                              for s, predecessors in value_predecessors(physical).items()}
        phases = partition_phases(physical)
        self.groups = tuple(group for group, _ in phases)
        self.partition_groups = tuple(parts for _, parts in phases)
        self.all_partitions = {p for s in physical.stages for p in s.partitions}
        self.schema = (PARTITION_SCHEMA if any(len(s.partitions) > s.replica_target
                       for s in physical.stages) else SCHEMA)
        if prior is not None:
            if prior['schema'] != self.schema:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'cannot reinterpret finite phase history as parked streaming')
            exchange.restore_parked(prior['deliveries'])
            self.edge_eoi = {tuple(x) for x in prior['edge_eoi']}
            self.satisfied_inputs = {tuple(x) for x in prior['satisfied_inputs']}
            active = member_partitions(physical, prior, 'active')
            self.visited = member_partitions(physical, prior, 'visited') | active
            self.completed = member_partitions(physical, prior, 'completed')
            if prior['phase_complete']:
                candidate = self.completed | active
                if candidate == self.all_partitions:
                    # A non-final cut may have observed completion while its
                    # participants were freezing. Reopen the last real owner to
                    # obtain final publication, without inventing a contribution.
                    self.epoch = image.cut.sequence
                    self.active_index = next(i for i, g in enumerate(self.partition_groups)
                                             if set(g) == active)
                    return
                self.completed = candidate
            self.epoch = image.cut.sequence
        self.active_index = self.choose()

    def choose(self):
        # A ready consumer runs promptly, before admitting another source window.
        for index in reversed(range(len(self.groups))):
            group = set(self.partition_groups[index])
            if any(self._value_inputs.get(s, set()) - self.completed for s in self.groups[index]):
                continue
            if group <= self.completed:
                continue
            inputs = [edge for edge in self._physical.exchanges
                      if set(edge.consumer_partitions) & group and not set(edge.producer_partitions) <= group]
            if any(edge.mode == 'finite_sealed' and edge.channel_id not in self._finite_seals for edge in inputs):
                continue
            pending = self._exchange.waiting_for(group)
            eof = bool(inputs) and all(edge.mode == 'finite_sealed' or
                    all((edge.edge_id, p) in self.edge_eoi for p in edge.producer_partitions) for edge in inputs)
            if pending or eof:
                return index
        for index, group in enumerate(self.groups):
            if set(self.partition_groups[index]) <= self.completed:
                continue
            if any(set(s.occurrences) & self._sources for s in self._physical.stages if s.stage_id in group):
                return index
        raise DataflowError('RESOURCE_INFEASIBLE', 'no runnable parked stage has input or an authenticated EOI')

    def launched(self, endpoints, by_partition):
        for endpoint in endpoints.values():
            partitions = set(endpoint._assignment.lease.partition_ids)
            self._exchange.bind_parked_consumer(endpoint, partitions)
            for edge in self._physical.exchanges:
                if (set(edge.producer_partitions) & partitions and all(
                        (edge.edge_id, p) in self.satisfied_inputs for p in edge.consumer_partitions)):
                    endpoint.send_business({'kind': 'close_output', 'edge_id': edge.edge_id})
            self._exchange.return_parked_credit(endpoint, partitions)
        yield from self._exchange.redeliver_parked(by_partition)
        for edge_id, producer in sorted(self.edge_eoi):
            spec = next(x for x in self._physical.exchanges if x.edge_id == edge_id)
            for partition in spec.consumer_partitions:
                target = by_partition.get(partition)
                if target is not None:
                    target.send_business({'kind': 'edge_eoi', 'edge_id': edge_id, 'producer_partition': producer})
        active_sources = any(set(s.occurrences) & self._sources
                             for s in self._physical.stages if s.stage_id in self.groups[self.active_index])
        if not active_sources and not self.can_drain_in_place():
            for endpoint in endpoints.values():
                endpoint.send_business({'kind': 'park'})

    def can_drain_in_place(self):
        """All other cohorts have finished and no parked channel requires handoff."""
        active = set(self.partition_groups[self.active_index])
        if self.all_partitions - active - self.completed:
            return False
        return not any(edge.mode == 'durable_incremental' and (
            active.intersection(edge.producer_partitions) or active.intersection(edge.consumer_partitions))
            for edge in self._physical.exchanges)

    def final(self, phase_done):
        return phase_done and self.all_partitions <= (
            self.completed | set(self.partition_groups[self.active_index]))

    def metadata(self, phase_done, satisfied_splits):
        stages = self._physical.stages
        value = {'schema': self.schema, 'epoch': self.epoch,
                'active_stages': list(self.groups[self.active_index]),
                'visited_stages': sorted(s.stage_id for s in stages if set(s.partitions) & self.visited),
                'completed_stages': sorted(s.stage_id for s in stages if set(s.partitions) <= self.completed),
                'phase_complete': phase_done,
                'finite_seals': {key: asdict(ref) for key, ref in self._finite_seals.items()},
                'satisfied_source_splits': list(satisfied_splits),
                'deliveries': self._exchange.parked_snapshot(),
                'edge_eoi': [list(x) for x in sorted(self.edge_eoi)],
                'satisfied_inputs': [list(x) for x in sorted(self.satisfied_inputs)]}
        if self.schema == PARTITION_SCHEMA:
            value.update(active_partitions=list(self.partition_groups[self.active_index]),
                visited_partitions=sorted(self.visited), completed_partitions=sorted(self.completed))
        # producer/consumer cut proofs enlarge entries after begin; enforce the
        # same bound again before replacing the pending carrier.
        document(value, self._bounds.max_state_bytes)
        return value

    def before_commit(self, cut, contributions, store):
        owned = {p: s for s in self._physical.stages for p in s.partitions}
        by_id = {c.participant_id: c for c in contributions.values()}
        from .contracts.dataflow_codec import compute_participant_id
        for partition, stage in owned.items():
            cid = compute_participant_id(stage.stage_id, partition)
            contribution = by_id.get(cid)
            if contribution is None:
                continue
            state = frontier(contribution, partition, store, self._bounds)
            for edge_id in state.get('closed_inputs', ()):
                self.satisfied_inputs.add((edge_id, partition))
            for edge_id, producer in self.edge_eoi:
                if producer == partition and edge_id not in state.get('emitted_eoi', ()):
                    raise DataflowError('CORRUPT_ARTIFACT', 'edge EOI lacks actual producer frontier')
            self._exchange.prove_parked(cut.cut_id, partition, state)
            # Nonrecoverable state cannot disappear merely because it has a cut.
            from demiflow.execution.contracts import state_codec
            from .dataflow_kernel_pack import read_kernel_image
            for occurrence, ref in contribution.operator_states.items():
                if occurrence not in self._transforms:
                    continue
                image = state_codec.loads(read_kernel_image(store, ref, self._bounds,
                    occurrence=occurrence, partition=partition), max_bytes=self._bounds.max_state_bytes)
                for value in [image.get('state'), *(image.get('worker_states') or [])]:
                    if value and value.get('mode') == 'nonrecoverable':
                        raise DataflowError('RESOURCE_INFEASIBLE', 'parked operator state has no recoverable adapter')

    def committed(self, image, phase_done):
        self.visited.update(self.partition_groups[self.active_index])
        if phase_done:
            self.completed.update(self.partition_groups[self.active_index])
        self._exchange.committed_parked()
        self.epoch = image.cut.sequence
        self.active_index = self.choose()


def validate_recovered(physical, cut, contributions, residency, *, facts, artifacts, current_commit, store):
    from .checkpoint.coordinator import _contribution_json, _contribution_from_json, _artifact_from_json
    from .contracts.dataflow_codec import compute_participant_id
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
