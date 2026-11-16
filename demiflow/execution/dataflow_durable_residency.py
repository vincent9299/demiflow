"""Journal-backed transfer of a bounded durable window between resident groups.

Parking is not consumption ACK. The producer's credit remains in its actual
snapshot; only an actual consumer frontier can mark a delivery consumed.
"""
import copy
import json

from .contracts.dataflow_errors import DataflowError
from .dataflow_residency import finite_phases, participants_for
from .recovery_contract import identity, document

SCHEMA = 'demiflow.durable_residency.v1'


def frontier(contribution, partition, store, bounds):
    ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
    if ref is None:
        raise DataflowError('CORRUPT_ARTIFACT', 'parked worker lacks its actual frontier')
    return json.loads(b''.join(store.read(ref, max_bytes=bounds.max_state_bytes)))


class DurableResidency:
    def __init__(self, execution):
        self.ex = execution
        self.entries = {}
        self.edge_eoi = set()
        self.satisfied_inputs = set()
        self.visited = set()
        self.completed = set()
        self.epoch = 0
        self.active_index = 0
        self.acked_live = set()
        self.park_sent = False
        self.groups = finite_phases(execution._physical, split_durable=True)
        prior = execution._checkpoint.residency
        if prior is not None:
            if prior['schema'] != SCHEMA:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'cannot reinterpret finite phase history as parked streaming')
            self.entries = copy.deepcopy(prior['deliveries'])
            self.edge_eoi = {tuple(x) for x in prior['edge_eoi']}
            self.satisfied_inputs = {tuple(x) for x in prior['satisfied_inputs']}
            for edge_id, consumer in self.satisfied_inputs:
                execution._satisfied_consumers.setdefault(edge_id, set()).add(consumer)
            self.visited = set(prior['visited_stages']) | set(prior['active_stages'])
            self.completed = set(prior['completed_stages'])
            if prior['phase_complete']:
                candidate = self.completed | set(prior['active_stages'])
                if candidate == {s.stage_id for s in execution._physical.stages}:
                    # A non-final cut may have observed completion while its
                    # participants were freezing. Reopen the last real owner to
                    # obtain final publication, without inventing a contribution.
                    self.epoch = execution._restore_image.cut.sequence
                    self.active_index = next(i for i, g in enumerate(self.groups)
                                             if set(g) == set(prior['active_stages']))
                    return
                self.completed = candidate
            self.epoch = execution._restore_image.cut.sequence
        self.active_index = self.choose()

    def choose(self):
        ex = self.ex
        owner = {p: s.stage_id for s in ex._physical.stages for p in s.partitions}
        # A ready consumer runs promptly, before admitting another source window.
        for index in reversed(range(len(self.groups))):
            group = set(self.groups[index])
            if group <= self.completed:
                continue
            inputs = [edge for edge in ex._physical.exchanges
                      if owner[edge.consumer_partitions[0]] in group and owner[edge.producer_partitions[0]] not in group]
            if any(edge.mode == 'finite_sealed' and edge.channel_id not in ex._finite_seals for edge in inputs):
                continue
            pending = any(not entry['consumed'] and owner[entry['payload']['target_partition']] in group
                          for entry in self.entries.values())
            eof = bool(inputs) and all(edge.mode == 'finite_sealed' or
                    all((edge.edge_id, p) in self.edge_eoi for p in edge.producer_partitions) for edge in inputs)
            if pending or eof:
                return index
        for index, group in enumerate(self.groups):
            if set(group) <= self.completed:
                continue
            if any(set(s.occurrences) & set(ex._graph.sources) for s in ex._physical.stages if s.stage_id in group):
                return index
        raise DataflowError('RESOURCE_INFEASIBLE', 'no runnable parked stage has input or an authenticated EOI')

    def record(self, payload):
        ex = self.ex
        key = payload['delivery_id']
        if key in self.entries:
            raise DataflowError('INVALID_CONTRACT', 'duplicate parked delivery identity')
        if 'ref' not in payload or 'peer' in payload:
            raise DataflowError('INVALID_CONTRACT', 'only durable records can transfer to an absent stage')
        if len(self.entries) >= ex._bounds.max_pending_events:
            raise DataflowError('BUDGET_EXCEEDED', 'parked delivery window exceeds event budget')
        self.entries[key] = {'payload': dict(payload), 'consumed': False,
                             'producer_cut': None, 'consumer_cut': None}
        document(self.entries, ex._bounds.max_state_bytes)

    def launched(self):
        ex = self.ex
        self.park_sent = False
        self.acked_live.clear()
        for endpoint in ex._endpoints.values():
            partitions = set(endpoint._assignment.lease.partition_ids)
            retained = [key for key, entry in self.entries.items()
                        if entry['payload']['target_partition'] in partitions]
            endpoint.send_business({'kind': 'park_policy', 'retain_consumed': retained})
            for edge in ex._physical.exchanges:
                if (set(edge.producer_partitions) & partitions and all(
                        (edge.edge_id, p) in self.satisfied_inputs for p in edge.consumer_partitions)):
                    endpoint.send_business({'kind': 'close_output', 'edge_id': edge.edge_id})
            # A previous ACKed consumer cut, not parking, authorizes credit return.
            for key, entry in self.entries.items():
                if entry['consumed'] and entry['payload']['producer_partition'] in partitions:
                    endpoint.send_business({'kind': 'ack', 'delivery_id': key})
                    self.acked_live.add(key)
            if any(key in self.acked_live and entry['payload']['producer_partition'] in partitions
                   for key, entry in self.entries.items()):
                endpoint.send_business({'kind': 'resume'})
        # JSON objects are canonicalized by key; delivery IDs do not define
        # business order. Preserve each original producer/channel sequence.
        ordered = sorted(self.entries.items(), key=lambda item: (
            item[1]['payload']['edge_id'], item[1]['payload']['producer_partition'],
            item[1]['payload']['route_epoch'], item[1]['payload']['route_sequence'],
            item[1]['payload']['target_partition']))
        for key, entry in ordered:
            payload = entry['payload']
            target = ex._by_partition.get(payload['target_partition'])
            if target is not None and not entry['consumed']:
                ex._deliveries[key] = (None, target)
                ex._idle.discard(target._allocation_id)
                target.send_business(dict(payload, kind='data'))
        for edge_id, producer in sorted(self.edge_eoi):
            spec = next(x for x in ex._physical.exchanges if x.edge_id == edge_id)
            for partition in spec.consumer_partitions:
                target = ex._by_partition.get(partition)
                if target is not None:
                    target.send_business({'kind': 'edge_eoi', 'edge_id': edge_id, 'producer_partition': producer})
        active_sources = any(set(s.occurrences) & set(ex._graph.sources)
                             for s in ex._physical.stages if s.stage_id in self.groups[self.active_index])
        if not active_sources:
            self.park()

    def park(self):
        if self.park_sent:
            return
        self.park_sent = True
        for endpoint in self.ex._endpoints.values():
            endpoint.send_business({'kind': 'park'})

    def actual_ack(self, key):
        entry = self.entries.get(key)
        if entry is None:
            return False
        # The commit will bind this ACK to the actual consumer snapshot.
        entry['consumed'] = True
        return True

    def can_cut(self):
        # Lost consumer ACKs can be reconciled from an actual consumer snapshot.
        return all(key in self.entries for key in self.ex._deliveries)

    def final(self, phase_done):
        return phase_done and set(s.stage_id for s in self.ex._physical.stages) <= (
            self.completed | set(self.groups[self.active_index]))

    def metadata(self, phase_done):
        from .dataflow_checkpoint import _artifact_json
        ex = self.ex
        value = {'schema': SCHEMA, 'epoch': self.epoch,
                'active_stages': list(self.groups[self.active_index]),
                'visited_stages': sorted(self.visited), 'completed_stages': sorted(self.completed),
                'phase_complete': phase_done,
                'finite_seals': {key: _artifact_json(ref) for key, ref in ex._finite_seals.items()},
                'satisfied_source_splits': sorted(ex._checkpoint._operator_complete),
                'deliveries': copy.deepcopy({key: value for key, value in self.entries.items() if key not in self.acked_live}),
                'edge_eoi': [list(x) for x in sorted(self.edge_eoi)],
                'satisfied_inputs': [list(x) for x in sorted(self.satisfied_inputs)]}
        # producer/consumer cut proofs enlarge entries after begin; enforce the
        # same bound again before replacing the pending carrier.
        document(value, ex._bounds.max_state_bytes)
        return value

    def before_commit(self, cut, contributions):
        ex = self.ex
        owned = {p: s for s in ex._physical.stages for p in s.partitions}
        by_id = {c.participant_id: c for c in contributions.values()}
        from .contracts.dataflow_codec import compute_participant_id
        for partition, stage in owned.items():
            cid = compute_participant_id(stage.stage_id, partition)
            contribution = by_id.get(cid)
            if contribution is None:
                continue
            state = frontier(contribution, partition, ex._store, ex._bounds)
            for edge_id in state.get('closed_inputs', ()):
                self.satisfied_inputs.add((edge_id, partition))
            for edge_id, producer in self.edge_eoi:
                if producer == partition and edge_id not in state.get('emitted_eoi', ()):
                    raise DataflowError('CORRUPT_ARTIFACT', 'edge EOI lacks actual producer frontier')
            published = state.get('durable_deliveries', {})
            consumed = set(state.get('consumed_deliveries', ()))
            for key, entry in self.entries.items():
                payload = entry['payload']
                if payload['producer_partition'] == partition and not entry['consumed']:
                    if published.get(key) != payload:
                        raise DataflowError('CORRUPT_ARTIFACT', 'park lacks original producer delivery snapshot')
                    if entry['producer_cut'] is None:
                        entry['producer_cut'] = cut.cut_id
                if payload['target_partition'] == partition and key in consumed:
                    entry['consumed'] = True
                    entry['consumer_cut'] = cut.cut_id
                    ex._deliveries.pop(key, None)
                elif payload['target_partition'] == partition and entry['consumed'] and entry['consumer_cut'] is None:
                    raise DataflowError('CORRUPT_ARTIFACT', 'consumer ACK has no matching input frontier')
            # Nonrecoverable state cannot disappear merely because it has a cut.
            from .kernels import state_codec
            from .dataflow_kernel_pack import read_kernel_image
            transforms = {n.occurrence_id for n in ex._graph.nodes if n.kind == 'transform'}
            for occurrence, ref in contribution.operator_states.items():
                if occurrence not in transforms:
                    continue
                image = state_codec.loads(read_kernel_image(ex._store, ref, ex._bounds,
                    occurrence=occurrence, partition=partition), max_bytes=ex._bounds.max_state_bytes)
                for value in [image.get('state'), *(image.get('worker_states') or [])]:
                    if value and value.get('mode') == 'nonrecoverable':
                        raise DataflowError('RESOURCE_INFEASIBLE', 'parked operator state has no recoverable adapter')
        ex._checkpoint._pending[cut.cut_id].residency = self.metadata(set(ex._endpoints) <= ex._done)

    def committed(self, image, phase_done):
        self.visited.update(self.groups[self.active_index])
        if phase_done:
            self.completed.update(self.groups[self.active_index])
        for key in self.acked_live:
            self.entries.pop(key, None)
        self.acked_live.clear()
        self.epoch = image.cut.sequence
        self.active_index = self.choose()


def validate_recovered(physical, cut, contributions, residency, *, facts, artifacts, current_commit, store):
    from .dataflow_checkpoint import _contribution_json, _contribution_from_json, _artifact_from_json
    from .contracts.dataflow_codec import compute_participant_id
    def reject(reason):
        raise DataflowError('CORRUPT_ARTIFACT', reason)
    keys = {'schema', 'epoch', 'active_stages', 'visited_stages', 'completed_stages',
            'phase_complete', 'finite_seals', 'satisfied_source_splits', 'deliveries', 'edge_eoi', 'satisfied_inputs'}
    if type(residency) is not dict or set(residency) != keys or residency['schema'] != SCHEMA:
        reject('invalid durable residency schema')
    stages = {s.stage_id for s in physical.stages}
    for name in ('active_stages', 'visited_stages', 'completed_stages'):
        values = residency[name]
        if (type(values) is not list or any(type(x) is not str for x in values)
                or len(values) != len(set(values)) or not set(values) <= stages):
            reject('invalid durable residency stage membership')
    active, visited = set(residency['active_stages']), set(residency['visited_stages'])
    completed = set(residency['completed_stages'])
    if (not active or completed & active or not completed <= visited
            or type(residency['phase_complete']) is not bool
            or type(residency['epoch']) is not int or not 0 <= residency['epoch'] < cut.sequence):
        reject('invalid parked generation/completion frontier')
    if set(cut.participants) != set(participants_for(physical, active)):
        reject('active parked cut participants differ')
    expected = set(participants_for(physical, active | visited))
    if {c.participant_id for c in contributions} != expected or len(contributions) != len(expected):
        reject('parked recovery invented or omitted a visited worker')
    if cut.final and (not residency['phase_complete'] or completed | active != stages):
        reject('final parked cut precedes all stage completion')
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
            completed_proofs.update(old['active_stages'])
    if not completed <= completed_proofs:
        reject('completed parked stages lack their actual ACKed completion cut')
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
