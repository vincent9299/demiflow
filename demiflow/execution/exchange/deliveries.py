"""Driver-process control plane for deliveries. Payloads stay at the peer or store."""
from __future__ import annotations

import copy
from dataclasses import asdict

from ..contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import document, identity


class DeliveryExchange:
    def __init__(self):
        self._inflight = {}
        self._edge_acks = {}
        self._acked = 0
        # Parked payload/proof survives worker retirement. Live endpoints never
        # enter a recovery carrier or substitute for a consumer's cut.
        self._parked = {}
        self._credited = set()
        self.edge_eoi = set()
        self.satisfied_inputs = set()  # Actual cut frontier proofs, not immediate events.
        self._satisfied_consumers = {}  # Immediate authenticated events, including internal edges.
        self._finite_completions = {}
        self.finite_seals = {}

    def _require_new(self, key):
        if key in self._inflight or key in self._parked:
            raise DataflowError("INVALID_CONTRACT", "duplicate in-flight delivery")

    def terminal(self, key, producer):
        self._require_new(key)
        self._inflight[key] = (producer, None, 'terminal')

    def route(self, payload, spec, producer, by_partition, *, parking, bounds):
        key = payload['delivery_id']
        self._require_new(key)
        if (spec is None or payload['producer_partition'] not in spec.producer_partitions
                or payload['target_partition'] not in spec.consumer_partitions):
            raise DataflowError("FENCED", "delivery is outside physical channel membership")
        target = by_partition.get(payload['target_partition'])
        if parking and spec.mode == 'durable_incremental':
            if 'ref' not in payload or 'peer' in payload:
                raise DataflowError('INVALID_CONTRACT', 'only durable records can transfer to an absent stage')
            if len(self._parked) >= bounds.max_pending_events:
                raise DataflowError('BUDGET_EXCEEDED', 'parked delivery window exceeds event budget')
            entry = {'payload': dict(payload), 'consumed': False, 'producer_cut': None, 'consumer_cut': None}
            self._parked[key] = entry
            try:
                document(self._parked, bounds.max_state_bytes)
            except Exception:
                del self._parked[key]
                raise
        elif target is None:
            raise DataflowError('INVALID_CONTRACT', 'streaming consumer is absent without a durable park owner')
        # An absent consumer is bound only when its partition is resident.
        # Do not keep a retired producer endpoint alive across a parked wave.
        self._inflight[key] = (producer if target is not None else None, target, spec.edge_id)
        if target is not None:
            target.send_business(dict(payload, kind='data'))
        return target

    def deliver_finite(self, spec, seal, by_partition):
        for partition in spec.consumer_partitions:
            target = by_partition.get(partition)
            key = identity([seal.digest, partition, 'scope-delivery'])
            if target is None or key in self._inflight:
                continue
            self._inflight[key] = (None, target, None)
            target.send_business({'kind': 'finite_input', 'edge_id': spec.edge_id,
                                  'seal_ref': asdict(seal), 'delivery_id': key})
            yield target

    def acknowledge(self, key, receiver):
        pair = self._inflight.get(key)
        if pair is None or receiver is not pair[1] or (receiver is None and key in self._parked):
            raise DataflowError("FENCED", "ack is not from the expected consumer")
        del self._inflight[key]
        producer, _, edge = pair
        self._acked += 1
        if edge is not None:
            self._edge_acks[edge] = self._edge_acks.get(edge, 0) + 1
        entry = self._parked.get(key)
        if entry is not None:
            entry['consumed'] = True
        if producer is not None:
            producer.send_business({'kind': 'ack', 'delivery_id': key})
            if entry is not None:
                self._credited.add(key)

    def can_cut(self):
        return self._inflight.keys() <= self._parked.keys()

    def snapshot_acks(self):
        return self._acked, self._edge_acks.copy()

    def published(self, sampled_count):
        self._acked -= sampled_count

    def edge_progress(self, baseline):
        return max((count - baseline.get(edge, 0) for edge, count in self._edge_acks.items()), default=0)

    def restore_parked(self, entries):
        self._parked = copy.deepcopy(entries)

    def parked_snapshot(self):
        return copy.deepcopy({k: entry for k, entry in self._parked.items() if k not in self._credited})

    def waiting_for(self, partitions):
        return any(not e['consumed'] and e['payload']['target_partition'] in partitions
                   for e in self._parked.values())

    def parked_consumed(self):
        return all(e['consumed'] for e in self._parked.values())

    def bind_parked_consumer(self, endpoint, partitions):
        retained = [key for key, entry in self._parked.items()
                    if entry['payload']['target_partition'] in partitions]
        endpoint.send_business({'kind': 'park_policy', 'retain_consumed': retained})

    def return_parked_credit(self, endpoint, partitions):
        returned = False
        for key, entry in self._parked.items():
            if entry['consumed'] and entry['payload']['producer_partition'] in partitions and key not in self._credited:
                endpoint.send_business({'kind': 'ack', 'delivery_id': key})
                self._credited.add(key)
                returned = True
        if returned:
            endpoint.send_business({'kind': 'resume'})

    def redeliver_parked(self, by_partition):
        # Canonical JSON orders object keys, whereas business order comes from
        # the original producer and route sequence.
        ordered = sorted(self._parked.items(), key=lambda pair: tuple(pair[1]['payload'][field]
            for field in ('edge_id', 'producer_partition', 'route_epoch', 'route_sequence', 'target_partition')))
        for key, entry in ordered:
            payload = entry['payload']
            target = by_partition.get(payload['target_partition'])
            if target is not None and not entry['consumed']:
                prior = self._inflight.get(key)
                # Per-edge progress counts only deliveries observed in this
                # driver generation. Cold replay begins with an empty frontier.
                self._inflight[key] = (None, target, prior[2] if prior is not None else None)
                target.send_business(dict(payload, kind='data'))
                yield target

    def prove_parked(self, cut_id, partition, frontier):
        published = frontier.get('durable_deliveries', {})
        consumed = set(frontier.get('consumed_deliveries', ()))
        for key, entry in self._parked.items():
            payload = entry['payload']
            if payload['producer_partition'] == partition and not entry['consumed']:
                if published.get(key) != payload:
                    raise DataflowError('CORRUPT_ARTIFACT', 'park lacks original producer delivery snapshot')
                if entry['producer_cut'] is None:
                    entry['producer_cut'] = cut_id
            if payload['target_partition'] != partition:
                continue
            if key in consumed:
                entry['consumed'] = True
                entry['consumer_cut'] = cut_id
                self._inflight.pop(key, None)
            elif entry['consumed'] and entry['consumer_cut'] is None:
                raise DataflowError('CORRUPT_ARTIFACT', 'consumer ACK has no matching input frontier')

    def committed_parked(self):
        for key in self._credited:
            del self._parked[key]
        self._credited.clear()

    def restore_finite(self, seals):
        from demiflow.execution.contracts.dataflow import ArtifactRef
        self.finite_seals = {key: ArtifactRef(**ref) for key, ref in seals.items()}

    def restore_parked_channels(self, prior):
        self.restore_parked(prior['deliveries'])
        self.edge_eoi = {tuple(x) for x in prior['edge_eoi']}
        self.satisfied_inputs = {tuple(x) for x in prior['satisfied_inputs']}
        for edge_id, consumer in self.satisfied_inputs:
            self.record_input_satisfied(edge_id, consumer)

    def record_input_satisfied(self, edge_id, consumer):
        self._satisfied_consumers.setdefault(edge_id, set()).add(consumer)

    def consumers_satisfied(self, edge_id, partitions):
        return set(partitions) <= self._satisfied_consumers.get(edge_id, set())

    def close_satisfied_outputs(self, spec, by_partition):
        if self.consumers_satisfied(spec.edge_id, spec.consumer_partitions):
            for producer in spec.producer_partitions:
                target = by_partition.get(producer)
                if target is not None:
                    yield target, {'kind': 'close_output', 'edge_id': spec.edge_id}

    def record_finite_completion(self, spec, completion, *, store):
        from demiflow.execution.exchange.dataflow_channels import seal_scope
        members = self._finite_completions.setdefault(spec.channel_id, {})
        producer = completion.partition_id
        if producer in members and members[producer] != completion:
            raise DataflowError('INVALID_CONTRACT', 'conflicting finite producer completion')
        members[producer] = completion
        if set(members) == set(spec.producer_partitions):
            seal = seal_scope(spec.scope_id, tuple(members.values()), store=store)
            self.finite_seals[spec.channel_id] = seal
            return seal
        return None

    def relay_edge_eoi(self, payload, spec, by_partition, *, parking):
        if parking:
            self.edge_eoi.add((spec.edge_id, payload['producer_partition']))
        for target_id in spec.consumer_partitions:
            target = by_partition.get(target_id)
            if target is not None:
                yield target, dict(payload, kind='edge_eoi')

    def prove_frontier(self, cut_id, partition, state):
        for edge_id in state.get('closed_inputs', ()):
            self.satisfied_inputs.add((edge_id, partition))
        for edge_id, producer in self.edge_eoi:
            if producer == partition and edge_id not in state.get('emitted_eoi', ()):
                raise DataflowError('CORRUPT_ARTIFACT', 'edge EOI lacks actual producer frontier')
        self.prove_parked(cut_id, partition, state)

    def launch_parked(self, assignments, by_partition, exchanges):
        for endpoint, partitions in assignments:
            self.bind_parked_consumer(endpoint, partitions)
            for edge in exchanges:
                if (set(edge.producer_partitions) & partitions and all(
                        (edge.edge_id, p) in self.satisfied_inputs for p in edge.consumer_partitions)):
                    endpoint.send_business({'kind': 'close_output', 'edge_id': edge.edge_id})
            self.return_parked_credit(endpoint, partitions)
        yield from self.redeliver_parked(by_partition)
        for edge_id, producer in sorted(self.edge_eoi):
            spec = next(x for x in exchanges if x.edge_id == edge_id)
            for partition in spec.consumer_partitions:
                target = by_partition.get(partition)
                if target is not None:
                    target.send_business({'kind': 'edge_eoi', 'edge_id': edge_id, 'producer_partition': producer})
