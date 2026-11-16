"""Bounded callback observations carried by the original worker/cut protocol.

These counters describe observed work, not effect billing or commit authority.
Latency is measured around the callback, excluding downstream delivery waits.
"""
from threading import Lock

from .contracts.dataflow_errors import DataflowError


_LIFECYCLE_FIELDS = ('created', 'started', 'stopped', 'closed',
                     'start_failed', 'stop_failed', 'close_failed')


def validated_lifecycle(value):
    if (type(value) is not dict or set(value) != set(_LIFECYCLE_FIELDS)
            or any(type(v) is not int or v < 0 for v in value.values())
            or any(value[ok] + value[bad] > value['created'] for ok, bad in
                   (('started', 'start_failed'), ('stopped', 'stop_failed'), ('closed', 'close_failed')))):
        raise DataflowError('INVALID_CONTRACT', 'invalid actor lifecycle observation')
    return dict(value)


def empty():
    return {'count': 0, 'total_ns': 0, 'min_ns': 0, 'max_ns': 0, 'miss': {}}


def validated(value):
    fields = {'count', 'total_ns', 'min_ns', 'max_ns', 'miss'}
    if (type(value) is not dict or set(value) != fields
            or any(type(value[k]) is not int or value[k] < 0 for k in fields - {'miss'})
            or type(value['miss']) is not dict or len(value['miss']) > 33
            or any(type(k) is not str or not k or len(k) > 128
                   or type(v) is not int or v < 0 for k, v in value['miss'].items())
            or sum(value['miss'].values()) > value['count']
            or value['min_ns'] > value['max_ns']
            or value['max_ns'] > value['total_ns']
            or value['count'] == 0 and any(value[k] for k in ('total_ns', 'min_ns', 'max_ns'))):
        raise DataflowError('INVALID_CONTRACT', 'invalid callback observation')
    return {**value, 'miss': dict(value['miss'])}


class CallbackObservations:
    def __init__(self):
        self._lock = Lock()
        self._value = empty()
        self._lifecycle = dict.fromkeys(_LIFECYCLE_FIELDS, 0)
        self._handoff = {'capacity': 1, 'current': 0, 'peak': 0,
                         'encoded_bytes': 0, 'peak_encoded_bytes': 0}

    def lifecycle(self, field, completed):
        failure = {'started': 'start_failed', 'stopped': 'stop_failed', 'closed': 'close_failed'}
        with self._lock:
            self._lifecycle[field if completed else failure[field]] += 1

    def lifecycle_snapshot(self):
        with self._lock:
            return dict(self._lifecycle)

    def hold_output(self, encoded_bytes):
        with self._lock:
            value = self._handoff
            if value['current']:
                raise DataflowError('INTERNAL_PROTOCOL', 'a kernel output handoff is already occupied')
            value.update(current=1, peak=1, encoded_bytes=encoded_bytes,
                         peak_encoded_bytes=max(value['peak_encoded_bytes'], encoded_bytes))

    def release_output(self):
        with self._lock:
            self._handoff.update(current=0, encoded_bytes=0)

    def output_handoff(self):
        with self._lock:
            return dict(self._handoff)

    def observe(self, duration_ns, miss=None):
        with self._lock:
            value = self._value
            value['min_ns'] = min(value['min_ns'], duration_ns) if value['count'] else duration_ns
            value['max_ns'] = max(value['max_ns'], duration_ns)
            value['total_ns'] += duration_ns
            value['count'] += 1
            if miss is not None:
                if len(miss) > 128 or miss not in value['miss'] and len(value['miss']) >= 32:
                    miss = 'other_exception'
                value['miss'][miss] = value['miss'].get(miss, 0) + 1

    def snapshot(self):
        with self._lock:
            return {**self._value, 'miss': dict(self._value['miss'])}

    def restore(self, value):
        with self._lock:
            self._value = validated(value)


def observe_operator(execution, event, payload):
    lifecycle = payload.get('actor_lifecycle')
    if lifecycle is not None:
        lifecycle = validated_lifecycle(lifecycle)
        slots = getattr(execution, '_operator_lifecycles', None)
        if slots is None:
            execution._operator_lifecycles = slots = {}
        key = (payload['occurrence_id'], payload['partition_id'])
        previous = slots.get(key)
        if previous is not None and previous[0] == event.boot_id and any(
                lifecycle[k] < previous[1][k] for k in _LIFECYCLE_FIELDS):
            raise DataflowError('FENCED', 'actor lifecycle moved backwards within one boot')
        slots[key] = (event.boot_id, lifecycle)
        checked_key = execution._report_keys.get(payload['occurrence_id'])
        if checked_key is not None:
            total = dict.fromkeys(_LIFECYCLE_FIELDS, 0)
            for (occurrence, _), (_, observed) in slots.items():
                if occurrence == payload['occurrence_id']:
                    for name in total:
                        total[name] += observed[name]
            execution._checked.worker_actor_lifecycle(checked_key, total)
    handoff = payload.get('output_handoff')
    if handoff is not None:
        fields = {'capacity', 'current', 'peak', 'encoded_bytes', 'peak_encoded_bytes'}
        if (type(handoff) is not dict or set(handoff) != fields
                or any(type(v) is not int or v < 0 for v in handoff.values())
                or not 0 <= handoff['current'] <= handoff['peak'] <= handoff['capacity'] == 1
                or handoff['encoded_bytes'] > handoff['peak_encoded_bytes']
                or not handoff['current'] and handoff['encoded_bytes']):
            raise DataflowError('INVALID_CONTRACT', 'invalid kernel output handoff observation')
        slots = getattr(execution, '_operator_handoffs', None)
        if slots is None:
            execution._operator_handoffs = slots = {}
        key = (payload['occurrence_id'], payload['partition_id'])
        previous = slots.get(key)
        if previous is not None and previous[0] == event.boot_id and any(
                handoff[k] < previous[1][k] for k in ('peak', 'peak_encoded_bytes')):
            raise DataflowError('FENCED', 'kernel output handoff peak moved backwards')
        slots[key] = (event.boot_id, dict(handoff))
    value = payload.get('callback_observations')
    if value is None:
        return
    value = validated(value)
    slots = getattr(execution, '_operator_observations', None)
    if slots is None:
        execution._operator_observations = slots = {}
    key = (payload['occurrence_id'], payload['partition_id'])
    previous = slots.get(key)
    if previous is not None and previous[0] == event.boot_id:
        old = previous[1]
        if any(value[k] < old[k] for k in ('count', 'total_ns', 'max_ns')) or any(
                value['miss'].get(k, 0) < v for k, v in old['miss'].items()):
            raise DataflowError('FENCED', 'callback observations moved backwards within one boot')
    # A new boot restores the actual committed prefix, not uncommitted work
    # observed before the prior process died. Never add that prefix twice.
    slots[key] = (event.boot_id, value)


def restore_operator_observations(execution, image):
    from .contracts.dataflow_codec import compute_participant_id
    from .kernels import state_codec
    participants = {compute_participant_id(s.stage_id, p): p
                    for s in execution._physical.stages for p in s.partitions}
    nodes = {n.occurrence_id for n in execution._graph.nodes}
    slots = {}
    for contribution in image.contributions:
        partition = participants[contribution.participant_id]
        for occurrence, ref in contribution.operator_states.items():
            if occurrence not in nodes:
                continue
            state = state_codec.loads(b''.join(execution._store.read(ref,
                max_bytes=execution._bounds.max_state_bytes)), max_bytes=execution._bounds.max_state_bytes)
            value = state.get('callback_observations')
            if value is not None:
                slots[(occurrence, partition)] = (contribution.boot_id, validated(value))
    execution._operator_observations = slots


def stream_observations(execution, stats):
    from .recovery_contract import document
    nodes = {node.occurrence_id: node for node in execution._graph.nodes}
    labels = getattr(execution, '_observation_labels', None)
    if labels is None:
        facts = execution._action.context.journal.source_binding(execution._action.action_id + '-dataflow-sealed')
        labels = facts.get('observation_labels', {}) if facts is not None else {}
        if (type(labels) is not dict or not set(labels) <= set(nodes)
                or any(type(v) is not str or not v for v in labels.values())
                or len(set(labels.values())) != len(labels)):
            raise DataflowError('CORRUPT_ARTIFACT', 'sealed observation labels differ from graph')
        document(labels, execution._bounds.max_control_bytes)
        execution._observation_labels = labels
    policies, totals, misses = {}, {}, {}
    for occurrence, node in nodes.items():
        name = labels.get(occurrence, occurrence)
        cfg = node.kernel.configuration
        if cfg.get('streaming_map'):
            policies[name] = {k: cfg[k] for k in ('concurrency', 'execution', 'callable_scope')}
            policies[name]['queue_depth'] = cfg.get('queue_depth') or cfg['concurrency']
            policies[name]['catch'] = list(cfg.get('catch', ()))
    for (occurrence, _), (_, value) in getattr(execution, '_operator_observations', {}).items():
        name = labels.get(occurrence, occurrence)
        total = totals.setdefault(name, empty())
        total['min_ns'] = min(total['min_ns'], value['min_ns']) if total['count'] else value['min_ns']
        total['max_ns'] = max(total['max_ns'], value['max_ns'])
        for k in ('count', 'total_ns'):
            total[k] += value[k]
        for reason, count in value['miss'].items():
            key = name + ':' + reason
            misses[key] = misses.get(key, 0) + count
    stats.miss = misses
    stats.stage_policies = policies
    stats.metrics['stage_policies'] = policies
    # The compatibility collection names the ACTUAL mechanism. A synchronous
    # generator handoff is one held output, not a retired asyncio input queue.
    # Peaks are measured in this attempt and do not claim full process memory
    # or concurrent invocation/transport buffer coverage.
    stats.metrics['queues'] = [{'stage': labels.get(occurrence, occurrence),
        'occurrence_id': occurrence, 'partition_id': partition,
        'kind': 'synchronous_output_handoff', 'scope': 'attempt', **value}
        for (occurrence, partition), (_, value)
        in getattr(execution, '_operator_handoffs', {}).items()]
    stats.metrics['stage_processing_latency'] = {name: {
        'count': v['count'], 'total_s': v['total_ns'] / 1e9,
        'min_s': v['min_ns'] / 1e9, 'max_s': v['max_ns'] / 1e9,
        'mean_s': v['total_ns'] / 1e9 / v['count'] if v['count'] else 0,
    } for name, v in totals.items()}

    from .dataflow_model_observations import stream_model_observations
    stream_model_observations(execution, stats)
