"""Bounded callback observations carried by the original worker/cut protocol.

These counters describe observed work, not effect billing or commit authority.
Latency is measured around the callback, excluding downstream delivery waits.
"""

import time
from demiflow.data.runtime_contract import RuntimeContractError
from threading import Lock

from demiflow.execution.contracts.dataflow_errors import DataflowError


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
    if (type(value) is not dict or set(value) - {'dead_batches', 'dead_batches_dropped'} != fields
            or any(type(value[k]) is not int or value[k] < 0 for k in fields - {'miss'})
            or type(value['miss']) is not dict or len(value['miss']) > 33
            or any(type(k) is not str or not k or len(k) > 128
                   or type(v) is not int or v < 0 for k, v in value['miss'].items())
            or sum(value['miss'].values()) > value['count']
            or value['min_ns'] > value['max_ns']
            or value['max_ns'] > value['total_ns']
            or value['count'] == 0 and any(value[k] for k in ('total_ns', 'min_ns', 'max_ns'))):
        raise DataflowError('INVALID_CONTRACT', 'invalid callback observation')
    dead = value.get('dead_batches', [])
    dropped = value.get('dead_batches_dropped', 0)
    if (type(dead) is not list or len(dead) > 32 or type(dropped) is not int or dropped < 0
            or any(type(item) is not dict or set(item) != {'reason', 'row_count', 'payload'}
                or type(item['reason']) is not str or not 1 <= len(item['reason']) <= 128
                or type(item['row_count']) is not int or item['row_count'] < 1
                or item['payload'] is not None and (type(item['payload']) is not str
                    or len(item['payload']) > 32768) for item in dead)
            or sum(len(item['payload'] or '') + 256 for item in dead) > 32768):
        raise DataflowError('INVALID_CONTRACT', 'invalid bounded dead batch observations')
    result = {**value, 'miss': dict(value['miss'])}
    if 'dead_batches' in value:
        result['dead_batches'] = [dict(item) for item in dead]
    return result


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
            return validated(self._value)

    def dead_batch(self, rows, reason, *, max_bytes):
        """Small diagnostics only; failed business payload is not a retry ledger."""
        from demiflow.execution.contracts.record_codec import _encode_payload
        payload = None
        if max_bytes >= 512:
            try:
                payload = _encode_payload({'rows': rows}, max_bytes=max_bytes // 2).hex()
            except DataflowError as error:
                if error.code != 'BUDGET_EXCEEDED':
                    raise
        with self._lock:
            dead = self._value.setdefault('dead_batches', [])
            retained = sum(len(item['payload'] or '') + 256 for item in dead)
            if len(dead) < 32 and retained + len(payload or '') + 256 <= min(32768, max_bytes):
                dead.append({'reason': reason[:128], 'row_count': len(rows), 'payload': payload})
            else:
                self._value['dead_batches_dropped'] = self._value.get('dead_batches_dropped', 0) + 1

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


def validated_node_counters(metrics):
    if (type(metrics) is not dict or set(metrics) != {'rows_input', 'rows_output'}
            or any(type(v) is not int or v < 0 for v in metrics.values())):
        raise DataflowError('INVALID_CONTRACT', 'node counters must be nonnegative integers')
    return dict(metrics)


def restore_node_metrics(execution, image):
    """Recover only the cumulative counters in the original ACKed worker image.

    A colocated worker contributes the same frontier for each physical stage.
    Authenticate it once and check its full occurrence/partition ownership;
    neither callback observations nor a source offset can stand in for these
    counters. Edge delivery ACK counters are attempt-local and remain zero.
    """
    import json
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    from demiflow.execution.recovery_contract import identity
    participants = {compute_participant_id(stage.stage_id, partition): (stage, partition)
                    for stage in execution._physical.stages for partition in stage.partitions}
    frontiers = {}
    for contribution in image.contributions:
        if contribution.participant_id not in participants:
            raise DataflowError('FENCED', 'restored metric participant differs from physical plan')
        stage, partition = participants[contribution.participant_id]
        ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
        if ref is None:
            continue  # Legacy worker has no restored cumulative counters.
        frontiers.setdefault((ref, partition), set()).update(stage.occurrences)
    restored = {}
    for (ref, partition), assigned in frontiers.items():
        frontier = json.loads(b''.join(execution._store.read(ref,
            max_bytes=execution._bounds.max_state_bytes)))
        if type(frontier) is not dict or frontier.get('schema') != 'demiflow.worker-frontier.v3':
            raise DataflowError('INCOMPATIBLE_HISTORY', 'unknown worker completion frontier')
        metrics = frontier.get('metrics', {})
        if type(metrics) is not dict or metrics and set(metrics) != assigned:
            raise DataflowError('FENCED', 'restored metric ownership differs from assigned stages')
        for occurrence in assigned:
            value = validated_node_counters(metrics.get(occurrence, {'rows_input': 0, 'rows_output': 0}))
            key = (occurrence, partition)
            if key in restored and restored[key] != value:
                raise DataflowError('FENCED', 'conflicting restored node counters')
            restored[key] = value
        del frontier
    execution._node_metrics = restored


def restore_operator_observations(execution, image):
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    from demiflow.execution.contracts import state_codec
    from demiflow.execution.storage.streams import read_kernel_image
    participants = {compute_participant_id(s.stage_id, p): p
                    for s in execution._physical.stages for p in s.partitions}
    nodes = {n.occurrence_id for n in execution._graph.nodes}
    slots = {}
    for contribution in image.contributions:
        partition = participants[contribution.participant_id]
        for occurrence, ref in contribution.operator_states.items():
            if occurrence not in nodes:
                continue
            state = state_codec.loads(read_kernel_image(execution._store, ref, execution._bounds,
                occurrence=occurrence, partition=partition), max_bytes=execution._bounds.max_state_bytes)
            value = state.get('callback_observations')
            if value is not None:
                slots[(occurrence, partition)] = (contribution.boot_id, validated(value))
    execution._operator_observations = slots


class _ObservedTiming:
    def __init__(self, value):
        self.value = dict(value)

    def summary(self):
        # Older cut observations contain aggregates, not a histogram. The
        # observed maximum is an honest conservative quantile upper bound;
        # never fabricate samples or a finer histogram from an average.
        v = self.value
        upper = v['max_ns'] / 1e9 if v['count'] else None
        return {'count': v['count'], 'total_s': v['total_ns'] / 1e9,
                'p50_upper_s': upper, 'p95_upper_s': upper,
                'method': 'observed_maximum_upper_bound'}


def stream_observations(execution, stats):
    from demiflow.execution.recovery_contract import document
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
    # Order and names come from the SAME sealed graph, including repeated
    # occurrences. Source/terminal control nodes are not business stages.
    stages = {}
    for occurrence in nodes:
        if occurrence in labels:
            counters = [v for (o, _), v in execution._node_metrics.items() if o == occurrence]
            stages[labels[occurrence]] = {
                'in': sum(v['rows_input'] for v in counters),
                'emitted': sum(v['rows_output'] for v in counters)}
    if not stages:
        terminals = set(execution._graph.terminals)
        stages['output'] = {'in': 0, 'emitted': sum(
            v['rows_input'] for (o, _), v in execution._node_metrics.items() if o in terminals)}
    stats.stages = stages
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
    stats.timings = {name: _ObservedTiming(value) for name, value in totals.items()}
    stats.dead_batches = []
    stats.dead_batches_dropped = 0
    remaining = min(32768, execution._bounds.max_control_bytes // 2)
    from demiflow.execution.contracts.record_codec import _decode_payload
    for (occurrence, _), (_, value) in getattr(execution, '_operator_observations', {}).items():
        stats.dead_batches_dropped += value.get('dead_batches_dropped', 0)
        for item in value.get('dead_batches', ()):
            if len(stats.dead_batches) >= 32:
                stats.dead_batches_dropped += 1
                continue
            diagnostic = {'stage': labels.get(occurrence, occurrence), 'reason': item['reason'],
                          'row_count': item['row_count'], 'payload': 'not_retained'}
            payload = item['payload']
            if payload is not None and len(payload) <= remaining:
                try:
                    rows = _decode_payload(bytes.fromhex(payload), max_bytes=len(payload))['rows']
                except (ValueError, KeyError) as error:
                    raise DataflowError('INVALID_CONTRACT', 'invalid dead batch payload') from error
                if type(rows) is not list or len(rows) != item['row_count']:
                    raise DataflowError('INVALID_CONTRACT', 'dead batch row count differs')
                diagnostic.update(rows=rows, payload='retained')
                remaining -= len(payload)
            stats.dead_batches.append(diagnostic)
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

    from demiflow.execution.journal.dataflow_model_observations import stream_model_observations
    stream_model_observations(execution, stats)


def match_observation(self, identity, *, reused=False):
    if self.observe is None:
        return None
    occurrence = self.occurrences.get(identity, 0)
    self.occurrences[identity] = occurrence + 1
    key = (self.matching_action, identity, occurrence)
    self.observations[key] = {'sequence': 1, 'state': 'reused' if reused else 'running',
                              'rows_input': None, 'rows_output': None, 'calls': None,
                              'timing_scope': 'unknown'}
    return key

def bind_observation_scope(self, key, *, plan_id, dag_id, plan_revision, occurrence_id):
    "Bind a native producer's observation to its sealed execution instance.\n\n        The scope is written with the existing observation, never reconstructed\n        by a history reader from whichever plan happens to be selected later.\n        "
    if key is None or self.observe is None:
        return
    from demiflow.execution.run_journal import _node_document
    scope = dict(plan_id=plan_id, dag_id=dag_id, plan_revision=plan_revision,
                 occurrence_id=occurrence_id)
    with self.observation_lock:
        item = self.observations[tuple(key)]
        if any(name in item and item[name] != value for name, value in scope.items()):
            raise RuntimeContractError('Native observation execution scope changed')
        candidate = {**item, **scope, 'sequence': item['sequence'] + 1}
        _node_document(candidate)
        item.update(candidate)

def progress(self, key, **counts):
    if key is None or self.observe is None:
        return
    with self.observation_lock:
        item = self.observations[tuple(key)]
        for name, value in counts.items():
            if name not in {'rows_input', 'rows_output', 'calls', 'dropped', 'errors'} or type(value) is not int or value < 0:
                raise RuntimeContractError('Invalid native observation counter')
            item[name] = (item.get(name) or 0) + value
        item['sequence'] += 1

def rows(self, rows, key, counter):
    """Observe a native iterator boundary without retaining its payload."""
    if key is None:
        yield from rows
        return
    self.progress(key, **{counter: 0})
    iterator = iter(rows)
    try:
        for row in iterator:
            self.progress(key, **{counter: 1})
            yield row
    finally:
        close = getattr(iterator, 'close', None)
        if close is not None:
            close()

def duration(self, key, elapsed):
    if key is not None and self.observe is not None:
        with self.observation_lock:
            item = self.observations[tuple(key)]
            item['elapsed_s'] = item.get('elapsed_s', 0.0) + elapsed
            item['timing_scope'] = 'node'
            item['sequence'] += 1

def group_duration(self, keys, elapsed):
    if self.observe is None or not keys or len(keys) > 128:
        return
    members = [key[1] + ':' + str(key[2]) for key in keys]
    with self.observation_lock:
        for key in keys:
            item = self.observations[tuple(key)]
            item['elapsed_s'] = item.get('elapsed_s', 0.0) + elapsed
            item['timing_scope'] = 'group'
            item['group_members'] = members
            item['detail'] = 'Cumulative worker time for the fused group, including read/decode/spill; not individual node wall time.'
            item['sequence'] += 1

def actor_created(self, owner, key):
    if key is None or self.observe is None:
        return
    with self.observation_lock:
        binding = (tuple(key), id(owner))
        if binding not in self.actor_states:
            if len(self.actor_states) >= 4096:
                raise RuntimeContractError('Actor observation count exceeds budget')
            self.actor_states[binding] = dict(created=True, started=None, stopped=None, closed=None)
            self.node_actors.setdefault(tuple(key), {})[id(owner)] = self.actor_states[binding]
            self.actor_bindings.setdefault(id(owner), []).append(tuple(key))
        self.actor_lifecycle(owner, 'created', True)

def actor_lifecycle(self, owner, name, completed):
    if self.observe is None:
        return
    with self.observation_lock:
        for key in self.actor_bindings.get(id(owner), ()):
            self.actor_states[(key, id(owner))][name] = completed
            states = self.node_actors[key].values()
            self.observations[key]['lifecycle'] = {
                field: False if any(s[field] is False for s in states) else
                       True if all(s[field] is True for s in states) else None
                for field in ('created', 'started', 'stopped', 'closed')}
            self.observations[key]['sequence'] += 1

def worker_actor_lifecycle(self, key, counters):
    """Project actual worker hook outcomes, never driver declaration objects."""
    if self.observe is None:
        return
    from demiflow.execution.journal.dataflow_operator_observations import validated_lifecycle
    counts = validated_lifecycle(counters)
    if not counts['created']:
        return
    with self.observation_lock:
        item = self.observations[tuple(key)]
        item['lifecycle'] = {'created': True}
        for field, failure in (('started', 'start_failed'), ('stopped', 'stop_failed'), ('closed', 'close_failed')):
            item['lifecycle'][field] = (False if counts[failure] else
                True if counts[field] == counts['created'] else None)
        item['sequence'] += 1

def flush_observations(self, *, final=False, succeeded=False, force=False):
    if self.observe is None:
        return
    now = time.monotonic()
    if not final and not force and now - self.last_flush < 0.5:
        return
    updates = []
    with self.observation_lock:
        for key, item in self.observations.items():
            if key[0] != self.matching_action:
                continue
            previous_time, previous_sequence = self.node_flushed.get(key, (0, -1))
            if not final and not force and (now - previous_time < 1 or previous_sequence == item['sequence']):
                continue
            if final and item['state'] != 'reused':
                item['state'] = ('succeeded' if succeeded and (item['rows_output'] is not None or key[1] in self.values) else 'failed' if item.get('errors') or False in item.get('lifecycle', {}).values() else 'incomplete')
                item['sequence'] += 1
            updates.append({'action_id': key[0], 'node_id': key[1], 'occurrence': key[2], 'document': dict(item)})
        if updates:
            try:
                self.observe(updates)
            except Exception as exc:
                raise RuntimeContractError('Native node observations could not be persisted') from exc
            for update in updates:
                key = (update['action_id'], update['node_id'], update['occurrence'])
                self.node_flushed[key] = (now, update['document']['sequence'])
            self.last_flush = now
