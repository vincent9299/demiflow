"""Actual embedding actor observations on the existing bounded progress channel.

Per-instance histograms stay separate; they are not billing or recovery state.
No worker-supplied path is used to audit historical journals.
"""
from __future__ import annotations

import json
import math
from collections.abc import Mapping

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document

_COUNTS = {'call_records', 'reused', 'input_tokens', 'output_tokens', 'records_without_usage', 'errors'}
_TIMINGS = {'first_byte_s', 'first_event_s', 'first_content_s', 'max_body_gap_s'}
_HIST = {'count', 'total_s', 'bucket_s', 'overflow_count', 'p50_upper_s', 'p95_upper_s'}


def _number(value):
    return (type(value) is int and value >= 0
            or type(value) is float and math.isfinite(value) and value >= 0)


def _histogram(value):
    return (isinstance(value, Mapping) and set(value) == _HIST
        and type(value['count']) is int and value['count'] >= 0
        and type(value['overflow_count']) is int and 0 <= value['overflow_count'] <= value['count']
        and _number(value['total_s']) and type(value['bucket_s']) is float and value['bucket_s'] == .25
        and all(value[k] is None or _number(value[k]) for k in ('p50_upper_s', 'p95_upper_s')))


def validate_summaries(value, *, maximum, max_bytes):
    if type(value) is not list or len(value) > maximum:
        raise DataflowError('INVALID_CONTRACT', 'embedding observation instance count exceeds assignment')
    for item in value:
        fields = _COUNTS | {'latency', 'stream_timings', 'profiled_batches', 'phases', 'shared_admission'}
        if (type(item) is not dict or set(item) != fields
                or any(type(item[k]) is not int or item[k] < 0 for k in _COUNTS | {'profiled_batches'})
                or item['reused'] > item['call_records'] or item['records_without_usage'] > item['call_records']
                or not _histogram(item['latency'])
                or type(item['stream_timings']) is not dict or set(item['stream_timings']) != _TIMINGS
                or not all(_histogram(h) for h in item['stream_timings'].values())
                or type(item['phases']) is not dict or len(item['phases']) > 32):
            raise DataflowError('INVALID_CONTRACT', 'invalid embedding actor observation')
        for key, phase in item['phases'].items():
            if (type(key) is not str or not 1 <= len(key) <= 64
                    or type(phase) is not dict or set(phase) != {'count', 'total', 'max'}
                    or type(phase['count']) is not int or not 0 <= phase['count'] <= item['profiled_batches']
                    or not _number(phase['total']) or not _number(phase['max']) or phase['max'] > phase['total']):
                raise DataflowError('INVALID_CONTRACT', 'invalid embedding phase observation')
        gate = item['shared_admission']
        if (type(gate) is not dict or set(gate) not in ({'requests', 'peak', 'wait_s'},
                {'requests', 'peak', 'wait_s', 'adaptive'})
                or any(type(gate[k]) is not int or gate[k] < 0 for k in ('requests', 'peak'))
                or not _number(gate['wait_s'])):
            raise DataflowError('INVALID_CONTRACT', 'invalid embedding admission observation')
        if 'adaptive' in gate:
            _validate_adaptive(gate['adaptive'])
    # Snapshot copies cannot retain mutable actor state. The original canonical
    # writer checks depth/nodes/bytes before a bounded JSON copy is allocated.
    return json.loads(document(value, max_bytes))


def _validate_adaptive(value):
    from .adaptive_requests import DEFAULTS, request_admission_policy
    counters = {'capacity', 'maximum', 'active', 'peak', 'admitted', 'fresh_responses',
                'adjustments', 'coalesced_transient_failures', 'deferred_transient_failures',
                'transient_window_samples', 'transient_window_failures'}
    fields = counters | {'last_reason', 'policy', 'observed_at', 'stopped', 'recovery_remaining_s'}
    if (type(value) is not dict or set(value) != fields
            or any(type(value[k]) is not int or value[k] < 0 for k in counters)
            or any(type(value[k]) is not str or len(value[k]) > 128 for k in ('last_reason', 'stopped'))
            or not _number(value['observed_at']) or not _number(value['recovery_remaining_s'])
            or type(value['policy']) is not dict or set(value['policy']) != set(DEFAULTS)):
        raise DataflowError('INVALID_CONTRACT', 'invalid adaptive request observation')
    try:
        request_admission_policy(value['policy'])
    except (TypeError, ValueError) as error:
        raise DataflowError('INVALID_CONTRACT', 'invalid observed adaptive policy') from error


def worker_model_observations(kernel, *, final=False):
    if kernel is None or getattr(getattr(kernel, '_spec', None), 'kind', None) != 'embedding':
        return None
    if getattr(kernel, '_closed', False) and not final:
        return getattr(kernel, '_model_observations', None)
    if kernel.pending_calls and not final:
        return getattr(kernel, '_model_observations', None)
    from ..embeddings.runtime import EmbeddingActor
    summaries = []
    for instance in kernel._instances:
        if instance is None:
            continue
        actor = kernel._state_owner(instance)
        if not isinstance(actor, EmbeddingActor):
            continue
        gate = actor.request_gate
        summaries.append({**actor.call_metrics.summary(), 'shared_admission': {
            'requests': gate.admitted, 'peak': gate.peak, 'wait_s': gate.wait_s,
            **({'adaptive': gate.snapshot()} if hasattr(gate, 'snapshot') else {})}})
    if not summaries:
        return None
    result = validate_summaries(summaries, maximum=kernel._spec.configuration['concurrency'],
                               max_bytes=kernel._services.bounds.max_control_bytes)
    kernel._model_observations = result
    return result


def observe_models(execution, event, payload):
    value = payload.get('model_observations')
    if value is None:
        return
    from .contracts.dataflow_codec import _thaw_json
    value = _thaw_json(value)
    node = next(node for node in execution._graph.nodes if node.occurrence_id == payload['occurrence_id'])
    if node.kernel.kind != 'embedding':
        raise DataflowError('FENCED', 'model observations require an assigned embedding node')
    value = validate_summaries(value, maximum=node.kernel.configuration['concurrency'],
                               max_bytes=execution._bounds.max_control_bytes)
    slots = getattr(execution, '_model_observations', None)
    if slots is None:
        execution._model_observations = slots = {}
    key = (payload['occurrence_id'], payload['partition_id'])
    prior = slots.get(key)
    if prior is not None and prior[0] == event.boot_id:
        if len(value) < len(prior[1]) or any(current[k] < old[k]
                for old, current in zip(prior[1], value) for k in _COUNTS):
            raise DataflowError('FENCED', 'embedding observation moved backwards within one worker boot')
    slots[key] = (event.boot_id, value)


def stream_model_observations(execution, stats):
    labels = getattr(execution, '_observation_labels', {})
    stats.metrics['models'] = {f'{labels.get(occ, occ)}:{partition}:{index}': value
        for (occ, partition), (_, values) in getattr(execution, '_model_observations', {}).items()
        for index, value in enumerate(values)}
    stats.metrics['models_scope'] = 'Current worker boot observations per instance; not committed effects or billing'
    stats.metrics.setdefault('native_journal_totals', {})
    stats.metrics.setdefault('native_journal_totals_skipped', [])
    stats.metrics['journal_totals_scope'] = 'Entire journal, including earlier actions; models describes this action only'


def finish_journal_audit(execution, stats):
    # Once after actor drain. Paths/options are driver-side sealed declarations.
    nodes = getattr(execution._services.journal_service, '_private_embedding_nodes', {})
    if not nodes:
        return
    from ..operator_llm.call_ref import journal_totals
    labels = getattr(execution, '_observation_labels', {})
    seen = set()
    for occurrence, config in nodes.items():
        path = config['private_journal']['path']
        if config.get('collect_journal_totals', True):
            if path not in seen:
                stats.metrics['native_journal_totals'][path] = journal_totals(path)
                seen.add(path)
        else:
            stats.metrics['native_journal_totals_skipped'].append({
                'stage': labels.get(occurrence, occurrence), 'path': path,
                'reason': 'collect_journal_totals=False'})
