"""Actual model actor observations on the existing bounded progress channel.

Per-instance histograms stay separate; they are not billing or recovery state.
No worker-supplied path is used to audit historical journals.
"""
from __future__ import annotations

import json
import math
from collections.abc import Mapping

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import document

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


def validate_summaries(value, *, maximum, max_bytes, kind='embedding'):
    if kind not in {'embedding', 'prompt', 'agent'}:
        raise DataflowError('INVALID_CONTRACT', 'unknown model observation kind')
    if type(value) is not list or len(value) > maximum:
        raise DataflowError('INVALID_CONTRACT', 'model observation instance count exceeds assignment')
    for item in value:
        fields = _COUNTS | {'latency', 'stream_timings'}
        counters = _COUNTS
        if kind == 'embedding':
            fields = fields | {'profiled_batches', 'phases', 'shared_admission'}
            counters = counters | {'profiled_batches'}
        if (type(item) is not dict or set(item) not in (fields, fields | {'shared_admission'})
                or any(type(item[k]) is not int or item[k] < 0 for k in counters)
                or item['reused'] > item['call_records'] or item['records_without_usage'] > item['call_records']
                or not _histogram(item['latency'])
                or type(item['stream_timings']) is not dict or set(item['stream_timings']) != _TIMINGS
                or not all(_histogram(h) for h in item['stream_timings'].values())
                or kind == 'embedding' and (type(item['phases']) is not dict or len(item['phases']) > 32)):
            raise DataflowError('INVALID_CONTRACT', 'invalid model actor observation')
        for key, phase in item.get('phases', {}).items():
            if (type(key) is not str or not 1 <= len(key) <= 64
                    or type(phase) is not dict or set(phase) != {'count', 'total', 'max'}
                    or type(phase['count']) is not int or not 0 <= phase['count'] <= item['profiled_batches']
                    or not _number(phase['total']) or not _number(phase['max']) or phase['max'] > phase['total']):
                raise DataflowError('INVALID_CONTRACT', 'invalid embedding phase observation')
        if 'shared_admission' not in item:
            continue
        gate = item['shared_admission']
        if (type(gate) is not dict or set(gate) not in ({'requests', 'peak', 'wait_s'},
                {'requests', 'peak', 'wait_s', 'adaptive'})
                or any(type(gate[k]) is not int or gate[k] < 0 for k in ('requests', 'peak'))
                or not _number(gate['wait_s'])):
            raise DataflowError('INVALID_CONTRACT', 'invalid model admission observation')
        if 'adaptive' in gate:
            _validate_adaptive(gate['adaptive'])
    # Snapshot copies cannot retain mutable actor state. The original canonical
    # writer checks depth/nodes/bytes before a bounded JSON copy is allocated.
    return json.loads(document(value, max_bytes))


def _validate_adaptive(value):
    from demiflow.services.adaptive_requests import DEFAULTS, request_admission_policy
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





def validate_routed_model_summaries(value, *, configuration, max_bytes):
    routes = set(configuration['routes'])
    maximum = configuration.get('concurrency', 1)
    if (not 1 <= len(routes) <= 32 or type(value) is not list or len(value) > maximum * len(routes)):
        raise DataflowError('INVALID_CONTRACT', 'routed model observations exceed declared instance window')
    groups = {}
    for entry in value:
        if (type(entry) is not dict or set(entry) != {'instance', 'route', 'summary'}
                or type(entry['instance']) is not int or not 0 <= entry['instance'] < maximum
                or type(entry['route']) is not str or entry['route'] not in routes):
            raise DataflowError('FENCED', 'routed observation differs from the sealed routes')
        seen = groups.setdefault(entry['instance'], set())
        if entry['route'] in seen:
            raise DataflowError('INVALID_CONTRACT', 'duplicate routed model observation')
        seen.add(entry['route'])
    if any(seen != routes for seen in groups.values()):
        raise DataflowError('INVALID_CONTRACT', 'routed observation omitted a declared child')
    validate_summaries([entry['summary'] for entry in value], maximum=maximum * len(routes),
                       max_bytes=max_bytes, kind='prompt')
    return json.loads(document(value, max_bytes))





def observe_models(execution, event, payload):
    value = payload.get('model_observations')
    if value is None:
        return
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    value = _thaw_json(value)
    node = next(node for node in execution._graph.nodes if node.occurrence_id == payload['occurrence_id'])
    if node.kernel.kind not in {'embedding', 'prompt', 'agent'}:
        raise DataflowError('FENCED', 'model observations require an assigned model node')
    routed = node.kernel.kind == 'prompt' and 'routes' in node.kernel.configuration
    if routed:
        value = validate_routed_model_summaries(value, configuration=node.kernel.configuration,
                                 max_bytes=execution._bounds.max_control_bytes)
    else:
        value = validate_summaries(value, maximum=node.kernel.configuration.get('concurrency', 1),
                                   max_bytes=execution._bounds.max_control_bytes, kind=node.kernel.kind)
    slots = getattr(execution, '_model_observations', None)
    if slots is None:
        execution._model_observations = slots = {}
    key = (payload['occurrence_id'], payload['partition_id'])
    prior = slots.get(key)
    if prior is not None and prior[0] == event.boot_id:
        if routed:
            old_slots = {(v['instance'], v['route']): v['summary'] for v in prior[1]}
            new_slots = {(v['instance'], v['route']): v['summary'] for v in value}
        else:
            old_slots, new_slots = dict(enumerate(prior[1])), dict(enumerate(value))
        if not set(old_slots) <= set(new_slots) or any(new_slots[slot][k] < old[k]
                for slot, old in old_slots.items() for k in _COUNTS):
            raise DataflowError('FENCED', 'model observation moved backwards within one worker boot')
    slots[key] = (event.boot_id, value)


def stream_model_observations(execution, stats):
    labels = getattr(execution, '_observation_labels', {})
    models = {}
    for (occ, partition), (_, values) in getattr(execution, '_model_observations', {}).items():
        for index, value in enumerate(values):
            label = labels.get(occ, occ)
            if 'route' in value:
                # Canonical tuple text prevents collisions between route names,
                # labels and partitions containing punctuation.
                name = json.dumps([label, partition, value['instance'], value['route']], ensure_ascii=False)
                models[name] = {**value['summary'], 'route': value['route']}
            else:
                models[f'{label}:{partition}:{index}'] = value
    stats.metrics['models'] = models
    stats.metrics['models_scope'] = 'Current worker boot observations per instance; not committed effects or billing'
    stats.metrics.setdefault('native_journal_totals', {})
    stats.metrics.setdefault('native_journal_totals_skipped', [])
    stats.metrics['journal_totals_scope'] = 'Entire private journal, including earlier actions; models are current worker boot observations'


def finish_journal_audit(execution, stats):
    # Once after actor drain. Paths/options are driver-side sealed declarations.
    service = execution._services.journal_service
    embeddings = getattr(service, '_private_embedding_nodes', {})
    prompts = getattr(service, '_private_prompt_nodes', {})
    if not embeddings and not prompts:
        return
    from itertools import chain
    from demiflow.execution.storage.artifacts import resolve_local_artifact
    from demiflow.operator_llm.call_ref import journal_totals
    labels = getattr(execution, '_observation_labels', {})
    seen = set()
    declarations = chain(((occ, None, cfg) for occ, cfg in embeddings.items()),
                         ((occ, route, cfg) for (occ, route), cfg in prompts.items()))
    for occurrence, route, config in declarations:
        path = str(resolve_local_artifact(config['private_journal']['path']).resolve())
        if config.get('collect_journal_totals', True):
            if path not in seen:
                stats.metrics['native_journal_totals'][path] = journal_totals(path)
                seen.add(path)
        else:
            stats.metrics['native_journal_totals_skipped'].append({
                'stage': labels.get(occurrence, occurrence), 'path': path,
                **({'route': route} if route is not None else {}),
                'reason': 'collect_journal_totals=False'})
