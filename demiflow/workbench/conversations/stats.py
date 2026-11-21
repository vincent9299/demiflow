"""Bounded, replayable session statistics from persisted public provider facts.

DSH counters follow its session-stats/token-meter projections. Missing old
telemetry is reported as coverage, never reconstructed from text or tool count.
The cache is disposable: a restart replays the same durable events in pages.
"""
from collections import OrderedDict
from dataclasses import dataclass, field
import json
import math
import threading
import weakref

from .common import identifier, reject

FORMAT = 'demiflow_native_metrics_v1'
ROWS_PER_READ = 2048
BYTES_PER_READ = 2 * 1024**2
CACHED_SESSIONS = 16
_caches = weakref.WeakKeyDictionary()
_cache_lock = threading.Lock()


def number(value, *, count=False):
    return (type(value) is int if count else type(value) in (int, float)) and 0 <= value <= 2**53 - 1 and math.isfinite(value)


def validate_metric(value):
    """Only this fixed plugin's numeric projection may enter the metric fold."""
    common = {'format', 'kind', 'event_sequence', 'native_turn'}
    fields = {
        'step_started': {'native_step'}, 'step_ended': {'native_step'}, 'retry_started': {'native_step'},
        'turn_ended': set(), 'tool': {'call_id', 'duration_ms'},
        'model': {'native_step', 'attempt_id', 'message_id', 'usage', 'timing'},
        'attempt': {'native_step', 'attempt_id', 'usage'},
    }
    if (type(value) is not dict or value.get('format') != FORMAT or value.get('kind') not in fields
            or set(value) != common | fields[value['kind']]):
        reject('Invalid native metric fields', 'invalid_request')
    for key in ('event_sequence', 'native_turn', 'native_step'):
        if key in value and not number(value[key], count=True): reject('Invalid native metric identity', 'invalid_request')
    for key in ('call_id', 'attempt_id', 'message_id'):
        if key in value and (type(value[key]) is not str or not value[key] or len(value[key].encode()) > 256):
            reject('Invalid native metric identity', 'invalid_request')
    if value['kind'] == 'tool' and not number(value['duration_ms']): reject('Invalid native tool duration', 'invalid_request')
    if value['kind'] in {'model', 'attempt'}:
        usage = value['usage']
        required = {'inputTokens', 'outputTokens'}
        optional = {'totalTokens', 'cacheReadTokens', 'cacheWriteTokens'}
        if usage is not None and (type(usage) is not dict or not required <= usage.keys()
                or usage.keys() - required - optional or any(not number(v, count=True) for v in usage.values())):
            reject('Invalid native token usage', 'invalid_request')
        timing = value.get('timing', {})
        if (type(timing) is not dict or timing.keys() - {'llm_ms', 'ttft_ms', 'decode_ms'}
                or any(not number(v) for v in timing.values())):
            reject('Invalid native model timing', 'invalid_request')
    return value


@dataclass
class _Totals:
    sequence: int = 0
    rows: int = 0
    bytes: int = 0
    native_sequences: dict = field(default_factory=dict)
    last_usage: dict = field(default_factory=dict)
    last_model: dict = field(default_factory=dict)
    captured_turns: set = field(default_factory=set)
    epochs: set = field(default_factory=set)
    invalid: int = 0
    steps: int = 0
    models: int = 0
    attempts: int = 0
    usages: int = 0
    model_usages: int = 0
    timed_models: int = 0
    llm_ms: float = 0
    tool_ms: float = 0
    decode_ms: float = 0
    decode_tokens: int = 0
    uncached: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    context: dict = field(default_factory=lambda: dict(used=None, size=None))

    def apply(self, event, provider):
        self.epochs.add((event['turn_id'], event['provider_epoch']))
        if event.get('type') != 'usage.observed' or type(event.get('data')) is not dict: return
        data = event['data']
        # ACP used/size is occupancy, not cumulative billed input/output usage.
        if provider == 'deepseek' and number(data.get('used'), count=True) and number(data.get('size'), count=True) and data['size'] > 0:
            self.context = dict(used=data['used'], size=data['size'])
        if provider != 'deepseek' or data.get('format') != FORMAT: return
        try: validate_metric(data)
        except ValueError:
            self.invalid += 1
            return
        epoch = (event['turn_id'], event['provider_epoch'])
        if data['event_sequence'] <= self.native_sequences.get(epoch, -1): return
        self.native_sequences[epoch] = data['event_sequence']
        self.captured_turns.add(event['turn_id'])
        if data['kind'] == 'step_ended': self.steps += 1
        if data['kind'] == 'tool': self.tool_ms += data['duration_ms']
        if data['kind'] == 'retry_started':
            previous = self.last_usage.get(epoch)
            if previous and previous[:2] == (data['native_turn'], data['native_step']): self.last_usage.pop(epoch)
        if data['kind'] not in {'model', 'attempt'}: return
        identity = (data['kind'], data['attempt_id'], data.get('message_id'))
        if self.last_model.get(epoch) == identity: return
        self.last_model[epoch] = identity
        if data['kind'] == 'model': self.models += 1
        else: self.attempts += 1
        usage, timing = data['usage'], data.get('timing', {})
        if 'llm_ms' in timing:
            self.timed_models += 1
            self.llm_ms += timing['llm_ms']
        if usage is None: return
        # Native TokenUsage input is UNcached; its optional cache buckets are
        # disjoint and default to zero in the official tokenUsage projection.
        self.usages += 1
        if data['kind'] == 'model': self.model_usages += 1
        key = (data['native_turn'], data['native_step'])
        buckets = (usage['inputTokens'], usage['outputTokens'], usage.get('cacheReadTokens', 0), usage.get('cacheWriteTokens', 0))
        previous = self.last_usage.get(epoch)
        old = previous[2] if previous and previous[:2] == key else (0, 0, 0, 0)
        # Native tokenUsage replaces samples in one step; retry-started closes
        # that replacement slot so a newly billed attempt adds to the total.
        self.uncached += buckets[0] - old[0]
        self.output += buckets[1] - old[1]
        self.cache_read += buckets[2] - old[2]
        self.cache_write += buckets[3] - old[3]
        self.last_usage[epoch] = (*key, buckets)
        if 'decode_ms' in timing:
            self.decode_ms += timing['decode_ms']
            self.decode_tokens += usage['outputTokens']


class _Cache:
    def __init__(self):
        self.lock = threading.Lock()
        self.sessions = OrderedDict()

    def read(self, store, workspace, sid):
        # Authorization is checked before a cache hit. Narrow metadata queries
        # neither expose prompt text nor invoke the mutating worker reconciler.
        with self.lock, store.connect() as db:
            session = store._session(db, workspace, identifier(sid))
            turns = {row['id']: row['operation'] for row in db.execute(
                'SELECT id,operation FROM turns WHERE session=? ORDER BY created LIMIT ?',
                (sid, store.limits.turns + 1))}
            if len(turns) > store.limits.turns: reject('Session turn count exceeds statistics budget', 'resource_limit')
            key = (workspace, sid)
            totals = self.sessions.pop(key, None) or _Totals()
            if totals.sequence > session['sequence']: totals = _Totals()
            self.sessions[key] = totals
            while len(self.sessions) > CACHED_SESSIONS: self.sessions.popitem(last=False)
            used = rows = 0
            # The primary (session,sequence) index makes every poll incremental.
            # Fetch one bounded row at a time; no entire transcript materializes.
            for row in db.execute('SELECT sequence,turn,size,body FROM events WHERE session=? AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?',
                                  (sid, totals.sequence, session['sequence'], ROWS_PER_READ)):
                if used + row['size'] > BYTES_PER_READ: break
                used += row['size']; rows += 1
                if turns.get(row['turn']) == 'message':
                    totals.apply(json.loads(row['body']), session['provider'])
                elif turns.get(row['turn']) == 'compact':
                    event = json.loads(row['body'])
                    if event.get('type') == 'compaction.completed' and event.get('data', {}).get('status') == 'compacted':
                        totals.context = dict(used=None, size=None)
                totals.sequence = row['sequence']
            totals.rows += rows; totals.bytes += used
            complete = totals.sequence == session['sequence']
            count = sum(operation != 'compact' for operation in turns.values())
            captured = len(totals.captured_turns)
            metric_complete = (captured == count and len(totals.native_sequences) == len(totals.epochs)
                               and totals.usages == totals.models + totals.attempts and totals.timed_models == totals.models and not totals.invalid)
            metric_state = 'unavailable' if not captured else 'complete' if metric_complete else 'partial'
            observed = complete and captured > 0
            billed = totals.uncached + totals.cache_read + totals.cache_write
            tokens = dict(input=billed, uncached_input=totals.uncached, output=totals.output,
                          total=billed + totals.output, cache_read=totals.cache_read, cache_write=totals.cache_write)
            has_tokens = observed and totals.usages > 0 and all(number(value, count=True) for value in tokens.values())
            if not has_tokens: tokens = dict.fromkeys(tokens)
            return dict(format='demiflow_development_stats_v1', session_id=sid, provider=session['provider'],
                turns=count, turns_basis='conversation_requests', steps=totals.steps if observed else None,
                llm_ms=totals.llm_ms if observed and totals.timed_models and number(totals.llm_ms) else None,
                tool_ms=totals.tool_ms if observed and number(totals.tool_ms) else None, tokens=tokens,
                cache_hit_ratio=totals.cache_read / billed if has_tokens and billed > 0 else None,
                context=totals.context if complete else dict(used=None, size=None),
                decode_tokens_per_second=totals.decode_tokens * 1000 / totals.decode_ms if observed and totals.decode_ms > 0 else None,
                coverage=dict(state='complete' if complete else 'partial', through_sequence=totals.sequence,
                    last_sequence=session['sequence'], events_scanned=totals.rows, bytes_scanned=totals.bytes,
                    metrics_state=metric_state, reported_turns=captured, total_turns=count,
                    reported_epochs=len(totals.native_sequences), total_epochs=len(totals.epochs),
                    models=totals.models, models_with_usage=totals.model_usages, models_with_timing=totals.timed_models,
                    attempts=totals.attempts, attempts_with_usage=totals.usages-totals.model_usages,
                    invalid_metrics=totals.invalid, excluded_operations=['compact'],
                    row_limit=ROWS_PER_READ, byte_limit=BYTES_PER_READ),
                unavailable=[name for name, available in (('native_metrics', observed), ('tokens', has_tokens),
                    ('llm_timing', observed and totals.timed_models > 0), ('decode_speed', observed and totals.decode_ms > 0),
                    ('context', complete and totals.context['used'] is not None)) if not available])


def read(store, workspace, sid):
    with _cache_lock:
        cache = _caches.setdefault(store, _Cache())
    return cache.read(store, workspace, sid)
