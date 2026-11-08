"""Shared bounded GPU execution. Importing configuration never imports Torch."""
from collections import OrderedDict, deque
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time

MiB = 1024 * 1024
DEFAULTS = dict(streams=2, max_entries=32, cache_memory_mib=4096,
                memory_limit_mib=65536, max_input_tokens=8192, max_graph_tokens=4096,
                min_observations=8, max_captures=128, capture_budget_s=60,
                verify_every=64)


def check_encoding_budget(request, max_tokens):
    # Bound native encoder materialization before tokenization. A byte budget
    # cannot predict token count, so the encoded token limit is checked too.
    # Include worst-case repetition of state when native question splitting runs.
    budget = max_tokens * 128
    size = 0
    for part in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(request):
        size += len(part.encode('utf-8'))
        if size > budget:
            raise ValueError('Request exceeds CUDA graph encoding byte budget')
    if size * len(request['questions']) > 16 * 1024 * 1024:
        raise ValueError('Native question expansion exceeds encoding budget')


def graph_config(value):
    """Closed, inert opt-in settings; no device, files or model are inspected."""
    if not isinstance(value, dict) or set(value) - DEFAULTS.keys():
        raise ValueError('cuda_graphs requires a mapping of supported limits')
    result = {**DEFAULTS, **value}
    limits = dict(streams=(1, 4), max_entries=(1, 256), cache_memory_mib=(64, 16384),
                  memory_limit_mib=(1024, 262144), max_input_tokens=(1, 32768),
                  max_graph_tokens=(1, 8192), min_observations=(1, 100),
                  max_captures=(1, 4096), capture_budget_s=(1, 3600), verify_every=(1, 1024))
    for key, (low, high) in limits.items():
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise ValueError(f'cuda_graphs.{key} must be an integer in {low}..{high}')
    if result['max_graph_tokens'] > result['max_input_tokens']:
        raise ValueError('max_graph_tokens exceeds max_input_tokens')
    if result['cache_memory_mib'] >= result['memory_limit_mib']:
        raise ValueError('Graph cache must leave memory for weights and workspaces')
    return result


class ExecutionGate:
    """Writers drain GPU readers before capture/free; waiting writers get priority."""
    def __init__(self):
        self.condition = threading.Condition()
        self.readers = self.waiting = 0
        self.writing = False

    @contextmanager
    def enter(self, exclusive=False):
        with self.condition:
            if exclusive:
                self.waiting += 1
                try:
                    self.condition.wait_for(lambda: not self.writing and not self.readers)
                    self.writing = True
                finally:
                    self.waiting -= 1
            else:
                self.condition.wait_for(lambda: not self.writing and not self.waiting)
                self.readers += 1
        try:
            yield
        finally:
            with self.condition:
                if exclusive:
                    self.writing = False
                else:
                    self.readers -= 1
                self.condition.notify_all()


class GraphCache:
    """CPU policy; caller holds an exclusive GPU gate for put/clear/eviction."""
    def __init__(self, config):
        self.config = config
        self.entries = OrderedDict()
        self.history = OrderedDict()
        self.bytes = self.captures = self.evictions = 0
        self.capture_seconds = 0.0
        self.disabled = False
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                self.entries.move_to_end(key)
            return entry

    def observe(self, key):
        with self.lock:
            # A graph belongs to one lane. Shape-wide frequency would let a
            # rarely used copy inherit another lane's popularity and evict
            # useful graphs for different inputs.
            count = min(self.history.pop(key, 0) + 1, 65535)
            self.history[key] = count
            while len(self.history) > 4 * self.config['max_entries']:
                self.history.popitem(last=False)
            return count >= self.config['min_observations']

    def _victims(self, size):
        remaining, count = self.bytes, len(self.entries)
        victims = []
        # Least frequent first, LRU to break ties. All metadata is bounded.
        for key in sorted(self.entries, key=lambda key: self.history.get(key, 0)):
            if count < self.config['max_entries'] and remaining + size <= self.config['cache_memory_mib'] * MiB:
                break
            victims.append(key)
            remaining -= self.entries[key].bytes
            count -= 1
        return victims

    def admit(self, size, key=None):
        allowed = (not self.disabled and size <= self.config['cache_memory_mib'] * MiB
                and self.captures < self.config['max_captures']
                and self.capture_seconds < self.config['capture_budget_s'])
        if not allowed or key is None:
            return allowed
        with self.lock:
            frequency = self.history.get(key, 0)
            return all(frequency > self.history.get(victim, 0) for victim in self._victims(size))

    def make_room(self, size):
        with self.lock:
            for key in self._victims(size):
                entry = self.entries.pop(key)
                self.bytes -= entry.bytes
                entry.close()
                self.evictions += 1

    def put(self, key, entry):
        with self.lock:
            if self.bytes + entry.bytes > self.config['cache_memory_mib'] * MiB:
                raise RuntimeError('Graph cache reservation exceeded')
            self.entries[key] = entry
            self.bytes += entry.bytes

    def clear(self):
        with self.lock:
            for entry in self.entries.values():
                entry.close()
            self.entries.clear()
            self.bytes = 0


def configure_allocator(config):
    """Called before loading weights in the isolated worker process."""
    import torch
    if not callable(getattr(torch._C, '_cuda_clearCublasWorkspaces', None)):
        raise ValueError('Torch runtime cannot isolate CUDA graph cuBLAS workspaces')
    if torch.cuda.memory.get_allocator_backend() != 'native':
        raise ValueError('CUDA graphs require the native Torch CUDA allocator')
    total = torch.cuda.get_device_properties(0).total_memory
    limit = config['memory_limit_mib'] * MiB
    if limit > total - 2048 * MiB:
        raise ValueError('memory_limit_mib must leave at least 2048 MiB device headroom')
    torch.cuda.set_per_process_memory_fraction(limit / total, 0)


def graph_pool_bytes(torch, graph):
    """Account reserved graph memory without materializing allocation snapshots.

    Torch 2.13's filtered memorySnapshot binding incorrectly decrefs borrowed
    tuple items. Non-immortal pool IDs (>256) can corrupt the Python heap.
    The statistics API creates owned pool-ID keys and reads the same allocator
    reserved-byte counters. Missing accounting must fail closed, not report 0.
    """
    pools = torch.cuda.memory_stats_as_nested_dict().get('reserved_bytes_by_private_pools', {})
    entry = pools.get(graph.pool())
    size = entry.get('all', {}).get('current') if isinstance(entry, dict) else None
    if type(size) is not int or size < 0:
        raise RuntimeError('Torch runtime does not expose reserved bytes for this graph pool')
    return size



class BoundedExecution:
    """Fixed GPU lanes, admitted before submission; no extra request queue."""
    def _initialize(self, configuration, workspace_estimator, mode):
        import torch
        self.torch, self.mode = torch, mode
        self.config = graph_config(configuration)
        self.parallel = self.config['streams']
        self.workspace_estimator = workspace_estimator
        self.gate, self.cache = ExecutionGate(), GraphCache(self.config)
        self.condition, self.encoding = threading.Condition(), threading.Lock()
        self.failed, self.closed, self.accepting = None, False, True
        self.stats = dict(hits=0, native=0, checks=0, mismatches=0, capture_rejected=0,
                          reserved_peak_bytes=0, queue_wait_seconds=0.0, queue_wait_max_seconds=0.0)
        self.workspace_used = 0
        self.pending = deque()
        torch.cuda.synchronize()
        self.total_memory = torch.cuda.get_device_properties(0).total_memory
        self.memory_limit = self.config['memory_limit_mib'] * MiB
        self.lanes = []

    def _add_lane(self, model, **extra):
        stream = self.torch.cuda.Stream()
        stream.wait_stream(self.torch.cuda.current_stream())
        self.lanes.append(dict(model=model, stream=stream, busy=False,
            executor=ThreadPoolExecutor(max_workers=1, thread_name_prefix=f'cuda-lane-{len(self.lanes)}'), **extra))

    def _finish_init(self):
        self.torch.cuda.synchronize()
        self.torch.cuda.empty_cache()
        self.workspace_limit = (self.memory_limit - self.torch.cuda.memory_reserved()
                                - self.config['cache_memory_mib'] * MiB)
        if self.workspace_limit < 256 * MiB:
            raise ValueError('Memory limit leaves insufficient inference workspace')

    def _estimate(self, tokens):
        estimate = self.workspace_estimator(tokens)
        if type(estimate) is not int or estimate <= 0:
            raise ValueError('Backend workspace estimate must be positive bytes')
        return estimate

    def _healthy(self):
        if self.closed or self.failed:
            raise RuntimeError(self.failed or 'Graph execution is closed')

    def _submit(self, request, plan, tokens, shape=None, workspace_extra=0):
        if not 0 < tokens <= self.config['max_input_tokens']:
            raise ValueError('Request exceeds cuda_graphs.max_input_tokens; input was not truncated')
        required = self._estimate(tokens) + workspace_extra
        if required > self.workspace_limit:
            raise ValueError('Request exceeds the declared inference workspace budget')
        started = time.monotonic()
        with self.condition:
            # The HTTP owner already bounds callers. Preserve their admission
            # order instead of letting a newly arriving caller repeatedly win
            # the Condition race against an older request.
            ticket = object()
            self.pending.append(ticket)
            try:
                self.condition.wait_for(lambda: not self.accepting or self.failed or (
                    self.pending[0] is ticket and any(not lane['busy'] for lane in self.lanes)
                    and self.workspace_used + required <= self.workspace_limit))
                self._healthy()
                if not self.accepting: raise RuntimeError('Graph execution is closing')
                idle = [i for i, lane in enumerate(self.lanes) if not lane['busy']]
                index = next((i for i in idle if self.cache.get((i, shape)) is not None), idle[0])
                self.lanes[index]['busy'] = True
                self.workspace_used += required
                waited = time.monotonic() - started
                self.stats['queue_wait_seconds'] += waited
                self.stats['queue_wait_max_seconds'] = max(self.stats['queue_wait_max_seconds'], waited)
            finally:
                self.pending.remove(ticket)
                self.condition.notify_all()
        try:
            return self.lanes[index]['executor'].submit(self._execute, index, request, plan).result()
        except RuntimeError as error:
            self.failed = 'GPU execution failed: ' + str(error)[:512]
            with self.condition: self.condition.notify_all()
            raise
        finally:
            with self.condition:
                self.lanes[index]['busy'] = False
                self.workspace_used -= required
                self.condition.notify_all()

    def execution_status(self):
        with self.gate.enter():
            with self.cache.lock:
                return dict(self.stats, mode=self.mode, streams=self.parallel,
                    allocator_reserved_bytes=self.torch.cuda.memory_reserved(),
                    allocator_peak_bytes=self.torch.cuda.max_memory_reserved(),
                    entries=len(self.cache.entries), cache_bytes=self.cache.bytes,
                    observations=len(self.cache.history), captures=self.cache.captures,
                    evictions=self.cache.evictions, capture_seconds=self.cache.capture_seconds,
                    graph_disabled=self.cache.disabled, failed=self.failed, limits=self.config)

    def close(self):
        with self.condition:
            self.accepting = False
            self.condition.notify_all()
            self.condition.wait_for(lambda: not any(lane['busy'] for lane in self.lanes))
            self.closed = True
        try:
            with self.gate.enter(exclusive=True):
                self.torch.cuda.synchronize()
                self.cache.clear()
                self.torch.cuda.empty_cache()
        finally:
            for lane in self.lanes:
                lane['executor'].shutdown(wait=True)
            self.lanes.clear()


class RequestLease:
    """All GPU work in a native call is protected, including work outside hooks."""
    def __init__(self, gate, synchronize):
        self.gate, self.synchronize = gate, synchronize
        self.reader = None

    def __enter__(self):
        self.reader = self.gate.enter()
        self.reader.__enter__()
        return self

    def __exit__(self, *error):
        try: self.synchronize()
        finally: self.reader.__exit__(*error)

    @contextmanager
    def exclusive(self):
        self.synchronize()
        self.reader.__exit__(None, None, None)
        try:
            with self.gate.enter(exclusive=True): yield
        finally:
            self.reader = self.gate.enter()
            self.reader.__enter__()


def qwen_workspace_bytes(config, tokens):
    """Conservative Qwen3.5 workspace recipe, explicitly selected by its backends."""
    linear = tokens * (config.hidden_size * 32 + config.intermediate_size * 8
                      + config.linear_num_value_heads * config.linear_value_head_dim * 24)
    return 32 * MiB + linear + 4 * config.num_attention_heads * tokens * tokens
