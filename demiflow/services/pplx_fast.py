"""Native pplx FastOptions behind one GPU thread; no application decisions."""
from concurrent.futures import ThreadPoolExecutor
import threading

try:
    from .model_adapters import validate_request, error_result
    from .cuda_graphs import check_encoding_budget
except ImportError:
    from model_adapters import validate_request, error_result
    from cuda_graphs import check_encoding_budget


DEFAULTS = dict(compile='layer', cuda_graphs=True, buckets=[1024, 2048, 4096, 8192, 16384],
                token_budget=16384, max_input_tokens=32768, memory_limit_mib=65536,
                prune_last=True, prefetch=True)


def fast_config(value):
    if not isinstance(value, dict) or set(value) - DEFAULTS.keys():
        raise ValueError('pplx_fast requires supported native execution options')
    result = {**DEFAULTS, **value}
    if result['compile'] not in (None, 'layer'):
        raise ValueError('pplx_fast.compile must be null or layer')
    for key in ('cuda_graphs', 'prune_last', 'prefetch'):
        if type(result[key]) is not bool:
            raise ValueError('pplx_fast.' + key + ' must be boolean')
    if result['cuda_graphs'] and result['compile'] != 'layer':
        raise ValueError('Native CUDA graphs require layer compilation')
    for key, low, high in [('token_budget', 1, 32768), ('max_input_tokens', 1, 32768),
                           ('memory_limit_mib', 1024, 262144)]:
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise ValueError('Invalid pplx_fast.' + key)
    buckets = result['buckets']
    if (not isinstance(buckets, (list, tuple)) or not 1 <= len(buckets) <= 8
            or any(type(n) is not int or not 1 <= n <= result['token_budget'] for n in buckets)
            or list(buckets) != sorted(set(buckets)) or sum(buckets) > 131072):
        raise ValueError('pplx_fast.buckets must be 1..8 ascending unique token sizes within token_budget')
    result['buckets'] = list(buckets)
    return result


class PplxFastAdapter:
    """Keep native encoding/calibration and isolate its process-global packing state.

    Only one submitted GPU task exists at a time. Encoder plans are checked one
    request at a time and released before the bounded native pack is executed.
    The allocator cap bounds Torch allocations; it is not a total device/RSS cap.
    """
    parallel = 1

    def __init__(self, decider, configuration):
        self.config = fast_config(configuration)
        self.decider = decider
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='pplx-fast')
        self.failed = None
        self.closed = False
        self.requests = self.packs = self.long_requests = 0
        try:
            self.pool.submit(self._initialize).result()
        except BaseException:
            self.pool.shutdown(wait=True)
            raise

    def _initialize(self):
        from pplx_decider import FastOptions
        from pplx_decider.fast import FastPath
        self.decider.fast = FastPath(self.decider, FastOptions(**{
            k: self.config[k] for k in ('compile', 'cuda_graphs', 'buckets', 'token_budget',
                                      'prune_last', 'prefetch')}, fp8=False))
        # Native warmup currently prepares only its first static bucket. Other
        # buckets compile lazily on this same thread, never HTTP worker threads.
        self.decider.warmup()

    def _infer(self, requests):
        results = [None] * len(requests)
        group, tokens = [], 0

        def flush():
            if not group:
                return
            values = self.decider.decide([r for _, r in group])
            if not isinstance(values, list) or len(values) != len(group):
                raise RuntimeError('Native fast batch response count differs')
            for (index, _), value in zip(group, values, strict=True):
                results[index] = value
            self.requests += len(group)
            self.packs += 1
            group.clear()

        for index, request in enumerate(requests):
            try:
                request = validate_request(request)
                if request.get('images'):
                    raise ValueError('pplx_fast currently accepts text requests only')
                check_encoding_budget(request, self.config['max_input_tokens'])
                plan = self.decider.encoder.plan(request, self.decider.max_length)
                count = sum(chunk.length for chunk in plan)
                del plan
                if not 0 < count <= self.config['max_input_tokens']:
                    raise ValueError('Request exceeds pplx_fast.max_input_tokens; no truncation')
            except (ValueError, TypeError, KeyError) as exc:
                results[index] = error_result(exc)
                continue
            if tokens + count > self.config['token_budget']:
                flush()
                tokens = 0
            group.append((index, request))
            tokens += count
            if count > self.config['token_budget']:
                # Native FastPath processes an oversized single sequence alone
                # without the static graph, preserving the declared input limit.
                self.long_requests += 1
                flush()
                tokens = 0
        flush()
        return results

    def batch(self, requests):
        if not isinstance(requests, list) or not 1 <= len(requests) <= 64:
            raise ValueError('Fast batch requires 1..64 requests')
        with self.lock:
            if self.closed or self.failed:
                raise RuntimeError('Fast execution is closed or failed')
            try:
                return self.pool.submit(self._infer, requests).result()
            except Exception as exc:
                self.failed = type(exc).__name__
                raise

    def single(self, request):
        value = self.batch([request])[0]
        if 'error' in value:
            raise ValueError(value['error']['detail'])
        return value

    def execution_status(self):
        return dict(mode='pplx_fast', options=self.config, requests=self.requests,
                    packs=self.packs, long_requests=self.long_requests, failed=self.failed)

    def close(self):
        with self.lock:
            self.closed = True
            self.pool.shutdown(wait=True)
