"""Owned JEV diagnostic process; temporary controls never enter production."""
import json
import os
import threading
import importlib.util
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import platform_profile_worker as base

original = base.model_worker.make_server


def instrument(adapter, *args, **kwargs):
    server = original(adapter, *args, **kwargs)
    from cuda_graphs import GraphCache
    baseline_path = os.environ.get('DEMIFLOW_JEV_BASELINE')
    BaselineCache = None
    if baseline_path:
        path = Path(baseline_path)/'demiflow/services/cuda_graphs.py'
        spec = importlib.util.spec_from_file_location('benchmark_old_cache', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        class BaselineCache(module.GraphCache):
            def observe(self, key): return super().observe(key[1])
            def admit(self, size, key=None): return super().admit(size, key[1] if key is not None else None)
    counters = dict(cached_idle=0, cached_busy=0, absent=0)
    lock = threading.Lock()
    submit = adapter._submit

    def measured(request, plan, tokens, shape=None, workspace_extra=0):
        with adapter.condition, adapter.cache.lock:
            indices = [i for i in range(len(adapter.lanes)) if (i, shape) in adapter.cache.entries]
            kind = ('cached_idle' if any(not adapter.lanes[i]['busy'] for i in indices)
                    else 'cached_busy' if indices else 'absent')
        with lock: counters[kind] += 1
        return submit(request, plan, tokens, shape, workspace_extra)
    adapter._submit = measured
    # Separate rendering/tokenization work from the protocol's lock and checks.
    base.metrics.wrap(adapter.protocol.formatter, 'render', 'prepare.render')
    base.metrics.wrap(adapter.protocol.tokenizer, 'encode', 'prepare.tokenize')
    handler = server.RequestHandlerClass
    get = handler.do_GET

    def control(self):
        url = urlsplit(self.path)
        if not url.path.startswith('/__jev/') or self.headers.get('X-Benchmark-Token') != os.environ['DEMIFLOW_PROFILE_TOKEN']:
            return get(self)
        if base.metrics.active: raise RuntimeError('Control requires idle benchmark')
        if url.path == '/__jev/mode':
            mode = parse_qs(url.query)['value'][0]
            if mode not in ('native', 'graphs'): raise ValueError('Unknown mode')
            adapter.cache.disabled = mode == 'native'
            body = {'mode': mode}
        elif url.path == '/__jev/policy':
            policy = parse_qs(url.query)['value'][0]
            if policy not in ('baseline', 'candidate') or BaselineCache is None: raise ValueError('Missing policy baseline')
            with adapter.condition:
                if adapter.pending or any(lane['busy'] for lane in adapter.lanes): raise RuntimeError('Policy requires drained lanes')
            with adapter.gate.enter(exclusive=True):
                adapter.torch.cuda.synchronize(); adapter.cache.clear(); adapter.torch.cuda.empty_cache()
                adapter.cache = (BaselineCache if policy == 'baseline' else GraphCache)(adapter.config)
                for name in ('observe', 'get', 'admit', 'make_room', 'put'):
                    base.metrics.wrap(adapter.cache, name, 'cache.'+name)
            body = {'policy': policy}
        elif url.path == '/__jev/cache':
            with adapter.cache.lock:
                entries = [{'lane': k[0], 'shape': k[1], 'bytes': v.bytes, 'uses': v.uses,
                            'frequency': adapter.cache.history.get(k[1] if BaselineCache is not None and isinstance(adapter.cache, BaselineCache) else k, 0)} for k, v in adapter.cache.entries.items()]
                history = list(adapter.cache.history.items())
            with lock:
                body = dict(entries=entries, history=history, admission=dict(counters))
                counters.update({key: 0 for key in counters})
        else: return get(self)
        raw = json.dumps(body).encode()
        self.send_response(200); self.send_header('Content-Length', str(len(raw)))
        self.end_headers(); self.wfile.write(raw)
    handler.do_GET = control
    return server


base.model_worker.make_server = instrument
if __name__ == '__main__': base.model_worker.main()
