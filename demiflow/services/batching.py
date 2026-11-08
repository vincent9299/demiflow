"""Bounded service-side microbatching of independent synchronous requests."""
from collections import deque
from concurrent.futures import Future
import json
import threading
import time


DEFAULTS = dict(max_size=8, wait_ms=10, max_pending_rows=64,
                max_pending_bytes=16*1024**2, max_batch_bytes=4*1024**2,
                max_request_bytes=1024**2)


def batching_config(value):
    if not isinstance(value, dict) or set(value) - DEFAULTS.keys():
        raise ValueError('batching requires supported queue limits')
    result = {**DEFAULTS, **value}
    for key, low, high in [('max_size', 2, 64), ('wait_ms', 0, 1000),
                           ('max_pending_rows', 2, 512), ('max_pending_bytes', 1, 64*1024**2),
                           ('max_batch_bytes', 1, 16*1024**2), ('max_request_bytes', 1, 16*1024**2)]:
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise ValueError('Invalid batching.' + key)
    if (result['max_size'] > result['max_pending_rows']
            or not result['max_request_bytes'] <= result['max_batch_bytes'] <= result['max_pending_bytes']):
        raise ValueError('Batching limits must fit the pending request budget')
    return result


class BatchingAdapter:
    """One queue and worker per service. No files, result cache or request journal."""
    parallel = 1

    def __init__(self, backend, configuration):
        if not callable(getattr(backend, 'batch', None)):
            raise ValueError('Service-side batching needs a native batch adapter')
        self.backend = backend
        self.config = batching_config(configuration)
        self.max_request_bytes = self.config['max_request_bytes']
        self.condition = threading.Condition()
        self.queue = deque()
        self.pending_rows = self.pending_bytes = 0
        self.closed = False
        self.batches = self.rows = 0
        self.batch_sizes = {}
        self.worker = threading.Thread(target=self._run, name='service-batching', daemon=False)
        self.worker.start()

    @property
    def failed(self):
        return getattr(self.backend, 'failed', None)

    def single(self, request):
        size = 0
        for part in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(request):
            size += len(part.encode())
            if size > self.max_request_bytes:
                raise ValueError('Request exceeds batching.max_request_bytes')
        future = Future()
        with self.condition:
            if self.closed or self.failed:
                raise RuntimeError('Batch service is closed or failed')
            if (self.pending_rows >= self.config['max_pending_rows']
                    or self.pending_bytes + size > self.config['max_pending_bytes']):
                raise RuntimeError('Batch service pending capacity exhausted')
            self.queue.append((request, size, future, time.monotonic()))
            self.pending_rows += 1
            self.pending_bytes += size
            self.condition.notify()
        value = future.result()
        if 'error' in value:
            error = value['error']
            if error.get('status') == 422:
                raise ValueError(error.get('detail', 'Invalid batch item'))
            raise RuntimeError(error.get('detail', 'Batch item failed'))
        return value

    def _run(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.closed or self.queue)
                if not self.queue:
                    return
                deadline = self.queue[0][3] + self.config['wait_ms']/1000
                while not self.closed and len(self.queue) < self.config['max_size']:
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        break
                    self.condition.wait(remaining)
                group, size = [], 0
                while self.queue and len(group) < self.config['max_size']:
                    if size + self.queue[0][1] > self.config['max_batch_bytes']:
                        break
                    item = self.queue.popleft()
                    group.append(item)
                    size += item[1]
            try:
                values = self.backend.batch([item[0] for item in group])
                if len(values) != len(group):
                    raise RuntimeError('Batch response count differs')
                for item, value in zip(group, values, strict=True):
                    item[2].set_result(value)
            except BaseException as exc:
                for item in group:
                    if not item[2].done():
                        item[2].set_exception(exc)
            finally:
                with self.condition:
                    self.pending_rows -= len(group)
                    self.pending_bytes -= size
                    self.rows += len(group)
                    self.batches += 1
                    self.batch_sizes[len(group)] = self.batch_sizes.get(len(group), 0) + 1

    def batch(self, requests):
        # Explicit validation/batch HTTP calls already form a group. The native
        # backend owns GPU serialization across both routes.
        return self.backend.batch(requests)

    def execution_status(self):
        with self.condition:
            status = dict(mode='service_batching', limits=self.config, pending_rows=self.pending_rows,
                          pending_bytes=self.pending_bytes, batches=self.batches, rows=self.rows,
                          batch_sizes=dict(self.batch_sizes))
        if callable(getattr(self.backend, 'execution_status', None)):
            status['backend'] = self.backend.execution_status()
        return status

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        self.worker.join()
        if callable(getattr(self.backend, 'close', None)):
            self.backend.close()
