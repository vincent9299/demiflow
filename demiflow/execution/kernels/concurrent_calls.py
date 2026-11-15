"""Bounded invocation slots inside one kernel; no journal or scheduling authority."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import ContextVar
import queue
import threading
from contextvars import copy_context
import time

from ..contracts.dataflow_errors import DataflowError

INVOCATION_SCOPE = ContextVar('dataflow_invocation_scope', default=None)
_END = object()


class ConcurrentCalls:
    """At most N inputs, N single-record outputs, and N active calls.

    Completed results drain without waiting for slower calls. Slow consumers backpressure the producing
    invocation itself; a generator result is never collected into a list.
    """
    def __init__(self, count, *, check_stop):
        self.count = count
        self.check_stop = check_stop
        self.slots = deque(range(count))
        self.jobs = deque()
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=count, thread_name_prefix='demiflow-kernel-call')

    @property
    def pending(self):
        return bool(self.jobs)

    def submit(self, compute, *, scope, input=None):
        while not self.slots:
            yield from self.poll(wait_for=True, max_records=1)
        slot = self.slots.popleft()
        self._launch(slot, compute, input=input, scope=scope(slot), emitted=0)
        yield from self.poll(max_records=1)

    def _launch(self, slot, compute, *, input, scope, emitted):
        output = queue.Queue(maxsize=1)
        metadata = {'input': input, 'scope': scope, 'emitted': emitted}
        def send(value):
            while not self.stop.is_set():
                try:
                    output.put(value, timeout=.05)
                    return
                except queue.Full:
                    pass
        def run():
            token = INVOCATION_SCOPE.set(scope)
            iterator = None
            try:
                iterator = iter(compute(slot))
                for index, record in enumerate(iterator):
                    if self.stop.is_set():
                        break
                    self.check_stop()
                    if index >= emitted:
                        send(('row', record))
            except BaseException as exc:
                send(('error', exc))
            finally:
                try:
                    close = getattr(iterator, 'close', None)
                    if close is not None:
                        close()
                except BaseException as exc:
                    send(('error', exc))
                finally:
                    INVOCATION_SCOPE.reset(token)
                    send(_END)
        future = self.pool.submit(copy_context().run, run)
        from ...resources.capacity import track_owned_thread
        for thread in tuple(self.pool._threads):
            track_owned_thread(thread)
        self.jobs.append((slot, output, future, metadata))

    def snapshot(self):
        # Only the business thread changes membership/delivery counters. Futures
        # may finish concurrently, but their unconsumed results stay in queues.
        return [{'slot': slot, **metadata} for slot, _, _, metadata in self.jobs]

    def restore(self, images, *, slots, compute):
        from ..contracts.dataflow import Record
        if self.jobs or not isinstance(images, list) or not isinstance(slots, list):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Invalid invocation snapshot')
        occupied = []
        for item in images:
            if type(item) is not dict or set(item) != {'slot', 'input', 'scope', 'emitted'}:
                raise DataflowError('CORRUPT_ARTIFACT', 'Invalid pending invocation')
            slot, scope, emitted, input = (item[k] for k in ('slot', 'scope', 'emitted', 'input'))
            if (type(slot) is not int or type(emitted) is not int or emitted < 0
                    or type(scope) is not str or len(scope) != 64
                    or any(c not in '0123456789abcdef' for c in scope)
                    or not (isinstance(input, Record) or type(input) is tuple
                        and input and all(isinstance(row, Record) for row in input))):
                raise DataflowError('CORRUPT_ARTIFACT', 'Invalid invocation identity or input')
            occupied.append(slot)
        if (any(type(slot) is not int for slot in slots)
                or sorted(slots + occupied) != list(range(self.count))):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Invocation slot membership differs')
        self.slots = deque(slots)
        for item in images:
            self._launch(item['slot'], lambda slot, input=item['input']: compute(slot, input),
                         input=item['input'], scope=item['scope'], emitted=item['emitted'])

    def poll(self, *, wait_for=False, max_records=1):
        emitted = 0
        while self.jobs and emitted < max_records:
            self.check_stop()
            available = False
            for job in tuple(self.jobs):
                slot, output, future, metadata = job
                try:
                    value = output.get_nowait()
                except queue.Empty:
                    continue
                available = True
                if value is _END:
                    self.jobs.remove(job)
                    self.slots.append(slot)
                    # Refill released capacity before blocking on another call.
                    return
                if value[0] == 'error':
                    raise value[1]
                emitted += 1
                metadata['emitted'] += 1
                yield value[1]
                if emitted >= max_records:
                    return
            if not available:
                if not wait_for:
                    return
                time.sleep(.005)

    def finish(self):
        while self.jobs:
            yield from self.poll(wait_for=True, max_records=1)

    def close(self, *, timeout):
        self.stop.set()
        futures = [future for _, _, future, _ in self.jobs]
        self.pool.shutdown(wait=False, cancel_futures=True)
        _, pending = wait(futures, timeout=timeout) if futures else ((), ())
        if pending:
            raise DataflowError('DEADLINE_EXCEEDED', 'Kernel invocations did not drain before actor cleanup')
        self.jobs.clear()
