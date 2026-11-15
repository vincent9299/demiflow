"""Bounded invocation slots inside one kernel; no journal or scheduling authority."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import ContextVar
import queue
import threading
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

    def submit(self, compute, *, scope):
        while not self.slots:
            yield from self.poll(wait_for=True, max_records=1)
        slot = self.slots.popleft()
        output = queue.Queue(maxsize=1)
        invocation_scope = scope(slot)
        def send(value):
            while not self.stop.is_set():
                try:
                    output.put(value, timeout=.05)
                    return
                except queue.Full:
                    pass
        def run():
            token = INVOCATION_SCOPE.set(invocation_scope)
            iterator = None
            try:
                iterator = iter(compute(slot))
                for record in iterator:
                    if self.stop.is_set():
                        break
                    self.check_stop()
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
        future = self.pool.submit(run)
        self.jobs.append((slot, output, future))
        yield from self.poll(max_records=1)

    def poll(self, *, wait_for=False, max_records=1):
        emitted = 0
        while self.jobs and emitted < max_records:
            self.check_stop()
            available = False
            for job in tuple(self.jobs):
                slot, output, future = job
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
        futures = [future for _, _, future in self.jobs]
        self.pool.shutdown(wait=False, cancel_futures=True)
        _, pending = wait(futures, timeout=timeout) if futures else ((), ())
        if pending:
            raise DataflowError('DEADLINE_EXCEEDED', 'Kernel invocations did not drain before actor cleanup')
        self.jobs.clear()
