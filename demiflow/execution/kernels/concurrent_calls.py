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

    With a byte window, a call is admitted only while the window holds its charge: the input's actual
    encoded bytes plus ``output_reserve`` for the one output it may hold. A call that does not fit
    waits for earlier calls to finish; the window always holds one call on its own.
    """
    def __init__(self, count, *, check_stop, byte_limit=None, output_reserve=0, clock=None):
        self.count = count
        # Optional node busy clock: busy while any invocation is in flight.
        # Membership changes only on the business thread (launch/poll/close).
        self.clock = clock
        self.check_stop = check_stop
        self.byte_limit = byte_limit
        self.output_reserve = output_reserve
        self.bytes_in_flight = 0
        self._charges = {}
        # Attempt-local observations, changed only on the business thread.
        self.peak = 0
        self.byte_waits = 0
        self.slots = deque(range(count))
        self.jobs = deque()
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=count, thread_name_prefix='demiflow-kernel-call')

    @property
    def pending(self):
        return bool(self.jobs)

    def concurrency(self):
        """Attempt-local facts: declared slots, most calls in flight, admissions that waited for bytes."""
        return {'capacity': self.count, 'peak': self.peak, 'byte_waits': self.byte_waits,
                'byte_limit': self.byte_limit, 'output_reserve': self.output_reserve}

    def _charge(self, input):
        return input.encoded_bytes + self.output_reserve if self.byte_limit is not None else 0

    def submit(self, compute, *, scope, input=None):
        charge = self._charge(input)
        if self.byte_limit is not None and charge > self.byte_limit:
            raise DataflowError('BUDGET_EXCEEDED', 'call input exceeds the sealed worker call window')
        waited = False
        while True:
            short = self.byte_limit is not None and self.bytes_in_flight + charge > self.byte_limit
            if self.slots and not short:
                break
            if self.slots and not waited:
                # A free slot, but earlier calls hold the bytes: the window, not
                # the declared concurrency, is what limits this admission.
                self.byte_waits += 1
                waited = True
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
        if self.clock is not None and not self.jobs:
            self.clock.begin()
        self.jobs.append((slot, output, future, metadata))
        self.peak = max(self.peak, len(self.jobs))
        if self.byte_limit is not None:
            self._charges[slot] = self._charge(input)
            self.bytes_in_flight += self._charges[slot]

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
        if self.byte_limit is not None and (any(not isinstance(item['input'], Record) for item in images)
                or sum(self._charge(item['input']) for item in images) > self.byte_limit):
            raise DataflowError('CORRUPT_ARTIFACT', 'pending call inputs exceed their sealed worker window')
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
                    if self.clock is not None and not self.jobs:
                        self.clock.end()
                    if self.byte_limit is not None:
                        self.bytes_in_flight -= self._charges.pop(slot)
                    self.slots.append(slot)
                    # Refill released capacity before blocking on another call.
                    return
                if value[0] == 'error':
                    raise value[1]
                if self.byte_limit is not None and self.output_reserve:
                    # The window admitted this call for its input and reserved
                    # output; an output beyond that would use unadmitted memory.
                    size, charge = getattr(value[1], 'encoded_bytes', 0), self._charges[slot]
                    if size > charge:
                        raise DataflowError('BUDGET_EXCEEDED',
                            f'a call returned a {size}-byte row; its window reserved {charge} bytes '
                            f'for its input and declared output')
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
        if self.clock is not None and self.jobs:
            self.clock.end()
        self.jobs.clear()
        self._charges.clear()
        self.bytes_in_flight = 0
