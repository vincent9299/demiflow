"""Run-scoped admission, host pacing and fatal service circuit boundaries."""
import asyncio
import time
from contextlib import asynccontextmanager


class ServiceStopped(RuntimeError):
    """Fatal service state: callers must stop dispatch, not turn it into a row verdict."""


async def drain_on_service_stop(awaitable, *, enabled):
    """Finish an already-admitted journaled exchange on cooperative stream stop.

    Ordinary caller cancellation still aborts promptly. The exchange owns its
    deadline and persistence; this helper never admits new work or publishes a
    result after cancellation. Both prompt and embedding actors use this rule.
    """
    task = asyncio.create_task(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        draining = bool(enabled and cancelled.args and isinstance(cancelled.args[0], ServiceStopped))
        if draining:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError as interrupted:
                    if interrupted.args and isinstance(interrupted.args[0], ServiceStopped):
                        continue
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
                except Exception:
                    break
        else:
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise cancelled


class _RequestReservation:
    """Hold capacity and the start order until the actual request begins."""
    def __init__(self, gate, started, *, pace_held=True):
        self.gate, self.started = gate, started
        self.used = False
        self.pace_held = pace_held

    def release_pace(self):
        if self.pace_held:
            self.pace_held = False
            self.gate.pace.release()

    async def __aenter__(self):
        if self.used: raise RuntimeError('Request reservation already consumed')
        self.gate.check()
        self.used = True
        gate = self.gate
        gate.next_start = time.monotonic() + gate.interval_s
        gate.wait_s += time.monotonic() - self.started
        gate.active += 1; gate.admitted += 1; gate.peak = max(gate.peak, gate.active)
        self.release_pace()
        return self

    async def __aexit__(self, *_):
        self.gate.active -= 1


class RequestGate:
    def __init__(self, concurrency, *, failures=5, interval_s=0):
        if concurrency < 1 or failures < 1 or interval_s < 0: raise ValueError('invalid request gate')
        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)
        self.pace = asyncio.Lock()
        self.interval_s, self.failures = interval_s, failures
        self.next_start, self.consecutive, self.stopped = 0., 0, ''
        self.active, self.peak, self.admitted, self.wait_s = 0, 0, 0, 0.

    def begin_action(self):
        """Rebind run-scoped primitives/observations to the new event loop."""
        if self.active: raise RuntimeError('RequestGate is still active in another action')
        self.sem=asyncio.Semaphore(self.concurrency)
        self.pace=asyncio.Lock()
        self.next_start,self.consecutive,self.stopped=0.,0,''
        self.active,self.peak,self.admitted,self.wait_s=0,0,0,0.

    def check(self):
        if self.stopped: raise ServiceStopped(self.stopped)

    def result(self, *, success=False, transient=False, fatal='', elapsed_s=None, backpressure=False):
        if fatal: self.stopped = fatal
        elif success: self.consecutive = 0
        elif transient or backpressure:
            self.consecutive += 1
            if self.consecutive >= self.failures: self.stopped = 'service_consecutive_failure_limit'
        self.check()

    @asynccontextmanager
    async def reserve(self):
        """Wait before starting a worker deadline; count only actual admission.

        The consumer enters the returned permit immediately before HTTP. A
        cancelled reservation releases both locks without spending an attempt.
        Closing this context releases capacity, even if no HTTP was needed.
        """
        started = time.monotonic()
        self.check()
        async with self.sem:
            self.check()
            # With pacing disabled, independent preparations must not serialize
            # behind a start-order lock. Capacity still bounds both preparation
            # and the actual request. Paced gates retain ordered starts.
            paced = self.interval_s > 0
            if paced:
                await self.pace.acquire()
            permit = _RequestReservation(self, started, pace_held=paced)
            try:
                if self.interval_s:
                    await asyncio.sleep(max(0, self.next_start-time.monotonic()))
                self.check()
                yield permit
            finally:
                permit.release_pace()

    @asynccontextmanager
    async def enter(self):
        async with self.reserve() as permit:
            async with permit:
                yield


class LatencySummary:
    """Bounded histogram; quantiles are upper bounds in 0.25-second buckets."""
    def __init__(self):
        self.buckets={};self.count=0;self.total=0.
    def observe(self,seconds):
        import math
        # Values above one day occupy one overflow bucket, explicitly labelled.
        bucket=min(345601,max(0,math.ceil(seconds*4)))
        self.buckets[bucket]=self.buckets.get(bucket,0)+1
        self.count+=1;self.total+=seconds
    def summary(self):
        import math
        result={'count':self.count,'total_s':self.total,'bucket_s':.25,'overflow_count':self.buckets.get(345601,0)}
        for label,p in [('p50_upper_s',.5),('p95_upper_s',.95)]:
            cumulative=0;result[label]=None
            for bucket,n in sorted(self.buckets.items()):
                cumulative+=n
                if cumulative>=math.ceil(self.count*p):
                    result[label]=bucket/4 if bucket<345601 else None;break
        return result


class ObservedQueue(asyncio.Queue):
    def __init__(self,depth):
        super().__init__(depth);self.peak=0;self.wait=LatencySummary()
    def put_nowait(self,item):
        super().put_nowait((time.monotonic(),item));self.peak=max(self.peak,self.qsize())
    def get_nowait(self):
        queued,item=super().get_nowait()
        from demiflow.execution.stream import SENTINEL
        if item is not SENTINEL:self.wait.observe(time.monotonic()-queued)
        return item
