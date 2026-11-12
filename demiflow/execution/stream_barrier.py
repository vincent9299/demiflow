"""Ordered stream cuts, one coordinating checkpoint, and explicit admission.

Trigger frequency does not control data admission. Only the unconfirmed input
ledger (bounded rows and logical bytes), channel capacity, and EOF may wait.
"""
import asyncio
import inspect
import time
from collections import deque


class _CheckpointBarrier:
    def __init__(self, stages, queues, commit, *, progress=None, final=False, track=None):
        self.stages, self.queues, self.commit = stages, queues, commit
        self.progress, self.final, self.track = progress, final, track
        self.arrivals = {}
        self.local = [asyncio.Event() for _ in stages]
        self.done = asyncio.Event()
        self.states, self.buffers = {}, {}
        self.snapshot_bytes = 0
        self.task, self.error = None, None
        self.status = 'created'
        self.started = time.monotonic()
        self.stats = getattr(progress, 'stats', None)
        self.count('created')
        timeout = progress.run.limits['checkpoint_timeout_s'] if progress is not None else None
        self.timer = (asyncio.get_running_loop().call_later(timeout, self.fail,
                      TimeoutError('Checkpoint coordination deadline exceeded'))
                      if timeout is not None else None)

    def count(self, name):
        if self.stats is not None:
            metrics = self.stats.metrics.setdefault('checkpoints', {})
            metrics[name] = metrics.get(name, 0) + 1

    @staticmethod
    def tokens(stage):
        return 1 if getattr(stage, "native_snapshot", False) else stage.concurrency

    def fail(self, error):
        if self.done.is_set():
            return
        self.error = error
        self.status = 'cancelled' if isinstance(error, asyncio.CancelledError) else 'failed'
        self.count(self.status)
        if self.timer is not None:
            self.timer.cancel()
        for event in self.local:
            event.set()
        self.done.set()

    def check(self):
        if self.error is not None:
            raise self.error

    async def arrive(self, stage, batch=None):
        self.check()
        index = self.stages.index(stage)
        self.status = 'aligning'
        self.arrivals[index] = self.arrivals.get(index, 0) + 1
        try:
            if self.arrivals[index] == self.tokens(stage):
                if self.progress is not None:
                    started = time.monotonic()
                    await self.progress.align(index, self, batch)
                    if self.stats is not None:
                        self.stats.observe_duration('checkpoint.snapshot', time.monotonic() - started)
                        self.stats.observe_duration('checkpoint.arrival', started - self.started)
                self.check()
                if index + 1 < len(self.stages):
                    for _ in range(self.tokens(self.stages[index + 1])):
                        await self.queues[index + 1].put(self)
                        self.check()
                else:
                    publication = self.progress.seal(self) if self.progress is not None else self.commit
                    self.status = 'sealed'
                    self.count('sealed')
                    async def publish():
                        try:
                            started = time.monotonic()
                            self.check()
                            result = publication()
                            if inspect.isawaitable(result):
                                await result
                            self.check()
                            self.status = 'published'
                            self.count('published')
                            if self.stats is not None:
                                self.stats.observe_duration('checkpoint.publish', time.monotonic() - started)
                            if self.timer is not None:
                                self.timer.cancel()
                            self.done.set()
                        except BaseException as error:
                            self.fail(error)
                            raise
                    self.task = asyncio.create_task(publish(), name='demiflow-checkpoint')
                    if self.track is not None:
                        self.track(self.task)
                ends = (self.progress.group_ends if self.progress is not None
                        else range(len(self.stages)))
                for member, end in enumerate(ends):
                    if end == index:
                        self.local[member].set()
            await self.local[index].wait()
            self.check()
        except BaseException as error:
            self.fail(error)
            raise

    async def wait(self):
        await self.done.wait()
        self.check()
        if self.task is not None:
            await self.task


class CheckpointControl:
    """The source and timer serialize insertion through the same channel gate."""
    def __init__(self, progress, stages, queues, track):
        self.progress, self.stages, self.queues, self.track = progress, stages, queues, track
        self.gate = asyncio.Lock()
        self.changed, self.capacity, self.finished = (asyncio.Event() for _ in range(3))
        self.pending = None
        self.unconfirmed = deque()
        self.bytes = 0
        self.last_ordinal = progress.inputs
        self.next_tick = time.monotonic() + self.interval
        self.final_requested = False
        self.error = None
        self.capacity_requested = False

    @property
    def interval(self):
        return self.progress.run.limits['checkpoint_interval_ms'] / 1000

    def check(self):
        if self.error is not None:
            raise self.error
        if self.pending is not None:
            self.pending.check()

    def fail(self, error):
        if self.error is None:
            self.error = error
        if self.pending is not None:
            self.pending.fail(self.error)
        self.changed.set()
        self.capacity.set()
        self.finished.set()

    async def admit(self, size):
        limits = self.progress.run.limits
        if size > limits['checkpoint_unconfirmed_bytes']:
            raise ValueError('Source row exceeds unconfirmed byte budget')
        while (len(self.unconfirmed) >= limits['checkpoint_unconfirmed_rows'] or
               self.bytes + size > limits['checkpoint_unconfirmed_bytes']):
            self.check()
            self.capacity.clear()
            self.capacity_requested = True
            self.changed.set()
            await self.capacity.wait()
        self.check()

    def accepted(self, ordinal, size):
        self.unconfirmed.append((ordinal, size))
        self.bytes += size
        if ordinal - self.last_ordinal >= self.progress.batch_rows:
            self.changed.set()

    async def finish(self):
        self.final_requested = True
        self.changed.set()
        await self.finished.wait()
        self.check()

    async def run(self):
        try:
            while True:
                self.check()
                self.changed.clear()
                if self.pending is not None:
                    # Only the controller waits for publication; the source has
                    # its own admission ledger and can cross further triggers.
                    await self.pending.wait()
                    while self.unconfirmed and self.unconfirmed[0][0] <= self.progress.inputs:
                        _, amount = self.unconfirmed.popleft()
                        self.bytes -= amount
                    self.capacity_requested = False
                    self.capacity.set()
                    if self.pending.final:
                        self.finished.set()
                        return
                    self.pending = None
                count = self.progress.ordinal - self.last_ordinal
                replay_cut = getattr(self.progress, 'pending_cut', None)
                due = time.monotonic() >= self.next_tick
                # A capacity waiter may need a cut before the row threshold.
                limits = self.progress.run.limits
                full = (len(self.unconfirmed) >= limits['checkpoint_unconfirmed_rows'] or
                        self.capacity_requested)
                trigger = ((self.progress.ordinal >= replay_cut or full) if replay_cut is not None
                           else count and (count >= self.progress.batch_rows or due or full))
                if self.final_requested or trigger:
                    # Start the deadline before waiting for a blocked source put.
                    barrier = _CheckpointBarrier(self.stages, self.queues, None,
                        progress=self.progress, final=self.final_requested, track=self.track)
                    self.pending = barrier
                    async with self.gate:
                        self.check()
                        barrier.next_ordinal = self.progress.ordinal
                        source = getattr(self.progress, 'source', None)
                        if replay_cut is None and source is not None and barrier.next_ordinal == source['data']['rows']:
                            # The immutable source snapshot proves exhaustion.
                            # Drain native calls/tail buffers at this cut instead
                            # of publishing another empty checkpoint at EOF.
                            barrier.final = True
                        self.last_ordinal = barrier.next_ordinal
                        self.progress.pending_cut = None
                        for _ in range(barrier.tokens(self.stages[0])):
                            await self.queues[0].put(barrier)
                            self.check()
                    self.next_tick = time.monotonic() + self.interval
                    continue
                timeout = max(.001, self.next_tick - time.monotonic())
                if due:
                    self.next_tick = time.monotonic() + self.interval
                    timeout = self.interval
                try:
                    await asyncio.wait_for(self.changed.wait(), timeout)
                except asyncio.TimeoutError:
                    pass
        except BaseException as error:
            self.fail(error)
            raise
