"""Reusable local queue consumer, isolated named pools and escort snapshots."""
from dataclasses import dataclass, field
import threading
import time


def run_worker(queue, consume, verify, *, pool='default', batch_size=8,
               backoff_s=60, heartbeat_s=10, stop=None, existing=None, idle_s=1,
               exit_when_idle=False, actual_cost=None):
    """Consume task payloads; complete only after the artifact validator succeeds.

    consume receives TaskHandle so it can use task_id for content-addressed output.
    existing(handle) can return an already durable result to avoid network work.
    Neither a failed consumer nor a false validator produces a done record.
    """
    if heartbeat_s <= 0 or idle_s <= 0:
        raise ValueError('worker intervals must be positive')
    stop = stop or threading.Event()
    worker = queue.register_worker(pool)
    finished, errors = threading.Event(), []
    handles = []

    def pulse():
        while not finished.wait(heartbeat_s):
            try:
                queue.heartbeat(worker)
            except Exception as error:
                errors.append(error)
                stop.set()
                return

    thread = threading.Thread(target=pulse, daemon=True)
    thread.start()
    try:
        while not stop.is_set():
            handles = queue.claim(worker, limit=batch_size)
            if not handles:
                if exit_when_idle:
                    break
                stop.wait(idle_s)
                continue
            while handles and not stop.is_set():
                handle = handles[0]
                try:
                    result = existing(handle) if existing else None
                    if result is None or not verify(result):
                        result = consume(handle)
                    if not verify(result):
                        raise ValueError('Artifact validation failed')
                except Exception as error:
                    handles.pop(0)
                    queue.fail(handle, error, backoff_s=backoff_s)
                else:
                    # A commit error is uncertain: do not turn it into a retry.
                    handles.pop(0)
                    cost = actual_cost(handle, result) if actual_cost else None
                    queue.complete(handle, result, actual_cost=cost)
            if errors:
                raise errors[0]
    finally:
        finished.set()
        thread.join()
        for handle in handles:
            # Only unprocessed claims are released. An uncertain complete may
            # already have committed; ownership checks protect that response.
            try:
                queue.fail(handle, 'worker stopped', backoff_s=backoff_s)
            except ValueError:
                pass
        try:
            queue.unregister_worker(worker)
        except ValueError:
            # An uncertain commit left a claim; keep it for explicit inspection
            # or dead-process recovery, never release it as unprocessed work.
            pass


@dataclass
class PoolPolicy:
    minimum: int = 1
    maximum: int = 40
    step: int = 1
    target: int = 1
    previous_rate: float | None = None
    direction: int = 1

    def __post_init__(self):
        if not 1 <= self.minimum <= self.target <= self.maximum or self.step < 1:
            raise ValueError('Invalid pool limits')

    def observe(self, rate):
        if rate < 0:
            raise ValueError('Rate must be nonnegative')
        if self.previous_rate is not None and rate < self.previous_rate * 0.95:
            self.direction *= -1
        self.previous_rate = rate
        self.target = min(self.maximum, max(self.minimum, self.target + self.direction * self.step))
        return self.target


class NamedPoolSupervisor:
    """Each named pool scales only its own workers; tick replaces exited threads.

    Supervision is cooperative: consumers need finite I/O timeouts. For untrusted
    or unbounded consumers run each supervisor in its own managed process.
    """
    def __init__(self, queue, consume, verify, pools, *, snapshot_path=None,
                 snapshot_interval_s=1200, worker_options=None):
        if snapshot_interval_s <= 0:
            raise ValueError('snapshot_interval_s must be positive')
        self.queue, self.consume, self.verify = queue, consume, verify
        self.pools = dict(pools)
        if len({id(p) for p in self.pools.values()}) != len(self.pools):
            raise ValueError('Pool policies must be independent objects')
        self.workers = {name: [] for name in self.pools}
        self.snapshot_path, self.snapshot_interval_s = snapshot_path, snapshot_interval_s
        self.worker_options = dict(worker_options or {})
        if {'pool', 'stop'} & self.worker_options.keys():
            raise ValueError('Supervisor owns pool and stop')
        self.last_snapshot = time.monotonic()
        self.errors = []

    def _run(self, name, stop):
        try:
            run_worker(self.queue, self.consume, self.verify, pool=name, stop=stop, **self.worker_options)
        except Exception as error:
            self.errors.append({'pool': name, 'error': repr(error)})

    def tick(self, rates=None):
        self.queue.reclaim()
        for name, policy in self.pools.items():
            if rates and name in rates:
                policy.observe(rates[name])
            workers = [(thread, stop) for thread, stop in self.workers[name] if thread.is_alive()]
            active = [(thread, stop) for thread, stop in workers if not stop.is_set()]
            for _, stop in active[policy.target:]:
                stop.set()
            for _ in range(max(0, policy.target - len(active))):
                stop = threading.Event()
                thread = threading.Thread(target=self._run, args=(name, stop), daemon=True)
                thread.start()
                workers.append((thread, stop))
            self.workers[name] = workers
        if self.snapshot_path and time.monotonic() - self.last_snapshot >= self.snapshot_interval_s:
            self.queue.backup(self.snapshot_path)
            self.last_snapshot = time.monotonic()
        return {'queue': self.queue.snapshot(), 'pools': {name: policy.target for name, policy in self.pools.items()},
                'errors': list(self.errors)}

    def close(self):
        for workers in self.workers.values():
            for _, stop in workers:
                stop.set()
        for workers in self.workers.values():
            for thread, _ in workers:
                thread.join()
        if self.snapshot_path:
            self.queue.backup(self.snapshot_path)
