"""Action-owned, reusable subprocesses for pure bounded CPU/file operations.

Functions (including a tokenizer) are serialized once per worker generation.
Admission precedes serialization. Frames are byte capped; this is not an RSS
limit. Callers must bound arguments, decoded objects and function allocations.
Timeout/cancellation kills the leased process group; no operation is retried.
"""
import asyncio
import os
import signal
import struct
import sys
import time

import cloudpickle


class IsolatedWorkerPool:
    def __init__(self, functions, *, workers, max_message_bytes=64*1024*1024, max_tasks=128):
        if not functions or not all(isinstance(k, str) and callable(v) for k, v in functions.items()):
            raise ValueError('functions must be a nonempty named callable mapping')
        if any(type(v) is not int or v < 1 for v in (workers, max_message_bytes, max_tasks)):
            raise ValueError('worker and message limits must be positive integers')
        self.functions, self.limit, self.max_tasks = functions, max_message_bytes, max_tasks
        self.slots = asyncio.Queue(workers)
        self.workers = [{'process': None, 'tasks': 0} for _ in range(workers)]
        for worker in self.workers:
            self.slots.put_nowait(worker)
        self.closed = False
        self.initialization = None
        self.metrics = dict(starts=0, calls=0, failures=0, timeouts=0, sent_bytes=0,
                            received_bytes=0, wait_s=0., operation_s=0., peak_active=0)
        self.active = 0

    def snapshot_metrics(self):
        return dict(self.metrics)

    def _encode(self, value):
        payload = cloudpickle.dumps(value)
        if len(payload) > self.limit:
            raise ValueError('Isolated worker message exceeds byte limit')
        return payload

    async def _send(self, process, payload):
        process.stdin.write(struct.pack('!Q', len(payload)))
        # Bound the transport's pending buffer even for large initialization.
        for offset in range(0, len(payload), 65536):
            process.stdin.write(payload[offset:offset+65536])
            await process.stdin.drain()
        self.metrics['sent_bytes'] += len(payload)

    async def _exchange(self, worker, name, args, kwargs):
        if worker['process'] is None:
            if self.initialization is None:
                self.initialization = self._encode(self.functions)
            launch = asyncio.create_task(asyncio.create_subprocess_exec(
                sys.executable, '-m', __name__, str(self.limit),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                start_new_session=True))
            try:
                worker['process'] = await asyncio.shield(launch)
            except asyncio.CancelledError:
                # A cancelled launch may already own a live child. Record it
                # before run() reaps the leased worker in its finally block.
                while not launch.done():
                    try:
                        await asyncio.shield(launch)
                    except asyncio.CancelledError:
                        continue
                worker['process'] = launch.result()
                raise
            self.metrics['starts'] += 1
            await self._send(worker['process'], self.initialization)
        process = worker['process']
        await self._send(process, self._encode((name, args, kwargs)))
        length = struct.unpack('!Q', await process.stdout.readexactly(8))[0]
        if length > self.limit:
            raise ValueError('Isolated worker response exceeds byte limit')
        payload = await process.stdout.readexactly(length)
        self.metrics['received_bytes'] += length
        ok, result = cloudpickle.loads(payload)
        worker['tasks'] += 1
        if not ok:
            raise result
        return result

    async def _stop(self, worker):
        process = worker['process']
        if process is not None:
            # Pure operations own no durable commits. Reap before releasing slot.
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            await process.wait()
            worker.update(process=None, tasks=0)

    async def run(self, name, *args, timeout_s, **kwargs):
        if name not in self.functions or timeout_s <= 0:
            raise ValueError('Unknown operation or invalid deadline')
        if self.closed:
            raise RuntimeError('Isolated worker pool is closed')
        waited = time.monotonic()
        worker = await self.slots.get()
        self.metrics['wait_s'] += time.monotonic() - waited
        started = time.monotonic()
        self.active += 1
        self.metrics['peak_active'] = max(self.metrics['peak_active'], self.active)
        try:
            if self.closed:
                raise RuntimeError('Isolated worker pool is closed')
            self.metrics['calls'] += 1
            return await asyncio.wait_for(self._exchange(worker, name, args, kwargs), timeout_s)
        except BaseException as exc:
            self.metrics['failures'] += 1
            self.metrics['timeouts'] += isinstance(exc, TimeoutError)
            raise
        finally:
            # An interrupted/corrupt exchange cannot be reused. Completed task
            # errors also replace the worker, keeping the failure boundary simple.
            if sys.exc_info()[0] is not None or worker['tasks'] >= self.max_tasks:
                cleanup = asyncio.create_task(self._stop(worker))
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                cleanup.result()
            self.metrics['operation_s'] += time.monotonic() - started
            self.active -= 1
            self.slots.put_nowait(worker)

    async def aclose(self):
        self.closed = True
        await asyncio.gather(*(self._stop(w) for w in self.workers))
        self.initialization = None


def _main():
    if sys.platform.startswith('linux'):
        import ctypes
        parent = os.getppid()
        if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL) != 0:
            raise OSError(ctypes.get_errno(), 'Unable to set worker parent-death signal')
        if parent == 1 or os.getppid() != parent:
            return
    limit = int(sys.argv[1])
    source, target = sys.stdin.buffer, sys.stdout.buffer
    sys.stdout = sys.stderr  # Function/library diagnostics cannot corrupt framing.
    def read():
        header = source.read(8)
        if not header:
            raise EOFError
        if len(header) != 8:
            raise ValueError('Truncated worker frame')
        length = struct.unpack('!Q', header)[0]
        if length > limit:
            raise ValueError('Worker frame exceeds byte limit')
        payload = source.read(length)
        if len(payload) != length:
            raise ValueError('Truncated worker payload')
        return cloudpickle.loads(payload)
    functions = read()
    while True:
        try:
            name, args, kwargs = read()
        except EOFError:
            return
        try:
            result = (True, functions[name](*args, **kwargs))
        except Exception as exc:
            result = (False, exc)
        payload = cloudpickle.dumps(result)
        if len(payload) > limit:
            payload = cloudpickle.dumps((False, ValueError('Worker result exceeds byte limit')))
        target.write(struct.pack('!Q', len(payload)))
        target.write(payload)
        target.flush()


if __name__ == '__main__':
    _main()
