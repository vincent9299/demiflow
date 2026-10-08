"""Bounded blocking work owned by a model actor/client, never a global queue."""
import asyncio
from concurrent.futures import ThreadPoolExecutor


class BlockingIOPool:
    """Admit before submit; drain started work before cancellation and shutdown.

    Worker count bounds submitted jobs, including payloads retained by jobs.
    Waiting caller payloads remain subject to the operator's concurrency/queues.
    Functions must have finite I/O deadlines and their own allocation budgets.
    """
    def __init__(self, workers, *, name):
        if type(workers) is not int or workers < 1:
            raise ValueError('workers must be a positive integer')
        self.workers, self.name = workers, name
        self._slots = asyncio.Semaphore(workers)
        self._executor = None
        self._closing = False
        self._pending = set()

    async def run(self, fn, *args, **kwargs):
        async with self._slots:
            if self._closing:
                raise RuntimeError('Blocking I/O pool is closed')
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix=self.name)
            future = asyncio.get_running_loop().run_in_executor(self._executor, lambda: fn(*args, **kwargs))
            self._pending.add(future)
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # Repeated cancellation cannot release capacity while a blocking
                # commit/decode still owns the job's buffers or database handle.
                while not future.done():
                    try:
                        await asyncio.shield(future)
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        break
                if not future.cancelled():
                    future.exception()
                raise
            finally:
                self._pending.discard(future)

    async def drain(self):
        if self._pending:
            await asyncio.gather(*(asyncio.shield(f) for f in tuple(self._pending)), return_exceptions=True)

    async def aclose(self):
        self._closing = True
        await self.drain()
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
