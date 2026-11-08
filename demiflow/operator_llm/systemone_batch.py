"""Bounded transport coalescing. Each caller retains its ordinary row journal."""
import asyncio
from collections import deque
from dataclasses import dataclass
import json
import math


def encode_payload(value, limit):
    # Walk before encoding so a single giant string/container cannot cause an
    # unbounded JSONEncoder allocation. These limits concern encoded payloads,
    # not a claim that third-party model RSS is bounded by this value.
    remaining=limit
    def inspect(item, depth=0):
        nonlocal remaining
        if depth>32: raise ValueError('Batch request nesting exceeds 32')
        remaining-=1
        if isinstance(item,str): remaining-=len(item)
        elif isinstance(item,dict):
            for key,child in item.items(): inspect(key,depth+1); inspect(child,depth+1)
        elif isinstance(item,(list,tuple)):
            for child in item: inspect(child,depth+1)
        if remaining<0: raise ValueError('Batch request exceeds byte budget')
    inspect(value)
    output=bytearray()
    for chunk in json.JSONEncoder(ensure_ascii=False,allow_nan=False,separators=(',',':')).iterencode(value):
        raw=chunk.encode()
        if len(output)+len(raw)>limit: raise ValueError('Batch request exceeds byte budget')
        output.extend(raw)
    return bytes(output)


def batch_options(value):
    if value is None: return None
    fields = {'max_size','wait_ms','max_queue_rows','max_queue_bytes','max_batch_bytes','parallel'}
    if not isinstance(value,dict) or set(value)-fields:
        raise ValueError('systemone_batch requires a bounded batching policy')
    result = {'max_size':8,'wait_ms':5,'max_queue_rows':64,'max_queue_bytes':16*1024**2,
              'max_batch_bytes':4*1024**2,'parallel':1,**value}
    for key, maximum in [('max_size',64),('max_queue_rows',4096),('max_queue_bytes',64*1024**2),
                         ('max_batch_bytes',16*1024**2),('parallel',64)]:
        if type(result[key]) is not int or not 1<=result[key]<=maximum:
            raise ValueError('Invalid systemone_batch.'+key)
    if (type(result['wait_ms']) not in (int,float) or not math.isfinite(result['wait_ms'])
            or not 0<=result['wait_ms']<=1000):
        raise ValueError('systemone_batch.wait_ms must be 0..1000')
    if result['max_batch_bytes']>result['max_queue_bytes']:
        raise ValueError('Batch bytes must not exceed queue bytes')
    return result


@dataclass
class _Item:
    payload: bytes
    result: asyncio.Future
    arrived: float
    sent: bool = False


class BatchTransport:
    """A transport queue, not an executor: no retries, budgets or journal writes.

    Buffered bytes cover serialized inputs. Upstream node concurrency bounds
    caller objects waiting for admission; one detached response per row remains
    owned by its existing client. In-flight batches are bounded by parallel.
    """
    def __init__(self, options, send):
        self.options, self.send = batch_options(options), send
        self.queue = deque()
        self.bytes = 0
        self.condition = asyncio.Condition()
        self.workers = []
        self.closed = False

    async def submit(self, payload):
        if not isinstance(payload,bytes): raise TypeError('Batch input must be encoded bytes')
        # Include envelope/separator headroom before any combined allocation.
        if len(payload)+1025>self.options['max_batch_bytes']:
            raise ValueError('Single request exceeds systemone_batch.max_batch_bytes')
        loop = asyncio.get_running_loop()
        item = _Item(payload,loop.create_future(),loop.time())
        async with self.condition:
            await self.condition.wait_for(lambda: self.closed or (
                len(self.queue)<self.options['max_queue_rows']
                and self.bytes+len(payload)<=self.options['max_queue_bytes']))
            if self.closed: raise RuntimeError('Batch transport is closed')
            # The batching deadline starts on admission, not when waiting for room.
            item.arrived = loop.time()
            self.queue.append(item); self.bytes += len(payload)
            if not self.workers:
                self.workers = [asyncio.create_task(self._worker()) for _ in range(self.options['parallel'])]
            self.condition.notify_all()
        try:
            return await asyncio.shield(item.result)
        except asyncio.CancelledError as error:
            async with self.condition:
                if not item.sent:
                    error.batch_cancelled_before_dispatch=True
                    try:
                        self.queue.remove(item); self.bytes -= len(payload)
                    except ValueError: pass
                    self.condition.notify_all()
                item.result.cancel()
            raise

    async def _take(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.closed or self.queue)
            if not self.queue: return None
            deadline = self.queue[0].arrived+self.options['wait_ms']/1000
            while not self.closed and len(self.queue)<self.options['max_size']:
                if sum(len(x.payload) for x in self.queue)+1024>=self.options['max_batch_bytes']: break
                remaining=deadline-asyncio.get_running_loop().time()
                if remaining<=0: break
                try: await asyncio.wait_for(self.condition.wait(),remaining)
                except TimeoutError: break
                if not self.queue: return []
            batch=[]; size=1024
            while self.queue and len(batch)<self.options['max_size']:
                candidate=self.queue[0]
                if size+len(candidate.payload)+1>self.options['max_batch_bytes']: break
                item=self.queue.popleft(); self.bytes -= len(item.payload)
                item.sent=True; batch.append(item); size+=len(item.payload)+1
            self.condition.notify_all()
            return batch

    async def _worker(self):
        while True:
            batch=await self._take()
            if batch is None: return
            if not batch: continue
            try:
                results=await self.send([item.payload for item in batch])
                if len(results)!=len(batch): raise ValueError('Batch response count differs')
            except BaseException as exc:
                for item in batch:
                    if not item.result.done():
                        if isinstance(exc,asyncio.CancelledError): item.result.cancel()
                        else:
                            # Row execution attaches its own journal refs/call
                            # metadata to exceptions. Never share that mutable
                            # exception object between independent rows.
                            error=type(exc).__new__(type(exc),*exc.args)
                            error.args=exc.args
                            error.__dict__.update(exc.__dict__)
                            item.result.set_exception(error)
                if isinstance(exc,asyncio.CancelledError): raise
            else:
                for item,result in zip(batch,results,strict=True):
                    if not item.result.done(): item.result.set_result(result)

    async def aclose(self):
        async with self.condition:
            if self.closed: return
            self.closed=True
            for item in self.queue:
                if not item.result.done(): item.result.set_exception(RuntimeError('Batch transport closed before send'))
            self.queue.clear(); self.bytes=0; self.condition.notify_all()
        # Actual HTTP requests have their own finite timeout. Their surviving
        # rows may still persist results; close never launches an extra request.
        await asyncio.gather(*self.workers,return_exceptions=True)
