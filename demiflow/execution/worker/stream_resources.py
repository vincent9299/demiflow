"""Action-scoped lifecycle and observations for native streaming Dataset nodes."""

import asyncio
import time
from demiflow.execution.journal.run_telemetry import LatencySummary
import inspect


class StreamResources:
    def __init__(self, actors):
        self.actors = list(dict((id(a),a) for a in actors).values())
        resources = [r for a in self.actors for r in getattr(a,'resources',())]
        self.resources = list(dict((id(r),r) for r in resources).values())
        from demiflow.collect.client_scope import ClientScope
        self.clients = ClientScope()
        targets = [a.uri for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(targets)) != len(targets): raise ValueError('Duplicate save_lance target in stream')
        names = [a.stage for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(names)) != len(names): raise ValueError('Duplicate save_lance stage name')

    async def prepare(self):
        with self.clients.active():
            from demiflow.execution.operator_bindings.dataflow_search_runtime import prepare_search_bindings
            bindings = prepare_search_bindings(self.actors)
            gates={id(gate):gate for actor in self.actors
                   if (gate:=getattr(getattr(actor,'_runtime',None),'request_gate',None)) is not None}
            for gate in gates.values():
                reset=getattr(gate,'begin_action',None)
                if reset is not None: reset()
            for actor in self.actors:
                begin = getattr(actor, 'astart', None)
                if begin is not None:
                    from demiflow.execution.checked import current
                    checked = current()
                    completed = False
                    try:
                        binding = bindings.get(id(actor))
                        if binding is not None:
                            binding()
                        await begin()
                        completed = True
                    finally:
                        if checked is not None:
                            checked.actor_lifecycle(actor, 'started', completed)

    async def close(self):
        errors = []
        owned=list(dict((id(r),r) for r in self.actors+self.resources).values())
        for resource in reversed(owned):
            close = getattr(resource, 'aclose', None)
            if close is not None:
                from demiflow.execution.checked import current
                checked = current()
                completed = False
                try:
                    with self.clients.active():
                        result = close()
                        if inspect.isawaitable(result): await result
                    completed = True
                except BaseException as exc: errors.append(exc)
                finally:
                    if checked is not None:
                        checked.actor_lifecycle(resource, 'closed', completed)
        try:
            await self.clients.close()
        except BaseException as exc:
            errors.append(exc)
        if errors: raise BaseExceptionGroup('Stream resource cleanup failed', errors)


class ObservedQueue(asyncio.Queue):
    def __init__(self, depth, *, max_item_bytes=None, max_bytes=None):
        if max_item_bytes is not None and (type(max_item_bytes) is not int or max_item_bytes < 1):
            raise ValueError('Queue item byte budget must be positive')
        if max_bytes is not None and (max_item_bytes is None or max_bytes < max_item_bytes):
            raise ValueError('Queue byte capacity must cover one maximum item')
        super().__init__(depth);self.peak=0;self.wait=LatencySummary()
        self.max_item_bytes, self.max_bytes = max_item_bytes, max_bytes
        self.bytes = self.peak_bytes = 0

    def full(self):
        # Reserve a complete item before admission. Workers own dequeued rows;
        # their independent in-flight allowance is admitted with the plan.
        return super().full() or self.max_bytes is not None and self.bytes + self.max_item_bytes > self.max_bytes

    def put_nowait(self,item):
        size = 0
        if self.max_item_bytes is not None:
            from demiflow.data.runtime_contract import check_size
            from demiflow.execution.row_scope import ScopedRow
            size = check_size(item.row if type(item) is ScopedRow else item, self.max_item_bytes)
            if type(item) is ScopedRow:
                size += 256
                if size > self.max_item_bytes:
                    raise ValueError("Native sidecar exceeds queue item budget")
        super().put_nowait((time.monotonic(),item,size))
        self.bytes += size
        self.peak_bytes = max(self.peak_bytes, self.bytes)
        self.peak=max(self.peak,self.qsize())
    def get_nowait(self):
        queued,item,size=super().get_nowait()
        self.bytes -= size
        self.wait.observe(time.monotonic()-queued)
        return item
