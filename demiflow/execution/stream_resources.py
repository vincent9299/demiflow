"""Action-scoped lifecycle and observations for native streaming Dataset nodes."""
import inspect
from .request_limits import LatencySummary


class CallMetrics:
    def __init__(self): self.reset()
    def reset(self):
        self.counts = dict(call_records=0,reused=0,input_tokens=0,output_tokens=0,records_without_usage=0,errors=0)
        self.latency = LatencySummary()
        self.stream_timings = {name: LatencySummary() for name in ('first_byte_s', 'first_event_s', 'first_content_s', 'max_body_gap_s')}
    def observe(self, call, *, error=False):
        self.counts['errors'] += int(error)
        if not call: return
        self.counts['call_records'] += 1
        reused = bool(call.get('reused'))
        self.counts['reused'] += int(reused)
        usage = call.get('usage') or {}
        self.counts['records_without_usage'] += int(not usage)
        if not reused:
            self.counts['input_tokens'] += usage.get('prompt_tokens', usage.get('input_tokens',0)) or 0
            self.counts['output_tokens'] += usage.get('completion_tokens', usage.get('output_tokens',0)) or 0
            if call.get('elapsed_s') is not None: self.latency.observe(call['elapsed_s'])
            for name, histogram in self.stream_timings.items():
                value = (call.get('stream') or {}).get(name)
                if value is not None: histogram.observe(value)
    def summary(self):
        return {**self.counts,'latency':self.latency.summary(),
                'stream_timings':{name:histogram.summary() for name,histogram in self.stream_timings.items()}}


class StreamResources:
    def __init__(self, actors):
        self.actors = list(dict((id(a),a) for a in actors).values())
        resources = [r for a in self.actors for r in getattr(a,'resources',())]
        self.resources = list(dict((id(r),r) for r in resources).values())
        from ..collect.client_scope import ClientScope
        self.clients = ClientScope()
        targets = [a.uri for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(targets)) != len(targets): raise ValueError('Duplicate save_lance target in stream')
        names = [a.stage for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(names)) != len(names): raise ValueError('Duplicate save_lance stage name')

    async def prepare(self):
        with self.clients.active():
            from ..collect.search_contract import is_native_search_actor
            searches = {id(actor) for actor in self.actors if is_native_search_actor(actor)}
            if searches:
                from demiflow.execution.operator_bindings.dataflow_search_runtime import SearchEffects
                from demiflow.execution.native_queue.queue_producer import QueueProducer
                from .run_context import current_action
                producer = next((a for a in self.actors if type(a) is QueueProducer), None)
            gates={id(gate):gate for actor in self.actors
                   if (gate:=getattr(getattr(actor,'_runtime',None),'request_gate',None)) is not None}
            for gate in gates.values():
                reset=getattr(gate,'begin_action',None)
                if reset is not None: reset()
            for actor in self.actors:
                begin = getattr(actor, 'astart', None)
                if begin is not None:
                    from .checked import current
                    checked = current()
                    completed = False
                    try:
                        if id(actor) in searches:
                            effects = SearchEffects.for_actor(current_action(), actor, actor.node_id, producer=producer)
                            actor.bind_search_effects(effects)
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
                from .checked import current
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
