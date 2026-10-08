"""Action-scoped lifecycle and observations for native streaming Dataset nodes."""
import inspect
from .request_limits import ObservedQueue, LatencySummary


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
    def __init__(self, actors, queue_factory=None):
        self.actors = list(dict((id(a),a) for a in actors).values())
        resources = [r for a in self.actors for r in getattr(a,'resources',())]
        self.resources = list(dict((id(r),r) for r in resources).values())
        self.queues = []; self.queue_factory = queue_factory
        self.started = []
        targets = [a.uri for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(targets)) != len(targets): raise ValueError('Duplicate save_lance target in stream')
        names = [a.stage for a in self.actors if getattr(a,'is_stream_sink',False)]
        if len(set(names)) != len(names): raise ValueError('Duplicate save_lance stage name')

    def make_queue(self, depth):
        q = self.queue_factory(depth) if self.queue_factory else ObservedQueue(depth)
        self.queues.append(q)
        return q

    async def prepare(self):
        gates={id(gate):gate for actor in self.actors
               if (gate:=getattr(getattr(actor,'_runtime',None),'request_gate',None)) is not None}
        for gate in gates.values():
            reset=getattr(gate,'begin_action',None)
            if reset is not None: reset()
        for actor in self.actors:
            begin = getattr(actor, 'astart', None)
            if begin is not None:
                self.started.append(actor)
                await begin()

    def snapshot(self, stats):
        started_ids={id(a) for a in self.started}
        stats.outputs = {a.stage:a.reference() for a in self.actors
                         if id(a) in started_ids and getattr(a,'is_stream_sink',False) and a.version is not None}
        stats.metrics = {
            'stages':stats.stages, 'stage_processing_latency':stats.timing_summary(),
            'stage_policies': stats.stage_policies,
            'queues':[{'stage':name, 'capacity':q.maxsize, 'peak':getattr(q,'peak',None),
                       'wait':q.wait.summary() if hasattr(q,'wait') else None}
                      for name,q in zip(stats.stages,self.queues)],
            'resources':{f'{type(r).__name__}:{i}':r.snapshot_metrics() for i,r in enumerate(self.resources)
                         if callable(getattr(r,'snapshot_metrics',None))},
            'models':{}, 'native_journal_totals':{}, 'native_journal_totals_skipped':[],
            'journal_totals_scope':'Entire journal, including earlier actions; models describes this action only'}
        from demiflow.operator_llm.call_ref import journal_totals
        for index, actor in enumerate(self.actors):
            observation = getattr(actor,'call_metrics',None)
            if observation is not None and id(actor) in started_ids:
                name = f'{actor.label}:{index}'
                stats.metrics['models'][name] = observation.summary()
                gate = actor._runtime.request_gate
                if gate is not None:
                    stats.metrics['models'][name]['shared_admission'] = {'requests':gate.admitted,'peak':gate.peak,'wait_s':gate.wait_s,
                        **({'adaptive':gate.snapshot()} if hasattr(gate,'snapshot') else {})}
                options = actor._runtime.options or {}
                path = (options.get('sqlite_journal') or options.get('offline_store') or {}).get('path')
                if path:
                    if options.get('collect_journal_totals', True):
                        if str(path) not in stats.metrics['native_journal_totals']:
                            stats.metrics['native_journal_totals'][str(path)] = journal_totals(path)
                    else:
                        stats.metrics['native_journal_totals_skipped'].append(
                            {'stage': name, 'path': str(path), 'reason': 'collect_journal_totals=False'})

    async def close(self, *, action_cleanups=True):
        errors = []
        owned=list(dict((id(r),r) for r in self.actors+self.resources).values())
        for resource in reversed(owned):
            close = getattr(resource, 'aclose', None)
            if close is not None:
                try:
                    result = close()
                    if inspect.isawaitable(result): await result
                except Exception as exc: errors.append(exc)
        from .resource_registry import stream_cleanups
        for cleanup in stream_cleanups() if action_cleanups else ():
            try:
                result=cleanup()
                if inspect.isawaitable(result): await result
            except Exception as exc: errors.append(exc)
        if errors: raise ExceptionGroup('Stream resource cleanup failed', errors)
