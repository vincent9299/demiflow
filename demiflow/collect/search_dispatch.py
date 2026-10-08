"""Single-provider dispatch with independent native pools and durable choices.

Provider choice is transport policy supplied by the caller. This module does
not rewrite queries, infer language, classify content, or fan out one query.
Completed selections survive restarts; uncertain reservations never redispatch.
"""
import asyncio
import copy
import json
import math
import re
import sqlite3
import time

from .native_search.config import digest, runtime_id
from demiflow.execution.request_limits import RequestGate, ServiceStopped


def dispatch_policy(value):
    if value is None:return None
    if not isinstance(value,dict) or set(value)-{'identity','primary_concurrency','primary_weight','backends','failure_limit'}:
        raise ValueError('Invalid search dispatch declaration')
    result={'primary_concurrency':4,'primary_weight':1.,'failure_limit':5,**copy.deepcopy(value)}
    if not isinstance(result.get('identity'),str) or not result['identity'].strip():
        raise ValueError('Search dispatch requires an explicit stable identity')
    entries=result.get('backends')
    if not isinstance(entries,list) or not 1<=len(entries)<=7:
        raise ValueError('Search dispatch requires 1..7 additional backends')
    names={'primary'}
    for entry in [{'name':'primary','concurrency':result['primary_concurrency'],'weight':result['primary_weight']},*entries]:
        if not isinstance(entry,dict):raise ValueError('Invalid search backend')
        if set(entry)-{'name','concurrency','weight','query_pattern','options'}:
            raise ValueError('Unknown search backend field')
        name=entry.get('name')
        if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]*',name):
            raise ValueError('Invalid search backend name')
        if entry is not entries and name!='primary':
            if name in names:raise ValueError('Duplicate search backend')
            names.add(name)
        elif entry in entries:raise ValueError('primary is reserved for the original backend')
        capacity=entry.get('concurrency',1);weight=entry.get('weight',1.)
        if type(capacity) is not int or not 1<=capacity<=256:raise ValueError('Invalid backend concurrency')
        if type(weight) not in (int,float) or not math.isfinite(weight) or not 0<weight<=100:
            raise ValueError('Invalid backend weight')
        if entry.get('query_pattern') is not None:
            if not isinstance(entry['query_pattern'],str):raise ValueError('Invalid backend query pattern')
            re.compile(entry['query_pattern'])
    if type(result['failure_limit']) is not int or result['failure_limit']<1:
        raise ValueError('Invalid dispatch failure limit')
    return result


class SearchDispatcher:
    def __init__(self,*,path,policy,backends):
        self.path=path;self.policy=dispatch_policy(policy);self.backends=backends
        self.condition=asyncio.Condition();self.initialization=asyncio.Lock()
        self.initialized=False;self.closed=False;self.inflight={};self.cursor=0
        self.gate=RequestGate(1,failures=self.policy['failure_limit'])
        self.metrics={'dispatches':0,'reused_dispatch':0,'reused_existing':0}
        self.identity=digest(['native-search-dispatch-1',runtime_id(),self.policy['identity']])
        for backend in self.backends:
            backend['active']=0;backend['dispatched']=0
            semantic=backend['config'].snapshot();semantic.pop('proxy')
            backend['semantic']=digest(semantic)
            backend['pattern']=re.compile(backend['query_pattern']) if backend.get('query_pattern') else None

    def db(self):
        return sqlite3.connect(self.path,timeout=30)

    async def initialize(self):
        async with self.initialization:
            if self.initialized:return
            for backend in self.backends:await backend['native'].initialize()
            def create():
                with self.db() as db:
                    db.execute('CREATE TABLE IF NOT EXISTS native_search_dispatch '
                        '(identity TEXT PRIMARY KEY, dispatcher TEXT NOT NULL, backend TEXT NOT NULL, '
                        'semantic TEXT NOT NULL, state TEXT NOT NULL, value TEXT, observed_at REAL NOT NULL)')
            await asyncio.to_thread(create);self.initialized=True

    def read(self,identity):
        with self.db() as db:
            row=db.execute('SELECT backend,semantic,state,value FROM native_search_dispatch WHERE identity=?',(identity,)).fetchone()
            return dict(zip(('backend','semantic','state','value'),row)) if row else None

    def save(self,identity,backend,value=None):
        with self.db() as db:
            existing=db.execute('SELECT backend,semantic FROM native_search_dispatch WHERE identity=?',(identity,)).fetchone()
            if existing and existing!=(backend['name'],backend['semantic']):
                raise RuntimeError('Search dispatch identity conflict')
            if value is None:
                cursor=db.execute('INSERT OR IGNORE INTO native_search_dispatch VALUES (?,?,?,?,?,?,?)',
                    (identity,self.identity,backend['name'],backend['semantic'],'reserved',None,time.time()))
                if cursor.rowcount!=1:raise ServiceStopped('search_dispatch_already_reserved')
            else:
                db.execute('INSERT INTO native_search_dispatch VALUES (?,?,?,?,?,?,?) '
                    'ON CONFLICT(identity) DO UPDATE SET state=excluded.state,value=excluded.value,observed_at=excluded.observed_at',
                    (identity,self.identity,backend['name'],backend['semantic'],'completed',json.dumps(value,ensure_ascii=False),time.time()))

    def compatible(self,backend,query,parameters):
        pattern=backend['pattern']
        if pattern is not None and not pattern.search(query):return False
        engines=parameters.get('engines')
        if engines is not None:
            names={s['name'] for s in backend['config'].source_configs()}
            return bool(names.intersection(engines))
        return True

    def parameters(self,backend,parameters):
        result=copy.deepcopy(parameters)
        if result.get('engines') is not None:
            names={s['name'] for s in backend['config'].source_configs()}
            result['engines']=[n for n in result['engines'] if n in names]
        return result

    def available(self,backend):
        native=backend['native']
        try:
            native.check_admission();native.query_gate.check()
        except ServiceStopped:return False
        if getattr(native,'policy',None) is None:
            return any(r['until']<=time.time() for r in native.routes)
        return True

    async def choose(self,query,parameters):
        async with self.condition:
            while True:
                if self.closed:raise ServiceStopped('search_dispatch_closed')
                self.gate.check()
                compatible=[(i,b) for i,b in enumerate(self.backends) if self.compatible(b,query,parameters)]
                healthy=[(i,b) for i,b in compatible if self.available(b)]
                if not healthy:raise ServiceStopped('all_search_backends_unavailable')
                ready=[(i,b) for i,b in healthy if b['active']<b['concurrency']]
                if ready:
                    index,backend=min(ready,key=lambda x:(x[1]['active']/x[1]['concurrency']/x[1]['weight'],
                                                         (x[0]-self.cursor)%len(self.backends)))
                    backend['active']+=1;self.cursor=(index+1)%len(self.backends)
                    return backend
                try:await asyncio.wait_for(self.condition.wait(),.5)
                except asyncio.TimeoutError:pass

    async def search(self,query,**parameters):
        if not isinstance(query,str) or not query.strip():raise ValueError('Empty search query')
        identity=digest([self.identity,query,parameters])
        entry=self.inflight.get(identity)
        if entry is None:
            entry={'task':asyncio.create_task(self.execute(identity,query,parameters)),'waiters':0}
            self.inflight[identity]=entry
        entry['waiters']+=1
        try:return copy.deepcopy(await asyncio.shield(entry['task']))
        finally:
            entry['waiters']-=1
            if not entry['waiters']:
                if not entry['task'].done():
                    entry['task'].cancel();await asyncio.gather(entry['task'],return_exceptions=True)
                self.inflight.pop(identity,None)

    async def execute(self,identity,query,parameters):
        await self.initialize()
        saved=await asyncio.to_thread(self.read,identity)
        if saved:
            backend=next((b for b in self.backends if b['name']==saved['backend']),None)
            if backend is None or backend['semantic']!=saved['semantic']:
                raise ValueError('Saved backend semantics changed; declare an explicit new dispatch identity')
            if saved['state']=='completed':
                self.metrics['reused_dispatch']+=1;return json.loads(saved['value'])
            value=await backend['native'].cached_result(query,**self.parameters(backend,parameters))
            if value is None:raise ServiceStopped('search_dispatch_outcome_unknown')
            await asyncio.to_thread(self.save,identity,backend,value)
            self.metrics['reused_dispatch']+=1;return value
        # Existing exact selected results, including failures, win over a new
        # scheduling policy. No HTTP or model payload change during migration.
        for backend in self.backends:
            if not self.compatible(backend,query,parameters):continue
            value=await backend['native'].cached_result(query,**self.parameters(backend,parameters))
            if value is not None:
                await asyncio.to_thread(self.save,identity,backend,value)
                self.metrics['reused_existing']+=1;return value
        backend=await self.choose(query,parameters)
        try:
            await asyncio.to_thread(self.save,identity,backend)
            self.metrics['dispatches']+=1;backend['dispatched']+=1
            try:
                value=await backend['native'].search(query,**self.parameters(backend,parameters))
            except ServiceStopped:
                # A backend breaker can trip after its full result committed.
                # Keep that exact result and let later queries use healthy peers.
                value=await backend['native'].cached_result(query,**self.parameters(backend,parameters))
                if value is None:raise
            await asyncio.to_thread(self.save,identity,backend,value)
            usable=bool(value.get('candidates')) or value['status'] in {'ok','no_results'}
            self.gate.result(success=usable,transient=not usable)
            return value
        finally:
            async with self.condition:
                backend['active']-=1;self.condition.notify_all()

    def snapshot_metrics(self):
        return {**self.metrics,'stopped':self.gate.stopped,'backends':{
            b['name']:{'active':b['active'],'capacity':b['concurrency'],'dispatched':b['dispatched'],
                       'native':b['native'].snapshot_metrics()} for b in self.backends}}

    async def aclose(self):
        self.closed=True
        async with self.condition:self.condition.notify_all()
        tasks=[e['task'] for e in self.inflight.values()]
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await asyncio.gather(*(b['native'].aclose() for b in self.backends))
