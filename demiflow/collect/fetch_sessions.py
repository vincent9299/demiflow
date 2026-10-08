"""On-demand renewable download routes, primary or explicit static fallback.

One lease/client/relay per slot. Generations close before replacement, creation
rate is durable per journal, and no background replenishment runs. The opaque
factory contract is shared with search; no search worker is started here.
"""
import asyncio
import copy
import hashlib
import importlib
import json
import math
import os
import re
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from demiflow.execution.request_limits import ServiceStopped
from .native_search.config import public,resolve,validate_secrets
from .proxy import ProxyPool,proxy_declaration

DEFAULTS=dict(factory=None,factory_options={},identity_envs=[],size=4,ttl_s=300.,interval_s=3.,
    creation_window_s=60.,max_creations_per_window=16,acquisition_timeout_s=30.)


def fetch_session_policy(value):
    if not isinstance(value,dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid fetch session pool declaration')
    p={**copy.deepcopy(DEFAULTS),**copy.deepcopy(value)}
    if not isinstance(p['factory'],str) or not re.fullmatch(r'[A-Za-z_][\w.]*:[A-Za-z_]\w*',p['factory']):
        raise ValueError('Fetch session factory requires module:function')
    if not isinstance(p['factory_options'],dict):raise ValueError('Invalid factory options')
    validate_secrets(p['factory_options'])
    if len(json.dumps(p['factory_options']).encode())>16384:raise ValueError('Factory options exceed 16 KiB')
    if not isinstance(p['identity_envs'],list) or len(p['identity_envs'])>16 or any(
        not isinstance(k,str) or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,255}',k) for k in p['identity_envs']):
        raise ValueError('Invalid factory identity environment names')
    for k in ('size','max_creations_per_window'):
        if type(p[k]) is not int or p[k]<1:raise ValueError('Invalid '+k)
    if p['size']>64 or not p['size']<=p['max_creations_per_window']<=4096:
        raise ValueError('Invalid fetch session capacity/creation budget')
    for k in ('ttl_s','interval_s','creation_window_s','acquisition_timeout_s'):
        if type(p[k]) not in (int,float) or not math.isfinite(p[k]) or p[k]<=0:
            raise ValueError('Invalid '+k)
    if p['ttl_s']>86400 or p['acquisition_timeout_s']>300 or p['creation_window_s']>86400 or p['interval_s']>p['ttl_s']:
        raise ValueError('Fetch session time budget exceeded')
    return p


def policy_identity(policy):
    env={}
    for key in policy['identity_envs']:
        if key not in os.environ:raise ValueError('Missing fetch session identity environment: '+key)
        env[key]=os.environ[key]
    return {'factory':policy['factory'],'factory_options':policy['factory_options'],
        'credential_digest':hashlib.sha256(json.dumps(env,sort_keys=True).encode()).hexdigest()}


class FetchSessionPool:
    def __init__(self,web,domain,policy):
        self.web,self.domain,self.policy=web,domain,fetch_session_policy(policy)
        self.identity=hashlib.sha256(json.dumps([domain,policy_identity(self.policy)],sort_keys=True).encode()).hexdigest()
        module,name=self.policy['factory'].split(':');self.factory=getattr(importlib.import_module(module),name)
        self.condition=asyncio.Condition();self.slots=[];self.closed=False
        self.metrics=dict(created=0,retired=0,requests=0,peak_leased=0)
        with web._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS fetch_session_creations (identity TEXT PRIMARY KEY, recent_json TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS fetch_session_events
                (id INTEGER PRIMARY KEY, pool TEXT, generation TEXT, host TEXT,
                 status TEXT, http_status INTEGER, elapsed_s REAL, observed_at REAL)''')

    def reserve_creation(self):
        now=time.time()
        with self.web._db() as db:
            db.execute('BEGIN IMMEDIATE')
            value=db.execute('SELECT CASE WHEN length(recent_json)<=131072 THEN recent_json ELSE NULL END '
                'FROM fetch_session_creations WHERE identity=?',(self.identity,)).fetchone()
            if value and value[0] is None:raise ValueError('Fetch session creation state exceeds bound')
            recent=json.loads(value[0]) if value else []
            if not isinstance(recent,list) or len(recent)>4096:raise ValueError('Invalid fetch creation state')
            recent=[t for t in recent if t>now-self.policy['creation_window_s']]
            if len(recent)>=self.policy['max_creations_per_window']:
                raise ServiceStopped('fetch_session_creation_budget')
            recent.append(now)
            db.execute('INSERT OR REPLACE INTO fetch_session_creations VALUES (?,?)',(self.identity,json.dumps(recent)))

    def make_route(self):
        self.reserve_creation()
        token=uuid.uuid4().hex;before=set(os.environ)
        declared=self.factory(token=token,ttl_s=self.policy['ttl_s'],options=copy.deepcopy(self.policy['factory_options']))
        declaration=proxy_declaration(declared)
        # Only remove generation-owned keys; shared/base credentials stay owned
        # by the caller. A factory should expose every generated Secret here.
        encoded=public(declaration)
        def names(x):
            if isinstance(x,dict):
                if set(x)=={'secret_env'}:yield x['secret_env']
                else:
                    for v in x.values():yield from names(v)
            elif isinstance(x,(list,tuple)):
                for v in x:yield from names(v)
        owned={k:os.environ[k] for k in names(encoded) if k not in before and k in os.environ}
        route=dict(name='download_session_'+token,proxy=declaration,owned_env=owned,
            expires=time.monotonic()+self.policy['ttl_s'],next_ready=0.,busy=False,bad=False,
            client=None,transport=ProxyPool(timeout_s=self.web.timeout_s,max_connections=2),_fetch_session_pool=self)
        self.metrics['created']+=1
        return route

    async def close_route(self,route):
        try:
            if route['client'] is not None:await self.web.connections.close_client(route['client'])
        finally:
            try:await route['transport'].aclose()
            finally:
                for key,value in route['owned_env'].items():
                    if os.environ.get(key)==value:os.environ.pop(key,None)

    @asynccontextmanager
    async def lease(self):
        async with asyncio.timeout(self.policy['acquisition_timeout_s']):
            async with self.condition:
                while True:
                    if self.closed:raise ServiceStopped('fetch_session_pool_closed')
                    now=time.monotonic()
                    for route in list(self.slots):
                        if not route['busy'] and (route['bad'] or route['expires']<=now):
                            await self.close_route(route);self.slots.remove(route);self.metrics['retired']+=1
                    available=[r for r in self.slots if not r['busy'] and r['next_ready']<=now]
                    if available:
                        route=min(available,key=lambda r:r['next_ready']);break
                    if len(self.slots)<self.policy['size']:
                        route=self.make_route();self.slots.append(route);break
                    delay=min((r['next_ready']-now for r in self.slots if not r['busy']),default=.25)
                    try:await asyncio.wait_for(self.condition.wait(),timeout=max(.01,min(delay,.25)))
                    except asyncio.TimeoutError:pass
                route['busy']=True;route['next_ready']=time.monotonic()+self.policy['interval_s']
                self.metrics['peak_leased']=max(self.metrics['peak_leased'],sum(r['busy'] for r in self.slots))
        try:yield route
        finally:
            async with self.condition:
                route['busy']=False;self.condition.notify_all()

    async def client(self,route):
        if route['client'] is None:
            proxy=await route['transport'].transport(resolve(route['proxy']))
            route['client']=self.web.connections.client((id(self),'session',route['name']),
                proxy=proxy,capacity=1,keepalive=1,
                timeout=httpx.Timeout(self.web.timeout_s,connect=self.web.connect_timeout_s),
                headers={'User-Agent':'demiflow-evidence/1','Accept-Encoding':'gzip, deflate'})
        return route['client']

    def record(self,route,host,status,code,elapsed,retry_after_s=0):
        self.metrics['requests']+=1
        route['bad']=status=='network_error' or code in (403,429) or (code is not None and code>=500)
        with self.web._db() as db:
            db.execute('INSERT INTO fetch_session_events(pool,generation,host,status,http_status,elapsed_s,observed_at) '
                'VALUES (?,?,?,?,?,?,?)',(self.identity,route['name'],host,status,code,elapsed,time.time()))

    def snapshot_metrics(self):
        return {**self.metrics,'slots':len(self.slots),'active':sum(r['busy'] for r in self.slots),
            'closed':self.closed,'proxy_chains':{r['name']:r['transport'].snapshot_metrics() for r in self.slots}}

    async def aclose(self):
        async with self.condition:
            self.closed=True;self.condition.notify_all()
        results=await asyncio.gather(*(self.close_route(r) for r in self.slots),return_exceptions=True)
        for result in results:
            if isinstance(result,BaseException):raise result
