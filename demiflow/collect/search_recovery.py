"""Bounded in-process search recovery, without replaying completed requests.

Admitted queries drain (including their existing fallback allowance). Waiting
queries consume no HTTP or source deadline. One new query probes recovery after
the pause; failed probes and repeated outages have durable finite bounds.
"""
import asyncio
import copy
import time
from contextlib import asynccontextmanager

from demiflow.execution.request_limits import ServiceStopped


class SearchRecovery:
    def __init__(self, policy, *, failure_limit, capacity, inventory, persist, recovered):
        self.policy=policy;self.failure_limit=failure_limit
        self.capacity=capacity;self.inventory=inventory;self.persist=persist;self.recovered=recovered
        self.condition=asyncio.Condition();self.active=0;self.probing=False
        self.value=dict(mode='running',paused_until=0.,consecutive=0,
            failed_probes=0,episodes=[],pauses=0,recoveries=0,last_reason='',fatal='',short_since=None,
            last_inventory={},last_observed_at=0.)

    def restore(self,value):
        if value is None:return
        if value.get('mode') not in {'running','paused','stopped'}:
            raise ValueError('Invalid persisted search recovery state')
        self.value=copy.deepcopy(value)

    def state(self):return copy.deepcopy(self.value)

    def snapshot(self):
        return {**self.state(),'active_queries':self.active,'probe_active':self.probing,
                'route_inventory':self.inventory()}

    def check(self):
        if self.value['fatal']:raise ServiceStopped(self.value['fatal'])

    async def stop(self,reason):
        async with self.condition:
            self.value.update(mode='stopped',fatal=reason,last_reason=reason)
            await self.persist(self.state(),{'observed_at':time.time(),'reason':reason,'state':self.state()})
            self.condition.notify_all()
            self.check()

    @asynccontextmanager
    async def admit(self, *, wait_for_recovery=True):
        async with self.condition:
            while True:
                self.check();now=time.time();v=self.value
                probe=v['mode']=='paused'
                if ((not probe and self.active<self.capacity()) or
                        (probe and not self.active and not self.probing and now>=v['paused_until'])):
                    self.active+=1
                    if probe:self.probing=True
                    lease={'probe':probe,'observed':False}
                    break
                if probe and not wait_for_recovery:
                    # An optional independent source may serve this query.
                    # No primary HTTP was admitted, no probe budget is spent,
                    # and the primary circuit remains unchanged.
                    raise ServiceStopped('search_recovery_paused')
                delay=max(.001,v['paused_until']-now) if probe and now<v['paused_until'] else 1.
                try:await asyncio.wait_for(self.condition.wait(),timeout=min(1.,delay))
                except asyncio.TimeoutError:pass
        try:yield lease
        finally:
            async with self.condition:
                self.active-=1
                if lease['probe']:self.probing=False
                self.condition.notify_all()

    async def observe(self,lease,*,success,window_stop=False,unavailable=''):
        """Only fresh completed queries, or explicit local acquisition faults.

        Cache hits and cancellation never spend a recovery probe or clear a
        circuit. Responses admitted before a pause cannot reopen that circuit.
        """
        async with self.condition:
            self.check();v=self.value;now=time.time();event=None
            lease['observed']=True
            v['last_inventory']=self.inventory();v['last_observed_at']=now
            inventory=v['last_inventory']
            # A renewable pool can continually create untested declarations.
            # They are eligible attempts, but cannot establish target health.
            # Busy/paced healthy leases remain counted by the pool; static
            # pools without health evidence retain their eligible inventory.
            count=(inventory.get('healthy', inventory['viable'])
                   if self.policy.get('pause_route_inventory') == 'healthy'
                   else inventory['viable'])
            if count>=self.policy['pause_min_routes']:v['short_since']=None
            elif v['short_since'] is None:v['short_since']=now
            shortage=v['short_since'] is not None and now-v['short_since']>=self.policy['pause_shortfall_s']
            if lease['probe']:
                if success:
                    await self.recovered()
                    v.update(mode='running',paused_until=0.,consecutive=0,failed_probes=0,short_since=None)
                    v['recoveries']+=1;event='search_resumed'
                else:
                    v['failed_probes']+=1
                    if v['failed_probes']>=self.policy['pause_max_probes']:
                        v.update(mode='stopped',fatal='search_recovery_probe_limit')
                        event=v['fatal']
                    else:
                        v['paused_until']=now+min(self.policy['pause_max_s'],
                            self.policy['pause_s']*2**v['failed_probes'])
                        event='search_probe_failed'
            elif v['mode']=='running':
                if not unavailable:v['consecutive']=0 if success else v['consecutive']+1
                failed=not success and (window_stop or v['consecutive']>=self.failure_limit)
                should_pause=failed and (self.policy['pause_trigger']=='query_failures' or shortage)
                if unavailable or should_pause:
                    v['episodes']=[t for t in v['episodes'] if now-t<self.policy['pause_window_s']]
                    if len(v['episodes'])>=self.policy['pause_max_episodes']:
                        v.update(mode='stopped',fatal='search_recovery_episode_limit')
                        event=v['fatal']
                    else:
                        v['episodes'].append(now);v['pauses']+=1
                        v.update(mode='paused',paused_until=now+self.policy['pause_s'],failed_probes=0)
                        event=unavailable or ('search_route_shortage' if self.policy['pause_trigger']=='route_shortage'
                                            else 'search_pool_failure_window' if window_stop
                                            else 'search_consecutive_query_failures')
            if event:v['last_reason']=event
            await self.persist(self.state(),{'observed_at':now,'reason':event,'state':self.state()} if event else None)
            self.condition.notify_all()
            self.check()
