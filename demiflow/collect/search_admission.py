"""Bounded search admission controlled by fresh network observations.

Scheduling lives outside native source fingerprints. No provider, domain,
credential generation, query rewriting, or automatic request replay belongs here.
"""
import asyncio
import copy
import math
import time


DEFAULTS = dict(min_concurrency=1, initial_concurrency=2, max_concurrency=4,
    initial_interval_s=2., min_interval_s=1., max_interval_s=10.,
    window_s=60., min_samples=20, recovery_s=120., latency_p95_s=8.,
    decrease_failure_ratio=.2, increase_failure_ratio=.02,
    failure_window_limit=None, failure_window_s=120., failure_window_ratio=.25, stop_s=300.,
    decrease_on_single_congestion=True, decrease_min_samples=8,
    failure_window_scope='attempt', adjustment_scope='attempt', route_cooldown_wait_s=0.,
    transient_failure_action='stop', pause_s=30., pause_max_s=300.,
    pause_max_probes=3, pause_max_episodes=6, pause_window_s=3600.,
    pause_trigger='query_failures', pause_min_routes=8, pause_shortfall_s=10.,
    pause_route_inventory='eligible')

RECOVERY_FIELDS = {'transient_failure_action','pause_s','pause_max_s',
                   'pause_max_probes','pause_max_episodes','pause_window_s',
                   'pause_trigger','pause_min_routes','pause_shortfall_s','pause_route_inventory'}


def adaptive_policy(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid adaptive search policy fields')
    result = {**DEFAULTS, **copy.deepcopy(value)}
    for key, number in result.items():
        if key == 'transient_failure_action':
            if not isinstance(number,str) or number not in {'stop','pause'}:
                raise ValueError('Transient failure action must be stop or pause')
            continue
        if key == 'pause_trigger':
            if not isinstance(number,str) or number not in {'query_failures','route_shortage'}:
                raise ValueError('Pause trigger must be query_failures or route_shortage')
            continue
        if key == 'pause_route_inventory':
            if not isinstance(number, str) or number not in {'eligible', 'healthy'}:
                raise ValueError('Pause route inventory must be eligible or healthy')
            continue
        if key in {'initial_interval_s','min_interval_s','max_interval_s'}:
            if type(number) not in (int,float) or not math.isfinite(number) or number < 0:
                raise ValueError('Adaptive pacing must be finite and nonnegative: '+key)
            continue
        if key == 'route_cooldown_wait_s':
            if type(number) not in (int,float) or not math.isfinite(number) or not 0<=number<=300:
                raise ValueError('Route cooldown wait must be within 0..300 seconds')
            continue
        if key in {'failure_window_scope','adjustment_scope'}:
            if not isinstance(number,str) or number not in {'attempt','query'}:
                raise ValueError(key+' must be attempt or query')
            continue
        if key == 'decrease_on_single_congestion':
            if type(number) is not bool:
                raise ValueError('decrease_on_single_congestion must be boolean')
            continue
        if key == 'failure_window_limit':
            if number is not None and (type(number) is not int or number < 2):
                raise ValueError('Failure window limit must be None or an integer >= 2')
            continue
        if type(number) not in (int, float) or not math.isfinite(number) or number <= 0:
            raise ValueError('Adaptive search policy must be finite and positive: ' + key)
        if isinstance(DEFAULTS[key], int) and type(number) is not int:
            raise ValueError('Adaptive search policy requires an integer: ' + key)
    if not 1 <= result['min_concurrency'] <= result['initial_concurrency'] <= result['max_concurrency'] <= 256:
        raise ValueError('Invalid adaptive concurrency bounds')
    if not result['min_interval_s'] <= result['initial_interval_s'] <= result['max_interval_s']:
        raise ValueError('Invalid adaptive pacing bounds')
    if result['min_interval_s']==0 and result['max_interval_s']!=0:
        raise ValueError('Disabled pacing requires all interval bounds to be zero')
    if not 0 < result['increase_failure_ratio'] < result['decrease_failure_ratio'] <= 1:
        raise ValueError('Invalid adaptive failure ratios')
    if result['failure_window_ratio'] > 1:
        raise ValueError('Failure window ratio must be at most one')
    if result['adjustment_scope']=='query' and result['failure_window_scope']!='query':
        raise ValueError('Query adjustment requires a query failure window')
    if result['min_samples'] < 2:
        raise ValueError('Adaptive window requires at least two samples')
    if result['pause_s']>result['pause_max_s'] or result['pause_max_s']>3600:
        raise ValueError('Search pause bounds must satisfy pause_s <= pause_max_s <= 3600')
    if result['transient_failure_action']=='pause' and result['adjustment_scope']!='query':
        raise ValueError('Search pause recovery requires query-scoped feedback')
    return result


def admission_identity_policy(policy):
    """Keep existing attempt-scoped controllers' durable identities stable."""
    result=copy.deepcopy(policy)
    # Recovery has its own durable state. Opting in must not reset existing
    # capacity observations or source/route identities.
    for key in RECOVERY_FIELDS:result.pop(key,None)
    if result.get('adjustment_scope')=='attempt':result.pop('adjustment_scope')
    return result


class ResizableAdmission:
    """Shrink affects future entries; admitted work always drains normally."""
    def __init__(self, limit):
        self.limit, self.active, self.peak = limit, 0, 0
        self.condition = asyncio.Condition()

    async def resize(self, limit):
        async with self.condition:
            self.limit = limit
            self.condition.notify_all()

    async def __aenter__(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
            self.peak = max(self.peak, self.active)
        return self

    async def __aexit__(self, *_):
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()


class SearchAdmission:
    def __init__(self, policy, *, now=None):
        self.policy = adaptive_policy(policy)
        self.concurrency = self.policy['initial_concurrency']
        self.interval_s = self.policy['initial_interval_s']
        self.since = time.time() if now is None else now
        self.recover_at = self.since
        self.samples = []
        self.total_samples = self.adjustments = 0
        self.last_reason = 'initial'
        self.failure_window = []
        self.stopped_until = 0.

    def state(self):
        return {'concurrency':self.concurrency, 'interval_s':self.interval_s,
            'window_started_at':self.since, 'recover_at':self.recover_at,
            'samples':copy.deepcopy(self.samples), 'total_samples':self.total_samples,
            'adjustments':self.adjustments, 'last_reason':self.last_reason,
            'failure_window':copy.deepcopy(self.failure_window), 'stopped_until':self.stopped_until}

    def restore(self, state):
        p = self.policy
        if (type(state.get('concurrency')) is not int
                or not p['min_concurrency'] <= state['concurrency'] <= p['max_concurrency']
                or not p['min_interval_s'] <= state.get('interval_s', -1) <= p['max_interval_s']):
            raise ValueError('Persisted search admission exceeds configured bounds')
        self.concurrency, self.interval_s = state['concurrency'], state['interval_s']
        self.since, self.recover_at = state['window_started_at'], state['recover_at']
        self.samples = copy.deepcopy(state['samples'][-512:])
        self.total_samples, self.adjustments = state['total_samples'], state['adjustments']
        self.last_reason = state['last_reason']
        self.failure_window = copy.deepcopy(state.get('failure_window', [])[-512:])
        self.stopped_until = state.get('stopped_until', 0.)

    def observe(self, *, success, congestion=False, latency_s=None, now=None):
        now = time.time() if now is None else now
        if latency_s is not None and (not math.isfinite(latency_s) or latency_s < 0):
            raise ValueError('Invalid HTTP latency observation')
        if now-self.since > 2*max(self.policy['window_s'],self.policy['recovery_s']):
            # A long pause/restart cannot turn yesterday's healthy samples
            # into a fresh ramp decision after one request.
            self.samples, self.since = [], now
        self.total_samples += 1
        self.samples.append([bool(success), latency_s])
        self.samples = self.samples[-512:]
        p = self.policy
        if p['failure_window_scope']=='attempt':
            failures_in_window,pool_stop=self._failure_signal(congestion or not success,now)
        else:
            failures_in_window,pool_stop=sum(v[1] for v in self.failure_window),False
        count = len(self.samples)
        failures = sum(not v[0] for v in self.samples)
        ratio = failures/count
        latencies = sorted(v[1] for v in self.samples if v[0] and v[1] is not None)
        p95 = latencies[max(0,math.ceil(.95*len(latencies))-1)] if latencies else None
        age = max(0., now-self.since)
        enough = count >= p['min_samples'] and age >= p['window_s']
        reason = None
        # Isolated network faults quarantine their route. A sustained pool-wide
        # failure ratio or high latency also reduces total admission.
        if congestion and p['decrease_on_single_congestion']:
            reason = 'source_congestion'
        elif count >= min(p['decrease_min_samples'], p['min_samples']) and ratio >= p['decrease_failure_ratio']:
            reason = 'network_failure_ratio'
        elif enough and p95 is not None and p95 > p['latency_p95_s']:
            reason = 'http_latency'
        before = {'concurrency':self.concurrency, 'interval_s':self.interval_s}
        if reason:
            self.concurrency = max(p['min_concurrency'], self.concurrency//2)
            self.interval_s = min(p['max_interval_s'], self.interval_s*1.5)
            self.recover_at = now+p['recovery_s']
        elif (enough and now >= self.recover_at and ratio <= p['increase_failure_ratio']
                and p95 is not None and p95 <= p['latency_p95_s']):
            self.concurrency = min(p['max_concurrency'], self.concurrency+1)
            self.interval_s = max(p['min_interval_s'], self.interval_s*.8)
            reason = 'healthy_window'
        if reason or enough:
            self.samples, self.since = [], now
        after = {'concurrency':self.concurrency, 'interval_s':self.interval_s}
        if pool_stop:
            self.stopped_until = now+p['stop_s']
            self.recover_at = max(self.recover_at, self.stopped_until)
            reason = 'pool_failure_window'
        if before == after and not pool_stop:
            return None
        self.last_reason = reason
        self.adjustments += 1
        return {'observed_at':now, 'reason':reason, 'before':before, 'after':after,
                'fresh_samples':count, 'failure_ratio':ratio, 'http_p95_s':p95, 'window_s':age,
                'stopped_until':self.stopped_until, 'failures_in_window':failures_in_window}

    def _failure_signal(self, failure, now):
        p=self.policy
        self.failure_window=[v for v in self.failure_window if now-v[0]<p['failure_window_s']]
        self.failure_window.append([now,bool(failure)])
        self.failure_window=self.failure_window[-512:]
        failures=sum(v[1] for v in self.failure_window)
        stop=(p['failure_window_limit'] is not None and failures>=p['failure_window_limit']
            and failures/len(self.failure_window)>=p['failure_window_ratio'] and self.stopped_until<=now)
        return failures,stop

    def observe_query(self, *, success, now=None):
        """Optional circuit denominator: fresh queries after bounded fallback.

        The caller separately supplies capacity/latency observations according
        to adjustment_scope. Route quarantine always uses actual attempts.
        A recovered route failure is not a failed consumer query. Replays must
        never call this method or the adjustment observer.
        """
        if self.policy['failure_window_scope']!='query':
            return None
        now=time.time() if now is None else now
        failures,stop=self._failure_signal(not success,now)
        if not stop:
            return None
        self.stopped_until=now+self.policy['stop_s']
        self.recover_at=max(self.recover_at,self.stopped_until)
        self.last_reason='pool_failure_window';self.adjustments+=1
        capacity={'concurrency':self.concurrency,'interval_s':self.interval_s}
        return {'observed_at':now,'reason':self.last_reason,'before':capacity,'after':capacity,
            'fresh_samples':len(self.failure_window),'failure_ratio':failures/len(self.failure_window),
            'http_p95_s':None,'window_s':now-self.failure_window[0][0],
            'stopped_until':self.stopped_until,'failures_in_window':failures,
            'failure_window_scope':'query'}

    def snapshot(self):
        state = self.state()
        state['window_samples'] = len(state.pop('samples'))
        state['failure_window_samples'] = len(state['failure_window'])
        state['failure_window_failures'] = sum(v[1] for v in state.pop('failure_window'))
        return {**state, 'policy':copy.deepcopy(self.policy)}
