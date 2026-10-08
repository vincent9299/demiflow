"""Optional AIMD capacity for a shared model/service gate; never retries work."""
import asyncio
import copy
import json
import math
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from .request_limits import RequestGate


DEFAULTS = dict(min_concurrency=1, initial_concurrency=4, max_concurrency=16,
    increase_step=2, window_s=120., min_samples=24, recovery_s=180., latency_p95_s=240.,
    coalesce_inflight_failures=False, transient_min_failures=1,
    transient_window_s=60., transient_failure_ratio=0.)


def request_admission_policy(value):
    if value is None:return None
    if not isinstance(value,dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid adaptive request policy fields')
    result={**DEFAULTS,**copy.deepcopy(value)}
    for key,value in result.items():
        if key == 'coalesce_inflight_failures':
            if type(value) is not bool:
                raise ValueError('coalesce_inflight_failures must be a boolean')
            continue
        if key == 'transient_failure_ratio':
            if type(value) not in (int,float) or not math.isfinite(value) or not 0<=value<=1:
                raise ValueError('transient_failure_ratio must be in [0, 1]')
            continue
        if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
            raise ValueError('Adaptive request policy must be finite and positive: '+key)
        if isinstance(DEFAULTS[key],int) and type(value) is not int:
            raise ValueError('Adaptive request policy requires an integer: '+key)
    if not 1<=result['min_concurrency']<=result['initial_concurrency']<=result['max_concurrency']<=1024:
        raise ValueError('Invalid adaptive request concurrency bounds')
    if result['transient_min_failures']>256:
        raise ValueError('transient_min_failures exceeds the 256-observation window')
    return result


class AdaptiveRequestGate(RequestGate):
    """Grow after complete fresh windows, shrink on transient failures/latency.

    Lookup/replay happens before gate entry in the native model runtime. Only
    dispatched work contributes latency observations. Capacity changes affect
    subsequent entry; active calls are never cancelled by resizing.
    """
    def __init__(self, policy, *, failures=5, state_path=None):
        self.policy=request_admission_policy(policy)
        if self.policy is None:raise ValueError('An explicit admission policy is required')
        super().__init__(self.policy['max_concurrency'],failures=failures)
        self.state_path=Path(state_path) if state_path else None
        self._reset_adaptive()

    def _reset_adaptive(self):
        self.capacity=self.policy['initial_concurrency']
        self.changed=asyncio.Event()
        self.window_started=time.monotonic()
        self.recover_at=self.window_started
        self.latencies=[]
        self.fresh_responses=0
        self.adjustments=0
        self.last_reason='initial'
        # Constant-size state: old in-flight failures need not each halve an
        # already-reduced capacity. No request identities or payloads retained.
        self.last_decrease_at=None
        self.coalesced_transient_failures=0
        # Numeric observations only: bounded by both age and 256 completions.
        # This controls resizing, never the independent fatal/stop boundary.
        self.transient_observations=deque(maxlen=256)
        self.deferred_transient_failures=0

    def begin_action(self):
        super().begin_action()
        self._reset_adaptive()
        self._persist(None)

    def snapshot(self):
        return {'capacity':self.capacity,'maximum':self.concurrency,'active':self.active,
            'peak':self.peak,'admitted':self.admitted,'fresh_responses':self.fresh_responses,
            'adjustments':self.adjustments,'last_reason':self.last_reason,'policy':self.policy,
            'coalesced_transient_failures':self.coalesced_transient_failures,
            'deferred_transient_failures':self.deferred_transient_failures,
            'transient_window_samples':len(self.transient_observations),
            'transient_window_failures':sum(failed for _,failed in self.transient_observations),
            'observed_at':time.time(),'stopped':self.stopped,
            'recovery_remaining_s':max(0,self.recover_at-time.monotonic())}

    def _persist(self,event):
        if self.state_path is None:return
        self.state_path.parent.mkdir(parents=True,exist_ok=True)
        temporary=self.state_path.with_suffix(self.state_path.suffix+'.tmp')
        temporary.write_text(json.dumps(self.snapshot(),ensure_ascii=False)+'\n')
        temporary.replace(self.state_path)
        if event is not None:
            with self.state_path.with_suffix('.events.jsonl').open('a') as stream:
                stream.write(json.dumps(event,ensure_ascii=False)+'\n')

    def result(self, *, success=False, transient=False, fatal='', elapsed_s=None, backpressure=False):
        transient=bool(transient or backpressure)
        event=None
        now=time.monotonic()
        p=self.policy
        before=self.capacity
        reason=None
        coalesced = bool(transient and p['coalesce_inflight_failures']
            and self.last_decrease_at is not None
            and type(elapsed_s) in (int,float) and math.isfinite(elapsed_s) and elapsed_s >= 0
            and now-elapsed_s < self.last_decrease_at)
        while self.transient_observations and now-self.transient_observations[0][0]>p['transient_window_s']:
            self.transient_observations.popleft()
        if (success or transient) and not coalesced:
            self.transient_observations.append((now,bool(transient)))
        failed=sum(value for _,value in self.transient_observations)
        reduce_transient = bool(transient and not coalesced and (backpressure or
            (failed>=p['transient_min_failures']
             and failed/len(self.transient_observations)>=p['transient_failure_ratio'])))
        if coalesced:
            self.coalesced_transient_failures+=1
        elif reduce_transient:
            self.capacity=max(p['min_concurrency'],self.capacity//2)
            self.recover_at=now+p['recovery_s']
            reason='backpressure' if backpressure else 'transient_failure'
            self.last_decrease_at=now
            self.transient_observations.clear()
        elif transient:
            self.deferred_transient_failures+=1
        elif success and elapsed_s is not None:
            self.fresh_responses+=1
            self.latencies.append(elapsed_s)
            self.latencies=self.latencies[-1024:]
            if len(self.latencies)>=p['min_samples'] and now-self.window_started>=p['window_s']:
                p95=sorted(self.latencies)[math.ceil(.95*len(self.latencies))-1]
                if p95>p['latency_p95_s']:
                    self.capacity=max(p['min_concurrency'],self.capacity//2)
                    self.recover_at=now+p['recovery_s']
                    reason='latency_p95'
                    self.last_decrease_at=now
                    self.transient_observations.clear()
                elif now>=self.recover_at:
                    self.capacity=min(p['max_concurrency'],self.capacity+p['increase_step'])
                    reason='healthy_window'
                self.window_started,self.latencies=now,[]
        if reduce_transient:
            self.window_started,self.latencies=now,[]
        if before!=self.capacity:
            self.adjustments+=1
            self.last_reason=reason
            self.changed.set()
            event={'observed_at':time.time(),'reason':reason,'before':before,'after':self.capacity,
                   'fresh_responses':self.fresh_responses,'active':self.active}
        try:
            super().result(success=success,transient=transient,fatal=fatal,elapsed_s=elapsed_s)
        finally:
            self._persist(event)

    @asynccontextmanager
    async def enter(self):
        started=time.monotonic()
        self.check()
        # No await between checking capacity, clearing the event, and deciding
        # to wait: a release/resize cannot be lost in the single event loop.
        while self.active>=self.capacity:
            self.changed.clear()
            await self.changed.wait()
            self.check()
        self.check()
        self.active+=1;self.admitted+=1
        self.peak=max(self.peak,self.active);self.wait_s+=time.monotonic()-started
        try:yield
        finally:
            self.active-=1
            self.changed.set()
