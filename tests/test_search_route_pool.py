"""Pooled routes preserve evidence and isolate cooldowns with bounded attempts."""
import asyncio
import time
import pytest

from demiflow.collect.native_search import NativeSearchSession, SearchConfig
from demiflow.collect.search_routes import SearchRoutePool
from demiflow.execution.request_limits import ServiceStopped


def fixture(monkeypatch,failures=()):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':
            return {'results':[r for g in message['groups'] for r in g['results']]}
        if message['op']=='search':
            async with self.http_gate.enter():
                route=self.config.proxy;calls.append((route,message['query'],time.monotonic()))
                if route in failures:
                    return {'status':'captcha','reason':'fixture','results':[],
                            'http':[{'host':'search.example','http_status':302,'elapsed_s':.001}]}
                return {'status':'ok','reason':'','results':[{'url':'https://source.example/'+message['query'],'title':'result','content':'body'}],
                        'http':[{'host':'search.example','http_status':200,'elapsed_s':.001}]}
        raise AssertionError(message['op'])
    monkeypatch.setattr(NativeSearchSession,'call',call)
    return calls


def pool(tmp_path,interval=0):
    return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
        routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':interval} for n in ['a','b']],
        cooldown_s=60,max_route_attempts=2)


def test_query_feedback_quarantines_failed_route_without_pool_slowdown(tmp_path,monkeypatch):
    calls=fixture(monkeypatch,failures={'http://a.example:3128'})
    async def run():
        session=SearchRoutePool(cache_path=tmp_path/'query-feedback.sqlite',
            config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
            routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ['a','b']],
            cooldown_s=60,max_route_attempts=2,failure_limit=1,
            adaptive={'adjustment_scope':'query','failure_window_scope':'query',
                'initial_interval_s':.001,'min_interval_s':.001,'min_samples':2,
                'failure_window_limit':2})
        try:
            before=session.admission.concurrency
            value=await session.search('recoverable')
            assert value['status']=='ok' and len(calls)==2
            assert session.routes[0]['until']>time.time()
            assert session.admission.concurrency==before
            assert session.admission.total_samples==1
            assert session.admission.samples[0][0] is True
            assert session.admission.failure_window[-1][1] is False
            assert await session.search('recoverable')==value
            assert len(calls)==2 and session.admission.total_samples==1
        finally:await session.aclose()
    asyncio.run(run())


def test_query_feedback_still_slows_and_stops_for_failed_queries(tmp_path,monkeypatch):
    calls=fixture(monkeypatch,failures={'http://a.example:3128','http://b.example:3128'})
    async def run():
        session=SearchRoutePool(cache_path=tmp_path/'query-failures.sqlite',
            config=SearchConfig(engines=['google'],language='all',source_interval_s=0,suspend_s=.001),
            routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ['a','b']],
            cooldown_s=.001,max_route_attempts=1,failure_limit=1,
            adaptive={'adjustment_scope':'query','failure_window_scope':'query',
                'initial_concurrency':4,'max_concurrency':4,'initial_interval_s':.001,
                'min_interval_s':.001,'min_samples':2,'failure_window_limit':2})
        try:
            assert (await session.search('one'))['status']=='search_failed'
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await session.search('two')
            assert len(calls)==2 and session.admission.concurrency==2
            assert session.http_gate.interval_s>.001
            assert session.admission.stopped_until>time.time()
        finally:await session.aclose()
    asyncio.run(run())


def cooling_pool(tmp_path,wait_s):
    return SearchRoutePool(cache_path=tmp_path/'cooling.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
        routes=[{'name':str(i),'proxy':'http://r'+str(i)+'.example:1','interval_s':0} for i in range(2)],
        adaptive={'route_cooldown_wait_s':wait_s,'initial_interval_s':.001,'min_interval_s':.001},
        max_route_attempts=2)


def test_short_cooldown_waits_then_resumes_without_early_http(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        session=cooling_pool(tmp_path,.3)
        try:
            await session.initialize()
            until=time.time()+.08
            for route in session.routes:route['until']=until
            task=asyncio.create_task(session.search('later'))
            await asyncio.sleep(.02)
            assert not calls and not task.done() and session.route_attempts==0
            assert (await asyncio.wait_for(task,2))['status']=='ok'
            assert len(calls)==1 and time.time()>=until
            metrics=session.snapshot_metrics()
            assert metrics['route_cooldown_waits']>=1 and metrics['route_cooldown_wait_s']>=.04
        finally:await session.aclose()
    asyncio.run(run())


def test_long_cooldown_or_repeated_extensions_cannot_wait_indefinitely(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        session=cooling_pool(tmp_path,.12)
        try:
            await session.initialize()
            for route in session.routes:route['until']=time.time()+2
            with pytest.raises(ServiceStopped,match='all_search_routes_cooling_down'):
                await asyncio.wait_for(session.search('far'),.5)
            assert session.cooldown_waits==0 and not calls
            for route in session.routes:route['until']=time.time()+.05
            task=asyncio.create_task(session.search('extended'))
            async def extend():
                for _ in range(8):
                    await asyncio.sleep(.02)
                    async with session.condition:
                        for route in session.routes:route['until']=time.time()+.05
                        session.condition.notify_all()
            extender=asyncio.create_task(extend())
            with pytest.raises(ServiceStopped,match='all_search_routes_cooling_down'):
                await asyncio.wait_for(task,.6)
            await extender
            assert not calls and session.route_attempts==0
            assert 0<session.cooldown_wait_s<.3
        finally:await session.aclose()
    asyncio.run(run())


def test_cooling_wait_can_be_cancelled_and_pool_circuit_still_wins(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        session=cooling_pool(tmp_path,1)
        try:
            await session.initialize()
            for route in session.routes:route['until']=time.time()+.5
            cancelled=asyncio.create_task(session.search('cancelled'))
            await asyncio.sleep(.02);cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):await cancelled
            assert not any(r['busy'] for r in session.routes) and not session.inflight_queries
            stopped=asyncio.create_task(session.search('stopped'))
            await asyncio.sleep(.02)
            async with session.condition:
                session.admission.stopped_until=time.time()+60
                session.condition.notify_all()
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await asyncio.wait_for(stopped,.5)
            assert not calls and session.route_attempts==0
        finally:await session.aclose()
    asyncio.run(run())


def test_migration_keeps_last_selected_receipt_despite_other_good_copy(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        first=pool(tmp_path)
        try:
            chosen=await first.search('stable')
            prior=first.routes[0]['session'].config.snapshot()
            # Simulate a journal created before aggregate selections existed.
            with first.routes[0]['session']._db() as db:
                db.execute('DELETE FROM native_search_route_results')
        finally:await first.aclose()
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0,proxy='http://new.example:3128')
        other=NativeSearchSession(cache_path=tmp_path/'cache.sqlite',config=config)
        try:
            duplicate=await other.search('stable')
            assert duplicate['engine_receipts'][0]['receipt_id']!=chosen['engine_receipts'][0]['receipt_id']
        finally:await other.aclose()
        before=len(calls)
        for _ in range(2):
            current=SearchRoutePool(cache_path=tmp_path/'cache.sqlite',config=config,
                routes=[{'name':'new','proxy':config.proxy,'interval_s':0}],
                reuse_configs=[prior],max_route_attempts=1)
            try:
                replay=await current.search('stable')
                assert replay['candidates']==chosen['candidates']
                assert replay['engine_receipts']==chosen['engine_receipts']
                assert len(calls)==before
            finally:await current.aclose()
    asyncio.run(run())


def test_selected_receipt_is_not_reused_without_declared_old_proxy(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        first=pool(tmp_path)
        try:await first.search('same')
        finally:await first.aclose()
        fresh=SearchRoutePool(cache_path=tmp_path/'cache.sqlite',config=SearchConfig(
            engines=['google'],language='all',source_interval_s=0),
            routes=[{'name':'other','proxy':'http://other.example:3128','interval_s':0}],max_route_attempts=1)
        try:
            await fresh.search('same');assert len(calls)==2
        finally:await fresh.aclose()
    asyncio.run(run())


def test_captcha_quarantines_one_route_and_cooldown_survives_restart(tmp_path,monkeypatch):
    calls=fixture(monkeypatch,{'http://a.example:3128'})
    async def run():
        first=pool(tmp_path)
        try:
            result=await first.search('one')
            assert result['status']=='ok' and len(calls)==2
            assert first.routes[0]['until']>time.time()
            assert (await first.search('two'))['status']=='ok'
            assert calls[-1][0]=='http://b.example:3128'
        finally:await first.aclose()
        second=pool(tmp_path)
        try:
            assert (await second.search('three'))['status']=='ok'
            assert calls[-1][0]=='http://b.example:3128'
            with second.routes[0]['session']._db() as db:
                assert db.execute("select count(*) from native_search_route_events where status='captcha'").fetchone()[0]==1
                assert db.execute("select count(*) from native_search where json_extract(value,'$.status')='captcha'").fetchone()[0]==1
        finally:await second.aclose()
    asyncio.run(run())


def test_all_cooling_stops_instead_of_retrying_forever(tmp_path,monkeypatch):
    calls=fixture(monkeypatch,{'http://a.example:3128','http://b.example:3128'})
    async def run():
        session=pool(tmp_path)
        try:
            assert (await session.search('one'))['status']=='search_failed'
            with pytest.raises(ServiceStopped,match='all_search_routes_cooling_down'):
                await session.search('two')
            assert len(calls)==2
        finally:await session.aclose()
    asyncio.run(run())


def test_global_and_per_route_pacing_and_sibling_reuse(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        session=pool(tmp_path,.06);session.http_gate.interval_s=.02
        try:
            values=await asyncio.gather(*(session.search(str(i)) for i in range(6)))
            assert all(v['status']=='ok' for v in values) and len(calls)==6
            times=sorted(t for _,_,t in calls)
            assert all(b-a>=.018 for a,b in zip(times,times[1:]))
            for route in {r for r,_,_ in calls}:
                times=[t for r,_,t in calls if r==route]
                assert all(b-a>=.055 for a,b in zip(times,times[1:]))
            before=len(calls)
            assert (await session.search('0'))['status']=='ok'
            assert len(calls)==before
        finally:await session.aclose()
    asyncio.run(run())


def test_replayed_captcha_cannot_restart_expired_cooldown(tmp_path,monkeypatch):
    failures={'http://a.example:3128','http://b.example:3128'}
    calls=fixture(monkeypatch,failures)
    async def run():
        first=pool(tmp_path)
        try:
            assert (await first.search('failed'))['status']=='search_failed'
            prior=[r['until'] for r in first.routes]
            assert len(calls)==2
        finally:await first.aclose()
        # Simulate wall-clock cooldown expiry and process restart, retaining
        # the original failed receipts and health journal.
        future=max(prior)+1
        monkeypatch.setattr(time,'time',lambda:future)
        second=pool(tmp_path)
        try:
            assert (await second.search('failed'))['status']=='search_failed'
            assert len(calls)==2 and [r['until'] for r in second.routes]==prior
            assert [r['failures'] for r in second.routes]==[1,1]
            failures.clear()
            assert (await second.search('new-query'))['status']=='ok'
            assert len(calls)==3
            with second.routes[0]['session']._db() as db:
                assert db.execute("select count(*) from native_search_route_events where status='reused:captcha'").fetchone()[0]==0
                assert db.execute("select count(*) from native_search where json_extract(value,'$.status')='captcha'").fetchone()[0]==2
        finally:await second.aclose()
    asyncio.run(run())


def test_failed_query_is_frozen_across_restart_and_new_routes(tmp_path, monkeypatch):
    calls=fixture(monkeypatch, {'http://a.example:3128','http://b.example:3128'})
    async def run():
        first=pool(tmp_path)
        try:
            failed, duplicate=await asyncio.gather(first.search('failed'),first.search('failed'))
            assert failed==duplicate and failed['status']=='search_failed'
            assert len(calls)==2
            old=[r['session'].config.snapshot() for r in first.routes]
        finally:await first.aclose()
        current=SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
            config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
            routes=[{'name':'new','proxy':'http://new.example:3128','interval_s':0}],
            reuse_configs=old,max_route_attempts=1)
        try:
            assert await current.search('failed')==failed
            assert len(calls)==2
            assert (await current.search('fresh'))['status']=='ok'
            assert len(calls)==3
        finally:await current.aclose()
    asyncio.run(run())


def test_restore_failed_selection_validates_source_and_keeps_existing(tmp_path, monkeypatch):
    calls=fixture(monkeypatch, {'http://a.example:3128','http://b.example:3128'})
    async def run():
        first=pool(tmp_path)
        try:
            failed=await first.search('failed')
            with first.routes[0]['session']._db() as db:
                db.execute('DELETE FROM native_search_route_results')
            entry={'query':'failed','parameters':{},'result':failed}
            bad={**entry,'query':'different'}
            with pytest.raises(ValueError,match='identity mismatch'):
                await first.restore_results([bad],provenance='fixed-receipt@1')
            report=await first.restore_results([entry],provenance='fixed-receipt@1')
            assert report=={'checked':1,'inserted':1,'existing':0}
            assert (await first.restore_results([entry],provenance='fixed-receipt@1'))['inserted']==0
            assert await first.search('failed')==failed
            assert len(calls)==2
        finally:await first.aclose()
    asyncio.run(run())


def test_route_interval_does_not_spend_worker_deadline_or_delay_replay(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    native_call=NativeSearchSession.call
    async def deadline(self,context,message):
        async with asyncio.timeout(.05):
            return await native_call(self,context,message)
    monkeypatch.setattr(NativeSearchSession,'call',deadline)
    async def run():
        session=pool(tmp_path,.16)
        try:
            values=await asyncio.gather(*(session.search(str(i)) for i in range(6)))
            assert all(value['status']=='ok' for value in values)
            assert len(calls)==6
            async def no_admission():
                raise AssertionError('A cached result waited for route admission')
            for route in session.routes:
                route['session'].http_gate.wait_ready=no_admission
            assert (await session.search('0'))['status']=='ok'
            assert len(calls)==6
        finally:await session.aclose()
    asyncio.run(run())


def test_slow_route_waits_before_sibling_worker_deadline(tmp_path,monkeypatch):
    calls=[]
    async def bounded_call(self,context,message):
        if message['op']=='merge':
            return {'results':[r for g in message['groups'] for r in g['results']]}
        async with asyncio.timeout(.09):
            async with self.http_gate.enter():
                await asyncio.sleep(.06)
                calls.append(message['query'])
                return {'status':'ok','reason':'','results':[
                    {'url':'https://source.example/'+message['query'],'title':'result','content':'body'}],
                    'http':[{'host':'search.example','http_status':200,'elapsed_s':.06}]}
    monkeypatch.setattr(NativeSearchSession,'call',bounded_call)
    async def run():
        session=SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
            config=SearchConfig(engines=['google'],language='all',request_concurrency=1),
            routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ['a','b']],
            cooldown_s=60,max_route_attempts=1)
        try:
            results=await asyncio.gather(session.search('one'),session.search('two'))
            assert [r['status'] for r in results]==['ok','ok']
            assert sorted(calls)==['one','two']
            assert session.http_gate.peak==1
            assert all(not r['busy'] for r in session.routes)
            before=len(calls)
            assert (await session.search('one'))['status']=='ok'
            assert len(calls)==before
        finally:
            await session.aclose()
    asyncio.run(run())


def test_global_pacing_wait_precedes_worker_deadline(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    native_call=NativeSearchSession.call
    async def deadline(self,context,message):
        async with asyncio.timeout(.06):
            return await native_call(self,context,message)
    monkeypatch.setattr(NativeSearchSession,'call',deadline)
    async def run():
        session=pool(tmp_path)
        session.http_gate.interval_s=.13
        try:
            results=await asyncio.gather(*(session.search(str(i)) for i in range(6)))
            assert all(r['status']=='ok' for r in results)
            starts=sorted(c[2] for c in calls)
            assert len(starts)==6
            assert all(b-a>=.12 for a,b in zip(starts,starts[1:]))
            assert session.http_gate.admitted==6 and session.http_gate.active==0
            assert (await session.search('0'))['status']=='ok'
            assert session.http_gate.admitted==6
        finally:await session.aclose()
    asyncio.run(run())


def test_first_http_reservation_cancellation_and_additional_hops(tmp_path,monkeypatch):
    started=asyncio.Event()
    first=True
    calls=[]
    async def simulated(self,context,message):
        nonlocal first
        if message['op']=='merge':
            return {'results':[r for g in message['groups'] for r in g['results']]}
        if first:
            first=False;started.set()
            await asyncio.Event().wait()
        # Simulate worker initialization, then two distinct HTTP hops.
        await asyncio.sleep(.015)
        for _ in range(2):
            async with self.http_gate.enter():
                calls.append(time.monotonic())
        return {'status':'ok','reason':'','results':[{'url':'https://source.example/ok','title':'ok','content':'ok'}],
                'http':[{'host':'search.example','http_status':200,'elapsed_s':.001}]*2}
    monkeypatch.setattr(NativeSearchSession,'call',simulated)
    async def run():
        session=pool(tmp_path)
        session.http_gate.interval_s=.04
        try:
            cancelled=asyncio.create_task(session.search('cancel'))
            await asyncio.wait_for(started.wait(),1)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):await cancelled
            assert session.http_gate.admitted==0
            results=await asyncio.wait_for(asyncio.gather(session.search('one'),session.search('two')),2)
            assert all(r['status']=='ok' for r in results)
            assert len(calls)==4 and all(b-a>=.035 for a,b in zip(calls,calls[1:]))
            assert session.http_gate.admitted==4 and session.http_gate.active==0
            assert all(not r['session'].http_gate.reservations for r in session.routes)
        finally:await session.aclose()
    asyncio.run(run())


def test_fixed_routes_reuse_retired_session_history_without_reopening_transport(tmp_path, monkeypatch):
    """A dead proxy pool's successes, failures and unknown attempts survive migration."""
    calls = []
    async def call(self, context, message):
        if message['op'] == 'merge':
            return {'results': [r for group in message['groups'] for r in group['results']]}
        calls.append((str(self.config.proxy), message['query']))
        if message['query'] == 'unknown':
            raise asyncio.CancelledError()
        status = 'network_error' if message['query'] == 'failed' else 'ok'
        return {'status': status, 'reason': 'fixture',
                'results': [{'url': 'https://source.example/result', 'title': 'result'}] if status == 'ok' else [],
                'http': [{'host': 'search.example', 'http_status': 200 if status == 'ok' else None, 'elapsed_s': .01}]}
    monkeypatch.setattr(NativeSearchSession, 'call', call)
    async def run():
        config = SearchConfig(engines=['google'], language='all', source_interval_s=0,
                              suspend_s=0, failure_limit=100, query_failure_limit=100)
        path = tmp_path / 'migration.sqlite'
        old = SearchRoutePool(cache_path=path, config=config,
            routes=[{'name': 'old', 'proxy': 'http://old.example:3128', 'interval_s': 0}],
            max_route_attempts=1, failure_limit=100)
        good = await old.search('good'); failed = await old.search('failed')
        with pytest.raises(asyncio.CancelledError):
            await old.search('unknown')
        identity = 'a' * 64
        with old.routes[0]['session']._db() as db:
            db.execute('CREATE TABLE native_search_session_history '
                       '(pool TEXT, token TEXT, profile TEXT, created_at REAL)')
            db.execute('INSERT INTO native_search_session_history VALUES(?,?,?,?)',
                       (identity, 'retired', old.routes[0]['session'].profile, time.time()))
        await old.aclose()
        count = len(calls)
        current = SearchRoutePool(cache_path=path, config=config,
            routes=[{'name': 'fixed', 'proxy': 'http://new.example:3128', 'interval_s': 0}],
            max_route_attempts=1, failure_limit=100,
            reuse_session_pools=[{'identity': identity, 'search': config.snapshot()}])
        try:
            assert await current.search('good') == good
            assert await current.search('failed') == failed
            uncertain = await current.search('unknown')
            assert uncertain['engine_receipts'][0]['status'] == 'interrupted'
            assert len(calls) == count
            assert (await current.search('fresh'))['status'] == 'ok'
            assert calls[-1] == ('http://new.example:3128', 'fresh')
            assert len(calls) == count + 1
        finally:
            await current.aclose()
        with pytest.raises(ValueError, match='only an outgoing proxy change'):
            SearchRoutePool(cache_path=path, config=SearchConfig(engines=['bing']),
                routes=[{'name': 'fixed', 'proxy': None}], max_route_attempts=1,
                reuse_session_pools=[{'identity': identity, 'search': config.snapshot()}])
    asyncio.run(run())
