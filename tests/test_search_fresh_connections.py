"""Real local HTTP verifies transport renewal independently of adapter identity."""
import asyncio
import pytest
from test_native_search_runtime import server, engines
from demiflow.collect.native_search import SearchConfig
from demiflow.collect.search_routes import SearchRoutePool


@pytest.mark.parametrize('reuse',[True,False])
def test_connection_policy_and_saved_query_identity(server,engines,tmp_path,reuse):
    config=SearchConfig(engines=engines[:1],language='all',workers=1,
        request_concurrency=1,host_interval_s=0,source_interval_s=0)
    def pool(keep):return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',config=config,
        routes=[{'name':'route','proxy':None,'interval_s':0,'reuse_connections':keep}],
        max_route_attempts=1)
    async def run():
        session=pool(reuse)
        try:
            a=await session.search('first')
            assert (await session.search('second'))['status']=='ok'
            assert len(server[1])==2
            assert (server[1][0]['port']==server[1][1]['port']) is reuse
            profile=session.routes[0]['session'].profile
            assert await session.search('first')==a and len(server[1])==2
        finally:await session.aclose()
        other=pool(not reuse)
        try:
            assert await other.search('first')==a
            assert other.routes[0]['session'].profile==profile
            assert len(server[1])==2 and other.route_attempts==0
        finally:await other.aclose()
    asyncio.run(run())


def local_session_factory(*,token,ttl_s,options):
    return None


def test_renewable_pool_reuses_tcp_then_closes_expired_session(server,engines,tmp_path):
    from demiflow.collect.search_sessions import RenewableSearchRoutePool
    import time
    config=SearchConfig(engines=engines[:1],language='all',workers=4,
        request_concurrency=1,host_interval_s=0,source_interval_s=0)
    async def run():
        session=RenewableSearchRoutePool(cache_path=tmp_path/'renew.sqlite',config=config,routes=[],
            max_route_attempts=1,session_pool=dict(factory=__name__+':local_session_factory',
                size=1,min_healthy=1,interval_s=.001,refill_interval_s=.001,replacement_delay_s=.001))
        try:
            first=await session.search('first')
            await session.search('second')
            assert len(server[1])==2 and server[1][0]['port']==server[1][1]['port']
            old=session.routes[1]['session']
            session.routes[1]['entry']['expires_at']=time.time()-1
            await session.maintain();await asyncio.sleep(.01)
            await session.search('third')
            assert old.closed and server[1][2]['port']!=server[1][1]['port']
            assert await session.search('first')==first and len(server[1])==3
        finally:await session.aclose()
    asyncio.run(run())
