"""Shared HTTP capacity stays available when an unrelated host is busy."""
import asyncio
from urllib.parse import urlsplit

import pytest

from demiflow.collect.web import WebClient


def client(tmp_path,**options):
    return WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        fetch_concurrency=2,host_concurrency=1,host_interval_s=0,retries=0,**options)


async def test_host_waiters_leave_capacity_for_other_hosts(tmp_path):
    web=client(tmp_path);entered=asyncio.Event();release=asyncio.Event();fast=asyncio.Event();tasks=[]
    async def hop(url,totals,params=None,**options):
        if urlsplit(url).hostname=='slow.example':entered.set();await release.wait()
        else:fast.set()
        return (200,{},b'fixture',url),asyncio.get_running_loop().time()+1
    web._routed_hop=hop
    try:
        tasks.append(asyncio.create_task(web._attempt('https://slow.example/1')))
        await asyncio.wait_for(entered.wait(),1)
        tasks.append(asyncio.create_task(web._attempt('https://slow.example/2')))
        await asyncio.sleep(.01)
        tasks.append(asyncio.create_task(web._attempt('https://fast.example/1')))
        await asyncio.wait_for(fast.wait(),.2)
        assert not release.is_set()
        assert web.fetch_gate.active==1 and web.hosts['slow.example'].active==1
    finally:
        release.set();await asyncio.gather(*tasks);await web.aclose()
    assert web.fetch_gate.active==0


async def test_cross_host_redirects_release_global_capacity_before_waiting(tmp_path):
    web=client(tmp_path);web.fetch_gate.concurrency=2
    arrivals=asyncio.Event();count=0;tasks=[]
    async def hop(url,totals,params=None,deadline=None,**options):
        nonlocal count
        if url.endswith('/start'):
            count+=1
            if count==2:arrivals.set()
            await arrivals.wait()
            other='b.example' if urlsplit(url).hostname=='a.example' else 'a.example'
            return (302,{'location':'https://'+other+'/end'},b'',url),asyncio.get_running_loop().time()+1
        assert deadline is not None
        return (200,{},b'fixture',url),deadline
    web._routed_hop=hop
    try:
        tasks=[asyncio.create_task(web._attempt('https://'+host+'/start')) for host in ['a.example','b.example']]
        results=await asyncio.wait_for(asyncio.gather(*tasks),1)
        assert all(r[0]==200 for r in results)
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True);await web.aclose()
    assert web.fetch_gate.active==0 and all(g.active==0 for g in web.hosts.values())


async def test_redirect_host_wait_shares_deadline_and_releases_permits(tmp_path):
    web=client(tmp_path);busy=asyncio.Event();release=asyncio.Event();tasks=[]
    async def hop(url,totals,params=None,deadline=None,**options):
        if url.endswith('/hold'):
            busy.set();await release.wait()
            return (200,{},b'fixture',url),asyncio.get_running_loop().time()+1
        return (302,{'location':'https://busy.example/end'},b'',url),asyncio.get_running_loop().time()+.05
    web._routed_hop=hop
    try:
        tasks.append(asyncio.create_task(web._attempt('https://busy.example/hold')))
        await asyncio.wait_for(busy.wait(),1)
        with pytest.raises(TimeoutError):await web._attempt('https://other.example/start')
        assert web.fetch_gate.active==1
    finally:
        release.set();await asyncio.gather(*tasks);await web.aclose()
    assert web.fetch_gate.active==0 and all(g.active==0 for g in web.hosts.values())
