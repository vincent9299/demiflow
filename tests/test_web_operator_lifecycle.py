"""Dataset nodes own network resources through EOF, failure and cancellation."""
import asyncio

import pytest

from demiflow import data
from demiflow.collect.operators import SearchWeb
from demiflow.collect.session import WebSession


class ProbeSession(WebSession):
    def __init__(self, tmp_path):
        super().__init__(cache_path=tmp_path/'calls.sqlite', object_directory=tmp_path/'objects',
                         search_url='http://search.example', search_profile='fixture')
        self.events = []
        self.managers = []

    async def acquire_operator(self, owner):
        await super().acquire_operator(owner)
        if self.connections not in self.managers:
            self.managers.append(self.connections)
        self.events.append(('acquire', len(self._operator_owners)))

    async def release_operator(self, owner):
        await super().release_operator(owner)
        self.events.append(('release', len(self._operator_owners), self.connections.closed))

    async def search(self, query, **kwargs):
        assert not self.connections.closed
        self.events.append(('search', len(self._operator_owners)))
        await asyncio.sleep(.01)
        return {'status': 'ok', 'candidates': []}



def seed():
    return data.from_items([{'requests': [{'request_id': 'r', 'query': 'seed'}]}])














async def test_direct_async_calls_remain_caller_owned(tmp_path):
    web = WebSession(cache_path=tmp_path/'calls.sqlite', object_directory=tmp_path/'objects',
                     search_url='http://search.example', search_profile='fixture')
    web._ensure_connections()
    manager = web.connections
    assert not manager.closed and not web._operator_owners
    await web.aclose()
    assert manager.closed and manager.maintenance.done()


async def test_foreign_action_cannot_close_current_operators(tmp_path):
    web = ProbeSession(tmp_path)
    owner = object()
    await web.acquire_operator(owner)
    async def other_action():
        with pytest.raises(RuntimeError, match='another action'):
            await web.acquire_operator(object())
        with pytest.raises(RuntimeError, match='another action'):
            await web.aclose()
    await asyncio.create_task(other_action())
    assert not web.connections.closed and len(web._operator_owners) == 1
    await web.release_operator(owner)
    assert web.connections.closed and web.connections.maintenance.done()


@pytest.mark.parametrize('extra', [
    {'connection_policy': {'max_clients': 0}},
    {'connection_policy': {'maintenance_interval_s': .00001}},
    {'connection_policy': {'max_total_connections': True}},
    {'_connection_manager': object()},
])
def test_invalid_connection_declarations_fail_without_io(tmp_path, extra):
    with pytest.raises(ValueError):
        WebSession(cache_path=tmp_path/'absent.sqlite', object_directory=tmp_path/'objects', **extra)
    assert not (tmp_path/'absent.sqlite').exists()
