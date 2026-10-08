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

    async def fetch(self, url):
        await asyncio.sleep(.02)
        assert not self.connections.closed
        self.events.append(('fetch', len(self._operator_owners)))
        return {'status': 'ok', 'url': url, 'document_ref': None}


def seed():
    return data.from_items([{'requests': [{'request_id': 'r', 'query': 'seed'}]}])


def test_shared_operators_close_before_slow_non_network_tail(tmp_path):
    web = ProbeSession(tmp_path)
    checked = []
    async def tail(row):
        async with asyncio.timeout(1):
            while not web.connections.closed:
                await asyncio.sleep(.005)
        assert web.connections.maintenance.done()
        checked.append(True)
        return row
    pipeline = (seed().search_web(requests='requests', output='search', session=web)
        .map(lambda r: {**r, 'urls': [{'request_id': 'r', 'urls': ['http://example.org/a']}]})
        .fetch_documents(requests='urls', output='documents', session=web)
        .map_async(tail))
    assert web.connections is None and not (tmp_path/'calls.sqlite').exists()
    stats = pipeline.run_stream()
    releases = [event for event in web.events if event[0] == 'release']
    assert releases == [('release', 1, False), ('release', 0, True)]
    assert checked == [True] and len(web.managers) == 1
    assert stats.metrics['resources']['ProbeSession:0']['connections']['closed']
    assert not stats.metrics['resources']['ProbeSession:0']['connections']['maintenance_running']


def test_empty_and_filtered_nodes_create_no_http_clients_and_reexecute(tmp_path):
    web = ProbeSession(tmp_path)
    pipeline = seed().search_web(requests='requests', output='out', session=web, when=lambda _: False)
    first = pipeline.run_stream()
    second = pipeline.run_stream()
    data.from_items([]).search_web(requests='requests', output='out', session=web).run_stream()
    assert len(web.managers) == 3 and all(m.closed and m.maintenance.done() for m in web.managers)
    assert not (tmp_path/'calls.sqlite').exists()
    assert first.metrics['resources']['ProbeSession:0']['connections']['clients_created'] == 0
    assert second.metrics['resources']['ProbeSession:0']['connections']['clients_created'] == 0


def test_same_actor_at_two_nodes_is_released_only_after_both_finish(tmp_path):
    web = ProbeSession(tmp_path)
    actor = SearchWeb('requests', 'out', web, None, 5, 1)
    seed().map_async(actor).map_async(actor).run_stream()
    assert len([e for e in web.events if e[0] == 'search']) == 2
    assert [e for e in web.events if e[0] == 'release'] == [('release', 0, True)]


def test_failure_releases_shared_manager_and_keeps_original_error(tmp_path):
    web = ProbeSession(tmp_path)
    async def fail(row):
        raise ValueError('downstream fixture failure')
    pipeline = (seed().search_web(requests='requests', output='out', session=web)
                .map_async(fail)
                .fetch_documents(requests='requests', output='docs', session=web))
    with pytest.raises(ValueError, match='downstream fixture failure'):
        pipeline.run_stream()
    assert web.connections.closed and web.connections.maintenance.done()
    assert not web._operator_owners


def test_partial_start_cleanup_does_not_leave_maintenance_running(tmp_path):
    web = ProbeSession(tmp_path)
    class FailStart:
        concurrency = 1
        async def astart(self):
            raise ValueError('start fixture failure')
        async def __call__(self, row):
            return row
        async def aclose(self):
            pass
    pipeline = seed().search_web(requests='requests', output='out', session=web).map_async(FailStart())
    with pytest.raises(ValueError, match='start fixture failure'):
        pipeline.run_stream()
    assert web.connections.closed and web.connections.maintenance.done()
    assert not web._operator_owners


def test_external_cancellation_releases_operator_owned_manager(tmp_path):
    web = ProbeSession(tmp_path)
    class CancelAction:
        concurrency = 1
        async def astart(self):
            # This is the action's task, not an individual row worker.
            task = asyncio.current_task()
            asyncio.get_running_loop().call_later(.02, task.cancel)
        async def __call__(self, row):
            await asyncio.sleep(10)
            return row
        async def aclose(self):
            pass
    pipeline = seed().search_web(requests='requests', output='out', session=web).map_async(CancelAction())
    with pytest.raises(asyncio.CancelledError):
        pipeline.run_stream()
    assert web.connections.closed and web.connections.maintenance.done()
    assert not web._operator_owners


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
