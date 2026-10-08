"""Runtime admission pacing must preserve completed source request identities."""
import asyncio
import pytest
from demiflow.collect.session import WebSession
from demiflow.collect.native_search import NativeSearchSession


def test_http_pacing_is_lazy_and_preserves_profile(tmp_path, monkeypatch):
    profiles = []
    async def fake_search(self, query, **parameters):
        await self.initialize()
        profiles.append(self.profile)
        return self.http_gate.interval_s
    monkeypatch.setattr(NativeSearchSession,'search',fake_search)
    async def run(interval):
        session=WebSession(search={'engines':['google']},search_request_interval_s=interval,
                           cache_path=tmp_path/'search.sqlite',object_directory=tmp_path/'objects')
        try:
            return await session.search('example')
        finally:
            await session.aclose()
    first=WebSession(search_request_interval_s=5,cache_path=tmp_path/'absent/cache',object_directory=tmp_path/'objects')
    assert first.native is None and not (tmp_path/'absent').exists()
    assert asyncio.run(run(0))==0 and asyncio.run(run(5))==5
    assert profiles[0]==profiles[1]
    for value in (-1,float('nan'),True):
        with pytest.raises(ValueError):
            WebSession(search_request_interval_s=value,cache_path=tmp_path/'bad',object_directory=tmp_path/'objects')
