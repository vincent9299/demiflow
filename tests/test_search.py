"""demiflow.collect.search 测试：注册表/分派/K 封顶/连接失败判据。"""

from __future__ import annotations

import httpx
import pytest

from demiflow.collect import search as ms


class FakeEngine:
    name = "fake"
    k_cap = 3

    async def search(self, query, k, *, lang="en", client=None):
        return [{"tiers": [f"https://x/{query}/{i}"], "landing": None}
                for i in range(k)]


def test_register_and_dispatch():
    ms.register_engine(FakeEngine())
    eng = ms.get_engine("fake")
    assert eng.k_cap == 3


async def test_engine_search_k_cap():
    ms.register_engine(FakeEngine())
    rows = await ms.engine_search("fake", "q", k=99, lang="zh")
    assert len(rows) == 3                  # k_cap 封顶
    assert rows[0]["tiers"][0] == "https://x/q/0"


def test_unknown_engine_rejected():
    with pytest.raises(ValueError, match="未登记"):
        ms.get_engine("no_such_engine")


def test_structural_protocol():
    assert isinstance(FakeEngine(), ms.SearchEngine)


def test_is_connect_failure():
    try:
        try:
            raise httpx.ConnectError("refused")
        except httpx.ConnectError as e:
            raise RuntimeError("wrapper") from e
    except RuntimeError as e:
        assert ms.is_connect_failure(e)
    assert not ms.is_connect_failure(ValueError("other"))
