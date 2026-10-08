import asyncio

from demiflow.collect.operators import SearchWeb


class _Session:
    def __init__(self):
        self.calls = []
        self.fail_once = True

    async def search(self, query, **kwargs):
        self.calls.append(query)
        if query == "transient" and self.fail_once:
            self.fail_once = False
            raise RuntimeError("temporary")
        return {"status": "ok", "candidates": [{"img_src": query}]}


async def _run(tmp_path):
    session = _Session()
    checkpoint = tmp_path / "operator.sqlite"
    actor = SearchWeb("requests", "results", session, None, 5, 2, checkpoint=checkpoint)
    row = {"requests": [
        {"request_id": "one", "query": "one"},
        {"request_id": "two", "query": "transient"},
    ]}
    try:
        await actor(row)
    except RuntimeError:
        pass
    assert actor._checkpoint.stats() == {"completed": 1, "retryable": 1}
    output = await actor(row)
    assert [value["status"] for value in output["results"]] == ["ok", "ok"]
    assert actor._checkpoint.stats() == {"completed": 2}
    assert session.calls.count("one") == 1


def test_search_web_persists_child_tasks_and_retries(tmp_path):
    asyncio.run(_run(tmp_path))

class _FlakyRowOperator:
    def __init__(self):
        self.fail = True

    async def __call__(self, row):
        if self.fail:
            self.fail = False
            raise RuntimeError("temporary")
        return {**row, "done": True}


def test_generic_map_async_replays_without_upstream_rows(tmp_path):
    from demiflow import data

    operator = _FlakyRowOperator()
    checkpoint = tmp_path / "generic.sqlite"
    first = data.from_items([{"record_id": "r1"}]).map_async(
        operator, checkpoint=checkpoint, checkpoint_operator="generic")
    try:
        first.run_stream()
    except RuntimeError:
        pass
    resumed = data.from_items([]).map_async(
        operator, checkpoint=checkpoint, checkpoint_operator="generic")
    stats = resumed.run_stream()
    assert stats.emitted == 1
