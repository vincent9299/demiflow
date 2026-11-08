"""Real journal payloads through HTTP and Dataset, with exact output checks.

Only connects to an explicitly supplied loopback benchmark endpoint. Does not
prepare deployments or start/stop services. Run with an outer process timeout.
"""
import argparse
import asyncio
import json
from pathlib import Path
import time
from urllib.parse import urlsplit

from triage_gpu_probe import bounded_rows


async def main(args):
    url = urlsplit(args.url)
    if url.hostname != '127.0.0.1' or url.scheme != 'http' or url.path != '/v1':
        raise ValueError('Use an owned loopback benchmark endpoint ending in /v1')
    if args.workers not in (1, 2, 3, 4, 8): raise ValueError('Unbounded worker count')
    out = Path(args.output); out.mkdir(exist_ok=False)
    cases = bounded_rows(args.journal)
    payloads = [row['request']['payload'] for row in cases]
    expected = [{k: row['response']['body'][k] for k in ('answers', 'usage')} for row in cases]
    import httpx
    import yaml
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime
    from demiflow.operator_llm.call_ref import read_call
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    raw = Path(args.prompt).read_bytes()
    if len(raw) > 128*1024: raise ValueError('Prompt byte budget exceeded')
    doc = yaml.safe_load(raw)
    definition = doc['prompts']['triage_concept']
    assert definition['template'] == '{{ state }}'
    name = definition['model']['name']
    assert all(p['model'] == name and isinstance(p['state'], str) for p in payloads)
    definition['model'] = dict(name=name, transport='systemone', base_url=args.url)
    pack = parse_prompt_pack(yaml.safe_dump(doc, allow_unicode=True, sort_keys=False))
    report = dict(workers=args.workers, phases=[])

    def save(): (out/'report.json').write_text(json.dumps(report, indent=2))

    def finish(label, started, timestamp, cpu, latencies):
        wall = time.perf_counter()-started
        samples = sorted(latencies)
        assert len(samples) == 200
        result = dict(label=label, rows=200, wall_s=wall, rps=200/wall,
                      cpu_s=time.process_time()-cpu, started_at=timestamp, finished_at=time.time(),
                      p50_ms=samples[100]*1000, p95_ms=samples[190]*1000)
        report['phases'].append(result); save(); print(json.dumps(result), flush=True)

    async with httpx.AsyncClient(trust_env=False, timeout=120, limits=httpx.Limits(
            max_connections=args.workers, max_keepalive_connections=args.workers)) as client:
        # Explicit bounded warmup is excluded; no payload modification or result cache.
        for warm in range(3):
            position = 0
            async def warm_worker():
                nonlocal position
                while position < 200:
                    i = position; position += 1
                    response = await client.post(args.url+'/systemone', json=payloads[i])
                    response.raise_for_status()
                    body = response.json()
                    assert {k: body[k] for k in ('answers', 'usage')} == expected[i]
            await asyncio.gather(*(warm_worker() for _ in range(args.workers)))
        position = 0; latencies = []
        timestamp = time.time(); start = time.perf_counter(); cpu = time.process_time()
        async def worker():
            nonlocal position
            while position < 200:
                i = position; position += 1; before = time.perf_counter()
                response = await client.post(args.url+'/systemone', json=payloads[i])
                response.raise_for_status(); body = response.json()
                assert {k: body[k] for k in ('answers', 'usage')} == expected[i]
                latencies.append(time.perf_counter()-before)
        await asyncio.gather(*(worker() for _ in range(args.workers)))
        finish('http', start, timestamp, cpu, latencies)

    latencies = []
    original = AsyncOperatorLLMRuntime.call_with_trace
    async def timed(runtime, *a, **kw):
        start = time.perf_counter()
        try: return await original(runtime, *a, **kw)
        finally: latencies.append(time.perf_counter()-start)
    AsyncOperatorLLMRuntime.call_with_trace = timed
    path = out/'calls.sqlite'
    def execute(replay=False):
        options = dict(trust_env=False, stream=False, timeout_s=120,
                       sqlite_journal=dict(path=str(path), max_requests=200, read_only=replay))
        return data.from_items([dict(row_id=i, state=p['state']) for i,p in enumerate(payloads)]
            ).map_prompt_async('triage_concept', config=pack, inputs={'state':'state'},
                output='answer', call_output='call', error_output='error', options=options,
                concurrency=args.workers, queue_depth=args.workers, max_requests=0 if replay else 200
            ).checkpoint(out/('replay.jsonl' if replay else 'rows.jsonl'), version='v1').take_all()
    try:
        timestamp = time.time(); start = time.perf_counter(); cpu = time.process_time()
        rows = await asyncio.to_thread(execute)
        finish('dataset-journal-checkpoint', start, timestamp, cpu, latencies)
        assert len(rows) == 200
        for row in rows:
            i = row['row_id']; call = row['call']
            assert row['answer'] == expected[i]['answers']['triage_concept']
            assert call['reused'] is False
            saved = read_call(call['response_ref'])
            assert {k: saved['body'][k] for k in ('answers', 'usage')} == expected[i]
            request = read_call(call['request_ref'])
            assert request['payload'] == payloads[i]
        replay = await asyncio.to_thread(execute, True)
        assert all(row['call']['reused'] for row in replay)
        assert sorted((r['row_id'], r['answer']) for r in replay) == sorted((r['row_id'], r['answer']) for r in rows)
        journal = SQLitePromptJournal(path, read_only=True)
        try: report['journal'] = journal.stats()
        finally: journal.close()
        assert report['journal'] == dict(requests=200, responses=200, uncertain=0)
        report['passed'] = True; save()
    finally: AsyncOperatorLLMRuntime.call_with_trace = original


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('journal', 'prompt', 'url', 'output'): parser.add_argument('--'+name, required=True)
    parser.add_argument('--workers', type=int, required=True)
    asyncio.run(asyncio.wait_for(main(parser.parse_args()), 600))
