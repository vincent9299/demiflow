"""Alternating, unprofiled schema microbenchmarks and real read-only replay."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import random
import statistics
import time

from demiflow.operator_llm import runtime as runtime_module
from demiflow.operator_llm.parser import parse_prompt_pack, resolve_prompt
from schema_probe import variants


def bounded_read(path, maximum):
    with Path(path).open('rb') as stream: raw = stream.read(maximum + 1)
    if len(raw) > maximum: raise ValueError('Input exceeds experiment budget')
    return raw


async def main(args):
    if not 4 <= args.rounds <= 20: raise ValueError('Expected 4..20 rounds')
    out = Path(args.output); out.mkdir(exist_ok=False)
    source = Path(args.source)
    raw = bounded_read(source/'prompt.yaml', 1024**2)
    pack = parse_prompt_pack(raw.decode()); schema = resolve_prompt(pack, 'identity').response_schema
    cases_raw = bounded_read(args.cases, 32*1024**2)
    cases = [json.loads(line) for line in cases_raw.splitlines()[:100]]
    if len(cases) != 100: raise ValueError('Expected at least 100 cases')
    values = [{'payload':{**cases[i%100]['state'], 'smoke_id':i}} for i in range(1000)]
    methods = variants(schema); original = runtime_module.validate_instance
    report = {'rounds':[], 'micro':[], 'rows':1000, 'profiling':False,
              'prompt_sha256':hashlib.sha256(raw).hexdigest(), 'cases_sha256':hashlib.sha256(cases_raw).hexdigest()}
    gold = None
    async def replay(name, cycle):
        nonlocal gold
        runtime_module.validate_instance = methods[name]
        runtime = runtime_module.AsyncOperatorLLMRuntime(pack,
            runtime_module.InProcessOperatorLLMCoordinator(max_requests=0),
            {'trust_env':False, 'sqlite_journal':{'path':str(source/'calls.sqlite'), 'read_only':True}})
        runtime.concurrency = 4
        position = 0; answers = [None]*1000; latency = []
        async def work():
            nonlocal position
            while position < 1000:
                i = position; position += 1; start = time.perf_counter()
                answer, trace = await runtime.call_with_trace('identity', values[i])
                assert trace['reused'] is True
                answers[i] = answer; latency.append(time.perf_counter()-start)
        start = time.perf_counter(); c0 = time.process_time()
        try: await asyncio.gather(*(work() for _ in range(4)))
        finally: await runtime.aclose()
        elapsed = time.perf_counter()-start; consumed = time.process_time()-c0
        if gold is None: gold = answers
        else: assert answers == gold, name
        latency.sort()
        result = dict(variant=name, cycle=cycle, wall_s=elapsed, cpu_s=consumed,
                      rps=1000/elapsed, p95_ms=latency[950]*1000, p99_ms=latency[990]*1000)
        print(json.dumps(result), flush=True)
        return result
    try:
        report['warmup'] = []
        for _ in range(2):
            for name in methods: report['warmup'].append(await replay(name, -1))
        # Round-robin rotation plus reversal balances first/last position.
        names = list(methods)
        for cycle in range(args.rounds):
            order = names[cycle%4:]+names[:cycle%4]
            if (cycle//4)%2: order.reverse()
            for name in order: report['rounds'].append(await replay(name, cycle))
            (out/'report.json').write_text(json.dumps(report, indent=2))
        sample = gold[0]
        micro_methods = dict(methods)
        micro_methods['rust-predicate-only'] = lambda value,schema: methods['rust'].predicate(value)
        micro_methods['python-predicate-only'] = lambda value,schema: methods['python-compiled'].predicate(value)
        for cycle in range(12):
            order = list(micro_methods); random.Random(1729+cycle).shuffle(order)
            for name in order:
                fn = micro_methods[name]; count = 20000; c0 = time.thread_time_ns(); start = time.perf_counter_ns()
                for _ in range(count): fn(sample, schema)
                report['micro'].append(dict(variant=name, cycle=cycle, iterations=count,
                    wall_ns=(time.perf_counter_ns()-start)/count, cpu_ns=(time.thread_time_ns()-c0)/count))
        report['exact'] = True
        report['summary'] = {name:{key:statistics.median(r[key] for r in report['rounds'] if r['variant']==name)
                                 for key in ['wall_s','cpu_s','rps','p95_ms']} for name in methods}
        report['micro_summary_ns'] = {name:statistics.median(r['cpu_ns'] for r in report['micro'] if r['variant']==name)
                                     for name in micro_methods}
        (out/'report.json').write_text(json.dumps(report, indent=2))
        print(json.dumps({'summary':report['summary'], 'micro_ns':report['micro_summary_ns']}), flush=True)
    finally: runtime_module.validate_instance = original


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['source','cases','output']: p.add_argument('--'+name, required=True)
    p.add_argument('--rounds', type=int, default=12)
    asyncio.run(asyncio.wait_for(main(p.parse_args()), 300))
