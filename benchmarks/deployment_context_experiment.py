"""Measure declaration reuse with real config parsing and an inert test lease.

This isolates host context overhead; it is not model throughput evidence.
"""
import argparse
import ast
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path
import statistics
import time

import yaml

from demiflow.operator_llm import runtime as runtime_module
from demiflow.operator_llm.model import parse_prompt_model
from demiflow.services import deployment
from schema_cpu_experiment import bounded_read


async def main(args):
    original_source = bounded_read(args.baseline,1024**2)
    tree = ast.parse(original_source)
    cls = next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='AsyncOperatorLLMRuntime')
    fn = next(n for n in cls.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='transport_context')
    scope = vars(runtime_module).copy()
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<snapshot-transport-context>','exec'),scope)
    methods = {'baseline':scope['transport_context'],'reuse':runtime_module.AsyncOperatorLLMRuntime.transport_context}
    doc = yaml.safe_load(bounded_read(args.prompt,1024**2))
    model = parse_prompt_model(doc['prompts']['identity']['model'],label='model')
    original_factory = deployment.DeploymentService; original_method = methods['reuse']
    constructions = 0
    class Lease:
        base_url = 'http://fixture.invalid/v1'
        async def ensure_ready(self): await asyncio.sleep(0)
        @asynccontextmanager
        async def request_slot(self): yield
        async def aclose(self): pass
    def factory(*a,**kw):
        nonlocal constructions
        constructions += 1
        service = original_factory(*a,**kw)
        service.bind = lambda *a: Lease()
        return service
    deployment.DeploymentService = factory
    report = {'baseline_sha256':hashlib.sha256(original_source).hexdigest(),'rows':2000,'rounds':[]}
    try:
        for cycle in range(8):
            for name in (['baseline','reuse'] if cycle%2==0 else ['reuse','baseline']):
                runtime_module.AsyncOperatorLLMRuntime.transport_context = methods[name]
                rt = runtime_module.AsyncOperatorLLMRuntime(None,runtime_module.InProcessOperatorLLMCoordinator())
                count = 0; constructions = 0
                async def work():
                    nonlocal count
                    while count < 2000:
                        count += 1
                        async with rt.transport_context(model) as url: assert url == Lease.base_url
                start = time.perf_counter(); c0 = time.process_time()
                try: await asyncio.gather(*(work() for _ in range(4)))
                finally: await rt.aclose()
                row = dict(variant=name,cycle=cycle,wall_s=time.perf_counter()-start,cpu_s=time.process_time()-c0,constructions=constructions)
                assert constructions == (2000 if name=='baseline' else 1)
                report['rounds'].append(row)
        report['summary'] = {name:{key:statistics.median(r[key] for r in report['rounds'] if r['variant']==name)
                                  for key in ['wall_s','cpu_s','constructions']} for name in methods}
        Path(args.output).write_text(json.dumps(report,indent=2));print(json.dumps(report['summary']),flush=True)
    finally:
        deployment.DeploymentService = original_factory
        runtime_module.AsyncOperatorLLMRuntime.transport_context = original_method


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['baseline','prompt','output']:p.add_argument('--'+name,required=True)
    asyncio.run(asyncio.wait_for(main(p.parse_args()),60))
