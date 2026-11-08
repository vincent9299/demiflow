"""Unprofiled A/B on a single owned model; only the host schema function changes."""
import argparse
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import random
import statistics
import subprocess
import time

import httpx
import psutil
import yaml

from demiflow.operator_llm import runtime as runtime_module
from demiflow.operator_llm.parser import parse_prompt_pack, resolve_prompt
from demiflow.services.deployment import deployment_config, DeploymentService
from demiflow.services.prepare import prepare_deployment, adapter_version
from platform_cpu_profile import read_cases
from schema_cpu_experiment import bounded_read
from schema_probe import variants


async def main(args):
    if args.gpu not in (0,1) or not 4 <= args.rounds <= 12: raise ValueError('Experiment limits')
    if subprocess.check_output(['nvidia-smi',f'--id={args.gpu}','--query-compute-apps=pid','--format=csv,noheader']).strip():
        raise RuntimeError('GPU occupied')
    out = Path(args.output); out.mkdir(exist_ok=False)
    cfg = deployment_config(dict(profile=args.profile,weights=str(Path(args.weights).resolve()),
        python=str(Path(args.python).resolve()),gpus=[args.gpu],revision='schema-cpu-experiment-20261101',
        directory=str(out/'services'),performance='optimized',request_concurrency=4,
        options={'cuda_graphs':{'max_captures':32}},startup_timeout_s=900,shutdown_timeout_s=30))
    doc = yaml.safe_load(bounded_read(args.prompt,1024**2)); doc['prompts']['identity']['model'] = {'name':args.name,'deployment':cfg}
    pack = parse_prompt_pack(yaml.safe_dump(doc, allow_unicode=True)); schema = resolve_prompt(pack,'identity').response_schema
    (out/'prompt.yaml').write_text(yaml.safe_dump(doc,allow_unicode=True))
    cases = read_cases(args.cases)
    values = [{'payload':{**(c['state'] if isinstance(c['state'],dict) else {'text':c['state']}),'profile_row':i}} for i,c in enumerate(cases)]
    order = list(range(200)); random.Random(1729).shuffle(order)
    methods = variants(schema); original = runtime_module.validate_instance
    await asyncio.to_thread(prepare_deployment,args.name,cfg)
    owner = DeploymentService(args.name,cfg).bind()
    report = {'profile':args.profile,'adapter_version':adapter_version(),'config':cfg,'rounds':[], 'warmup':[], 'profiling':False}
    gold = None
    try:
        start = time.perf_counter(); await owner.ensure_ready(); report['startup_s'] = time.perf_counter()-start
        async with httpx.AsyncClient(trust_env=False,timeout=30) as http:
            async def status():
                response = await http.get(owner.base_url+'/models'); response.raise_for_status()
                caps = response.json()['data'][0]['capabilities']; s = caps['execution']
                assert caps['parallel']==2 and not caps['batch']['supported']
                assert not s['failed'] and not s['mismatches'] and s['cache_bytes']<=4*1024**3
                return s
            async def phase(name, cycle, journal=None):
                nonlocal gold
                runtime_module.validate_instance = methods[name]
                options = {'trust_env':False}
                if journal: options['sqlite_journal'] = {'path':str(out/'calls.sqlite'),'max_requests':200}
                rt = runtime_module.AsyncOperatorLLMRuntime(pack,runtime_module.InProcessOperatorLLMCoordinator(),options)
                rt.concurrency = 2
                position = 0; answers = [None]*200; lat = []
                async def work():
                    nonlocal position
                    while position < 200:
                        i = order[position]; position += 1; start = time.perf_counter()
                        value, trace = await rt.call_with_trace('identity',values[i]);answers[i]=value
                        if journal=='replay': assert trace['reused']
                        lat.append(time.perf_counter()-start)
                before = await status(); start = time.perf_counter(); c0 = time.process_time()
                try: await asyncio.gather(work(),work())
                finally: await rt.aclose()
                elapsed = time.perf_counter()-start; consumed = time.process_time()-c0
                if gold is None: gold = answers
                else: assert answers == gold, (name,cycle)
                after = await status(); lat.sort()
                if cycle >= 0: assert after['captures']==before['captures']==32
                if journal=='replay':
                    for key in ['native','hits','checks']: assert after[key]==before[key]
                return dict(variant=name,cycle=cycle,wall_s=elapsed,cpu_s=consumed,rps=200/elapsed,
                    p50_ms=lat[100]*1000,p95_ms=lat[190]*1000,p99_ms=lat[198]*1000,
                    client_rss_bytes=psutil.Process().memory_info().rss,
                    graph_hits=after['hits']-before['hits'],native=after['native']-before['native'],status=after)
            # Fixed capture count keeps model cache admission out of the host A/B.
            for _ in range(6):
                report['warmup'].append(await phase('baseline',-1))
                if (await status())['captures']==32: break
            assert (await status())['captures']==32, 'Could not stabilize capture count'
            names = ['baseline','python-lazy','python-compiled','rust']
            for cycle in range(args.rounds):
                names_order = names[cycle%4:]+names[:cycle%4]
                if (cycle//4)%2: names_order.reverse()
                for name in names_order:
                    row = await phase(name,cycle);report['rounds'].append(row)
                    print(json.dumps({k:row[k] for k in ['variant','cycle','wall_s','cpu_s','rps','p95_ms']}),flush=True)
                    (out/'report.json').write_text(json.dumps(report,indent=2))
            report['journal_write'] = await phase('rust',-1,journal='write')
            report['journal_replay'] = await phase('rust',-1,journal='replay')
            # Exercise the real public Dataset path with the experimental validator.
            from demiflow.data.api import DataAPI
            dataset_doc = deepcopy(doc)
            dataset_doc['prompts']['identity']['model'] = {'name':args.name,'transport':'systemone','base_url':owner.base_url}
            dataset_doc['prompts']['identity']['template'] = '{{ payload }}'
            dataset_pack = parse_prompt_pack(yaml.safe_dump(dataset_doc,allow_unicode=True))
            data = [{'row_id':i,'payload':json.dumps(values[i]['payload'],ensure_ascii=False,sort_keys=True,separators=(',',':'))} for i in order]
            def run_dataset():
                return DataAPI().from_items(data).map_prompt_async('identity',config=dataset_pack,inputs={'payload':'payload'},
                    output='answer',options={'trust_env':False},concurrency=2,queue_depth=2
                    ).checkpoint(str(out/'dataset.jsonl'),version='v1').take_all()
            result = await asyncio.to_thread(run_dataset)
            assert len(result)==200 and all(row['answer']==gold[row['row_id']]['result'] for row in result)
            report['dataset_exact'] = True
            report['summary'] = {name:{key:statistics.median(r[key] for r in report['rounds'] if r['variant']==name)
                for key in ['wall_s','cpu_s','rps','p95_ms','p99_ms']} for name in names}
            report['exact'] = True; report['final_status'] = await status()
            (out/'reference.json').write_text(json.dumps(gold)); (out/'report.json').write_text(json.dumps(report,indent=2))
            print(json.dumps(report['summary']),flush=True)
    finally:
        runtime_module.validate_instance = original
        await owner.aclose()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['profile','weights','python','name','cases','prompt','output']: p.add_argument('--'+name, required=True)
    p.add_argument('--gpu',type=int,default=1);p.add_argument('--rounds',type=int,default=8)
    asyncio.run(asyncio.wait_for(main(p.parse_args()),1800))
