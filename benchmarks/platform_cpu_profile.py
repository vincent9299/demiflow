"""Profile the real service, operator and journal paths in an owned GPU process.

Run with the demiflow interpreter. Model packages stay in their existing env.
The temporary benchmark control route exists only in platform_profile_worker.
"""
import argparse
import asyncio
import cProfile
import hashlib
import json
import os
from pathlib import Path
import random
import secrets
import signal
import subprocess
import sys
import time

import httpx
import psutil
import yaml

from profile_metrics import Metrics
from demiflow.services import profiles
from demiflow.services.deployment import deployment_config, DeploymentService
from demiflow.services.prepare import prepare_deployment, adapter_version
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator


def read_cases(path):
    with Path(path).open('rb') as stream: raw=stream.read(32*1024**2+1)
    if len(raw)>32*1024**2: raise ValueError('Case file exceeds 32 MiB')
    lines=raw.splitlines()
    if len(lines)!=200 or any(len(line)>1024**2 for line in lines): raise ValueError('Expected 200 bounded cases')
    return [json.loads(line) for line in lines]


def cpu(pid):
    value=psutil.Process(pid).cpu_times();return value.user+value.system


def instrument_client(metrics):
    from demiflow.operator_llm import messages, runtime, client
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    from demiflow.execution.blocking_io import BlockingIOPool
    metrics.wrap(messages,'render_input','client.render')
    metrics.wrap(runtime,'validate_instance','client.schema')
    for name in ['prepare','decode']:
        metrics.wrap(client.AsyncOperatorLLMClient,name,'client.'+name)
    for name in ['lookup','lookup_with_references','reserve','response']:
        if hasattr(SQLitePromptJournal,name):
            metrics.wrap(SQLitePromptJournal,name,'journal.'+('lookup' if name.startswith('lookup') else name))
    run=BlockingIOPool.run
    async def measured(self, fn, *args, **kwargs):
        def work():
            with metrics.measure(self.name,profile_root=True):return fn(*args,**kwargs)
        return await run(self,work)
    BlockingIOPool.run=measured


async def main(args):
    repo=Path(__file__).resolve().parents[1];out=Path(args.output).resolve();out.mkdir(exist_ok=False)
    if args.gpu not in (0,1):raise ValueError('Only authorized GPU 0/1')
    if subprocess.check_output(['nvidia-smi',f'--id={args.gpu}','--query-compute-apps=pid','--format=csv,noheader']).strip():raise RuntimeError('GPU is occupied')
    cases=read_cases(args.cases);token=secrets.token_hex(24);metrics=Metrics();instrument_client(metrics)
    config=deployment_config(dict(profile=args.profile,weights=str(Path(args.weights).resolve()),python=str(Path(args.python).resolve()),
        revision='platform-cpu-profile-20261101',gpus=[args.gpu],directory=str(out/'services'),performance='optimized',
        request_concurrency=4,startup_timeout_s=900,shutdown_timeout_s=30))
    original_build=profiles.build_service
    def build(*a,**kw):
        service=original_build(*a,**kw)
        argv=list(service.command);assert Path(argv[1]).name=='model_worker.py'
        argv[1]=str(repo/'benchmarks/platform_profile_worker.py');service.command=tuple(argv)
        service.env.update(DEMIFLOW_PROFILE_REPO=str(repo),DEMIFLOW_PROFILE_TOKEN=token,DEMIFLOW_PROFILE_OUTPUT=str(out/'worker'),PYTHONFAULTHANDLER='1')
        return service
    profiles.build_service=build
    await asyncio.to_thread(prepare_deployment,args.name,config)
    owner=DeploymentService(args.name,config).bind()
    report={'profile':args.profile,'adapter_version':adapter_version(),'config':config,'phases':[],
            'cases_sha256':hashlib.sha256(Path(args.cases).read_bytes()).hexdigest()}
    def save():(out/'report.json').write_text(json.dumps(report,indent=2))
    declaration=yaml.safe_load(Path(args.prompt).read_text());prompt=declaration['prompts']['identity'];prompt['model']={'name':args.name,'deployment':config}
    pack=parse_prompt_pack(yaml.safe_dump(declaration,allow_unicode=True));values=[{'payload':{**(c['state'] if isinstance(c['state'],dict) else {'text':c['state']}),'profile_row':i}} for i,c in enumerate(cases)]
    reference={};runtime_gold={};order=list(range(200));random.Random(1729).shuffle(order)
    try:
        start=time.perf_counter();await owner.ensure_ready();report['startup_s']=time.perf_counter()-start
        async with httpx.AsyncClient(timeout=300,trust_env=False,limits=httpx.Limits(max_connections=4,max_keepalive_connections=4)) as http:
            async def control(action,**params):
                response=await http.get(owner.base_url.removesuffix('/v1')+'/__benchmark/'+action,params=params,headers={'X-Benchmark-Token':token});response.raise_for_status();return response.json()
            async def phase(label,kind,indices,profile=False,journal=None,workers=2):
                assert len(indices)<=4096 and 1<=workers<=8
                record=await control('reset',profile=int(profile));pid=record['pid']
                metrics.reset(profile);position=0;lat=[];results={}
                options={'trust_env':False}
                if journal:options['sqlite_journal']={'path':str(out/(journal+'.sqlite')),'max_requests':200}
                runtime=AsyncOperatorLLMRuntime(pack,InProcessOperatorLLMCoordinator(),options) if kind=='runtime' else None
                if runtime:runtime.concurrency=workers
                async def work():
                    nonlocal position
                    while position<len(indices):
                        i=indices[position];position+=1;start=time.perf_counter()
                        if kind=='http':
                            async with owner.request_slot():
                                response=await http.post(owner.base_url+'/systemone',json={'model':args.name,**cases[i]});
                            if response.status_code!=200:
                                (out/'last-error.json').write_text(response.text);response.raise_for_status()
                            value=response.json()
                            value={k:value[k] for k in ['answers','usage']}
                            if i in reference:assert value==reference[i],('HTTP mismatch',i)
                            else:reference[i]=value
                        else:
                            value,trace=await runtime.call_with_trace('identity',values[i])
                            if i in runtime_gold:assert value==runtime_gold[i],('Runtime mismatch',i)
                            else:runtime_gold[i]=value
                            if label.startswith('replay'):assert trace['reused'] is True
                        results[i]=value;lat.append(time.perf_counter()-start)
                cp=cProfile.Profile(timer=time.thread_time_ns,timeunit=1e-9) if profile else None
                if cp:metrics.profiles[-1]=cp
                sampler=None;sample_log=None
                if profile:
                    sample_log=(out/(label+'-spy.log')).open('w')
                    sampler=subprocess.Popen([args.py_spy,'record','--pid',str(pid),'--rate','100','--gil',
                        '--full-filenames','--function','--format','raw','--output',str(out/(label+'-worker.raw'))],
                        stdout=sample_log,stderr=subprocess.STDOUT)
                    await asyncio.sleep(.3)
                    if sampler.poll() is not None:raise RuntimeError('Sampling attach failed')
                p0,w0=cpu(os.getpid()),cpu(pid);start=time.perf_counter()
                if cp:cp.enable()
                try:await asyncio.gather(*(work() for _ in range(workers)))
                finally:
                    try:
                        if runtime:await runtime.aclose()
                    finally:
                        if cp:cp.disable()
                        if sampler is not None:
                            if sampler.poll() is None:sampler.send_signal(signal.SIGINT)
                            try:await asyncio.to_thread(sampler.wait,10)
                            finally:
                                if sampler.poll() is None:
                                    sampler.kill();await asyncio.to_thread(sampler.wait,5)
                                sample_log.close()
                elapsed=time.perf_counter()-start;client_cpu,worker_cpu=cpu(os.getpid())-p0,cpu(pid)-w0
                server=await control('export',phase=label);metrics.export(out/'client'/label)
                caps=(await http.get(owner.base_url+'/models')).json()['data'][0]['capabilities'];status=caps['execution']
                assert caps['parallel']==2 and not caps['batch']['supported'] and not status['failed'] and not status['mismatches']
                assert status['cache_bytes']<=4096*1024**2
                if label.startswith('replay'):assert server['stages'].get('http.request',{}).get('calls',0)==0
                lat.sort();r=dict(phase=label,kind=kind,rows=len(indices),workers=workers,profile=profile,wall_s=elapsed,rps=len(indices)/elapsed,
                    p50_ms=lat[len(lat)//2]*1000,p95_ms=lat[int(len(lat)*.95)]*1000,p99_ms=lat[min(len(lat)-1,int(len(lat)*.99))]*1000,
                    client_cpu_s=client_cpu,worker_cpu_s=worker_cpu,worker_rss_bytes=psutil.Process(pid).memory_info().rss,
                    client_rss_bytes=psutil.Process().memory_info().rss,status=status,server_stages=server['stages'])
                report['phases'].append(r);save();print(json.dumps({k:r[k] for k in ['phase','rows','rps','wall_s','client_cpu_s','worker_cpu_s']}),flush=True)
            await phase('http-cold','http',order)
            await phase('http-warm','http',order*2)
            await phase('http-profile','http',order,True)
            await phase('http-steady','http',order)
            await phase('runtime-cold','runtime',order)
            await phase('runtime-profile','runtime',order,True)
            await phase('runtime-steady','runtime',order)
            # Exercise the actual Dataset actor/stream path. Scalar JSON columns
            # preserve sparse input dictionaries without Arrow null insertion.
            from copy import deepcopy
            from demiflow.data.api import DataAPI
            dataset_doc=deepcopy(declaration)
            dataset_doc['prompts']['identity']['model']={'name':args.name,'transport':'systemone','base_url':owner.base_url}
            dataset_doc['prompts']['identity']['template']='{{ payload }}'
            dataset_pack=parse_prompt_pack(yaml.safe_dump(dataset_doc,allow_unicode=True))
            data_rows=[{'row_id':i,'payload':json.dumps(values[i]['payload'],ensure_ascii=False,sort_keys=True,separators=(',',':'))} for i in order]
            pid=(await control('reset',profile=1))['pid'];metrics.reset(True)
            start=time.perf_counter();p0,w0=cpu(os.getpid()),cpu(pid)
            def dataset_run():
                with metrics.measure('dataset.take_all',profile_root=True):
                    return DataAPI().from_items(data_rows).map_prompt_async('identity',config=dataset_pack,
                        inputs={'payload':'payload'},output='answer',options={'trust_env':False},concurrency=2,queue_depth=2
                        ).checkpoint(str(out/'dataset-output.jsonl'),version='v1').take_all()
            result=await asyncio.to_thread(dataset_run)
            assert len(result)==200
            for row in result:assert row['answer']==runtime_gold[row['row_id']]['result']
            elapsed=time.perf_counter()-start;client_cpu,worker_cpu=cpu(os.getpid())-p0,cpu(pid)-w0
            server=await control('export',phase='dataset-profile');metrics.export(out/'client/dataset-profile')
            report['phases'].append(dict(phase='dataset-profile',kind='dataset',rows=200,profile=True,wall_s=elapsed,
                rps=200/elapsed,client_cpu_s=client_cpu,worker_cpu_s=worker_cpu,server_stages=server['stages']));save()
            print('dataset-profile',elapsed,flush=True)
            await phase('journal-profile','runtime',order,True,journal='calls')
            await phase('replay-steady','runtime',(order*11)[:2048],journal='calls',workers=4)
            await phase('replay-profile','runtime',(order*11)[:2048],True,journal='calls',workers=4)
            (out/'http-reference.json').write_text(json.dumps(reference));(out/'runtime-reference.json').write_text(json.dumps(runtime_gold))
        report['passed']=True;save()
    finally:
        await owner.aclose();profiles.build_service=original_build


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['profile','weights','python','name','cases','prompt','output']:p.add_argument('--'+name,required=True)
    p.add_argument('--py-spy',default=str(Path(sys.executable).with_name('py-spy')))
    p.add_argument('--gpu',type=int,default=1);args=p.parse_args()
    asyncio.run(asyncio.wait_for(main(args),1200))
