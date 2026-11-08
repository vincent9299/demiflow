"""Own one bounded service; test real triage HTTP, Dataset and journal replay."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time


async def main(args):
    if args.gpu not in (0,1) or args.streams not in (1,2,3,4): raise ValueError('GPU/stream limit')
    if subprocess.check_output(['nvidia-smi',f'--id={args.gpu}','--query-compute-apps=pid','--format=csv,noheader']).strip():
        raise RuntimeError('GPU occupied')
    out=Path(args.output); out.mkdir(exist_ok=False)
    source=Path(args.source).resolve(); bench=Path(__file__).resolve().parent
    # 2 x 200-row journals + checkpoints/replays, bounded 32 KiB input rows.
    if os.statvfs(out).f_bavail*os.statvfs(out).f_frsize < 256*1024**2:
        raise RuntimeError('Insufficient reserved experiment disk')
    with socket.socket() as available:
        available.bind(('127.0.0.1',0)); port=available.getsockname()[1]
    url=f'http://127.0.0.1:{port}/v1'
    from demiflow.services.http import ManagedHTTPService
    from demiflow.services.cuda_graphs import graph_config
    settings=graph_config({'streams':args.streams})
    command=[args.python,str(bench/'platform_profile_worker.py' if args.diagnostic else source/'demiflow/services/model_worker.py'),
        '--profile',args.profile,'--model',args.model,'--name',Path(args.model).name,'--port',str(port),
        '--max-inflight','8','--identity','{}','--cuda-graphs',json.dumps(settings)]
    token=secrets.token_hex(16)
    environment=dict(TRITON_CACHE_DIR=str(out.parent/'triton-cache'),
        DEMIFLOW_PROFILE_REPO=str(source),DEMIFLOW_PROFILE_TOKEN=token,DEMIFLOW_PROFILE_OUTPUT=str(out/'stages'))
    owner=ManagedHTTPService(command,base_url=url,root=source,gpus=[args.gpu],env=environment,
        expected_model=Path(args.model).name,log_path=out/'service.log',startup_timeout_s=600,
        log_max_bytes=8*1024**2,shutdown_timeout_s=20).bind()
    sampler=None; sampler_file=None
    report=dict(profile=args.profile,streams=args.streams,source=str(source),diagnostic=args.diagnostic,phases=[])
    try:
        await owner.ensure_ready()
        sampler_file=(out/'gpu.csv').open('w')
        sampler=await asyncio.create_subprocess_exec('nvidia-smi',f'--id={args.gpu}',
            '--query-gpu=timestamp,index,utilization.gpu,utilization.memory,memory.used,power.draw,clocks.sm',
            '--format=csv,noheader','--loop-ms=1000',stdout=sampler_file,stderr=asyncio.subprocess.DEVNULL)
        import httpx
        async with httpx.AsyncClient(trust_env=False,timeout=30) as client:
            for workers in (4,8):
                if args.diagnostic:
                    response=await client.get(url.rsplit('/v1',1)[0]+'/__benchmark/reset',headers={'X-Benchmark-Token':token})
                    response.raise_for_status()
                directory=out/f'workers-{workers}'
                cmd=[sys.executable,str(bench/'triage_e2e_probe.py'),'--journal',args.journal,'--prompt',args.prompt,
                     '--url',url,'--output',str(directory),'--workers',str(workers)]
                with (out/f'workers-{workers}.log').open('w') as log:
                    process=await asyncio.create_subprocess_exec(*cmd,stdout=log,stderr=asyncio.subprocess.STDOUT)
                    try:
                        code=await asyncio.wait_for(process.wait(),650)
                        if code: raise RuntimeError(f'Client failed: {directory}.log')
                    finally:
                        if process.returncode is None:
                            process.terminate()
                            try: await asyncio.wait_for(process.wait(),10)
                            except asyncio.TimeoutError: process.kill(); await process.wait()
                result=json.loads((directory/'report.json').read_text())
                assert result['passed']
                response=await client.get(url+'/models');response.raise_for_status()
                caps=response.json()['data'][0]['capabilities'];state=caps['execution']
                assert caps['parallel']==args.streams and not caps['batch']['supported']
                assert not state['failed'] and not state['mismatches'] and not state['graph_disabled']
                assert state['cache_bytes']<=4*1024**3
                result['execution']=state
                if args.diagnostic:
                    response=await client.get(url.rsplit('/v1',1)[0]+f'/__benchmark/export?phase=w{workers}',
                                              headers={'X-Benchmark-Token':token})
                    response.raise_for_status(); result['stages']=response.json()
                report['phases'].append(result)
                (out/'report.json').write_text(json.dumps(report,indent=2))
                print(json.dumps(dict(workers=workers,phases=result['phases'])),flush=True)
            if args.steady_rows:
                if not 200<=args.steady_rows<=2000: raise ValueError('Steady request budget exceeded')
                from triage_gpu_probe import bounded_rows
                cases=bounded_rows(args.journal)
                async def continuous(count):
                    position=0;latencies=[];started=time.perf_counter();timestamp=time.time()
                    async def worker():
                        nonlocal position
                        while position<count:
                            i=position%200;position+=1;before=time.perf_counter()
                            response=await client.post(url+'/systemone',json=cases[i]['request']['payload'])
                            response.raise_for_status();body=response.json();gold=cases[i]['response']['body']
                            assert {k:body[k] for k in ('model','answers','usage')}=={k:gold[k] for k in ('model','answers','usage')}
                            latencies.append(time.perf_counter()-before)
                    await asyncio.gather(*(worker() for _ in range(8)))
                    wall=time.perf_counter()-started;latencies.sort()
                    return dict(rows=count,wall_s=wall,rps=count/wall,started_at=timestamp,finished_at=time.time(),
                                p50_ms=latencies[len(latencies)//2]*1000,p95_ms=latencies[int(.95*len(latencies))]*1000)
                report['steady_warmup']=await continuous(1000)
                before=(await client.get(url+'/models')).json()['data'][0]['capabilities']['execution']
                report['steady']=await continuous(args.steady_rows)
                after=(await client.get(url+'/models')).json()['data'][0]['capabilities']['execution']
                assert not after['mismatches'] and not after['failed'] and not after['graph_disabled']
                report['steady'].update(execution_before=before,execution_after=after)
                print(json.dumps({k:v for k,v in report['steady'].items() if not k.startswith('execution')}),flush=True)
        report['passed']=True;(out/'report.json').write_text(json.dumps(report,indent=2))
    finally:
        if sampler is not None:
            sampler.terminate();await sampler.wait()
        if sampler_file is not None:sampler_file.close()
        await owner.aclose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['source','python','model','journal','prompt','output']:
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--profile',choices=['pplx','amos'],required=True)
    parser.add_argument('--gpu',type=int,required=True);parser.add_argument('--streams',type=int,default=2)
    parser.add_argument('--diagnostic',action='store_true')
    parser.add_argument('--steady-rows',type=int,default=0)
    asyncio.run(asyncio.wait_for(main(parser.parse_args()),1800))
