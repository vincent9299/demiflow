"""Sequential A/B: historical replay, loopback Dataset, or one owned GPU model."""
import argparse
import asyncio
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import threading


async def main(args):
    if not 1<=args.rounds<=12:raise ValueError('Expected at most 12 rounds')
    if not 1<=args.rows<=2000 or not 1<=args.workers<=8:raise ValueError('Experiment limits')
    from journal_cpu_worker import read,case_lines
    cases=case_lines(args.cases)[:200];prompt_size=len(read(args.prompt,1024**2))
    if not cases or any(len(row)>1024**2 for row in cases):raise ValueError('Case byte limit')
    # Reserve for request/response records, two checkpoint copies, SQLite page
    # and lease overhead before starting any write phase. Bodies are capped at
    # 8 KiB by the child, including real GPU responses.
    phases=2*args.rounds+(6 if args.scenario=='gpu' else 2)
    input_bytes=sum(len(cases[i%len(cases)]) for i in range(args.rows))
    disk_reservation=phases*(2*input_bytes+args.rows*(2*prompt_size+8*8192+16384))
    if args.scenario!='replay' and disk_reservation>1024**3:raise ValueError('Experiment disk reservation exceeds 1 GiB')
    out=Path(args.output);out.mkdir(exist_ok=False)
    report={'scenario':args.scenario,'rounds':[],'warmup':[],'profile':False,
            'disk_reservation_bytes':disk_reservation if args.scenario!='replay' else 0};gold=None
    owner=server=thread=None
    try:
        url=None
        if args.scenario=='http':
            class Handler(BaseHTTPRequestHandler):
                protocol_version='HTTP/1.1'
                def setup(self):
                    super().setup();self.connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                def do_POST(self):
                    size=int(self.headers['Content-Length'])
                    if not 0<size<=1024**2:self.send_error(413);return
                    payload=json.loads(self.rfile.read(size))
                    value=(int(hashlib.sha256(payload['state'].encode()).hexdigest()[:6],16)%1001)/1000
                    body={'model':payload['model'],'answers':{k:{'type':'noul','noul':value} for k in payload['questions']},
                          'usage':{'prompt_tokens':7,'completion_tokens':1}}
                    raw=json.dumps(body).encode();self.send_response(200)
                    self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
                def log_message(self,*a):pass
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            url=f'http://127.0.0.1:{server.server_port}/v1'
        elif args.scenario=='gpu':
            if args.gpu not in (0,1):raise ValueError('Only GPU 0/1 authorized')
            if subprocess.check_output(['nvidia-smi',f'--id={args.gpu}','--query-compute-apps=pid','--format=csv,noheader']).strip():
                raise RuntimeError('GPU occupied')
            from demiflow.services.deployment import deployment_config,DeploymentService
            from demiflow.services.prepare import prepare_deployment,adapter_version
            cfg=deployment_config(dict(profile='autotrust-jev',weights=args.weights,python=args.python,gpus=[args.gpu],
                directory=str(out/'services'),revision='journal-cpu-20261101',performance='optimized',
                request_concurrency=4,options={'cuda_graphs':{'max_captures':32}},startup_timeout_s=900,shutdown_timeout_s=30))
            await asyncio.to_thread(prepare_deployment,'AutoTrust-JEV-27B-v1.1',cfg)
            owner=DeploymentService('AutoTrust-JEV-27B-v1.1',cfg).bind();await owner.ensure_ready();url=owner.base_url
            report.update(config=cfg,adapter_version=adapter_version())
        async def invoke(variant,index,diagnostic=False):
            nonlocal gold
            destination=out/(f'{index}-{variant}'+('-diagnostic' if diagnostic else ''))
            cmd=[sys.executable,str(Path(__file__).with_name('journal_cpu_worker.py')),'--variant',variant,
                '--mode','replay' if args.scenario=='replay' else 'online','--baseline',args.baseline,
                '--prompt',args.prompt,'--cases',args.cases,'--output',str(destination),
                '--rows',str(args.rows),'--workers',str(args.workers),'--storage',args.storage]
            if args.source:cmd.extend(['--source',args.source])
            if url:cmd.extend(['--url',url])
            if diagnostic:cmd.append('--diagnostic')
            with (out/(destination.name+'.log')).open('w') as log:
                process=await asyncio.create_subprocess_exec(*cmd,stdout=log,stderr=asyncio.subprocess.STDOUT)
                try:
                    code=await asyncio.wait_for(process.wait(),260)
                    if code:raise RuntimeError('Benchmark child failed: '+str(destination)+'.log')
                finally:
                    if process.returncode is None:
                        process.terminate()
                        try:await asyncio.wait_for(process.wait(),10)
                        except asyncio.TimeoutError:process.kill();await process.wait()
            result=json.loads((destination/'report.json').read_text());result['cycle']=index
            if gold is None:gold=result['exact_sha256']
            else:assert result['exact_sha256']==gold,'Output/identity/usage/ref mismatch'
            print(json.dumps({k:result[k] for k in ['variant','cycle','wall_s','cpu_s','rps','p95_ms']}),flush=True)
            return result
        async def status():
            import httpx
            async with httpx.AsyncClient(trust_env=False,timeout=20) as http:
                response=await http.get(url+'/models');response.raise_for_status()
                caps=response.json()['data'][0]['capabilities'];state=caps['execution']
                assert caps['parallel']==2 and not caps['batch']['supported']
                assert not state['failed'] and not state['mismatches'] and state['cache_bytes']<=4*1024**3
                return state
        if args.scenario=='gpu':
            if args.rows!=200:raise ValueError('GPU experiment uses 200 distinct rows')
            for i in range(6):
                report['warmup'].append(await invoke('candidate',f'warm{i}'))
                if (await status())['captures']==32:break
            assert (await status())['captures']==32
        for cycle in range(args.rounds):
            for variant in (['baseline','candidate'] if cycle%2==0 else ['candidate','baseline']):
                report['rounds'].append(await invoke(variant,cycle))
                if args.scenario=='gpu':assert (await status())['captures']==32
                (out/'report.json').write_text(json.dumps(report,indent=2))
        report['summary']={v:{k:statistics.median(r[k] for r in report['rounds'] if r['variant']==v)
            for k in ['wall_s','cpu_s','rps','p95_ms','voluntary_switches','peak_rss_bytes']} for v in ['baseline','candidate']}
        if args.scenario=='gpu':report['final_status']=await status()
        if args.scenario=='http':
            report['diagnostics']=[await invoke(v,'diagnostic',True) for v in ['baseline','candidate']]
        report['exact']=True;(out/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report['summary']),flush=True)
    finally:
        if owner is not None:await owner.aclose()
        if server is not None:
            server.shutdown();server.server_close();thread.join()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['baseline','prompt','cases','output']:p.add_argument('--'+name,required=True)
    p.add_argument('--scenario',choices=['replay','http','gpu'],required=True)
    p.add_argument('--source');p.add_argument('--storage',choices=['legacy','throughput'],default='legacy')
    p.add_argument('--rounds',type=int,default=8);p.add_argument('--rows',type=int,default=1000)
    p.add_argument('--workers',type=int,default=4);p.add_argument('--gpu',type=int,default=1)
    p.add_argument('--weights');p.add_argument('--python')
    asyncio.run(asyncio.wait_for(main(p.parse_args()),1800))
