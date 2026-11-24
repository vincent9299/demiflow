"""Isolated baseline/candidate runner; no module replacement in production."""
import argparse
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import resource
import sys
import time


def read(path, maximum=32*1024**2):
    with Path(path).open('rb') as stream: data=stream.read(maximum+1)
    if len(data)>maximum: raise ValueError('Experiment input budget exceeded')
    return data


def case_lines(path):
    data=read(path)
    if data.count(b'\n')>512:raise ValueError('Case count exceeds 512 before splitting')
    lines=data.splitlines()
    if any(len(row)>1024**2 for row in lines):raise ValueError('Case row exceeds 1 MiB')
    return lines


def load_variant(args):
    import demiflow.operator_llm as package
    if 'demiflow.operator_llm.runtime' in sys.modules: raise RuntimeError('Load variant before runtime')
    hashes={}
    for name in ['call_ref','sqlite_journal','client']:
        path=(Path(args.baseline)/f'{name}.py' if args.variant=='baseline'
              else Path(__file__).resolve().parents[1]/'demiflow/operator_llm'/f'{name}.py')
        hashes[name]=hashlib.sha256(read(path,1024**2)).hexdigest()
        if args.variant=='baseline':
            full='demiflow.operator_llm.'+name
            spec=importlib.util.spec_from_file_location(full,path);module=importlib.util.module_from_spec(spec)
            sys.modules[full]=module;spec.loader.exec_module(module);setattr(package,name,module)
    return hashes


async def main(args):
    if not 1<=args.rows<=2000 or not 1<=args.workers<=8: raise ValueError('Experiment limits')
    hashes=load_variant(args)
    import yaml
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.call_ref import read_call
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    out=Path(args.output);out.mkdir(exist_ok=False)
    lines=case_lines(args.cases)
    cases=[json.loads(line) for line in lines[:200]]
    doc=yaml.safe_load(read(args.prompt,1024**2))
    if args.mode=='replay':
        if args.rows!=1000 or len(cases)<100:raise ValueError('Historical fixture requires 1000 identities')
        values=[{'payload':{**cases[i%100]['state'],'smoke_id':i}} for i in range(args.rows)]
        path=Path(args.source)/'calls.sqlite';options={'trust_env':False,'sqlite_journal':{'path':str(path),'read_only':True}}
    else:
        if len(cases)!=200:raise ValueError('Expected 200 online cases')
        doc['prompts']['identity']['model']={'name':'AutoTrust-JEV-27B-v1.1','transport':'systemone','base_url':args.url}
        values=[{'payload':{**(cases[i%200]['state'] if isinstance(cases[i%200]['state'],dict)
            else {'text':cases[i%200]['state']}),'profile_row':i}} for i in range(args.rows)]
        path=out/'calls.sqlite';options={'trust_env':False,'max_response_bytes':8192,'max_event_bytes':8192,
            'sqlite_journal':{'path':str(path),'max_requests':args.rows}}
        if args.storage=='throughput':options['sqlite_journal']['throughput']={}
    pack=parse_prompt_pack(yaml.safe_dump(doc,allow_unicode=True))
    submissions={}
    if args.diagnostic:
        from demiflow.services.blocking_io import BlockingIOPool
        original_run=BlockingIOPool.run
        async def counted(self,fn,*a,**kw):
            submissions[self.name]=submissions.get(self.name,0)+1
            return await original_run(self,fn,*a,**kw)
        BlockingIOPool.run=counted
    async def runtime_replay():
        runtime=AsyncOperatorLLMRuntime(pack,InProcessOperatorLLMCoordinator(max_requests=0),options)
        runtime.concurrency=args.workers;position=0;rows=[None]*args.rows;lat=[]
        async def work():
            nonlocal position
            while position<args.rows:
                i=position;position+=1;start=time.perf_counter()
                answer,trace=await runtime.call_with_trace('identity',values[i])
                assert trace['reused'] is True
                rows[i]={'row_id':i,'answer':answer['result'],'call':trace};lat.append(time.perf_counter()-start)
        try:await asyncio.gather(*(work() for _ in range(args.workers)))
        finally:await runtime.aclose()
        return rows,lat
    if args.mode=='replay':await runtime_replay()  # equal warmup, separate runtime
    timings=[];online_lat=[]
    if args.mode!='replay':
        original_call=AsyncOperatorLLMRuntime.call_with_trace
        async def timed_call(self,*a,**kw):
            start=time.perf_counter()
            try:return await original_call(self,*a,**kw)
            finally:online_lat.append(time.perf_counter()-start)
        AsyncOperatorLLMRuntime.call_with_trace=timed_call
    def checkpoint(replay):
        from demiflow.data.api import DataAPI
        dataset_doc=json.loads(json.dumps(doc));dataset_doc['prompts']['identity']['template']='{{ payload }}'
        dataset_pack=parse_prompt_pack(yaml.safe_dump(dataset_doc,allow_unicode=True))
        data=[{'row_id':i,'payload':json.dumps(v['payload'],ensure_ascii=False,sort_keys=True,separators=(',',':'))}
              for i,v in enumerate(values)]
        opts=json.loads(json.dumps(options))
        if replay:opts['sqlite_journal']['read_only']=True
        return DataAPI().from_items(data).map_prompt_async('identity',config=dataset_pack,
            inputs={'payload':'payload'},output='answer',call_output='call',options=opts,
            concurrency=args.workers,queue_depth=args.workers,max_requests=0 if replay else args.rows
            ).checkpoint(str(out/('replay.jsonl' if replay else 'rows.jsonl')),version='v1').take_all()
    r0=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();c0=time.process_time()
    if args.mode=='replay':rows,timings=await runtime_replay()
    else:rows=await asyncio.to_thread(checkpoint,False)
    elapsed=time.perf_counter()-start;used=time.process_time()-c0;r1=resource.getrusage(resource.RUSAGE_SELF)
    if args.mode!='replay':timings=list(online_lat)
    rows.sort(key=lambda row:row['row_id'])
    assert len(rows)==args.rows
    if args.mode!='replay':
        assert all(not row['call']['reused'] for row in rows)
        replay=await asyncio.to_thread(checkpoint,True);replay.sort(key=lambda row:row['row_id'])
        assert [r['answer'] for r in rows]==[r['answer'] for r in replay]
        assert all(r['call']['reused'] for r in replay)
    normalized=[]
    for row in rows:
        call=row['call'];assert call['request_id'] and call['response_ref']['kind']=='response'
        normalized.append([row['row_id'],row['answer'],call['request_id'],call.get('usage'),
            {k:v for k,v in call['response_ref'].items() if k!='journal_path'}])
    # Verify the trace points at committed provider records, outside timing.
    for index in sorted({0,args.rows//2,args.rows-1}):
        stored=read_call(rows[index]['call']['response_ref'])
        assert stored['status_code']==200 and stored['body']['answers']['identity']==rows[index]['answer']
    journal=SQLitePromptJournal(path,read_only=True,**({'throughput':{}} if args.storage=='throughput' else {}))
    try:stats=journal.stats()
    finally:journal.close()
    assert stats['responses']==args.rows and stats['uncertain']==0
    assert len(timings)==args.rows
    timings.sort()
    result=dict(variant=args.variant,mode=args.mode,storage=args.storage,rows=args.rows,workers=args.workers,
        wall_s=elapsed,cpu_s=used,rps=args.rows/elapsed,p95_ms=timings[int(args.rows*.95)]*1000,
        latency_scope='runtime request including journal and schema; action wall also includes Dataset checkpoint',
        voluntary_switches=r1.ru_nvcsw-r0.ru_nvcsw,involuntary_switches=r1.ru_nivcsw-r0.ru_nivcsw,
        peak_rss_bytes=r1.ru_maxrss*1024,source_hashes=hashes,journal=stats,submissions=submissions,
        exact_sha256=hashlib.sha256(json.dumps(normalized,ensure_ascii=False,sort_keys=True).encode()).hexdigest())
    (out/'report.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['baseline','prompt','cases','output']:p.add_argument('--'+name,required=True)
    p.add_argument('--variant',choices=['baseline','candidate'],required=True)
    p.add_argument('--mode',choices=['replay','online'],required=True)
    p.add_argument('--storage',choices=['legacy','throughput'],default='legacy')
    p.add_argument('--source');p.add_argument('--url');p.add_argument('--diagnostic',action='store_true')
    p.add_argument('--rows',type=int,default=1000);p.add_argument('--workers',type=int,default=4)
    asyncio.run(asyncio.wait_for(main(p.parse_args()),240))
