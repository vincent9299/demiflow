"""Model-quality evidence and a bounded, opt-in HTTP acceptance harness.

Importing this module does not load a model, contact a service or inspect GPUs.
The worker can also import it directly from its isolated Python environment.
"""
import asyncio
import hashlib
import json
import math
from pathlib import Path
import random
import time


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,
        allow_nan=False,separators=(',',':')).encode()).hexdigest()


def quality_policy(value):
    """An explicit numerical acceptance policy, independent of application thresholds.

    label_margin permits only changes between nearly tied answers in BOTH
    distributions. Raw label changes remain visible in v3 evidence.
    """
    if not isinstance(value,dict) or set(value)!={'probability_atol','label_margin'}:
        raise ValueError('Quality policy requires probability_atol and label_margin')
    if any(type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=1 for v in value.values()):
        raise ValueError('Quality policy values must be finite probabilities')
    if value['label_margin']>value['probability_atol']:
        raise ValueError('Label margin cannot exceed probability tolerance')
    return dict(value)


def read_evidence(path, identity, max_size, *, parallel=1, details=False):
    with Path(path).open('rb') as stream: raw=stream.read(1024*1024+1)
    if len(raw)>1024*1024: raise ValueError('Quality report exceeds 1 MiB')
    report=json.loads(raw)
    if not isinstance(report,dict): raise ValueError('Invalid quality evidence')
    version=report.get('schema_version')
    policy=quality_policy(report.get('policy')) if version=='demiflow_batch_quality_v3' else None
    if (version not in {'demiflow_batch_quality_v2','demiflow_batch_quality_v3'}
            or report.get('identity')!=identity or identity.get('hardware',{}).get('kind')!='cuda'
            or report.get('passed') is not True
            or type(report.get('sample_count')) is not int or report['sample_count']<200
            or type(report.get('max_size')) is not int or not max_size<=report['max_size']<=64
            or report.get('partner_rounds')!=3
            or report.get('label_mismatches')!=0
            or report.get('probability_failures')!=0 or not report.get('cases_sha256')
            or type(report.get('parallel')) is not int or not max(2,parallel)<=report['parallel']<=64):
        raise ValueError('Batch quality evidence is missing, stale, incomplete or incompatible')
    if report.get('tested_batch_sizes')!=list(range(1,report['max_size']+1)):
        raise ValueError('Quality evidence must cover every permitted batch size')
    coverage=report.get('input_coverage')
    if (not isinstance(coverage,dict) or not coverage.get('modalities') or not coverage.get('question_types')
            or not isinstance(coverage.get('modalities'),list)
            or any(x not in ('text','image') for x in coverage['modalities'])
            or not isinstance(coverage.get('question_types'),list)
            or any(x not in ('noul','choice') for x in coverage['question_types'])
            or not isinstance(coverage.get('choice_sizes'),list)
            or any(type(x) is not int or not 1<=x<=255 for x in coverage['choice_sizes'])
            or ('choice' in coverage['question_types'])!=bool(coverage['choice_sizes'])
            or not isinstance(coverage.get('shapes'),list) or not coverage['shapes']
            or any(not isinstance(x,str) for x in coverage['shapes'])
            or type(coverage.get('max_questions')) is not int or not 1<=coverage['max_questions']<=128):
        raise ValueError('Quality evidence requires explicit input coverage')
    atol=report.get('probability_atol')
    if (type(atol) not in (int,float) or not math.isfinite(atol)
            or not 0<=atol<=(policy['probability_atol'] if policy else 1e-3)
            or policy is not None and atol != policy['probability_atol']):
        raise ValueError('Batch quality tolerance must be predeclared and <= 1e-3')
    modes=['single','parallel_single']+['batch_'+str(n) for n in range(1,report['max_size']+1)]
    metrics=report.get('metrics',{})
    for mode in modes:
        metric=metrics.get(mode,{})
        if policy is not None and (type(metric.get('label_changes')) is not int or metric['label_changes']<0):
            raise ValueError('Numerical policy evidence must report raw label changes')
        expected=report['sample_count']*(3 if mode.startswith('batch_') else 1)
        if (metric.get('rows')!=expected or metric.get('label_mismatches')!=0
                or metric.get('probability_failures')!=0
                or metric.get('batch_size')!=(int(mode[6:]) if mode.startswith('batch_') else 1)
                or metric.get('parallel_requests')!=(1 if mode=='single' else report['parallel'])
                or type(metric.get('max_probability_delta')) not in (int,float)
                or not 0<=metric['max_probability_delta']<=atol):
            raise ValueError('Incomplete or failing per-mode quality evidence')
    maximum=report.get('max_probability_delta')
    if type(maximum) not in (int,float) or not math.isfinite(maximum) or not 0<=maximum<=atol:
        raise ValueError('Quality evidence error exceeds its declared tolerance')
    result=(hashlib.sha256(raw).hexdigest(),coverage)
    return result if details else result[0]


def input_coverage(cases):
    modalities=set(); kinds=set(); sizes=set(); shapes=set(); maximum=0
    for case in cases:
        if not isinstance(case,dict) or not isinstance(case.get('questions'),dict):
            raise ValueError('Acceptance cases require SystemOne questions')
        modality='image' if case.get('images') else 'text'
        modalities.add(modality)
        maximum=max(maximum,len(case['questions']))
        shape=[]
        for question in case['questions'].values():
            kinds.add(question['type'])
            if question['type']=='choice': sizes.add(len(question['criteria']))
            shape.append((question['type'],len(question['criteria']) if question['type']=='choice' else 0))
        shapes.add(json.dumps([modality,sorted(shape)],separators=(',',':')))
    return {'modalities':sorted(modalities),'question_types':sorted(kinds),
            'choice_sizes':sorted(sizes),'max_questions':maximum,'shapes':sorted(shapes)}


def covered_request(request, coverage):
    actual=input_coverage([request])
    return (set(actual['shapes'])<=set(coverage['shapes'])
        and set(actual['modalities'])<=set(coverage['modalities'])
        and set(actual['question_types'])<=set(coverage['question_types'])
        and set(actual['choice_sizes'])<=set(coverage['choice_sizes'])
        and actual['max_questions']<=coverage['max_questions'])


def validate_answers(case, response):
    """A consistently omitted question must not count as equivalent output."""
    answers=response.get('answers')
    if not isinstance(answers,dict) or set(answers)!=set(case['questions']):
        raise ValueError('Acceptance response question keys differ from the request')
    for key,question in case['questions'].items():
        answer=answers[key]
        if not isinstance(answer,dict) or answer.get('type')!=question['type']:
            raise ValueError('Acceptance response question type differs')
        if question['type']=='choice':
            probabilities=answer.get('probabilities')
            if not isinstance(probabilities,dict) or set(probabilities)!=set(question['criteria']):
                raise ValueError('Acceptance response choice keys differ from the request')


def compare(expected, actual, atol, *, label_margin=0):
    """Compare answers only; usage and timing may vary without changing quality."""
    a,b=expected.get('answers'),actual.get('answers')
    if not isinstance(a,dict) or not a or not isinstance(b,dict) or set(a)!=set(b):
        return 1,1,float('inf')
    labels=failures=0; maximum=0.0
    for key,answer in a.items():
        other=b[key]
        if not isinstance(answer,dict) or not isinstance(other,dict) or answer.get('type')!=other.get('type'):
            labels+=1; failures+=1; continue
        if answer['type']=='choice':
            changed=answer.get('choice')!=other.get('choice')
            left,right=answer.get('probabilities'),other.get('probabilities')
            if not isinstance(left,dict) or not isinstance(right,dict) or set(left)!=set(right):
                failures+=1; continue
            if not left or answer.get('choice') not in left or other.get('choice') not in right:
                failures+=1
            tied=False
            if changed and label_margin>0:
                x,y=answer.get('choice'),other.get('choice')
                values=[left.get(x),left.get(y),right.get(x),right.get(y)]
                if all(type(v) in (int,float) and math.isfinite(v) for v in values):
                    tied=(0<=left[x]-left[y]<=label_margin and 0<=right[y]-right[x]<=label_margin)
            labels+=changed and not tied
            pairs=[(left[k],right[k]) for k in left]
            for distribution in (left,right):
                if (all(type(x) in (int,float) for x in distribution.values())
                        and not math.isclose(sum(distribution.values()),1,abs_tol=1e-6)):
                    failures+=1
        elif answer['type']=='noul':
            pairs=[(answer.get('noul'),other.get('noul'))]
            if all(type(x) in (int,float) for x in pairs[0]):
                x,y=pairs[0]
                labels+=((x>=.5)!=(y>=.5) and not (label_margin>0 and max(abs(x-.5),abs(y-.5))<=label_margin))
        else:
            labels+=1; failures+=1; continue
        for x,y in pairs:
            if (type(x) not in (int,float) or type(y) not in (int,float)
                    or not math.isfinite(x) or not math.isfinite(y) or not 0<=x<=1 or not 0<=y<=1):
                failures+=1; continue
            delta=abs(x-y); maximum=max(maximum,delta); failures+=delta>atol
    return labels,failures,maximum


def load_cases(path):
    cases=[]; total=0
    with Path(path).open('rb') as stream:
        while True:
            line=stream.readline(1024*1024+1)
            if not line: break
            total+=len(line)
            if len(line)>1024*1024 or total>32*1024*1024 or len(cases)>=10000:
                raise ValueError('Cases exceed 1 MiB/row, 32 MiB total or 10000 rows')
            cases.append(json.loads(line))
    if len(cases)<200: raise ValueError('Quality acceptance requires at least 200 cases')
    return cases


async def benchmark(base_url, model, cases, *, max_size=8, parallel=4, probability_atol=1e-6,
                    memory_probe=None, policy=None):
    """Run only when explicitly invoked in a maintenance window.

    Fixed-size worker sets keep task/connection growth bounded. Memory probe,
    when supplied, reports aggregate MiB for this deployment's owned processes.
    """
    import httpx
    if not 1<=max_size<=64 or not 2<=parallel<=64: raise ValueError('Invalid acceptance capacity')
    policy=quality_policy(policy) if policy is not None else None
    if policy is not None: probability_atol=policy['probability_atol']
    if (type(probability_atol) not in (int,float) or not math.isfinite(probability_atol)
            or not 0<=probability_atol<=(1 if policy else 1e-3)): raise ValueError('Invalid probability tolerance')
    if not 200<=len(cases)<=10000: raise ValueError('Acceptance requires 200..10000 cases')
    try:
        from .model_adapters import validate_request
    except ImportError:
        from model_adapters import validate_request
    for case in cases:
        validate_request(case)
        if case.get('model',model)!=model: raise ValueError('Acceptance case targets another model')
    if len({json.dumps(case,ensure_ascii=False,separators=(',',':'),allow_nan=False) for case in cases})<200:
        raise ValueError('Acceptance requires at least 200 distinct cases')
    async with httpx.AsyncClient(base_url=base_url.rstrip('/')+'/',trust_env=False,timeout=600,
            # Long serialized batches can leave other pooled sockets idle for
            # more than HTTPX's five-second default. Keep this bounded pool for
            # the acceptance session instead of churning connections each round.
            limits=httpx.Limits(max_connections=parallel,max_keepalive_connections=parallel,
                               keepalive_expiry=None)) as client:
        async def exchange(path,payload=None):
            async with client.stream('GET' if payload is None else 'POST',path,json=payload) as response:
                response.raise_for_status(); raw=bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    if len(raw)+len(chunk)>16*1024*1024: raise ValueError('Acceptance response exceeds 16 MiB')
                    raw.extend(chunk)
            return json.loads(raw)
        models=(await exchange('models'))['data']
        caps=next(m['capabilities'] for m in models if m['id']==model)
        identity=caps['execution_identity']
        if caps.get('experimental_batch_size',0)<max_size and caps.get('batch',{}).get('max_size',0)<max_size:
            raise ValueError('Service has not enabled the requested validation batch size')
        baseline=[]; metrics={}; labels=failures=usage_failures=0; max_delta=0.0
        for case in cases[:3]:
            await exchange('systemone',{'model':model,**case})
        async def run(mode, order, size, workers, rounds=1, batched=False):
            nonlocal labels,failures,usage_failures,max_delta
            answers=[None]*len(cases); latencies=[]; position=0; peak=None
            mode_labels=mode_failures=mode_usage=mode_changes=0; mode_delta=0.0
            started=time.monotonic(); stop=asyncio.Event()
            async def sample():
                nonlocal peak
                while not stop.is_set():
                    measured=await asyncio.to_thread(memory_probe)
                    if measured is not None: peak=max(peak or 0,measured)
                    try: await asyncio.wait_for(stop.wait(),.5)
                    except TimeoutError: pass
            async def worker():
                nonlocal position,mode_labels,mode_failures,mode_usage,mode_delta,mode_changes
                while position<len(order):
                    indices=order[position:position+size]; position+=len(indices)
                    before=time.monotonic()
                    if not batched:
                        values=[await exchange('systemone',{'model':model,**cases[indices[0]]})]
                    else:
                        response=await exchange('systemone_batch',{'model':model,'requests':[cases[i] for i in indices]})
                        values=response.get('responses')
                        if not isinstance(values,list) or len(values)!=len(indices): raise ValueError('Invalid batch count')
                    latency=time.monotonic()-before
                    for index,value in zip(indices,values,strict=True):
                        if not isinstance(value,dict) or 'error' in value: raise ValueError('Acceptance request failed')
                        validate_answers(cases[index],value)
                        answers[index]=value; latencies.append(latency)
                        reference=baseline[index] if baseline else value
                        mismatch,invalid,delta=compare(reference,value,probability_atol,
                            label_margin=policy['label_margin'] if policy else 0)
                        mode_changes+=compare(reference,value,probability_atol)[0]
                        if max_size==1 and baseline and value.get('usage')!=baseline[index].get('usage'):
                            mode_usage+=1
                        mode_labels+=mismatch; mode_failures+=invalid; mode_delta=max(mode_delta,delta)
            sampling=asyncio.create_task(sample()) if memory_probe else None
            tasks=[]
            try:
                for iteration in range(rounds):
                    position=0
                    if batched: random.Random(1729+size*101+iteration).shuffle(order)
                    tasks=[asyncio.create_task(worker()) for _ in range(workers)]
                    await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done(): task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                stop.set()
                if sampling: await sampling
            elapsed=time.monotonic()-started; latencies.sort()
            metrics[mode]={'rows':len(cases)*rounds,'rows_per_second':len(cases)*rounds/elapsed,'elapsed_s':elapsed,
                'latency_p50_s':latencies[len(latencies)//2],
                'latency_p95_s':latencies[min(len(latencies)-1,int(len(latencies)*.95))],
                'peak_gpu_memory_mib':peak,'parallel_requests':workers,'batch_size':size,
                'label_mismatches':mode_labels,'probability_failures':mode_failures,'usage_mismatches':mode_usage,
                'label_changes':mode_changes,
                'max_probability_delta':mode_delta if math.isfinite(mode_delta) else None}
            labels+=mode_labels; failures+=mode_failures; usage_failures+=mode_usage; max_delta=max(max_delta,mode_delta)
            print(json.dumps({'mode':mode,**metrics[mode]}),flush=True)
            return answers
        order=list(range(len(cases)))
        baseline=await run('single',order,1,1)
        await run('parallel_single',order,1,parallel)
        for size in (range(1,max_size+1) if max_size>1 else ()):
            await run('batch_'+str(size),order,size,parallel,rounds=3,batched=True)
        return {'schema_version':('demiflow_batch_quality_v3' if policy else 'demiflow_batch_quality_v2') if max_size>1 else 'demiflow_single_performance_v1','identity':identity,
            **({'policy':policy} if policy else {}),
            'sample_count':len(cases),'cases_sha256':digest(cases),'max_size':max_size,'parallel':parallel,
            'tested_batch_sizes':list(range(1,max_size+1)) if max_size>1 else [],'input_coverage':input_coverage(cases),
            'partner_rounds':3 if max_size>1 else 0,'probability_atol':probability_atol,'label_mismatches':labels,
            'probability_failures':failures,'max_probability_delta':max_delta if math.isfinite(max_delta) else None,
            'usage_mismatches':usage_failures,'passed':labels==failures==usage_failures==0,'created_at':time.time(),'metrics':metrics}
