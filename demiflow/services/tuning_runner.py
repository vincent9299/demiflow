"""Explicit bounded deployment experiments, using the normal HTTP service owner."""
import asyncio
import hashlib
import json
import random
import time
import uuid
from pathlib import Path

from .deployment import canonical, deployment_config, fingerprint
from . import tuning


def request_digest(value):
    # Criteria/question/state field order can affect native tokenization. The
    # deployment's unordered configuration hash is not a request-corpus hash.
    raw=json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def load_cases(path, name):
    from .model_adapters import validate_request
    cases = []; total = 0; seen = set()
    with Path(path).open('rb') as source:
        while True:
            raw = source.readline(65537)
            if not raw: break
            total += len(raw)
            if len(raw)>65536 or total>16*1024**2 or len(cases)>=2000:
                raise ValueError('Tuning cases exceed 64 KiB/row, 16 MiB/file or 2000 rows')
            value = json.loads(raw)
            if not isinstance(value,dict): raise ValueError('Tuning case must be an object')
            if value.get('model',name) != name: raise ValueError('Tuning case targets another model')
            value = {'model': name, **value}
            validate_request(value)
            key = request_digest(value)
            if key in seen: raise ValueError('Tuning requires distinct cases, including holdout')
            seen.add(key); cases.append(value)
    if len(cases)<400: raise ValueError('Tuning needs 400+ distinct cases: 200+ search and 200+ holdout')
    random.Random(1729).shuffle(cases)
    split = len(cases)//2
    return cases[:split], cases[split:]


def response_digest(case, value):
    from .quality import validate_answers, compare
    if not isinstance(value,dict) or 'error' in value: raise ValueError('Tuning request returned an error')
    validate_answers(case,value)
    labels, invalid, _ = compare(value,value,0)
    if labels or invalid: raise ValueError('Native response has invalid probabilities or labels')
    if value.get('model') != case['model'] or not isinstance(value.get('usage'),dict):
        raise ValueError('Tuning response is missing model or usage')
    return fingerprint({key:value[key] for key in ('model','answers','usage')})


async def exchange(client, path, payload=None, *, response_budget=None):
    async with client.stream('GET' if payload is None else 'POST',path,json=payload) as response:
        raw = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=16384):
            if len(raw)+len(chunk)>256*1024: raise ValueError('Tuning response exceeds 256 KiB')
            if response_budget is not None:
                response_budget[0] += len(chunk)
                if response_budget[0]>32*1024**2:
                    raise ValueError('Tuning phase responses exceed 32 MiB')
            raw.extend(chunk)
        if response.is_error:
            # Read within the same byte limits before reporting status. Otherwise
            # a CUDA/runtime failure becomes an unhelpful generic HTTP 503.
            detail = bytes(raw[:2048]).decode('utf-8',errors='replace')
            raise ValueError(f'Tuning HTTP {response.status_code}: {detail}')
    return json.loads(raw)


async def measure(client, cases, workers, gold=None, seed=1729):
    order = list(range(len(cases))); random.Random(seed).shuffle(order)
    position = 0; latencies = []; answers = [None]*len(cases); mismatches = 0
    response_budget = [0]
    started = time.perf_counter()
    async def work():
        nonlocal position, mismatches
        while position<len(order):
            index = order[position]; position += 1
            before = time.perf_counter()
            body = await exchange(client,'systemone',cases[index],response_budget=response_budget)
            value = response_digest(cases[index],body)
            latencies.append(time.perf_counter()-before)
            answers[index] = value
            if gold is not None and value != gold[index]: mismatches += 1
    tasks = [asyncio.create_task(work()) for _ in range(workers)]
    try: await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
    elapsed = time.perf_counter()-started; latencies.sort()
    return dict(rows=len(cases),wall_s=elapsed,rps=len(cases)/elapsed,
                p50_ms=1000*latencies[len(latencies)//2],
                p95_ms=1000*latencies[min(len(latencies)-1,int(.95*len(latencies)))],
                mismatches=mismatches), answers


def idle(gpus):
    from .preparation_io import run_bounded
    result = run_bounded(['nvidia-smi','--id='+','.join(map(str,gpus)),
        '--query-compute-apps=pid','--format=csv,noheader'],timeout_s=10,output_bytes=65536)
    if result.strip(): raise ValueError('Tuning requires idle GPUs; existing services were not stopped')


def workspace(directory):
    directory.mkdir(parents=True,exist_ok=True)
    for index,_ in enumerate(directory.iterdir()):
        if index>=15: raise ValueError('Tuning history reached 16 runs; archive reviewed runs before tuning again')
    total = 0; count = 0
    for path in directory.rglob('*'):
        count += 1
        if count>4096: raise ValueError('Tuning history file-count budget exceeded')
        if path.is_symlink(): raise ValueError('Tuning history cannot contain symlinks')
        if path.is_file(): total += path.stat().st_size
    if total>256*1024**2: raise ValueError('Tuning history must leave 256 MiB for this run')
    import os
    disk = os.statvfs(directory)
    if disk.f_bavail*disk.f_frsize<512*1024**2: raise ValueError('Tuning needs 512 MiB free disk')
    result = directory/uuid.uuid4().hex; result.mkdir()
    return result


async def experiment(name, config, manifest, execution, search, holdout, budget, directory,
                     *, gold=None, validation=False):
    import httpx
    from .profiles import build_service
    await asyncio.to_thread(idle,config['gpus'])
    # Keep production deployment identity/ports, but isolate all logs and own an
    # action-scoped child. No prepare/install or persistent supervisor is used.
    prepared = {**manifest,'_execution':execution,'_log_directory':str(directory)}
    action = {**config,'lifetime':'action','shutdown_timeout_s':min(20,config['shutdown_timeout_s'])}
    owner = build_service(name,action,prepared).bind()
    try:
        await owner.ensure_ready()
        async with httpx.AsyncClient(base_url=owner.base_url.rstrip('/')+'/',trust_env=False,
                timeout=budget['request_timeout_s'],limits=httpx.Limits(
                    max_connections=execution['request_concurrency'],
                    max_keepalive_connections=execution['request_concurrency'])) as client:
            identity = (await exchange(client,'models'))['data'][0]['capabilities']['execution_identity']
            for key in ('model_contract','runtime','adapter_version','weights_signature'):
                if identity.get(key) != manifest.get(key):
                    raise ValueError('Experiment endpoint has a different deployment identity')
            if identity.get('execution_configuration') != execution:
                raise ValueError('Experiment endpoint has a different execution configuration')
            native_gold = gold
            if gold is None:
                _, native_gold = await measure(client,search+holdout,1)
            search_gold = native_gold[:len(search)]
            holdout_gold = native_gold[len(search):]
            phases = []
            for iteration in range(budget['warmup_rounds']):
                row,_ = await measure(client,search,execution['request_concurrency'],search_gold,iteration)
                if row['mismatches']: raise ValueError('Candidate warmup changed native outputs')
                phases.append(row)
            measured = []
            target = holdout if validation else search
            expected = holdout_gold if validation else search_gold
            for iteration in range(1 if validation else budget['rounds']):
                row,_ = await measure(client,target,execution['request_concurrency'],expected,1731+iteration)
                measured.append(row)
            caps = (await exchange(client,'models'))['data'][0]['capabilities']
            if caps['batch']['supported']: raise ValueError('Tuning must not enable cross-request batching')
            status = caps['execution']; tuning.check_execution(status,execution)
            result = dict(execution=execution,passed=all(r['mismatches']==0 for r in measured),
                          warmup=phases,rounds=measured,execution_status=status,
                          hardware=caps['execution_identity']['hardware'])
            if gold is None:
                result['reference'] = dict(rows=len(native_gold),outputs_sha256=fingerprint(native_gold))
            if validation:
                result.update(rows=len(holdout),mismatches=sum(r['mismatches'] for r in measured))
            return result,native_gold
    finally:
        # Cancellation never leaves an experiment model owning the GPU.
        closing = asyncio.create_task(owner.aclose())
        try: await asyncio.shield(closing)
        except asyncio.CancelledError:
            await closing
            raise


async def tune(name, configuration, cases_path, *, options=None, force=False):
    import httpx
    from .prepare import read_prepared, _versions, _write
    from .preparation_io import locked
    config = deployment_config(configuration); tuning.recipe(config)
    if config.get('performance') != 'auto':
        raise ValueError('Declare performance: auto before running services tune')
    budget = tuning.limits(options)
    search,holdout = load_cases(cases_path,name)
    path = tuning.report_path(name,config)
    manifest = await asyncio.to_thread(read_prepared,name,config,resolve_tuning=False)
    versions = await asyncio.to_thread(_versions,manifest['python'],config['profile'])
    if versions != manifest['runtime']: raise ValueError('Runtime changed after prepare')
    hardware = await asyncio.to_thread(tuning.hardware_identity,config['gpus'])
    expected = tuning.context(manifest,config,hardware)
    case_id = request_digest(search+holdout)
    # This lock only serializes publication for this declaration. The ordinary
    # HTTP owner still acquires the existing process/GPU resource locks.
    with locked(path.with_suffix('.lock'),.1):
        if path.exists() and not force:
            try:
                prior = tuning.read_report(path)
                tuning.validated_report(prior,expected,config)
            except ValueError: prior = None
            if prior and prior['cases_sha256']==case_id and prior['limits']==budget:
                return dict(report=prior,reused=True,path=str(path))
        work = workspace(path.parent/'tuning-runs')
        report = dict(schema_version=tuning.SCHEMA,quality_policy=tuning.POLICY,context=expected,
            limits=budget,cases_sha256=case_id,search_sha256=request_digest(search),
            holdout_sha256=request_digest(holdout),search_count=len(search),holdout_count=len(holdout),
            candidates=[],passed=False,created_at=time.time())
        def save_diagnostic():
            if len(canonical(report).encode())>tuning.MAX_REPORT: raise ValueError('Tuning report byte budget exceeded')
            _write(work/'report.json',report)
        try:
            async with asyncio.timeout(budget['timeout_s']):
                gold = None; started = time.monotonic(); durations = []
                for index,execution in enumerate(tuning.candidates(config,hardware,budget)):
                    # Leave a measured allowance for an independent process and
                    # holdout before the hard deadline, rather than publishing
                    # the fastest unfinished candidate when the budget expires.
                    if durations and gold is not None and tuning.rank(report['candidates'],budget):
                        allowance = max(30,1.5*max(durations[-2:]))
                        if budget['timeout_s']-(time.monotonic()-started)<2*allowance: break
                    trial_started = time.monotonic()
                    try:
                        value,reference = await experiment(name,config,manifest,execution,search,holdout,
                            budget,work/f'candidate-{index:02}',gold=gold)
                        if gold is None: gold = reference
                    except (ValueError,RuntimeError,OSError,httpx.HTTPError) as error:
                        value = dict(execution=execution,passed=False,error=str(error)[:2048])
                        if gold is None:
                            report['candidates'].append(value)
                            raise ValueError('Independent native reference failed: '+str(error)[:1536]) from error
                    report['candidates'].append(value); save_diagnostic()
                    durations.append(time.monotonic()-trial_started)
                eligible = tuning.rank(report['candidates'],budget)
                if not eligible: raise ValueError('No candidate satisfies exact quality and latency constraints')
                report['selected'] = eligible[0]
                report['validation'],_ = await experiment(name,config,manifest,eligible[0]['execution'],
                    search,holdout,budget,work/'holdout',gold=gold,validation=True)
                if not report['validation']['passed']:
                    raise ValueError('Independent holdout changed native outputs; result was not published')
                if report['validation']['rounds'][0]['p95_ms']>budget['max_p95_ms']:
                    raise ValueError('Independent holdout exceeded the latency constraint')
                report['passed'] = True
                report['sha256'] = fingerprint(report)
                tuning.validated_report(report,expected,config)
                # Environment and deployment inputs must not change during tuning.
                latest = await asyncio.to_thread(read_prepared,name,config,resolve_tuning=False)
                current_hw = await asyncio.to_thread(tuning.hardware_identity,config['gpus'])
                if tuning.context(latest,config,current_hw) != expected:
                    raise ValueError('Deployment identity changed during tuning')
                if await asyncio.to_thread(_versions,manifest['python'],config['profile']) != versions:
                    raise ValueError('Runtime changed during tuning')
                save_diagnostic(); _write(path,report)
        except BaseException as error:
            report['passed'] = False; report.pop('sha256',None)
            report['error'] = str(error)[:2048] or type(error).__name__; save_diagnostic()
            raise
        return dict(report=report,reused=False,path=str(path))
