"""Bounded one-instance JEV native/cold/steady diagnostic with exact outputs."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import random
import secrets
import shutil
import subprocess
import sys
import time

import httpx
import psutil

from platform_cpu_profile import read_cases, cpu
from demiflow.services import profiles
from demiflow.services.deployment import deployment_config, DeploymentService
from demiflow.services.prepare import prepare_deployment, adapter_version


async def main(args):
    if args.gpu not in (0, 1): raise ValueError('Only GPU 0/1 authorized')
    if subprocess.check_output(['nvidia-smi', f'--id={args.gpu}', '--query-compute-apps=pid', '--format=csv,noheader']).strip():
        raise RuntimeError('GPU occupied')
    out = Path(args.output); out.mkdir(exist_ok=False)
    repo = Path(__file__).resolve().parents[1]
    cases = read_cases(args.cases); token = secrets.token_hex(24)
    if args.baseline and not args.prompt: raise ValueError('Comparison requires prompt')
    prompt_bytes = Path(args.prompt).stat().st_size if args.prompt else 0
    if prompt_bytes > 1024**2: raise ValueError('Prompt exceeds 1 MiB')
    phases = 38 if args.baseline else 10
    input_bytes = sum(len(json.dumps(case, ensure_ascii=False).encode()) for case in cases)
    # Includes journal/lease/index overhead, response/reference copies and both
    # checkpoints; the child independently enforces an 8 KiB response cap.
    disk_reservation = phases*(2*input_bytes+200*(2*prompt_bytes+8*8192+16384))+32*1024**2
    if disk_reservation > 1024**3 or shutil.disk_usage(out).free < disk_reservation:
        raise ValueError('Experiment requires a bounded disk reservation below 1 GiB')
    cfg = deployment_config(dict(profile='autotrust-jev', weights=args.weights, python=args.python,
        revision='jev-bottleneck-20261101', gpus=[args.gpu], directory=str(out/'services'),
        performance='optimized', request_concurrency=4, startup_timeout_s=900, shutdown_timeout_s=30))
    build = profiles.build_service
    def replacement(*a, **kw):
        service = build(*a, **kw); argv = list(service.command)
        assert Path(argv[1]).name == 'model_worker.py'
        argv[1] = str(repo/'benchmarks/jev_bottleneck_worker.py'); service.command = tuple(argv)
        service.env.update(DEMIFLOW_PROFILE_REPO=str(repo), DEMIFLOW_PROFILE_TOKEN=token,
                           DEMIFLOW_PROFILE_OUTPUT=str(out/'worker'), PYTHONFAULTHANDLER='1')
        if args.baseline: service.env['DEMIFLOW_JEV_BASELINE'] = args.baseline
        return service
    profiles.build_service = replacement
    name = 'AutoTrust-JEV-27B-v1.1'
    owner = None; report = dict(config=cfg, adapter_version=adapter_version(), phases=[],
                               disk_reservation_bytes=disk_reservation)
    gold = {}; dataset_gold = None
    order = list(range(200)); random.Random(1729).shuffle(order)
    try:
        await asyncio.to_thread(prepare_deployment, name, cfg)
        owner = DeploymentService(name, cfg).bind(); await owner.ensure_ready()
        async with httpx.AsyncClient(timeout=180, trust_env=False,
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=4)) as http:
            async def control(route, **params):
                response = await http.get(owner.base_url.removesuffix('/v1')+'/'+route,
                    params=params, headers={'X-Benchmark-Token': token})
                response.raise_for_status(); return response.json()
            async def phase(label, workers=2):
                pid = (await control('__benchmark/reset'))['pid']
                position = 0; lat = []; before = cpu(pid); started = time.perf_counter()
                async def work():
                    nonlocal position
                    while position < len(order):
                        i = order[position]; position += 1; t0 = time.perf_counter()
                        response = await http.post(owner.base_url+'/systemone', json={'model': name, **cases[i]})
                        response.raise_for_status(); value = response.json()
                        value = {key: value[key] for key in ('answers', 'usage')}
                        if i in gold: assert value == gold[i], ('exact mismatch', label, i)
                        else: gold[i] = value
                        lat.append(time.perf_counter()-t0)
                await asyncio.gather(*(work() for _ in range(workers)))
                wall = time.perf_counter()-started; used = cpu(pid)-before
                stages = await control('__benchmark/export', phase=label)
                detail = await control('__jev/cache')
                response = await http.get(owner.base_url+'/models'); response.raise_for_status()
                caps = response.json()['data'][0]['capabilities']; state = caps['execution']
                assert not state['failed'] and not state['mismatches'] and state['cache_bytes'] <= 4096*1024**2
                lat.sort()
                row = dict(label=label, rows=200, workers=workers, wall_s=wall, rps=200/wall,
                           p95_ms=lat[190]*1000, worker_cpu_s=used, status=state, detail=detail,
                           stages=stages['stages'], worker_rss_bytes=psutil.Process(pid).memory_info().rss)
                report['phases'].append(row)
                (out/'report.json').write_text(json.dumps(report, indent=2))
                print(json.dumps({k: row[k] for k in ('label', 'rps', 'p95_ms', 'worker_cpu_s')}), flush=True)
            async def dataset_phase(label):
                nonlocal dataset_gold
                pid = (await control('__benchmark/reset'))['pid']; before = cpu(pid)
                destination = out/label
                cmd = [sys.executable, str(repo/'benchmarks/journal_cpu_worker.py'), '--variant', 'candidate',
                    '--mode', 'online', '--baseline', args.baseline, '--prompt', args.prompt, '--cases', args.cases,
                    '--output', str(destination), '--rows', '200', '--workers', '2', '--storage', 'legacy', '--url', owner.base_url]
                with (out/(label+'.log')).open('w') as log:
                    process = await asyncio.create_subprocess_exec(*cmd, stdout=log, stderr=asyncio.subprocess.STDOUT)
                    try:
                        code = await asyncio.wait_for(process.wait(), 180)
                        if code: raise RuntimeError('Dataset phase failed: '+label)
                    finally:
                        if process.returncode is None:
                            process.terminate()
                            try: await asyncio.wait_for(process.wait(), 10)
                            except asyncio.TimeoutError: process.kill(); await process.wait()
                result = json.loads((destination/'report.json').read_text())
                if dataset_gold is None: dataset_gold = result['exact_sha256']
                else: assert dataset_gold == result['exact_sha256'], ('Dataset exact mismatch', label)
                row = dict(result, label=label, worker_cpu_s=cpu(pid)-before,
                           stages=(await control('__benchmark/export', phase=label))['stages'],
                           detail=await control('__jev/cache'))
                response = await http.get(owner.base_url+'/models'); response.raise_for_status()
                caps = response.json()['data'][0]['capabilities']; row['status'] = caps['execution']
                assert not row['status']['failed'] and not row['status']['mismatches']
                assert row['status']['cache_bytes'] <= 4096*1024**2
                assert caps['parallel'] == 2 and not caps['batch']['supported']
                report['phases'].append(row); (out/'report.json').write_text(json.dumps(report, indent=2))
                print(json.dumps({k: row[k] for k in ('label', 'rps', 'p95_ms', 'worker_cpu_s')}), flush=True)
            await control('__jev/mode', value='native')
            await phase('native', 1)
            if args.baseline: await dataset_phase('dataset-native')
            await control('__jev/mode', value='graphs')
            if args.baseline:
                # ABBA, fresh equal-budget caches, same native engine and weights.
                for block, policy in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
                    await control('__jev/policy', value=policy)
                    for i in range(6): await dataset_phase(f'{block}-{policy}-warm-{i}')
                    for i in range(3): await dataset_phase(f'{block}-{policy}-measure-{i}')
            else:
                for i in range(8): await phase(f'graphs-{i}')
                await phase('graphs-four', 4)
            report['exact_sha256'] = hashlib.sha256(json.dumps(gold, sort_keys=True).encode()).hexdigest()
            report['passed'] = True
            (out/'reference.json').write_text(json.dumps(gold))
            (out/'report.json').write_text(json.dumps(report, indent=2))
    finally:
        if owner is not None: await owner.aclose()
        profiles.build_service = build


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('weights', 'python', 'cases', 'output'): parser.add_argument('--'+key, required=True)
    parser.add_argument('--gpu', type=int, default=1)
    parser.add_argument('--baseline'); parser.add_argument('--prompt')
    asyncio.run(asyncio.wait_for(main(parser.parse_args()), 1800))
