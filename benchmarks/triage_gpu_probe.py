"""Bounded native/kernel/stream experiments on an idle, explicitly selected GPU.

Uses original journal payloads. No production service or environment is mutated.
Run this in the candidate overlay interpreter, with an outer process timeout.
"""
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import fcntl
import gc
import hashlib
import inspect
import json
import os
from pathlib import Path
import random
import socket
import statistics
import subprocess
import sys
import threading
import time


def bounded_rows(path):
    rows = []; total = 0
    with Path(path).open('rb') as source:
        for _ in range(201):
            raw = source.readline(32769)
            if not raw: break
            total += len(raw)
            if len(raw) > 32768 or total > 8*1024**2: raise ValueError('Case byte budget exceeded')
            rows.append(json.loads(raw))
    if len(rows) != 200: raise ValueError('Expected exactly 200 bounded journal rows')
    return rows


def difference(actual, expected):
    if actual == expected: return None
    delta = 0.; labels = 0
    for key, answer in expected['answers'].items():
        other = actual.get('answers', {}).get(key, {})
        labels += other.get('choice') != answer.get('choice')
        for name, p in answer.get('probabilities', {}).items():
            q = other.get('probabilities', {}).get(name)
            if q is not None: delta = max(delta, abs(q-p))
    return dict(max_probability_delta=delta, label_mismatches=labels,
                usage_equal=actual.get('usage') == expected.get('usage'))


def main(args):
    if args.gpu not in (0, 1): raise ValueError('Only GPU 0/1 authorized')
    output = Path(args.output); output.mkdir(exist_ok=False)
    journal = bounded_rows(args.journal)
    cases = [r['request']['payload'] for r in journal]
    root = Path(__file__).resolve().parents[1]
    sys.path.append(str(root/'demiflow/services'))
    lock_dir = Path(os.environ.get('DEMIFLOW_RESOURCE_HOME', Path.home()/'.cache/demiflow/resources'))/socket.gethostname()
    lock_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir/f'gpu_{args.gpu}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if subprocess.check_output(['nvidia-smi', f'--id={args.gpu}', '--query-compute-apps=pid', '--format=csv,noheader']).strip():
            raise RuntimeError('GPU occupied; do not interrupt its owner')
        os.environ['CUDA_VISIBLE_DEVICES'] = str(args.gpu)
        os.environ.setdefault('TRITON_CACHE_DIR', str(output.parent/'triton-cache'))
        from cuda_graphs import graph_config, configure_allocator
        config = graph_config({'streams': 2})
        configure_allocator(config)
        import torch
        torch.set_num_threads(8)
        import transformers.models.qwen3_5.modeling_qwen3_5 as qwen
        from causal_conv1d import causal_conv1d_fn as fused_impl
        # Match Transformers' adapter: model kwargs (e.g. use_cache) are not
        # convolution parameters and are filtered before the package call.
        fused_parameters = set(inspect.signature(fused_impl).parameters)
        def fused(*a, **kw):
            return fused_impl(*a, **{k:v for k,v in kw.items() if k in fused_parameters})
        reference_conv = inspect.unwrap(qwen.causal_conv1d_fn)
        if 'F.conv1d' not in inspect.getsource(reference_conv): raise RuntimeError('Unreviewed reference convolution')
        def split(hidden_states, weight, bias=None, activation=None, **kwargs):
            result = fused(hidden_states.to(weight.dtype), weight, bias, activation=None)
            if activation is not None: result = qwen.ACT2FN[activation](result)
            return result.to(hidden_states.dtype)
        kernels = dict(reference=reference_conv, fused=fused, split=split)
        if args.kernel == 'triton':
            from qwen_norm_experiment import convolution
            kernels['triton'] = convolution(reference_conv, qwen)
        qwen.causal_conv1d_fn = reference_conv
        if args.profile == 'pplx':
            from pplx_decider import Decider
            from model_adapters import PplxAdapter
            from pplx_graphs import PplxGraphAdapter
            model = Decider.from_pretrained(args.model, device='cuda', cpu_threads=8)
            base = PplxAdapter(model)
            def build(cfg): return PplxGraphAdapter(model, cfg)
        else:
            from amos.serve import load_engine
            from model_adapters import AmosAdapter
            from native_graphs import NativeGraphAdapter, AmosBackend
            engine = load_engine(args.model, 'cuda', Path(args.model).name, strict=True)
            base = AmosAdapter(engine)
            def build(cfg): return NativeGraphAdapter(AmosBackend(engine), cfg)
        report = dict(profile=args.profile, kind=args.kind, kernel=args.kernel,
                      baseline=[], phases=[], quality={}, profiler={}, versions={})
        report['norm'] = args.norm
        report['source_sha256'] = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                  for name in ['triage_gpu_probe.py', 'qwen_norm_experiment.py']}
        import importlib.metadata as metadata
        report['versions'] = {name: metadata.version(name) for name in ['torch', 'transformers', 'causal-conv1d']}
        def save(): (output/'report.json').write_text(json.dumps(report, indent=2))
        def phase(label, adapter, indices, workers, gold=None):
            latencies = []; values = {}; timestamp = time.time()
            started = time.perf_counter(); cpu = time.process_time()
            pending = iter(indices); claim = threading.Lock()
            def work():
                results = []
                while True:
                    with claim:
                        index = next(pending, None)
                    if index is None: return results
                    start = time.perf_counter(); value = adapter.single(cases[index])
                    results.append((index, value, time.perf_counter()-start))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                # Fixed workers keep request pressure without per-batch barriers.
                # Total completed results are capped by the 200-row phase.
                futures = [pool.submit(work) for _ in range(workers)]
                for future in futures:
                    for i, value, elapsed in future.result(timeout=120):
                        values[i] = value; latencies.append(elapsed)
            wall = time.perf_counter()-started; used = time.process_time()-cpu
            mismatches = [(i, difference(v, gold[i])) for i, v in values.items()] if gold is not None else []
            mismatches = [(i, d) for i, d in mismatches if d is not None]
            latencies.sort()
            row = dict(label=label, rows=len(indices), workers=workers, wall_s=wall, rps=len(indices)/wall,
                       started_at=timestamp, finished_at=time.time(),
                       allocated_bytes=torch.cuda.memory_allocated(), reserved_bytes=torch.cuda.memory_reserved(),
                       cpu_s=used, p50_ms=latencies[len(latencies)//2]*1000,
                       p95_ms=latencies[int(.95*len(latencies))]*1000, mismatches=mismatches)
            if hasattr(adapter, 'execution_status'): row['execution'] = adapter.execution_status()
            report['phases'].append(row); save()
            print(json.dumps({k: row[k] for k in ['label', 'rps', 'p95_ms']}), 'mismatches', len(mismatches), flush=True)
            return values, row
        order = list(range(200)); random.Random(1729).shuffle(order)
        # Same weights/instance native baseline; retain production comparison separately.
        baseline, _ = phase('native-reference', base, order, 1)
        report['baseline'] = [baseline[i] for i in range(200)]
        production = []
        for i, value in baseline.items():
            body = journal[i]['response']['body']
            # AMOS includes model in its native result; pplx adds it in HTTP.
            expected = {k: body[k] for k in value}
            diff = difference(value, expected)
            if diff is not None: production.append((i, diff))
        report['production_comparison'] = dict(mismatches=production, rows=200); save()
        if args.kind == 'compare':
            # Same weights and workload; original native outputs stay the gold.
            # Reference is repeated last to expose thermal/order bias.
            for variant in ['reference', 'split', 'norm', 'combined', 'reference-repeat']:
                qwen.causal_conv1d_fn = kernels['split' if variant in ('split', 'combined') else 'reference']
                restore = None
                if variant in ('norm', 'combined'):
                    from qwen_norm_experiment import install
                    restore = install(qwen)
                adapter = build(graph_config({'streams': 2}))
                try:
                    for warm in range(3):
                        _, row = phase(f'{variant}-warm{warm}', adapter, order, 2, baseline)
                        if row['mismatches']: raise RuntimeError('Candidate warmup changed native output')
                    for cycle in range(2):
                        for workers in ([2, 8] if cycle == 0 else [8, 2]):
                            _, row = phase(f'{variant}-w{workers}-r{cycle}', adapter, order, workers, baseline)
                            state = row['execution']
                            if row['mismatches'] or state['mismatches'] or state['failed']:
                                raise RuntimeError('Candidate graph failed quality gate')
                finally:
                    adapter.close(); del adapter
                    if restore: restore()
                    gc.collect(); torch.cuda.empty_cache()
        elif args.kind == 'norm':
            from qwen_norm_experiment import install
            restore = install(qwen)
            try:
                _, row = phase('pointwise-norm-quality', base, order, 1, baseline)
                report['quality']['pointwise_norm'] = dict(passed=not row['mismatches'], mismatches=row['mismatches'])
                if row['mismatches']: raise RuntimeError('Pointwise norm changed native output')
            finally: restore()
        elif args.kind == 'diagnose':
            # Profiling is deliberately separate from timed throughput phases.
            with torch.profiler.profile(activities=[torch.profiler.ProfilerActivity.CPU,
                    torch.profiler.ProfilerActivity.CUDA], record_shapes=False) as profile:
                for i in order[:2]:
                    assert base.single(cases[i]) == baseline[i]
            events = profile.events()
            if len(events) > 100000: raise RuntimeError('Profiler event budget exceeded')
            cuda = defaultdict(lambda: dict(calls=0, device_us=0.))
            for event in events:
                if event.device_type == torch.autograd.DeviceType.CUDA:
                    row = cuda[event.name]; row['calls'] += 1; row['device_us'] += event.device_time_total
            operations = []
            for item in profile.key_averages():
                operations.append(dict(name=item.key, calls=item.count, cpu_us=item.cpu_time_total,
                                       device_us=item.device_time_total, self_device_us=item.self_device_time_total))
            report['profiler'] = dict(cuda=sorted([dict(name=k, **v) for k,v in cuda.items()],
                key=lambda x: x['device_us'], reverse=True)[:300],
                operations=sorted(operations, key=lambda x:x['device_us'], reverse=True)[:500])
            save()
            for name in ['fused', 'split']:
                qwen.causal_conv1d_fn = kernels[name]
                _, row = phase('kernel-'+name, base, order, 1, baseline)
                report['quality'][name] = dict(passed=not row['mismatches'], mismatches=row['mismatches'])
                # Locate convolution rounding differences on one native trajectory;
                # return the original result so errors cannot compound downstream.
                samples = []; candidate = kernels[name]
                def audit(*a, **kw):
                    expected = reference_conv(*a, **kw); actual = candidate(*a, **kw)
                    if len(samples) >= 128: raise RuntimeError('Convolution audit budget exceeded')
                    samples.append(dict(shape=list(expected.shape), equal=torch.equal(expected, actual),
                                        max_delta=(expected.float()-actual.float()).abs().max().item()))
                    return expected
                qwen.causal_conv1d_fn = audit
                assert base.single(cases[order[0]]) == baseline[order[0]]
                report['quality'][name]['convolution_audit'] = samples; save()
            qwen.causal_conv1d_fn = reference_conv
        else:
            qwen.causal_conv1d_fn = kernels[args.kernel]
            if args.norm == 'pointwise':
                from qwen_norm_experiment import install
                install(qwen)
                _, row = phase('pointwise-norm-quality', base, order, 1, baseline)
                if row['mismatches']: raise RuntimeError('Pointwise norm changed native output')
            if args.kernel != 'reference':
                _, row = phase('kernel-quality', base, order, 1, baseline)
                if row['mismatches']: raise RuntimeError('Kernel did not preserve exact native output')
            for streams in [1, 2, 3, 4]:
                adapter = build(graph_config({'streams': streams}))
                try:
                    for warm in range(3):
                        _, row = phase(f's{streams}-warm{warm}', adapter, order, streams, baseline)
                        if row['mismatches']: raise RuntimeError('Warmup changed native output')
                    workers_list = sorted({1, streams, min(8, streams*2)})
                    for cycle in range(2):
                        for workers in workers_list if cycle == 0 else reversed(workers_list):
                            _, row = phase(f's{streams}-w{workers}-r{cycle}', adapter, order, workers, baseline)
                            state = row['execution']
                            if row['mismatches'] or state['mismatches'] or state['failed']:
                                raise RuntimeError('Graph execution failed quality gate')
                            assert state['cache_bytes'] <= 4096*1024**2
                finally:
                    adapter.close(); del adapter; gc.collect(); torch.cuda.empty_cache()
        report['completed'] = True
        report['passed'] = (not production and (args.kind != 'diagnose'
                            or all(value['passed'] for value in report['quality'].values())))
        save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True, choices=['pplx', 'amos'])
    parser.add_argument('--model', required=True); parser.add_argument('--journal', required=True)
    parser.add_argument('--output', required=True); parser.add_argument('--gpu', type=int, required=True)
    parser.add_argument('--kind', choices=['diagnose', 'sweep', 'norm', 'compare'], required=True)
    parser.add_argument('--kernel', choices=['reference', 'fused', 'split', 'triton'], default='reference')
    parser.add_argument('--norm', choices=['none', 'pointwise'], default='none')
    main(parser.parse_args())
