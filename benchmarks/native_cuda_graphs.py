"""Explicit single-GPU native/graph regression. Run in the selected model environment.

Example: python benchmarks/native_cuda_graphs.py --profile amos --gpu 1 --model /weights \
  --cases /reviewed-cases.jsonl --output /tmp/graph-review.json
No service, environment, weights or business data are modified. An occupied GPU
is rejected. Keep the input distribution representative; this is not batch proof.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=['pplx','amos','d3'], default='pplx')
    parser.add_argument('--gpu', required=True, type=int)
    parser.add_argument('--model', required=True, type=Path)
    parser.add_argument('--cases', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--streams', type=int, default=2)
    parser.add_argument('--entries', type=int, default=16)
    parser.add_argument('--cold-shapes', action='store_true',
                        help='Force each distinct eligible shape through capture; use --streams 1')
    args = parser.parse_args()
    if args.gpu < 0 or args.output.exists():
        parser.error('Use a valid GPU and a new output path')
    if args.cold_shapes and args.streams != 1:
        parser.error('--cold-shapes requires --streams 1; run concurrency separately')
    root = Path(__file__).resolve().parents[1]
    sys.path.append(str(root / 'demiflow/services'))
    from cuda_graphs import configure_allocator, graph_config, check_encoding_budget
    config = graph_config({'streams': args.streams, 'max_entries': args.entries,
                           'min_observations': 1, 'max_captures': 512,
                           'capture_budget_s': 300, 'verify_every': 3})
    cases = []
    size = 0
    with args.cases.open('rb') as source:
        for _ in range(4097):
            line = source.readline(1024 * 1024 + 1)
            if not line:
                break
            size += len(line)
            if len(line) > 1024 * 1024 or size > 32 * 1024 * 1024:
                raise ValueError('Cases exceed the 1 MiB row / 32 MiB file budget')
            cases.append(json.loads(line))
    if not 1 <= len(cases) <= 4096:
        raise ValueError('Expected 1..4096 review cases')
    lock_path = Path(os.environ.get('DEMIFLOW_RESOURCE_HOME',
                                   Path.home() / '.cache/demiflow/resources'))
    lock_path = lock_path / socket.gethostname() / f'gpu_{args.gpu}.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        processes = subprocess.run(['nvidia-smi', f'--id={args.gpu}',
            '--query-compute-apps=pid', '--format=csv,noheader'],
            capture_output=True, check=True, timeout=10).stdout
        if processes.strip():
            raise RuntimeError('GPU already has compute processes; no existing service was touched')
        os.environ['CUDA_VISIBLE_DEVICES'] = str(args.gpu)
        configure_allocator(config)
        import torch
        torch.set_num_threads(8)
        if args.profile == 'pplx':
            from pplx_decider import Decider
            from pplx_graphs import PplxGraphAdapter
            from model_adapters import PplxAdapter, validate_request
            model = Decider.from_pretrained(args.model, device='cuda')
            base = PplxAdapter(model)
            def tokens(case):
                return sum(c.length for c in model.encoder.plan(validate_request(case), model.max_length))
            def build(): return PplxGraphAdapter(model, config)
        else:
            from native_graphs import NativeGraphAdapter, AmosBackend, D3Backend
            from model_adapters import AmosAdapter, D3Adapter
            if args.profile == 'amos':
                from amos.serve import load_engine
                native = load_engine(str(args.model), 'cuda', args.model.name, strict=True)
                base, backend = AmosAdapter(native), AmosBackend(native)
            else:
                sys.path.insert(0, str(args.model))
                from d3_server import D3, Decider
                native = Decider(D3.from_pretrained(str(args.model), device='cuda:0', verify='fast'))
                base, backend = D3Adapter(native), D3Backend(native)
            def tokens(case): return backend.prepare(case)[1]
            def build(): return NativeGraphAdapter(backend, config)
        baseline, output_bytes = [], 0
        for case in cases:
            check_encoding_budget(case, config['max_input_tokens'])
            if tokens(case) > config['max_input_tokens']:
                raise ValueError('Review case exceeds the configured input token budget')
            value = base.single(case)
            output_bytes += len(json.dumps(value).encode())
            if output_bytes > 64 * 1024 * 1024:
                raise ValueError('Baseline responses exceed the 64 MiB serialized budget')
            baseline.append(value)
        adapter = build()
        report = {'rows': len(cases), 'phases': []}
        def check(index):
            actual = adapter.single(cases[index])
            if actual != baseline[index]:
                raise AssertionError(f'Native output changed for case {index}')
        try:
            if args.cold_shapes:
                import torch
                captures = []
                for index in range(len(cases)):
                    with adapter.gate.enter(exclusive=True):
                        torch.cuda.synchronize()
                        adapter.cache.clear()
                        torch.cuda.empty_cache()
                    before = adapter.cache.captures
                    check(index)
                    check(index)
                    captures.append(adapter.cache.captures-before)
                # A long/media or memory-limited case may legitimately use native
                # execution. Record coverage instead of claiming it was graphed.
                if not sum(captures): raise AssertionError('No case exercised graph capture')
                report['cold_shapes'] = {'cases': len(cases), 'captures_per_case': captures,
                                         'execution': adapter.execution_status()}
            # Small adjacent repeats capture new shapes; fresh caller threads
            # across phases exercise cache lifetime independently of HTTP sockets.
            for phase in range(3):
                order = list(range(len(cases)))
                random.Random(1729 + phase).shuffle(order)
                order = [i for i in order for _ in range(3)]
                start = time.monotonic()
                with ThreadPoolExecutor(max_workers=args.streams) as pool:
                    # Only one bounded batch of futures is submitted at a time.
                    for offset in range(0, len(order), args.streams):
                        list(pool.map(check, order[offset:offset + args.streams]))
                status = adapter.execution_status()
                if status['mismatches'] or status['failed']:
                    raise AssertionError('Graph execution failed its internal quality gate')
                report['phases'].append({'phase': phase, 'requests': len(order),
                    'elapsed_s': time.monotonic() - start, 'exact_equal': True, 'execution': status})
        finally:
            adapter.close()
        report['passed'] = True
        args.output.write_text(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
