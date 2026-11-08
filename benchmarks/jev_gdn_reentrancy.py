"""Reproduce SM90 FlashInfer GDN cross-stream scratch aliasing without weights.

Run in the JEV interpreter, on an explicitly assigned idle GPU:
  python benchmarks/jev_gdn_reentrancy.py --gpu 0 --report /tmp/gdn-native.json
  python benchmarks/jev_gdn_reentrancy.py --gpu 0 --private-scratch --report /tmp/gdn-fixed.json

The optional candidate fix changes only this process. No package is installed
or edited. This kernel probe does not certify a complete model service.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gpu', type=int, required=True)
    parser.add_argument('--tokens', type=int, default=254)
    parser.add_argument('--rounds', type=int, default=100)
    parser.add_argument('--private-scratch', action='store_true')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.gpu < 0 or not 129 <= args.tokens <= 2048 or not 1 <= args.rounds <= 1000:
        parser.error('Require gpu >= 0, tokens in 129..2048, rounds in 1..1000')
    if args.report.exists():
        parser.error('Report exists; choose a new path')
    root = Path(os.environ.get('DEMIFLOW_RESOURCE_HOME', Path.home()/'.cache/demiflow/resources'))
    path = root/socket.gethostname()/f'gpu_{args.gpu}.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        occupied = subprocess.check_output([
            'nvidia-smi', f'--id={args.gpu}', '--query-compute-apps=pid',
            '--format=csv,noheader'], timeout=10).strip()
        if occupied:
            raise RuntimeError('Selected GPU already has compute processes')
        os.environ['CUDA_VISIBLE_DEVICES'] = str(args.gpu)
        run(args)


def run(args):
    import torch
    from flashinfer.gdn_prefill import chunk_gated_delta_rule
    from flashinfer.gdn_kernels.delta_rule_dsl import delta_rule_sm90, delta_rule_cp_sm90

    if torch.cuda.get_device_capability(0)[0] != 9:
        raise ValueError('This reproducer and candidate patch are scoped to SM90')
    torch.set_num_threads(4)
    torch.cuda.set_per_process_memory_fraction(4*1024**3/torch.cuda.get_device_properties(0).total_memory)
    scratch = {}
    if args.private_scratch:
        def allocate(name, num_bytes, device, zero_init=False):
            if type(num_bytes) is not int or not 0 <= num_bytes <= 256*1024**2:
                raise ValueError('Scratch allocation exceeds the 256 MiB probe budget')
            scratch[name] = max(scratch.get(name, 0), num_bytes)
            return (torch.zeros if zero_init else torch.empty)(num_bytes, dtype=torch.uint8, device=device)
        # Both modules import the process-wide allocator by name. Scope the
        # experiment to GDN; do not replace FlashInfer's global utility.
        delta_rule_sm90._get_cache_buf = allocate
        delta_rule_cp_sm90._get_cache_buf = allocate

    torch.manual_seed(17)
    q = torch.nn.functional.normalize(torch.randn(args.tokens, 16, 128, device='cuda'), dim=-1).bfloat16()
    k = torch.nn.functional.normalize(torch.randn_like(q, dtype=torch.float32), dim=-1).bfloat16()
    v = torch.randn(args.tokens, 48, 128, dtype=torch.bfloat16, device='cuda')
    base = dict(q=q, k=k, v=v,
                g=torch.full((args.tokens, 48), .98, device='cuda'),
                beta=torch.full((args.tokens, 48), .5, device='cuda'),
                initial_state=torch.zeros(1, 48, 128, 128, device='cuda'),
                cu_seqlens=torch.tensor([0, args.tokens], dtype=torch.int64, device='cuda'),
                output_final_state=True, backend='flashinfer')
    lanes = []
    with torch.inference_mode():
        for index in range(2):
            inputs = {key: value.clone() if isinstance(value, torch.Tensor) else value
                      for key, value in base.items()}
            if index:
                inputs['v'].neg_()  # Same shapes, different tasks expose shared-state races.
            stream = torch.cuda.Stream()
            stream.wait_stream(torch.cuda.current_stream())
            with torch.cuda.stream(stream):
                native = chunk_gated_delta_rule(**inputs)
                stream.synchronize()
            reference = tuple(t.cpu() for t in native)
            graph = torch.cuda.CUDAGraph()
            with torch.cuda.graph(graph, stream=stream):
                result = chunk_gated_delta_rule(**inputs)
            with torch.cuda.stream(stream):
                graph.replay()
                stream.synchronize()
            assert all(torch.equal(t.cpu(), expected) for t, expected in zip(result, reference))
            lanes.append((graph, stream, result, reference, inputs))
        serial = []
        for index in range(4):
            graph, stream, result, reference, _ = lanes[index % 2]
            with torch.cuda.stream(stream):
                graph.replay()
                stream.synchronize()
            serial.append(all(torch.equal(t.cpu(), expected) for t, expected in zip(result, reference)))
        for iteration in range(args.rounds):
            for graph, stream, *_ in lanes:
                with torch.cuda.stream(stream):
                    graph.replay()
            torch.cuda.synchronize()
            exact = [all(torch.equal(t.cpu(), expected) for t, expected in zip(lane[2], lane[3]))
                     for lane in lanes]
            if not all(exact):
                break
    report = dict(private_scratch=args.private_scratch, tokens=args.tokens,
                  iterations=iteration+1, serial_exact=serial, parallel_exact=all(exact),
                  lane_exact=exact, scratch_max_bytes=scratch,
                  allocator_peak_bytes=torch.cuda.max_memory_reserved())
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))
    if not all(serial) or not all(exact):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
