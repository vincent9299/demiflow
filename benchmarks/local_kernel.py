"""Compare unchanged Dataset graphs on legacy, thread and process local kernels.

python benchmarks/local_kernel.py --mode process --workers 4 --rows 300000
All inputs/outputs are temporary metadata; no production table or network I/O.
"""
import argparse
from contextlib import nullcontext
import hashlib
import json
import resource
import tempfile
import time

import lance
import pyarrow as pa

from demiflow import data


def cpu_map(row, loops):
    value = row['value']
    for _ in range(loops):
        value = (value * 1664525 + 1013904223) & 0xffffffff
    return {'key': row['key'], 'value': value}


def fold(acc, row):
    return {'key': row['key'], 'total': (acc or {}).get('total', 0) + row['value']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['legacy', 'thread', 'process'], required=True)
    parser.add_argument('--workload', choices=['map_reduce', 'join_reduce'], default='map_reduce')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rows', type=int, default=300000)
    parser.add_argument('--loops', type=int, default=200)
    parser.add_argument('--match-stride', type=int, default=4)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='demiflow-kernel-bench-') as temp:
        keys = max(1, args.rows // 20)
        source = lance.write_dataset(pa.table({'key': [i % keys for i in range(args.rows)],
            'value': list(range(args.rows)), 'wide': ['x' * 512] * args.rows}), temp+'/input.lance',
            max_rows_per_file=max(1,args.rows//8))
        right = lance.write_dataset(pa.table({'key': list(range(0, keys, args.match_stride)),
            'label': ['matched'] * len(range(0, keys, args.match_stride))}), temp+'/right.lance')
        context = nullcontext(None) if args.mode=='legacy' else data.local_execution(
            workers=args.workers, worker_mode=args.mode, partitions=args.workers*4, batch_rows=8192)
        with context as runtime:
            started = time.perf_counter()
            left = data.read_lance(source.uri, version=source.version,
                                   columns=['key', 'value'] if args.workload=='map_reduce' else ['key','value','wide'])
            if args.workload=='map_reduce':
                graph = left.map(cpu_map, fn_kwargs={'loops':args.loops}).reduce_by_key('key', fold)
            else:
                graph = left.join(data.read_lance(right.uri, version=right.version), on='key').reduce_by_key('key', fold)
            result = graph.take_all()
            elapsed = time.perf_counter()-started
            digest = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
            print(json.dumps({**vars(args), 'seconds':round(elapsed,3), 'output_rows':len(result),
                'sha256':digest, 'parent_peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'stats':runtime.stats if runtime else None},ensure_ascii=False))


if __name__=='__main__':
    main()
