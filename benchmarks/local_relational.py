"""可复现的本地 join→reduce 基准，输出结果摘要、耗时和父进程峰值 RSS。

不读生产数据、不调用外部服务。对照旧实现需显式提供可信源码路径。
"""
import argparse
import hashlib
import importlib.util
import json
import resource
import time

from demiflow.data.api import DataAPI
from demiflow.data import local_relational


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-file')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--groups', type=int, default=100_000)
    args = parser.parse_args()
    if args.rows < 0 or args.groups < 1 or args.workers < 1:
        parser.error('rows >= 0, groups >= 1, workers >= 1 required')
    implementation = local_relational
    if args.baseline_file:
        spec = importlib.util.spec_from_file_location('demiflow.data._baseline', args.baseline_file)
        implementation = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(implementation)
    api = DataAPI(workers=args.workers)
    left = api.from_iter(lambda: (
        {'k': f'Q{(i*7919)%args.groups}', 'i': i, 'payload': 'x'*80}
        for i in range(args.rows)))
    right = api.from_iter(lambda: (
        {'k': f'Q{i}', 'bucket': i % 31} for i in range(args.groups)))

    def reducer(acc, row):
        return {'k': row['k'], 'n': (acc or {}).get('n', 0) + 1,
                'sum': (acc or {}).get('sum', 0) + row['i'], 'bucket': row['bucket']}

    start = time.monotonic()
    digest = hashlib.sha256()
    count = 0
    joined = implementation.join(left, right, 'k')
    for row in implementation.reduce_by_key(joined, 'k', reducer).iter_rows():
        count += 1
        digest.update(json.dumps(row, sort_keys=True).encode())
    print(json.dumps({
        'engine': 'baseline' if args.baseline_file else 'optimized',
        'workers': args.workers, 'rows': args.rows, 'groups': args.groups,
        'result_groups': count, 'seconds': time.monotonic()-start,
        'sha256': digest.hexdigest(),
        # 不含工作进程/页缓存，不能作为整个任务的内存统计。
        'parent_max_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }), flush=True)


if __name__ == '__main__':
    main()
