"""Bounded A/B storage benchmark for the prompt journal write paths.

python benchmarks/journal_throughput.py --mode legacy --requests 1000 --concurrency 64
Compares the historic per-request FULL commit and file lease against the batched
single-shard and four-shard modes with the same request stream, payload sizes and
concurrency. It writes only to one temporary journal and reports request rate,
operation latency, group-commit counters and peak process/disk use.

Modes are storage configurations, not business configurations: every mode runs
the same lookup/reserve/response sequence the prompt client uses.
"""
import argparse
import json
import math
import resource
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def _percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def _disk_bytes(path):
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob('*') if item.is_file())


def _journal_bytes(path):
    shards = path.with_name(path.name + '.d')
    if shards.is_dir():
        return _disk_bytes(shards)
    return _disk_bytes(path) if path.exists() else 0


def _lease_files(path):
    directory = path.with_name(path.name + '.leases')
    return len(list(directory.iterdir())) if directory.is_dir() else 0


def options(args):
    if args.mode == 'legacy':
        return None
    config = {
        'shards': args.shards if args.mode == 'sharded' else 1,
        'batch_size': args.batch_size,
        'flush_interval_s': args.flush_interval_s,
        'queue_size': args.queue_size,
        'wal': args.wal,
        'max_record_bytes': args.max_record_bytes,
        'max_queue_bytes': args.max_queue_bytes,
    }
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['legacy', 'batched', 'sharded'], required=True)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--response-bytes', type=int, default=4096)
    parser.add_argument('--shards', type=int, default=4,
                        help='shard count for --mode sharded (1..8; ignored otherwise)')
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--flush-interval-s', type=float, default=.01)
    parser.add_argument('--queue-size', type=int, default=256)
    parser.add_argument('--max-record-bytes', type=int, default=1024 * 1024)
    parser.add_argument('--max-queue-bytes', type=int, default=8 * 1024 * 1024)
    parser.add_argument('--wal', action='store_true')
    parser.add_argument('--path', help='journal path (default: a temporary file)')
    parser.add_argument('--json', action='store_true', help='print one JSON object only')
    args = parser.parse_args()
    for name, value, low, high in (
            ('requests', args.requests, 1, 20000),
            ('concurrency', args.concurrency, 1, 256),
            ('response-bytes', args.response_bytes, 0, 1024 * 1024),
            ('shards', args.shards, 1, 8),
            ('batch-size', args.batch_size, 1, 4096),
            ('queue-size', args.queue_size, 1, 4096)):
        if not low <= value <= high:
            parser.error(f'{name} must be in {low}..{high}')
    if args.response_bytes and args.response_bytes > args.max_record_bytes:
        parser.error('response-bytes must not exceed max-record-bytes')
    workdir = None
    if args.path:
        path = Path(args.path)
    else:
        workdir = tempfile.mkdtemp(prefix='demiflow-journal-bench-')
        path = Path(workdir) / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(args))
    baseline_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    requests = [{'item': index, 'text': f'row-{index}'} for index in range(args.requests)]
    record = {'status_code': 200, 'elapsed_s': .001,
              'body': {'value': 'x' * args.response_bytes}}

    latencies = {'lookup': [], 'reserve': [], 'response': []}
    lock = threading.Lock()

    def run(request):
        started = time.perf_counter()
        assert journal.lookup(request) is None
        looked = time.perf_counter()
        assert journal.reserve(request)
        reserved = time.perf_counter()
        journal.response(request, record)
        finished = time.perf_counter()
        with lock:
            latencies['lookup'].append(looked - started)
            latencies['reserve'].append(reserved - looked)
            latencies['response'].append(finished - reserved)
        return finished - started

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        totals = list(pool.map(run, requests))
    elapsed = time.perf_counter() - started
    stats = journal.stats()
    metrics = journal.throughput_metrics() if args.mode != 'legacy' else {}
    assert stats == {'requests': args.requests, 'responses': args.requests, 'uncertain': 0}, stats
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    journal_bytes = _journal_bytes(path)
    leases = _lease_files(path)
    journal.close()
    report = {
        'mode': args.mode,
        'requests': args.requests,
        'concurrency': args.concurrency,
        'response_bytes': args.response_bytes,
        'wal': args.wal if args.mode != 'legacy' else False,
        'seconds': round(elapsed, 3),
        'requests_per_second': round(args.requests / elapsed, 1),
        'lookup_p50_ms': round(_percentile(latencies['lookup'], .5) * 1000, 3),
        'lookup_p95_ms': round(_percentile(latencies['lookup'], .95) * 1000, 3),
        'reserve_p50_ms': round(_percentile(latencies['reserve'], .5) * 1000, 3),
        'reserve_p95_ms': round(_percentile(latencies['reserve'], .95) * 1000, 3),
        'response_p50_ms': round(_percentile(latencies['response'], .5) * 1000, 3),
        'response_p95_ms': round(_percentile(latencies['response'], .95) * 1000, 3),
        'end_to_end_p95_ms': round(_percentile(totals, .95) * 1000, 3),
        'journal_metrics': {key: (round(value, 6) if isinstance(value, float) else value)
                            for key, value in metrics.items()},
        'journal_bytes': journal_bytes,
        'request_lease_files': leases,
        'baseline_rss_bytes': baseline_rss,
        'peak_rss_bytes': peak_rss,
        'journal_rss_growth_bytes': max(0, peak_rss - baseline_rss),
    }
    if args.json:
        print(json.dumps(report))
    else:
        print(f"{args.mode}: {report['requests_per_second']} req/s, "
              f"reserve p95 {report['reserve_p95_ms']} ms, "
              f"response p95 {report['response_p95_ms']} ms, "
              f"RSS +{(peak_rss - baseline_rss) / 2**20:.0f} MiB "
              f"({peak_rss / 2**20:.0f} MiB peak incl. imports/manifests), "
              f"journal {journal_bytes / 2**20:.1f} MiB")
        if metrics:
            print(f"  batches={metrics['batches']} rows/batch max={metrics['batch_rows_max']} "
                  f"commit mean={metrics['batch_seconds_mean'] * 1000:.2f} ms "
                  f"queue wait max={metrics['queue_wait_seconds_max'] * 1000:.2f} ms "
                  f"queued bytes max={metrics['queued_bytes_max']}")
        print(f"  request lease files={leases}")
    if workdir:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
