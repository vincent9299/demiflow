"""Real-provider canary for the batched prompt journal.

python benchmarks/journal_canary.py --url http://127.0.0.1:4001/v1 --model deepseek/deepseek-flash \
    --requests 1000 --concurrency 64 --journal /path/calls.sqlite --log calls.jsonl

Every row runs the production journal sequence: lookup, reserve, the real HTTP
request, then commit the complete response. A committed response is never sent
again; a reservation left unanswered by a crash stays uncertain until an
explicit requeue. The JSONL log records only committed request ids, so the
parent can prove that no id was called twice across a crash and restart.
"""
import argparse
import json
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def payloads(count):
    # The same bounded text batch for both the measured run and the restart.
    return [{'call': index, 'text': f'journal canary row {index:04d}'} for index in range(count)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--journal', required=True)
    parser.add_argument('--log', required=True)
    parser.add_argument('--max-tokens', type=int, default=16)
    parser.add_argument('--timeout-s', type=float, default=120)
    parser.add_argument('--attempts', type=int, default=3)
    parser.add_argument('--crash-after', type=int, default=0,
                        help='hard-exit after this many committed calls (0 = never)')
    parser.add_argument('--wal', action='store_true')
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--flush-interval-s', type=float, default=.01)
    parser.add_argument('--queue-size', type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.requests <= 5000 or not 1 <= args.concurrency <= 256:
        parser.error('requests must be 1..5000 and concurrency 1..256')
    import httpx
    journal = SQLitePromptJournal(args.journal, throughput={
        'shards': 1, 'batch_size': args.batch_size,
        'flush_interval_s': args.flush_interval_s, 'queue_size': args.queue_size,
        'wal': args.wal})
    client = httpx.Client(trust_env=False, timeout=httpx.Timeout(args.timeout_s),
                          limits=httpx.Limits(max_connections=args.concurrency,
                                              max_keepalive_connections=args.concurrency))
    # Phase one: isolate reservations a previous crashed run left unanswered.
    requests = payloads(args.requests)
    uncertain = []
    for request in requests:
        try:
            journal.lookup(request)
        except UncertainPromptCall:
            uncertain.append(request)
    if uncertain:
        journal.requeue_uncertain([request_key(request) for request in uncertain],
                                  actor='journal-canary', reason='crashed owner')
    committed, latencies = [], []
    lock = threading.Lock()
    state = {'called': 0, 'reused': 0, 'errors': 0, 'recovered': len(uncertain)}

    def deliver(request):
        row_started = time.perf_counter()
        if journal.lookup(request) is not None:
            with lock:
                state['reused'] += 1
            return
        if not journal.reserve(request):
            with lock:
                state['reused'] += 1
            return
        started = time.perf_counter()
        body = {'model': args.model, 'messages': [{'role': 'user', 'content': request['text']}],
                'max_tokens': args.max_tokens, 'temperature': 0}
        response, error = None, None
        for attempt in range(args.attempts):
            try:
                raw = client.post(args.url.rstrip('/') + '/chat/completions', json=body)
                if raw.status_code == 429 or raw.status_code >= 500:
                    error = f'HTTP {raw.status_code}'
                    time.sleep(min(2 ** attempt, 4))
                    continue
                response, error = raw, None
                break
            except httpx.HTTPError as exc:
                error = f'{type(exc).__name__}: {exc}'
                time.sleep(min(2 ** attempt, 4))
        elapsed = time.perf_counter() - started
        if response is None:
            with lock:
                state['errors'] += 1
            journal.failed(request, RuntimeError(error or 'provider unavailable'), elapsed)
            return
        try:
            decoded = response.json()
        except ValueError:
            decoded = {'text': response.text[:4096]}
        journal.response(request, {'status_code': response.status_code, 'elapsed_s': elapsed,
                                   'body': decoded})
        finished = time.perf_counter()
        with lock:
            state['called'] += 1
            committed.append(request['call'])
            latencies.append(finished - row_started)
            crash = (list(committed) if args.crash_after
                     and state['called'] >= args.crash_after else None)
        if crash is not None:
            with open(args.log, 'a') as stream:
                stream.write(json.dumps({'phase': 'crashed', 'committed': crash}) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            os._exit(0)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(deliver, requests))
    elapsed = time.perf_counter() - started
    with open(args.log, 'a') as stream:
        stream.write(json.dumps({'phase': 'complete', 'committed': committed}) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    ordered = sorted(latencies)
    report = {'seconds': round(elapsed, 3), 'requests': args.requests,
              'requests_per_second': round(args.requests / elapsed, 1),
              'row_p50_ms': round(ordered[len(ordered) // 2] * 1000, 1) if ordered else None,
              'row_p95_ms': round(ordered[math.ceil(len(ordered) * .95) - 1] * 1000, 1) if ordered else None,
              **state, 'journal': journal.stats(),
              'metrics': journal.throughput_metrics()}
    journal.close()
    client.close()
    print(json.dumps(report))
    return 0 if not state['errors'] else 1


if __name__ == '__main__':
    sys.exit(main())
