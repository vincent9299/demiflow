"""Real-provider canary for the batched prompt journal.

python benchmarks/journal_canary.py --url http://127.0.0.1:8090/v1 --model AutoTrust-JEV-27B-v1.1 \
    --requests 1000 --concurrency 64 --journal /tmp/canary.sqlite \
    --log benchmarks/canary_runs/clean.jsonl --summary benchmarks/canary_runs/clean.json

Every row runs the production journal sequence: lookup, reserve, the real HTTP
request, then commit the complete response. A committed response is never sent
again; a reservation left unanswered by a crash stays uncertain until an
explicit requeue. Only a 2xx response whose body matches the declared chat
completion contract counts as a valid model answer; anything else is saved as a
failure, so the report cannot claim a false success. The JSONL log records only
committed request ids (no provider text), and the JSON summary is bounded, so
the evidence can be replayed or audited without retaining responses.
"""
import argparse
import json
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def payloads(count):
    # The same bounded text batch for both the measured run and the restart.
    return [{'call': index, 'text': f'journal canary row {index:04d}'} for index in range(count)]


def valid_completion(decoded):
    """The declared chat-completion contract, not merely HTTP 200."""
    choices = decoded.get('choices') if isinstance(decoded, dict) else None
    if not isinstance(choices, list) or not choices:
        return 'missing choices'
    message = choices[0].get('message') if isinstance(choices[0], dict) else None
    if not isinstance(message, dict) or not isinstance(message.get('content'), str):
        return 'missing message content'
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--journal', required=True)
    parser.add_argument('--log', required=True)
    parser.add_argument('--summary', help='optional bounded JSON summary path')
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
    state = {'committed': 0, 'reused': 0, 'http_ok': 0, 'valid': 0, 'errors': 0,
             'recovered': len(uncertain)}

    def fail(request, message, elapsed):
        with lock:
            state['errors'] += 1
        journal.failed(request, RuntimeError(message), elapsed)

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
            fail(request, error or 'provider unavailable', elapsed)
            return
        if not 200 <= response.status_code < 300:
            fail(request, f'HTTP {response.status_code}; non-2xx is not a model answer', elapsed)
            return
        try:
            decoded = response.json()
        except ValueError:
            fail(request, 'HTTP 200 with a non-JSON body', elapsed)
            return
        invalid = valid_completion(decoded)
        if invalid is not None:
            fail(request, f'HTTP 200 with an invalid completion body: {invalid}', elapsed)
            return
        with lock:
            state['http_ok'] += 1
        journal.response(request, {'status_code': response.status_code, 'elapsed_s': elapsed,
                                   'body': decoded})
        finished = time.perf_counter()
        with lock:
            state['valid'] += 1
            state['committed'] += 1
            committed.append(request['call'])
            latencies.append(finished - row_started)
            crash = (list(committed) if args.crash_after
                     and state['committed'] >= args.crash_after else None)
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
    metrics = journal.throughput_metrics()
    report = {'seconds': round(elapsed, 3), 'requests': args.requests,
              'requests_per_second': round(args.requests / elapsed, 1),
              'row_p50_ms': round(ordered[len(ordered) // 2] * 1000, 1) if ordered else None,
              'row_p95_ms': round(ordered[math.ceil(len(ordered) * .95) - 1] * 1000, 1) if ordered else None,
              **state, 'journal': journal.stats(),
              'metrics': {name: (round(value, 6) if isinstance(value, float) else value)
                          for name, value in metrics.items()}}
    journal.close()
    client.close()
    if args.summary:
        Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary).write_text(json.dumps({
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
            'url': args.url, 'model': args.model, 'journal': str(args.journal),
            'log': args.log, 'concurrency': args.concurrency,
            'max_tokens': args.max_tokens,
            **report}, ensure_ascii=False, indent=2, sort_keys=True))
    print(json.dumps(report))
    return 0 if (not state['errors']
                 and state['valid'] + state['reused'] == args.requests) else 1


if __name__ == '__main__':
    sys.exit(main())
