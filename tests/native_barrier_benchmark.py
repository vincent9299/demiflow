"""Run in a subprocess with explicit baseline/candidate PYTHONPATH."""
import asyncio
import json
import os
from pathlib import Path
import resource
import sys
import threading
import time

import demiflow
from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.stream_materialize import StreamMaterialization

ROOT = Path(sys.argv[1])
DELAY = float(sys.argv[2])
WORKLOAD = sys.argv[3] if len(sys.argv) > 3 else "steady"
started = time.perf_counter()
publication_wait = 0
original = CommitCoordinator._finish


def slow(self, fact, sinks, acknowledge):
    global publication_wait
    if fact['intent']['sinks'] == ['output']:
        before = time.perf_counter()
        time.sleep(DELAY)
        publication_wait += time.perf_counter() - before
    return original(self, fact, sinks, acknowledge)


CommitCoordinator._finish = slow


async def first(row):
    await asyncio.sleep(.03 if WORKLOAD == 'mixed' and row['id'] % 127 == 0 else .0005)
    return row


async def second(row):
    await asyncio.sleep(.0005)
    return row


latencies = []
usage = resource.getrusage(resource.RUSAGE_SELF)
with RunContext.start(ROOT, limits={'max_row_bytes': 4096, 'max_batch_bytes': 65536}) as run:
    graph = data.from_items([{'id': i} for i in range(1536)]).map_async(first, concurrency=8).map_async(second, concurrency=8)
    with run.action(plan_identity('materialize', graph._source, graph._plan)) as action:
        terminal = StreamMaterialization(action, lambda: run_dataset_action(make_action_request(
            graph._executor, graph._source, LogicalPlan(), kind='iter_rows', arguments={}
        )))
        async def consume(row):
            latencies.append(time.perf_counter() - started)
            await terminal.append(row)
        stats = graph._run_stream(consume=consume, progress=terminal, source_iter=terminal.source_rows())
        terminal.finish()
    facts = run.journal.execution_commits('action-1')
    assert terminal.rows == 1536
elapsed = time.perf_counter() - started
latencies.sort()
end = resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'source': demiflow.__file__, 'rows': 1536, 'seconds': elapsed,
    'throughput_rows_s': 1536/elapsed, 'tail_completion_p95_s': latencies[int(.95*len(latencies))],
    'tail_completion_p99_s': latencies[int(.99*len(latencies))],
    'first_output_s': latencies[0], 'publication_wait_s': publication_wait,
    'cpu_user_s': end.ru_utime-usage.ru_utime, 'cpu_system_s': end.ru_stime-usage.ru_stime,
    'max_rss_kib': end.ru_maxrss, 'disk_bytes': sum(p.stat().st_size for p in ROOT.rglob('*') if p.is_file()),
    'fd_at_end': len(os.listdir('/proc/self/fd')), 'commits': len(facts),
    'metrics': stats.metrics, 'timings': stats.timing_summary(), 'workload': WORKLOAD, 'replay_rows': 0}, ensure_ascii=False))
