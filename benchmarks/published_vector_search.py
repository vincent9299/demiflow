"""Read-only streaming retrieval check against an existing published Lance index.

Sample at most 128 in-corpus image queries; exclude each query SHA from its own
results. This measures ANN fidelity, not text/image relevance. Native calls run
in supervised CPU-only workers. No table or index is created or modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def read(path, limit=8 * 2**20):
    path = Path(path)
    if path.stat().st_size > limit:
        raise ValueError('Benchmark JSON exceeds size budget')
    return json.loads(path.read_text())


def worker(root, case):
    import resource
    import threading
    import lance
    import numpy as np
    import pyarrow as pa
    from demiflow import data
    from demiflow.lance.search import VectorSearch

    root = Path(root)
    cfg = read(root / 'config.json')
    cpus = sorted(os.sched_getaffinity(0))[:cfg['threads']]
    os.sched_setaffinity(0, cpus)
    pa.set_cpu_count(len(cpus))
    pa.set_io_thread_count(2)
    ds = lance.dataset(cfg['uri'], version=cfg['version'],
        index_cache_size_bytes=16 * 2**20, metadata_cache_size_bytes=8 * 2**20)
    count = ds.count_rows()
    typ = ds.schema.field('embedding').type
    if (not pa.types.is_fixed_size_list(typ) or typ.value_type != pa.float32()
            or not 1 <= typ.list_size <= 8192 or not 1000 <= count <= 2_000_000):
        raise ValueError('Require 1000–2M rows and fixed float32 vectors of at most 8192 dimensions')
    meta = (ds.schema.metadata or {}).get(b'image_embeddings.contract')
    if not meta or len(meta) > 65536:
        raise ValueError('Missing or oversized embedding contract')
    digest = hashlib.sha256(meta).hexdigest()
    positions = np.random.default_rng(cfg['seed']).choice(count, cfg['queries'], replace=False)
    if case == 'exact':
        queries, ids = [], []
        for start in range(0, len(positions), 8):
            table = ds.take(positions[start:start + 8].tolist(), columns=['sha256', 'encoder_id', 'embedding'])
            if table.get_total_buffer_size() > 16 * 2**20:
                raise MemoryError('Query sample exceeds retained-buffer budget')
            for row in table.to_pylist():
                key = row['sha256']
                if (not isinstance(key, str) or len(key) != 64
                        or any(c not in '0123456789abcdef' for c in key)
                        or row['encoder_id'] != digest):
                    raise ValueError('Invalid SHA or mismatched encoder contract')
                ids.append(key)
                queries.append(row['embedding'])
        matrix = np.array(queries, dtype='float32')
        if not np.isfinite(matrix).all() or not np.allclose(np.linalg.norm(matrix, axis=1), 1, atol=1e-4):
            raise ValueError('Expected finite normalized queries')
        np.save(root / 'queries.npy', matrix)
        save(root / 'manifest.json', dict(uri=cfg['uri'], version=cfg['version'], rows=count,
            dimensions=typ.list_size, contract_sha256=digest, query_ids=ids,
            sample_positions=positions.tolist(), seed=cfg['seed'],
            query_kind='in-corpus image vectors; own SHA excluded by the same prefilter in exact and ANN',
            pylance_version=lance.__version__, build=getattr(lance, '__build_version__', None),
            schema=str(ds.schema.remove_metadata())))
    manifest = read(root / 'manifest.json')
    if manifest['contract_sha256'] != digest or manifest['rows'] != count:
        raise ValueError('Published snapshot does not match the prepared queries')
    matrix = np.load(root / 'queries.npy', mmap_mode='r')
    ids = manifest['query_ids']
    exact = read(root / 'exact.json')['hits'] if case != 'exact' else None
    settings = cfg['cases'][case]
    exclude_self = settings.get('exclude_self', True)
    if not exclude_self:
        exact = None  # Different candidate universe; latency measurement only.
    options = dict(use_index=case != 'exact', index_cache_size_bytes=2**30,
                   metadata_cache_size_bytes=32 * 2**20, io_buffer_size=16 * 2**20)
    if case != 'exact':
        options.update(nprobes=settings['nprobes'], ef=500, refine_factor=5)
    concurrency = settings['concurrency']
    # Inspect the native plan separately; benchmark actors open their own cache.
    native = dict(column='embedding', q=pa.array(matrix[0]), k=100, metric='cosine',
                  **{k: v for k, v in options.items() if k in ('use_index', 'nprobes', 'ef', 'refine_factor')})
    explain = ds.scanner(columns=['sha256', 'image_uri', '_distance'], nearest=native,
        filter=f"sha256 != '{ids[0]}'" if exclude_self else None, prefilter=True).explain_plan()
    if (case != 'exact') != ('ANN' in explain):
        raise RuntimeError('Unexpected native index execution plan')
    ds = None
    # One action owns one table handle. ANN round 0 warms that same handle;
    # rounds 1/2 are timed. OS caches are never flushed. Parallel round 1 may
    # overlap the final warmup requests, so round 2 is also reported separately.
    rounds = 1 if case == 'exact' else 3
    inputs = [dict(query_id=int(i), round=r, vector=matrix[i].tolist(),
                   predicate=f"sha256 != '{ids[i]}'" if exclude_self else None)
              for r in range(rounds)
              for i in np.random.default_rng(cfg['seed'] + r).permutation(len(ids))]
    timings, completed, hits = [], [], {}
    lock = threading.Lock()
    original = VectorSearch.__call__

    def measured(actor, row):
        started = time.perf_counter()
        result = original(actor, row)
        ended = time.perf_counter()
        result['_call_start'] = started
        result['_call_end'] = ended
        with lock:
            timings.append(dict(query_id=row['query_id'], round=row['round'],
                                start=started, end=ended, ms=(ended-started)*1000))
        return result

    def consume(row):
        idx, turn = row['query_id'], row['round']
        found = [hit['sha256'] for hit in row['hits']]
        if len(found) != 100 or len(set(found)) != 100 or (exclude_self and ids[idx] in found):
            raise RuntimeError('Missing/duplicate hits or self-match was not excluded')
        distances = [hit['_distance'] for hit in row['hits']]
        if any(a > b for a, b in zip(distances, distances[1:])):
            raise RuntimeError('Top-k candidates are not in increasing distance order')
        if turn == rounds - 1:
            hits[str(idx)] = found
        completed.append((idx, turn))
        return row

    VectorSearch.__call__ = measured
    begin = time.perf_counter()
    try:
        stream = data.from_items(inputs).search_vectors(query='vector', output='hits',
            uri=cfg['uri'], version=cfg['version'], vector_column='embedding',
            columns=['sha256', 'image_uri'], metric='cosine', top_k=100,
            filter_column='predicate', concurrency=concurrency, queue_depth=concurrency,
            options=options)
        stats = stream.map(consume).run_stream()
    finally:
        VectorSearch.__call__ = original
    seconds = time.perf_counter() - begin
    if len(completed) != len(inputs) or len(set(completed)) != len(inputs):
        raise RuntimeError('Stream lost or duplicated input rows')
    if stream._stages[-1]._dataset is not None:
        raise RuntimeError('Search actor retained its table after action completion')
    per_round = []
    for turn in range(rounds):
        selected = [t for t in timings if t['round'] == turn]
        window = max(t['end'] for t in selected) - min(t['start'] for t in selected)
        latency = [t['ms'] for t in selected]
        per_round.append(dict(round=turn, requests=len(selected), window_seconds=window,
            qps=len(selected)/window, p50_ms=float(np.percentile(latency, 50)),
            p95_ms=float(np.percentile(latency, 95)), max_ms=max(latency)))
    recall = {} if exact is None else {f'recall{k}': float(np.mean([
        len(set(hits[str(i)][:k]) & set(exact[str(i)][:k]))/k for i in range(len(ids))]))
        for k in (20, 100)}
    result = dict(case=case, settings=settings, options=options, query_count=len(ids),
        action_seconds=seconds, action_qps=len(inputs)/seconds, rounds=per_round,
        first_call_ms=min(timings, key=lambda t:t['start'])['ms'],
        **recall, hits=hits, timings=timings, explain_plan=explain,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        cpu_affinity=cpus, metrics=stats.metrics['resources'],
        note='Actor service latency includes open/cache/native search/conversion; excludes queue wait, encoding and image loading. No OS cache flush.')
    save(root / f'{case}.json', result)


def supervise(args, case):
    import psutil
    env = dict(os.environ, CUDA_VISIBLE_DEVICES='', LANCE_CPU_THREADS=str(args.threads),
        LANCE_IO_THREADS='2', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1',
        MKL_NUM_THREADS='1', LANCE_DEFAULT_IO_BUFFER_SIZE=str(16*2**20), LANCE_LOG='error')
    output = Path(args.output)
    started = time.monotonic()
    error = None
    with (output / f'{case}.log').open('w') as log:
        process = subprocess.Popen([sys.executable, __file__, '--worker', str(output), '--case', case],
            env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            while process.poll() is None:
                if time.monotonic() - started > args.timeout:
                    error = 'Worker exceeded wall-time budget'
                try:
                    proc = psutil.Process(process.pid)
                    rss = proc.memory_info().rss + sum(c.memory_info().rss for c in proc.children(recursive=True))
                    if rss > 8 * 2**30:
                        error = 'Worker RSS exceeded 8 GiB'
                except psutil.NoSuchProcess:
                    pass
                if sum(p.stat().st_size for p in output.iterdir() if p.is_file()) > 64 * 2**20:
                    error = 'Benchmark artifacts exceeded 64 MiB'
                if error:
                    break
                time.sleep(.2)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
    if error or process.returncode:
        raise RuntimeError(error or f'{case} exited {process.returncode}; see log')
    result = read(output / f'{case}.json')
    return {k: v for k, v in result.items() if k not in ('hits', 'timings', 'explain_plan')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri')
    parser.add_argument('--version', type=int)
    parser.add_argument('--output')
    parser.add_argument('--queries', type=int, default=64)
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--seed', type=int, default=20261003)
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--worker')
    parser.add_argument('--case')
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.case)
        return
    if (not args.uri or not args.version or args.version < 1 or not args.output
            or not 8 <= args.queries <= 128 or not 1 <= args.threads <= 16
            or not 1 <= args.timeout <= 600):
        parser.error('Require URI/version/output; queries 8–128, threads 1–16, timeout 1–600')
    output = Path(args.output).resolve()
    args.output = str(output)
    args.uri = str(Path(args.uri).resolve())
    if output == Path(args.uri) or Path(args.uri) in output.parents:
        parser.error('Benchmark output must be outside the published table')
    output.mkdir(parents=True, exist_ok=False)
    cfg = {**vars(args), 'cases': {'exact': dict(concurrency=1)}}
    cfg['cases'].update({f'probes{n}': dict(nprobes=n, concurrency=1) for n in (8,16,32,64)})
    save(output / 'config.json', cfg)
    results = []
    for case in list(cfg['cases']):
        result = supervise(args, case)
        results.append(result)
        save(output / 'results.json', results)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    passing = [r for r in results if r.get('recall100', 0) >= .98]
    chosen = min(passing, key=lambda r:r['rounds'][-1]['p50_ms']) if passing else results[-1]
    case = chosen['case'] + '_concurrency4'
    cfg['cases'][case] = {**chosen['settings'], 'concurrency':4}
    save(output / 'config.json', cfg)
    results.append(supervise(args, case))
    save(output / 'results.json', results)
    print(json.dumps(results[-1], ensure_ascii=False), flush=True)
    for concurrency in (1,4):
        case = chosen['case'] + f'_unfiltered_c{concurrency}'
        cfg['cases'][case] = {**chosen['settings'], 'concurrency':concurrency, 'exclude_self':False}
        save(output / 'config.json', cfg)
        results.append(supervise(args, case))
        save(output / 'results.json', results)
        print(json.dumps(results[-1], ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
