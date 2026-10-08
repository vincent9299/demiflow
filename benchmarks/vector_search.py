"""Bounded real-vector index smoke benchmark; never mutates source tables.

Run with repeated --source /absolute/table.lance@VERSION and --output DIRECTORY.
This deliberately caps the corpus at 10,000 vectors / 256 MiB and holds out query
images. It measures ANN fidelity to exact vector search, not semantic relevance.
Each index runs in a separately supervised CPU process with a temporary table.
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
import tempfile
import time


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def prepare(args, root):
    import lance
    import numpy as np
    import pyarrow as pa

    if not 1 <= len(args.source) <= 16 or not 1 <= args.queries <= 256:
        raise ValueError('Require 1–16 sources and 1–256 held-out queries')
    vectors, ids, seen, sources = [], [], set(), []
    contract = None
    encoder = None
    dimension = None
    total = raw_bytes = 0
    for ref in args.source:
        uri, version = ref.rsplit('@', 1)
        if int(version) < 1:
            raise ValueError('An explicit positive source version is required')
        ds = lance.dataset(uri, version=int(version), index_cache_size_bytes=16*2**20,
                           metadata_cache_size_bytes=4*2**20)
        typ = ds.schema.field('embedding').type
        if not pa.types.is_fixed_size_list(typ) or typ.value_type != pa.float32():
            raise ValueError('Expected fixed-size float32 embedding column')
        if not 1 <= typ.list_size <= 8192:
            raise ValueError('Benchmark dimension budget exceeded')
        meta = (ds.schema.metadata or {}).get(b'image_embeddings.contract')
        if not meta:
            raise ValueError('Missing image_embeddings.contract metadata')
        digest = hashlib.sha256(meta).hexdigest()
        if contract is not None and (contract != digest or dimension != typ.list_size):
            raise ValueError('Cannot combine different encoder contracts')
        contract, dimension = digest, typ.list_size
        count = ds.count_rows()
        total += count
        raw_bytes += count * dimension * 4
        if total > 10000 or raw_bytes > 256*2**20:
            raise ValueError('Smoke corpus exceeds 10,000 rows / 256 MiB; use a fixed smaller sample')
        sources.append(dict(uri=str(Path(uri).resolve()), version=int(version), rows=count))
        for batch in ds.scanner(columns=['sha256', 'encoder_id', 'embedding'],
                batch_size=128, batch_size_bytes=4*2**20, batch_readahead=1,
                fragment_readahead=1, io_buffer_size=8*2**20).to_batches():
            if batch.get_total_buffer_size() > 16*2**20:
                raise MemoryError('Source batch exceeds conversion budget')
            keys = batch['sha256'].to_pylist()
            encoders = batch['encoder_id'].to_pylist()
            arr = batch['embedding']
            if arr.null_count or arr.values.null_count:
                raise ValueError('Null vector in source')
            values = arr.values.to_numpy().reshape(-1, dimension)
            # Readers may return list arrays with a nonzero parent offset.
            values = values[arr.offset:arr.offset + len(arr)]
            if not np.isfinite(values).all():
                raise ValueError('Nonfinite vector in source')
            norms = np.linalg.norm(values, axis=1)
            if not np.allclose(norms, 1, atol=1e-4):
                raise ValueError('Expected normalized vectors')
            for key, current_encoder in zip(keys, encoders):
                if not isinstance(key, str) or len(key) != 64 or key in seen:
                    raise ValueError('IDs must be unique SHA256 strings, including across sources')
                if encoder is not None and encoder != current_encoder:
                    raise ValueError('Mixed encoder_id values')
                encoder = current_encoder
                seen.add(key)
            ids.extend(keys)
            vectors.append(values.copy())
    if total <= args.queries + 512:
        raise ValueError('Need at least 513 database vectors after query holdout')
    matrix = np.concatenate(vectors)
    order = np.random.default_rng(args.seed).permutation(len(ids))
    query_ids, db_ids = order[:args.queries], order[args.queries:]
    np.save(root/'db.npy', matrix[db_ids])
    np.save(root/'queries.npy', matrix[query_ids])
    split = dict(database=[ids[i] for i in db_ids], queries=[ids[i] for i in query_ids])
    write_json(root/'split.json', split)
    return dict(sources=sources, contract_sha256=contract, encoder_id=encoder,
        dimension=dimension, database_rows=len(db_ids), query_rows=len(query_ids),
        seed=args.seed, split_sha256=hashlib.sha256(json.dumps(split, sort_keys=True).encode()).hexdigest(),
        query_kind='held-out image vectors, not text queries',
        split=split, pylance_version=lance.__version__,
        pylance_build_version=getattr(lance, '__build_version__', None))


def worker(root, case):
    import resource
    import lance
    import numpy as np
    import pyarrow as pa

    root = Path(root)
    config = json.loads((root/'config.json').read_text())
    cpus = sorted(os.sched_getaffinity(0))[:config['threads']]
    os.sched_setaffinity(0, cpus)
    pa.set_cpu_count(len(cpus))
    pa.set_io_thread_count(2)
    db = np.load(root/'db.npy', mmap_mode='r')
    queries = np.load(root/'queries.npy', mmap_mode='r')
    split = json.loads((root/'split.json').read_text())
    reference = json.loads((root/'ground_truth.json').read_text()) if case != 'exact' else None
    definitions = {
        'exact': {},
        'IVF_FLAT': {},
        'IVF_RQ': {'num_bits': 5},
        'IVF_PQ': {'num_sub_vectors': 512},
        'IVF_HNSW_SQ': {'m': 16, 'ef_construction': 100},
    }
    if db.shape[1] % 512 and case == 'IVF_PQ':
        raise ValueError('This comparison fixes PQ num_sub_vectors=512; dimension must be divisible')
    start = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix=case+'-', dir=root) as work:
        uri = str(Path(work)/'vectors.lance')
        schema = pa.schema([('id', pa.string()), ('embedding', pa.list_(pa.float32(), db.shape[1]))])
        def batches():
            for offset in range(0, len(db), 128):
                values = db[offset:offset+128]
                yield pa.RecordBatch.from_arrays([pa.array(split['database'][offset:offset+len(values)]),
                    pa.FixedSizeListArray.from_arrays(pa.array(values.reshape(-1)), db.shape[1])], schema=schema)
        table = lance.write_dataset(pa.RecordBatchReader.from_batches(schema, batches()), uri,
            max_rows_per_file=10000, max_rows_per_group=128)
        write_seconds = time.perf_counter()-start
        build_seconds = 0.0
        if case != 'exact':
            begin = time.perf_counter()
            table.create_index('embedding', case, metric='cosine', num_partitions=16,
                               **definitions[case])
            build_seconds = time.perf_counter()-begin
        version = table.version
        index_bytes = sum(p.stat().st_size for p in Path(uri,'_indices').rglob('*') if p.is_file())
        table = None
        qarrays = [pa.array(v, type=pa.float32()) for v in queries]
        variants = [dict(name='exact', use_index=False)] if case == 'exact' else [
            dict(name='probes4', use_index=True, nprobes=4),
            dict(name='probes16', use_index=True, nprobes=16),
            *([dict(name='probes16_refine5', use_index=True, nprobes=16, refine_factor=5)]
              if case != 'IVF_FLAT' else []),
        ]
        results = []
        for variant in variants:
            ds = lance.dataset(uri, version=version, index_cache_size_bytes=128*2**20,
                               metadata_cache_size_bytes=8*2**20)
            nearest = {k:v for k,v in variant.items() if k != 'name'}
            if case == 'IVF_HNSW_SQ':
                nearest['ef'] = max(200, 100 * nearest.get('refine_factor', 1))
            if case == 'IVF_RQ':
                nearest['approx_mode'] = 'normal'
            def query(q):
                return ds.scanner(columns=['id'], nearest=dict(column='embedding',
                    q=q, k=100, metric='cosine', **nearest), batch_size=100,
                    batch_readahead=1, fragment_readahead=1, io_buffer_size=8*2**20).to_table()
            begin = time.perf_counter()
            query(qarrays[0])
            first_ms = (time.perf_counter()-begin)*1000
            latencies, recalls20, recalls100, matches = [], [], [], []
            for turn in range(config['repeats']):
                for idx in np.random.default_rng(config['seed'] + turn).permutation(len(queries)):
                    begin = time.perf_counter()
                    hits = query(qarrays[idx])['id'].to_pylist()
                    latencies.append((time.perf_counter()-begin)*1000)
                    if turn == 0:
                        matches.append((int(idx), hits))
                    if reference is not None:
                        exact = reference[str(idx)]
                        recalls20.append(len(set(exact[:20]) & set(hits[:20]))/20)
                        recalls100.append(len(set(exact[:100]) & set(hits[:100]))/100)
            if case == 'exact':
                write_json(root/'ground_truth.json', dict(matches))
            explain = ds.scanner(columns=['id'], nearest=dict(column='embedding', q=qarrays[0],
                k=100, metric='cosine', **nearest)).explain_plan()
            if (case != 'exact') != ('ANN' in explain):
                raise RuntimeError('Query plan did not use the expected index path')
            results.append(dict(variant=variant['name'], nearest=nearest,
                first_query_ms=first_ms, warmup='one query; page cache not flushed',
                recall20=float(np.mean(recalls20)) if recalls20 else 1.0,
                recall100=float(np.mean(recalls100)) if recalls100 else 1.0,
                p50_ms=float(np.percentile(latencies,50)), p95_ms=float(np.percentile(latencies,95)),
                mean_ms=float(np.mean(latencies)), serial_qps=1000/float(np.mean(latencies)),
                explain_plan=explain))
        # Also exercise the actual Dataset operator, including handle lifecycle
        # and four concurrent row queries. Result fidelity is measured separately.
        from demiflow import data
        inputs = [dict(query_id=i, vector=v.tolist()) for i,v in enumerate(queries)]
        got = {}
        options = {'use_index':case != 'exact', 'index_cache_size_bytes':128*2**20}
        if case != 'exact':
            options['nprobes'] = 16
            if config.get('stream_refine_factor') is not None:
                options['refine_factor'] = config['stream_refine_factor']
            if case == 'IVF_HNSW_SQ' and config.get('stream_ef') is not None:
                options['ef'] = config['stream_ef']
        begin = time.perf_counter()
        stats = (data.from_items(inputs).search_vectors(query='vector', output='hits', uri=uri,
            version=version, vector_column='embedding', columns=['id'], metric='cosine',
            top_k=100, concurrency=4, queue_depth=4, options=options)
            .map(lambda row: got.update({row['query_id']:[r['id'] for r in row['hits']]}) or row).run_stream())
        stream_seconds = time.perf_counter()-begin
        exact = reference or json.loads((root/'ground_truth.json').read_text())
        if len(got) != len(queries): raise RuntimeError('Stream lost query rows')
        stream_recall = float(np.mean([len(set(hits) & set(exact[str(i)]))/100 for i,hits in got.items()]))
        result = dict(case=case, version=version, build_parameters=definitions[case],
            num_partitions=16 if case != 'exact' else None, write_seconds=write_seconds,
            build_seconds=build_seconds, index_bytes=index_bytes, variants=results,
            stream=dict(seconds=stream_seconds, queries=len(got), concurrency=4,
                qps=len(got)/stream_seconds, recall100=stream_recall, options=options,
                metrics=stats.metrics['resources']),
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            cpu_affinity=cpus, note='Native query uses serial requests; stream uses four workers. Includes no encoder time.')
        write_json(root/(case+'.json'), result)
        print(json.dumps(dict(case=case, build_seconds=build_seconds, index_bytes=index_bytes)), flush=True)


def supervise(root, output, case, args):
    import psutil
    env = dict(os.environ, LANCE_CPU_THREADS=str(args.threads), LANCE_IO_THREADS='2',
        OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
        LANCE_DEFAULT_IO_BUFFER_SIZE=str(8*2**20), LANCE_LOG='error', CUDA_VISIBLE_DEVICES='')
    started = time.monotonic()
    with (output/(case+'.log')).open('w') as log:
        p = subprocess.Popen([sys.executable, __file__, '--worker', str(root), '--case', case],
            env=env, stdout=log, stderr=log, start_new_session=True)
        failure = None
        try:
            while p.poll() is None:
                if time.monotonic()-started > args.timeout:
                    failure = 'worker timeout'
                try:
                    proc = psutil.Process(p.pid)
                    rss = proc.memory_info().rss + sum(c.memory_info().rss for c in proc.children(recursive=True))
                    if rss > 4*2**30: failure = 'worker RSS exceeded 4 GiB'
                except psutil.NoSuchProcess:
                    pass
                disk = sum(f.stat().st_size for f in root.rglob('*') if f.is_file())
                if disk > 2*2**30: failure = 'scratch exceeded 2 GiB'
                if failure: break
                time.sleep(.2)
        finally:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGTERM)
                try: p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                    p.wait(timeout=5)
        result = root/(case+'.json')
        if failure or p.returncode != 0 or not result.exists():
            return dict(case=case, error=failure or f'worker exit {p.returncode}', log=str(output/(case+'.log')))
        return json.loads(result.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', action='append', default=[])
    parser.add_argument('--output')
    parser.add_argument('--queries', type=int, default=128)
    parser.add_argument('--seed', type=int, default=20261002)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--stream-ef', type=int)
    parser.add_argument('--stream-refine-factor', type=int)
    parser.add_argument('--cases', nargs='+', choices=['IVF_FLAT', 'IVF_RQ', 'IVF_HNSW_SQ', 'IVF_PQ'],
                        default=['IVF_FLAT', 'IVF_RQ', 'IVF_HNSW_SQ', 'IVF_PQ'])
    parser.add_argument('--worker')
    parser.add_argument('--case')
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.case)
        return
    if not args.output or not 1 <= args.threads <= 8 or not 1 <= args.repeats <= 3 or not 1 <= args.timeout <= 300:
        parser.error('Require output; threads 1–8, repeats 1–3, timeout 1–300 seconds')
    if args.stream_refine_factor is not None and args.stream_refine_factor < 1:
        parser.error('stream-refine-factor must be positive')
    candidates = 100 * (args.stream_refine_factor or 1)
    if args.stream_ef is not None and args.stream_ef < candidates:
        parser.error('stream-ef must be >= 100 * stream-refine-factor')
    if max(candidates, args.stream_ef or candidates + candidates//2) > 10000:
        parser.error('stream candidate count exceeds the smoke budget of 10000')
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    # Bounds apply before array concatenation: raw corpus <=256 MiB; preparation
    # may retain roughly four copies. Native workers have additional RSS/time guards.
    with tempfile.TemporaryDirectory(prefix='work-', dir=output) as work:
        root = Path(work)
        manifest = prepare(args, root)
        write_json(output/'manifest.json', manifest)
        write_json(root/'config.json', vars(args))
        results = []
        for case in ['exact', *dict.fromkeys(args.cases)]:
            result = supervise(root, output, case, args)
            results.append(result)
            write_json(output/'results.json', dict(manifest={k:v for k,v in manifest.items() if k != 'split'},
                settings=vars(args), results=results))
            print(json.dumps({k:v for k,v in result.items() if k in ('case','build_seconds','peak_rss_bytes','error')}, ensure_ascii=False), flush=True)
            if case == 'exact' and 'error' in result:
                raise RuntimeError('Exact baseline failed; see exact.log')


if __name__ == '__main__':
    main()
