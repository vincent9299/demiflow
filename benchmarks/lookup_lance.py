"""Compare two source trees on finite synthetic, pinned Lance lookups.

Run with the repository's existing Python environment; never installs packages.
Cold means a new actor/dataset handle, not a flushed OS page cache. Each worker
uses the same CPU affinity, thread counts, payload/cache limits and query list.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import statistics
import subprocess
import sys
import threading
import time


def file_manifest(root):
    result = {}
    for path in sorted(root.rglob('*')):
        if path.is_file():
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def io_counts():
    return {key: int(value) for key, value in (
        line.split(':') for line in Path('/proc/self/io').read_text().splitlines())}


class PeakMonitor:
    def __init__(self, directory):
        import psutil
        self.process = psutil.Process()
        self.directory = directory
        self.rss = self.disk = 0
        self.stop = threading.Event()

    def sample(self):
        self.rss = max(self.rss, self.process.memory_info().rss)
        self.disk = max(self.disk, sum(p.stat().st_size for p in self.directory.rglob('*') if p.is_file()))

    def loop(self):
        while not self.stop.wait(.02):
            self.sample()

    def __enter__(self):
        self.sample()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *error):
        self.stop.set()
        self.thread.join()
        self.sample()


class ObservedDataset:
    def __init__(self, native, counts):
        self.native, self.schema, self.counts = native, native.schema, counts

    def describe_indices(self):
        return self.native.describe_indices()

    def scanner(self, **arguments):
        self.counts['scanner_calls'] += 1
        def completed(stats):
            self.counts['scanner_callbacks'] += 1
            self.counts['scanner_rows'] += stats.all_counts.get('rows_scanned', 0)
            self.counts['scanner_bytes'] += stats.bytes_read
            self.counts['scanner_requests'] += stats.requests
        return self.native.scanner(**arguments, scan_stats_callback=completed)

    def _take_rows(self, ids, **arguments):
        self.counts['payload_take_calls'] += 1
        self.counts['payload_rows_requested'] += len(ids)
        return self.native._take_rows(ids, **arguments)


def input_keys(case, count, rows):
    if case.get('query_limit'):
        return ['key-00000000']*case['query_limit']
    population = random.Random(87).sample(range(rows), count)
    cardinality = 8 if case['pattern'] == 'repeated' else count
    prefix = 'missing' if case['missing'] else 'key'
    return [f'{prefix}-{population[i % cardinality]:08d}' for i in range(count)]


async def run_case(case, config, directory):
    import lance
    from demiflow.execution.stream_lookup import LanceLookup
    counts = dict(scanner_calls=0, scanner_callbacks=0, scanner_rows=0,
                  scanner_bytes=0, scanner_requests=0, payload_take_calls=0,
                  payload_rows_requested=0)
    native_open = lance.dataset
    lance.dataset = lambda **source: ObservedDataset(native_open(**source), counts)
    actor = LanceLookup(case['source'], 'id', None, 'matches', case.get('max_matches', 1), 1024**2)
    keys = input_keys(case, config['queries'], config['rows'])
    phases = []
    try:
        for phase in ('cold', 'hot'):
            before, before_io = dict(counts), io_counts()
            checksum, matched = hashlib.sha256(), 0
            with PeakMonitor(directory) as peak:
                start = time.perf_counter()
                if phase == 'cold':
                    await actor.astart()
                for key in keys:
                    if case.get('reset_result_cache'):
                        actor.cache.clear()
                        actor.cache_bytes = 0
                    result = await actor({'id': key})
                    matched += len(result['matches'])
                    checksum.update(json.dumps(result['matches'], sort_keys=True,
                                               separators=(',', ':')).encode())
                    if peak.rss > 768*1024**2:
                        raise MemoryError('Benchmark worker exceeded sampled 768 MiB RSS guard')
                seconds = time.perf_counter() - start
            after_io = io_counts()
            expected = 0 if case['missing'] else len(keys)*case.get('max_matches', 1)
            if matched != expected:
                raise AssertionError(f'Expected {expected} matches, received {matched}')
            phases.append(dict(phase=phase, seconds=seconds, queries=len(keys),
                queries_per_second=len(keys)/seconds, matched_rows=matched,
                checksum=checksum.hexdigest(), peak_rss_bytes=peak.rss,
                observed_temp_disk_bytes=peak.disk, cache_entries=len(actor.cache),
                cache_bytes=actor.cache_bytes,
                io_delta={name: after_io[name]-before_io[name] for name in before_io},
                **{name: counts[name]-before[name] for name in counts}))
    finally:
        await actor.aclose()
        lance.dataset = native_open
    if actor.table is not None or actor.cache or actor.cache_bytes:
        raise AssertionError('Actor did not release its owned table/cache')
    if phases[0]['checksum'] != phases[1]['checksum']:
        raise AssertionError('Cold and hot results differ')
    return dict(case=case['name'], phases=phases)


def worker(arguments):
    config = json.loads(Path(arguments.worker).read_text())
    directory = Path.cwd()
    os.environ.update(LANCE_CPU_THREADS='2', TOKIO_WORKER_THREADS='2',
                      OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='1',
                      TMPDIR=str(directory))
    os.sched_setaffinity(0, config['cpu_affinity'])
    resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))
    resource.setrlimit(resource.RLIMIT_FSIZE, (128*1024**2, 128*1024**2))
    resource.setrlimit(resource.RLIMIT_CPU, (180, 190))
    package_root = Path(arguments.import_root).resolve()
    sys.path.insert(0, str(package_root))
    import demiflow
    import lance
    import pyarrow
    imported = Path(demiflow.__file__).resolve()
    if not imported.is_relative_to(package_root):
        raise AssertionError(f'Wrong demiflow import: {imported}')
    async def cases():
        results = []
        for case in config['cases']:
            result = await run_case(case, config, directory)
            results.append(result)
            print(json.dumps(result), flush=True)
        return results
    results = asyncio.run(cases())
    implementation = package_root/'demiflow/execution/stream_lookup.py'
    print(json.dumps(dict(worker_complete=True, demiflow_file=str(imported),
        implementation_sha256=hashlib.sha256(implementation.read_bytes()).hexdigest(),
        lance=lance.__version__, pyarrow=pyarrow.__version__, python=sys.version,
        cpu_affinity=sorted(os.sched_getaffinity(0)),
        process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        max_sampled_rss_bytes=max(p['peak_rss_bytes'] for r in results for p in r['phases']),
        remaining_temp_disk_bytes=sum(p.stat().st_size for p in directory.rglob('*') if p.is_file()),
        case_count=len(results))), flush=True)


def prepare(config, output):
    import lance
    import pyarrow as pa
    cases = []
    schema = pa.schema([('id', pa.string()), ('value', pa.string()),
        ('nested', pa.list_(pa.struct([('n', pa.int64()), ('tag', pa.string())])))])
    for width in (256, 16384):
        uri = output/'inputs'/f'width-{width}.lance'
        def batches():
            for start in range(0, config['rows'], 128):
                yield pa.RecordBatch.from_pylist([
                    dict(id=f'key-{i:08d}', value=f'{i:08d}'+'x'*(width-8),
                         nested=[{'n': i, 'tag': 'x'*16} for _ in range(4)])
                    for i in range(start, min(start+128, config['rows']))], schema=schema)
        dataset = lance.write_dataset(pa.RecordBatchReader.from_batches(schema, batches()),
            str(uri), max_rows_per_file=max(128, config['rows']//4))
        unindexed_version = dataset.version
        dataset.create_scalar_index('id', 'BTREE')
        for indexed, version in ((False, unindexed_version), (True, dataset.version)):
            for pattern in ('distinct', 'repeated'):
                for missing in (False, True):
                    if width != 256 and (missing or pattern != 'distinct'):
                        continue
                    name = f'{"indexed" if indexed else "unindexed"}-{pattern}-{"miss" if missing else "hit"}-w{width}'
                    cases.append(dict(name=name, source=dict(uri=str(uri), version=version),
                                      pattern=pattern, missing=missing))
    uri = output/'inputs'/'multi-match.lance'
    batch = pa.RecordBatch.from_pylist([
        dict(id='key-00000000', value=f'{i:08d}'+'x'*248,
             nested=[{'n': i, 'tag': 'x'*16} for _ in range(4)]) for i in range(64)], schema=schema)
    dataset = lance.write_dataset(pa.Table.from_batches([batch]), str(uri))
    unindexed_version = dataset.version
    dataset.create_scalar_index('id', 'BTREE')
    for indexed, version in ((False, unindexed_version), (True, dataset.version)):
        cases.append(dict(name=f'{"indexed" if indexed else "unindexed"}-multi64-control',
            source=dict(uri=str(uri), version=version), pattern='repeated', missing=False,
            max_matches=64, query_limit=16, reset_result_cache=True))
    config['cases'] = cases


def compare(arguments):
    output = Path(arguments.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    affinity = sorted(os.sched_getaffinity(0))[:2]
    os.sched_setaffinity(0, affinity)
    os.environ.update(LANCE_CPU_THREADS='2', TOKIO_WORKER_THREADS='2',
                      OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='1')
    config = dict(rows=arguments.rows, queries=arguments.queries,
                  repeats=arguments.repeats, cpu_affinity=affinity,
                  max_matches=1, max_bytes=1024**2, cache_keys=128, cache_bytes=8*1024**2)
    prepare(config, output)
    config_path = output/'config.json'
    config_path.write_text(json.dumps(config, indent=2)+'\n')
    manifest = file_manifest(output/'inputs')
    (output/'input_manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    runs = []
    roots = dict(baseline=str(Path(arguments.baseline_root).resolve()),
                 candidate=str(Path(arguments.candidate_root).resolve()))
    implementations = {variant: hashlib.sha256(
        (Path(root)/'demiflow/execution/stream_lookup.py').read_bytes()).hexdigest()
        for variant, root in roots.items()}
    for repetition in range(arguments.repeats):
        order = ('baseline', 'candidate') if repetition % 2 == 0 else ('candidate', 'baseline')
        for variant in order:
            name = f'{variant}-{repetition}'
            directory = output/name
            directory.mkdir()
            command = [sys.executable, '-B', str(Path(__file__).resolve()),
                       '--worker', str(config_path), '--import-root', roots[variant]]
            start = time.perf_counter()
            completed = subprocess.run(command, cwd=directory, text=True,
                                       capture_output=True, timeout=240)
            (output/(name+'.stdout.jsonl')).write_text(completed.stdout)
            (output/(name+'.stderr.log')).write_text(completed.stderr)
            if completed.returncode:
                raise RuntimeError(f'{name} failed; inspect {output/(name+".stderr.log")}')
            records = [json.loads(line) for line in completed.stdout.splitlines()]
            if len(records) != len(config['cases'])+1 or not records[-1].get('worker_complete'):
                raise AssertionError(f'{name} did not finish every case')
            if records[-1]['implementation_sha256'] != implementations[variant]:
                raise AssertionError('Implementation changed during benchmark')
            runs.append(dict(variant=variant, repetition=repetition,
                             wall_seconds=time.perf_counter()-start, records=records))
            print(f'Finished {name} in {runs[-1]["wall_seconds"]:.2f}s', flush=True)
    if file_manifest(output/'inputs') != manifest:
        raise AssertionError('Benchmark changed its source datasets')
    comparisons = []
    for case in config['cases']:
        for phase in ('cold', 'hot'):
            samples = {variant: [p for run in runs if run['variant'] == variant
                for record in run['records'][:-1] if record['case'] == case['name']
                for p in record['phases'] if p['phase'] == phase] for variant in roots}
            if len({p['checksum'] for values in samples.values() for p in values}) != 1:
                raise AssertionError('Baseline and candidate results differ: '+case['name'])
            medians = {variant: statistics.median(p['seconds'] for p in values)
                       for variant, values in samples.items()}
            comparisons.append(dict(case=case['name'], phase=phase,
                median_seconds=medians, speedup=medians['baseline']/medians['candidate'],
                samples=samples))
    summary = dict(config=config, roots=roots, implementation_sha256=implementations,
        benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        runs=runs, comparisons=comparisons,
        source_unchanged=True,
        caveats=['Cold = new actor/dataset handle; OS page cache is not flushed.',
            'Scanner statistics exclude _take_rows; process I/O includes payload reads.',
            'RSS and temporary disk sampled every 20ms; ru_maxrss can include pre-exec parent high-water RSS.',
            'Operator timing includes open on cold pass and awaited row calls, excludes imports/close.',
            'No GPU, network, model service or production data is used.'])
    (output/'results.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(output/'results.json', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-root')
    parser.add_argument('--candidate-root', default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument('--output')
    parser.add_argument('--rows', type=int, default=2048)
    parser.add_argument('--queries', type=int, default=160)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    parser.add_argument('--import-root', help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.worker:
        worker(arguments)
        return
    if not arguments.baseline_root or not arguments.output:
        parser.error('--baseline-root and a new --output directory are required')
    if not (160 <= arguments.queries <= 512 and arguments.queries <= arguments.rows <= 32768
            and 1 <= arguments.repeats <= 5):
        parser.error('Require 160..512 queries, queries..32768 rows, and 1..5 repeats')
    compare(arguments)


if __name__ == '__main__':
    main()
