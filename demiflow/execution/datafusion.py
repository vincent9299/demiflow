"""Internal process-isolated execution for the Dataset compiler.

This module is platform implementation, not a supported business API. Submit
work through Dataset; only the ordinary writer publishes business destinations.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid


def _save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temporary.replace(path)


def _directory_bytes(path, *, max_entries=1_000_000, max_depth=64, max_seconds=5):
    """Sample owned regular files while spill directories may disappear.

    Stream entries, keeping at most max_depth scandir handles; never build a
    directory-wide list. Only vanished paths are ignored. Other I/O errors or
    scan limits fail the job, rather than returning an incomplete budget check.
    Elapsed checks cannot interrupt a blocked filesystem syscall.
    """
    deadline=time.monotonic()+max_seconds
    visited=0

    def scan(directory, depth):
        nonlocal visited
        if depth>max_depth:
            raise RuntimeError('Resource directory scan depth limit exceeded')
        try:
            entries=os.scandir(directory)
        except FileNotFoundError:
            return 0
        total=0
        with entries:
            for entry in entries:
                visited+=1
                if visited>max_entries or time.monotonic()>deadline:
                    raise RuntimeError('Resource directory scan budget exceeded')
                try:
                    mode=entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(mode.st_mode):
                        total+=scan(entry.path,depth+1)
                    elif stat.S_ISREG(mode.st_mode):
                        total+=mode.st_size
                except FileNotFoundError:
                    # The worker can retire a spill file/directory between
                    # enumeration and opening/stat. Continue with siblings.
                    continue
        return total

    return scan(path,1)


@dataclass(frozen=True)
class DataFusionOptions:
    memory_bytes: int = 8 * 1024**3
    max_rss_bytes: int = 20 * 1024**3
    max_scratch_bytes: int = 64 * 1024**3
    partitions: int = 8
    threads: int = 8
    batch_rows: int = 8192
    timeout_s: float = 300
    admission_timeout_s: float = 300
    cgroup_headroom_bytes: int = 2 * 1024**3
    # Large build-side payloads may exceed the pool even with partitioned hash
    # joins. False asks DataFusion to choose spillable sort-merge equijoins.
    prefer_hash_join: bool = True

    def __post_init__(self):
        if type(self.prefer_hash_join) is not bool:
            raise ValueError('prefer_hash_join must be a bool')
        for key in ('memory_bytes', 'max_rss_bytes', 'max_scratch_bytes',
                    'partitions', 'threads', 'batch_rows', 'cgroup_headroom_bytes'):
            if type(getattr(self, key)) is not int or getattr(self, key) <= 0:
                raise ValueError(f'{key} must be a positive integer')
        for key in ('timeout_s', 'admission_timeout_s'):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'{key} must be a finite positive number')
        if self.memory_bytes >= self.max_rss_bytes:
            raise ValueError('max_rss_bytes must leave room beyond the DataFusion pool')


class DataFusionQueryError(RuntimeError):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


class DataFusionQueryTimeout(DataFusionQueryError, TimeoutError):
    pass


@dataclass(frozen=True)
class DataFusionResult:
    uri: str
    version: int
    row_count: int
    report: dict

    def source(self):
        if not Path(self.uri).is_dir():
            raise RuntimeError('DataFusion result expired; consume or persist it inside its session')
        return {'uri': self.uri, 'version': self.version}

    def dataset(self):
        from .. import data
        return data.read_lance(**self.source())


def _cgroup_memory(root=Path('/sys/fs/cgroup')):
    """Conservative working set and limit; clean inactive file cache is reclaimable.

    Keep active cache, dirty/writeback pages, anonymous and kernel allocations
    charged. Missing/malformed statistics fall back to the raw current charge.
    RSS guards and the reservation/headroom remain separate constraints.
    """
    root = Path(root)
    try:
        limit = (root / 'memory.max').read_text().strip()
        if limit != 'max':
            current = int((root / 'memory.current').read_text())
            reclaimable = 0
            try:
                stats = {k: int(v) for k, v in
                         (line.split() for line in (root / 'memory.stat').read_text().splitlines())}
                reclaimable = max(0, stats['inactive_file'] - stats['file_dirty'] - stats['file_writeback'])
            except (OSError, ValueError, KeyError):
                pass
            return max(0, current - reclaimable), int(limit)
    except (OSError, ValueError):
        pass
    return None


def _cpu_capacity():
    available = len(os.sched_getaffinity(0))
    try:
        quota, period = Path('/sys/fs/cgroup/cpu.max').read_text().split()
        if quota != 'max':
            available = min(available, max(1, int(quota) // int(period)))
    except (OSError, ValueError):
        pass
    return available


def _stop(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


class DataFusionSession:
    """Internal query session, sharing admission slots by directory.

    Calls may run concurrently from threads or other sessions that use the same
    resource_directory and identical slot capacity. Limits are sampled guards,
    not kernel RSS limits. No query or Python batch transform is retried.
    """
    def __init__(self, *, resource_directory, options=None, max_concurrent=1,
                 temp_directory=None, diagnostics_directory=None):
        if not sys.platform.startswith('linux'):
            raise ValueError('Managed DataFusion currently requires Linux process/lock support')
        self.options = options or DataFusionOptions()
        if not isinstance(self.options, DataFusionOptions):
            raise TypeError('options must be DataFusionOptions')
        if type(max_concurrent) is not int or max_concurrent < 1:
            raise ValueError('max_concurrent must be a positive integer')
        self.max_concurrent = max_concurrent
        self.resources = Path(resource_directory).resolve()
        self.diagnostics = Path(diagnostics_directory).resolve() if diagnostics_directory else self.resources / 'queries'
        self.temp_directory = temp_directory
        self._condition = threading.Condition()
        self._active = 0
        self._closed = True
        self._temporary = None

    def __enter__(self):
        import fcntl
        if self._temporary is not None:
            raise RuntimeError('A DataFusion session can be entered only once')
        capacity = {'slots': self.max_concurrent, 'max_rss_bytes_per_slot': self.options.max_rss_bytes,
                    'threads_per_slot': self.options.threads}
        if self.options.threads * self.max_concurrent > _cpu_capacity():
            raise ValueError('DataFusion slots exceed the container CPU allowance')
        memory = _cgroup_memory()
        if memory and self.options.max_rss_bytes * self.max_concurrent + self.options.cgroup_headroom_bytes >= memory[1]:
            raise ValueError('DataFusion slot reservations exceed the container memory allowance')
        self.resources.mkdir(parents=True, exist_ok=True)
        self.diagnostics.mkdir(parents=True, exist_ok=True)
        with (self.resources / 'capacity.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = self.resources / 'capacity.json'
            if path.exists() and json.loads(path.read_text()) != capacity:
                raise ValueError('Existing DataFusion resource_directory has different slot capacity')
            if not path.exists():
                _save(path, capacity)
        self._temporary = tempfile.TemporaryDirectory(prefix='demiflow-datafusion-', dir=self.temp_directory)
        self._closed = False
        return self

    @contextmanager
    def _admit(self, slots_required=1):
        import fcntl
        if not 1 <= slots_required <= self.max_concurrent:
            raise ValueError('Invalid native execution slot reservation')
        deadline = time.monotonic() + self.options.admission_timeout_s
        while True:
            with self._condition:
                if self._closed:
                    raise RuntimeError('DataFusion session is closed')
            chosen = None
            # Serialize admission across processes, including reservations for
            # children that hold a slot but have not allocated their RSS yet.
            with (self.resources / 'capacity.lock').open('a+b') as capacity:
                fcntl.flock(capacity, fcntl.LOCK_EX)
                free = []
                busy = 0
                for slot in range(self.max_concurrent):
                    lock = (self.resources / f'slot-{slot}.lock').open('a+b')
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        free.append((slot, lock))
                    except BlockingIOError:
                        busy += 1
                        lock.close()
                memory = _cgroup_memory()
                # Existing RSS may be counted twice: deliberate conservative
                # admission, not a claim to accurately measure available RAM.
                needed = (busy + slots_required) * self.options.max_rss_bytes + self.options.cgroup_headroom_bytes
                if len(free) >= slots_required and (not memory or memory[0] + needed < memory[1]):
                    chosen, free = free[:slots_required], free[slots_required:]
                for _, lock in free:
                    lock.close()
            if chosen:
                try:
                    yield chosen[0][0], tuple(lock for _, lock in chosen)
                finally:
                    for _, lock in chosen:
                        lock.close()
                return
            if time.monotonic() >= deadline:
                raise TimeoutError('DataFusion resource admission timed out; no query was launched')
            time.sleep(0.05)

    def query(self, sql, *, sources, schema=None, views=None, batch_transform=None, label='query', _dataset_spec=None):
        """Execute one read-only SELECT and materialize a private typed result.

        sources maps SQL identifiers to {uri, version} or prior session results.
        Ordered views are SELECT expressions. batch_transform, if supplied, runs
        on Arrow batches *outside* DataFusion, never as an engine Python UDF.
        Use separate queries when a blocking stage needs a resource boundary.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError('A nonempty SQL query is required')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', label):
            raise ValueError('Invalid DataFusion query label')
        from ..lance.storage import resolve_local_uri
        bindings = {}
        for name, ref in sources.items():
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
                raise ValueError('Invalid source SQL identifier')
            if isinstance(ref, DataFusionResult):
                ref = ref.source()
            if _dataset_spec is not None and ref.get('format') == 'csv':
                from .csv_source import assert_csv_unchanged
                assert_csv_unchanged(ref)
                bindings[name] = dict(ref)
                continue
            if set(ref) != {'uri', 'version'} or type(ref['version']) is not int or ref['version'] < 1:
                raise ValueError('Each source requires exactly uri and a fixed positive version')
            bindings[name] = {'uri': str(resolve_local_uri(ref['uri'])), 'version': ref['version']}
        views = dict(views or {})
        if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name) or name in bindings for name in views):
            raise ValueError('Invalid or colliding view name')
        with self._condition:
            if self._closed:
                raise RuntimeError('Use DataFusion queries inside their session context')
            self._active += 1
        try:
            with self._admit() as (slot, lock):
                return self._run(sql, bindings, schema, views, batch_transform, label, slot, lock, dataset_spec=_dataset_spec)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _prepare_lance_merge(self, spec, query, source_rows):
        """Private storage job: stage files under admission, never commit here."""
        with self._condition:
            if self._closed:
                raise RuntimeError('Native execution session is closed')
            self._active += 1
        try:
            # A large merge borrows both existing reservations; total native
            # capacity stays unchanged and another native job cannot overlap it.
            slots = min(2, self.max_concurrent) if source_rows >= 2_000_000 else 1
            with self._admit(slots_required=slots) as (slot, lock):
                return self._run(None, {}, None, {}, None, 'lance-merge', slot, lock,
                                 lance_merge=(spec, query, source_rows))
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _prepare_lance_index(self, request):
        """Private bounded storage job; native index files are staged, never committed."""
        with self._condition:
            if self._closed:
                raise RuntimeError('Native execution session is closed')
            self._active += 1
        try:
            with self._admit() as (slot, lock):
                return self._run(None, {}, None, {}, None, 'lance-index', slot, lock,
                                 lance_index=request)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _run(self, sql, bindings, schema, views, transform, label, slot, lock, dataset_spec=None, lance_merge=None, lance_index=None):
        import cloudpickle
        import psutil
        query_id = label + '-' + uuid.uuid4().hex
        directory = Path(self._temporary.name) / query_id
        directory.mkdir()
        diagnostic = self.diagnostics / query_id
        diagnostic.mkdir()
        options = replace(self.options, max_rss_bytes=self.options.max_rss_bytes * len(lock))
        request = {'sql': sql, 'sources': bindings, 'schema': schema, 'views': views,
                   'batch_transform': transform, 'options': asdict(options),
                   'directory': str(directory), 'diagnostic': str(diagnostic),
                   'parent_pid': os.getpid(), 'slot': slot, 'dataset_spec': dataset_spec,
                   'lance_merge': lance_merge, 'lance_index': lance_index}
        payload = directory / 'request.pickle'
        payload.write_bytes(cloudpickle.dumps(request))
        description = {k: v for k, v in request.items() if k not in {'schema', 'batch_transform', 'dataset_spec', 'lance_merge'}}
        if lance_merge is not None:
            spec, query, source_rows = lance_merge
            description['lance_merge'] = {'target_uri': spec.uri, 'expected_version': spec.expected_version,
                'source_uri': query.uri, 'source_version': query.version, 'source_rows': source_rows,
                'on': spec.on, 'update_columns': spec.update_columns}
        _save(diagnostic / 'request.json', description)
        if sql is not None:
            (diagnostic / 'query.sql').write_text(sql)
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join(str(p or Path.cwd()) for p in sys.path)
        for name in ('TOKIO_WORKER_THREADS', 'RAYON_NUM_THREADS', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
            env[name] = str(self.options.threads)
        env['LANCE_MEM_POOL_SIZE'] = str(min(1024**3, self.options.memory_bytes // 4))
        if lance_merge is not None:
            # Lance embeds its own DataFusion runtime. Configure only this
            # isolated child; do not race other operations through os.environ.
            env['LANCE_MEM_POOL_SIZE'] = str(self.options.memory_bytes)
            # Column rewriting sorts by row address. Limit its concurrent
            # sort/merge buffers independently from the process CPU allowance.
            env['LANCE_CPU_THREADS'] = '1'
            env['LANCE_MAX_TEMP_DIRECTORY_SIZE'] = str(self.options.max_scratch_bytes)
            env.pop('LANCE_BYPASS_SPILLING', None)
            env['TMPDIR'] = str(directory)
        if lance_index is not None:
            env.update(LANCE_MEM_POOL_SIZE=str(self.options.memory_bytes),
                       LANCE_CPU_THREADS=str(self.options.threads),
                       LANCE_MAX_TEMP_DIRECTORY_SIZE=str(self.options.max_scratch_bytes),
                       LANCE_INCLUDE_VECTOR_CENTROIDS='false', CUDA_VISIBLE_DEVICES='', TMPDIR=str(directory))
            env.pop('LANCE_BYPASS_SPILLING', None)
        report = {'complete': False, 'success': False, 'query_id': query_id, 'diagnostic_directory': str(diagnostic),
                  'job': 'lance_index' if lance_index is not None else 'lance_merge' if lance_merge is not None else 'query',
                  'peak_index_bytes': 0,
                  'peak_rss_bytes': 0, 'peak_scratch_bytes': 0, 'guard': None, 'sample_interval_s': 0.1,
                  'cgroup_memory_policy': 'current-minus-clean-inactive-file-v1',
                  'options': asdict(options), 'slot': slot, 'reserved_slots': len(lock), 'started_at': time.time()}
        process = None
        started = time.monotonic()
        try:
            with (diagnostic / 'worker.log').open('w') as log:
                process = subprocess.Popen([sys.executable, '-m', 'demiflow.execution.datafusion_worker', str(payload)],
                    env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                    pass_fds=tuple(item.fileno() for item in lock))
                owned = psutil.Process(process.pid)
                report.update(pid=process.pid, create_time=owned.create_time())
                _save(diagnostic / 'result.json', report)
                last_disk = 0
                last_note = 0
                scratch = 0
                while process.poll() is None:
                    try:
                        rss = owned.memory_info().rss
                    except psutil.NoSuchProcess:
                        break
                    elapsed = time.monotonic() - started
                    if elapsed - last_disk >= 1:
                        scratch = _directory_bytes(directory)
                        last_disk = elapsed
                        if lance_index is not None and not lance_index.get('inspect_only'):
                            index_directory = Path(lance_index['uri']) / '_indices' / lance_index['uuid']
                            index_bytes = _directory_bytes(index_directory)
                            report['peak_index_bytes'] = max(report['peak_index_bytes'], index_bytes)
                            if index_bytes > lance_index['resources']['max_index_bytes']:
                                report['guard'] = 'index_bytes'
                    report['peak_rss_bytes'] = max(report['peak_rss_bytes'], rss)
                    report['peak_scratch_bytes'] = max(report['peak_scratch_bytes'], scratch)
                    memory = _cgroup_memory()
                    if self._closed:
                        report['guard'] = 'cancelled'
                    elif elapsed > self.options.timeout_s:
                        report['guard'] = 'timeout'
                    elif rss > options.max_rss_bytes:
                        report['guard'] = 'rss'
                    elif scratch > self.options.max_scratch_bytes:
                        report['guard'] = 'scratch'
                    elif memory and memory[0] > memory[1] - self.options.cgroup_headroom_bytes:
                        report['guard'] = 'cgroup_memory'
                    if report['guard']:
                        _stop(process)
                        break
                    if elapsed - last_note >= 5:
                        report['seconds'] = elapsed
                        _save(diagnostic / 'result.json', report)
                        last_note = elapsed
                    time.sleep(0.1)
                code = process.wait()
            report.update(exit_code=code, seconds=time.monotonic()-started)
            result_path = diagnostic / 'worker_result.json'
            result = json.loads(result_path.read_text()) if result_path.exists() else {}
            report['worker'] = result
            if not report['guard'] and lance_index is not None and result.get('error_origin') == 'lance_index':
                raise cloudpickle.loads((diagnostic / 'index_error.pickle').read_bytes())
            if not report['guard'] and lance_merge is not None and result.get('error_origin') == 'lance_merge':
                raise cloudpickle.loads((diagnostic / 'merge_error.pickle').read_bytes())
            if (not report['guard'] and dataset_spec and result.get('error_origin') == 'python_callback'):
                callback_error = cloudpickle.loads((diagnostic / 'callback_error.pickle').read_bytes())
                if isinstance(callback_error, Exception):
                    raise callback_error
            if report['guard'] or code or not result.get('complete') or not result.get('success'):
                error = DataFusionQueryTimeout if report['guard'] == 'timeout' else DataFusionQueryError
                kind = 'Lance index preparation' if lance_index is not None else 'Lance merge preparation' if lance_merge is not None else 'DataFusion query'
                raise error(f"{kind} failed ({report['guard'] or result.get('error_type') or code}); see {diagnostic}", report)
            if lance_index is not None:
                prepared = cloudpickle.loads((directory / 'prepared_index.pickle').read_bytes())
                report.update(success=True, job='lance_index')
                return prepared
            if lance_merge is not None:
                prepared = cloudpickle.loads((directory / 'prepared_merge.pickle').read_bytes())
                from ..lance.mutate import _PreparedMerge
                if not isinstance(prepared, _PreparedMerge):
                    raise RuntimeError('Invalid prepared Lance merge response')
                report.update(success=True, job='lance_merge', prepared_rows=prepared.input_rows)
                return prepared
            import lance
            output = directory / 'output.lance'
            ds = lance.dataset(str(output), version=1)
            if ds.count_rows() != result['rows']:
                raise DataFusionQueryError('DataFusion private output row count mismatch', report)
            report.update(success=True, output={'uri': str(output), 'version': 1, 'rows': ds.count_rows()})
            return DataFusionResult(str(output), 1, ds.count_rows(), report)
        except BaseException:
            if process is not None:
                _stop(process)
            shutil.rmtree(directory, ignore_errors=True)
            raise
        finally:
            report.update(complete=True, seconds=time.monotonic()-started, finished_at=time.time())
            _save(diagnostic / 'result.json', report)

    def __exit__(self, *exc):
        with self._condition:
            self._closed = True
            while self._active:
                self._condition.wait(timeout=0.1)
        self._temporary.cleanup()
