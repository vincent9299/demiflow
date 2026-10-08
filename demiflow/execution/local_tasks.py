"""Bounded local stage scheduler and reference partition kernels.

Tasks exchange Arrow batches or spill handles, never Dataset/executor objects.
Hash exchange co-locates an entire key. Reducers are ordered folds, not assumed
associative. The private worker entry points are the replacement boundary for a
future native kernel; Python callbacks remain explicit at that boundary.
"""
from __future__ import annotations

import copy
import hashlib
import itertools
import json
import multiprocessing
import mmap
import os
import pickle
import tempfile
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from contextlib import ExitStack, closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping

from ..data import plan as ops
from ..data.sources import LanceSource, RangeSource
from ..data.local_relational import (
    key_of, _write_records, _read_records, _merge_records, sorted_rows,
    _right_index, _matched_rows, _merge_row,
)
from .local_kernel import KeyedSource, UnionSource


@dataclass(frozen=True)
class Block:
    """A replayable local spill; key column is internal and never user-visible."""
    path: str
    rows: int
    bytes: int


@dataclass(frozen=True)
class ScanBlock:
    partition: object
    batch_rows: int


@dataclass(frozen=True)
class TaskOutput:
    value: object
    rows_input: int


@dataclass(frozen=True)
class KeyFilter:
    bloom_path: str
    values_path: str | None = None


@dataclass
class Runs:
    blocks: object
    keyed: bool = False
    map_input: object = None
    map_operations: tuple = ()


NARROW = (ops.MapOp, ops.BoundMapOp, ops.FilterOp, ops.FlatMapOp,
          ops.SelectColumnsOp, ops.DropColumnsOp, ops.RenameColumnsOp)


def _rows(block):
    if isinstance(block, Block):
        for _, payload in _read_records(block.path):
            yield pickle.loads(payload)
    elif hasattr(block, "to_pylist"):
        yield from block.to_pylist()
    elif isinstance(block, range):
        for value in block:
            yield {"id": value}
    else:
        yield from block


def _input_batches(block):
    if isinstance(block, ScanBlock):
        from ..lance.read import iter_lance_partition_batches
        yield from iter_lance_partition_batches(block.partition, batch_size=block.batch_rows)
    else:
        yield block


def _row_count(block):
    return block.rows if isinstance(block, Block) else block.num_rows if hasattr(block, 'num_rows') else len(block)


def _write_block(path, records):
    count = 0
    def counted():
        nonlocal count
        for key, row in records:
            count += 1
            yield key, pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
    _write_records(path, counted())
    return Block(str(path), count, Path(path).stat().st_size)


def _apply(rows, operations):
    """Instantiate callback state per task; fuse narrow operators in one loop."""
    from .executors.local import LocalDatasetExecutor
    for op in operations:
        if isinstance(op, ops.MapOp):
            rows = map(ops.StandardCallable(op.callable), rows)
        elif isinstance(op, ops.BoundMapOp):
            rows = map(ops.BoundCallable(op), rows)
        elif isinstance(op, ops.FilterOp):
            rows = filter(ops.StandardCallable(op.callable), rows)
        elif isinstance(op, ops.FlatMapOp):
            def flat(source=rows, fn=ops.StandardCallable(op.callable)):
                for row in source:
                    result = fn(row)
                    if isinstance(result, Mapping):
                        raise TypeError("flat_map callable must return an iterable of rows")
                    for value in result:
                        if not isinstance(value, Mapping):
                            raise TypeError("flat_map output rows must be mappings")
                        yield dict(value)
            rows = flat()
        elif isinstance(op, ops.SelectColumnsOp):
            rows = _select(rows, op.columns)
        elif isinstance(op, ops.DropColumnsOp):
            def drop(source=rows, columns=op.columns):
                for row in source:
                    missing = set(columns) - row.keys()
                    if missing:
                        raise KeyError(f"columns not found: {sorted(missing)}")
                    yield {k: v for k, v in row.items() if k not in columns}
            rows = drop()
        elif isinstance(op, ops.RenameColumnsOp):
            def rename(source=rows, names=op.names):
                for row in source:
                    if isinstance(names, Mapping):
                        targets = [names.get(k, k) for k in row]
                    else:
                        targets = names
                        if len(targets) != len(row):
                            raise ValueError("rename_columns list length must match schema")
                    if len(set(targets)) != len(targets):
                        raise ValueError("rename_columns produces duplicate column names")
                    yield dict(zip(targets, row.values()))
            rows = rename()
        elif isinstance(op, ops.MapBatchesOp):
            # A separate stage fixes global batch boundaries before this task.
            serial = LocalDatasetExecutor(workers=1)
            rows = serial._map_batches_stage(rows, op, 1)
        else:
            raise NotImplementedError(f"partition kernel cannot execute {type(op).__name__}")
    yield from rows


def _select(rows, columns):
    for row in rows:
        yield {k: row[k] for k in columns}


def _map_task(block, encoded, path):
    import cloudpickle
    operations = cloudpickle.loads(encoded)
    count = 0
    def rows():
        nonlocal count
        for batch in _input_batches(block):
            count += _row_count(batch)
            yield from _rows(batch)
    result = _write_block(path, (("", row) for row in _apply(rows(), operations)))
    return TaskOutput(result, count)


def _hash_key(key):
    return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "big")


def _bloom_bits(value, size):
    step = (((value >> 32) | (value << 32)) & ((1 << 64) - 1)) | 1
    return ((value + i * step) % (size * 8) for i in range(3))


def _may_match(value, bitmap):
    return all(bitmap[bit >> 3] & (1 << (bit & 7)) for bit in _bloom_bits(value, len(bitmap)))


def _shuffle_task(block, fields, partitions, directory, buffer_bytes, encoded_operations, filter_path):
    """One input block → bounded buffers of stable hash partitions."""
    buffers = [[] for _ in range(partitions)]
    sizes = [0] * partitions
    paths = {}
    counts = [0] * partitions
    input_rows = 0
    # At most partitions files open in one task; configured limit is 256.
    with ExitStack() as stack:
        bitmap = None
        exact_values = None
        if filter_path is not None:
            stream = stack.enter_context(open(filter_path.bloom_path, 'rb'))
            bitmap = stack.enter_context(mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ))
            if filter_path.values_path is not None:
                import pyarrow as pa
                mapped = stack.enter_context(pa.memory_map(filter_path.values_path, 'r'))
                exact_values = pa.ipc.open_file(mapped).read_all().column('key')
        streams = {}
        def flush(part):
            if not buffers[part]:
                return
            if part not in streams:
                path = str(Path(directory) / f"{uuid.uuid4().hex}-{part}.pkl")
                paths[part] = path
                streams[part] = stack.enter_context(open(path, "wb", buffering=64 * 1024))
            pickle.dump(buffers[part], streams[part], protocol=pickle.HIGHEST_PROTOCOL)
            buffers[part] = []
            sizes[part] = 0
        import cloudpickle
        operations = cloudpickle.loads(encoded_operations)
        # A raw Arrow batch has no opaque callbacks to move across. Test only
        # key columns, then decode full Python rows for likely matches. Exact
        # equality remains the join's responsibility (Bloom false positives).
        def rows():
            nonlocal input_rows
            for batch in _input_batches(block):
                input_rows += _row_count(batch)
                if bitmap is not None and not operations and hasattr(batch, 'schema'):
                    import pyarrow as pa
                    import pyarrow.compute as pc
                    for field in fields:
                        if field not in batch.schema.names:
                            raise KeyError(field)
                    if exact_values is not None and batch.column(fields[0]).type == exact_values.type:
                        batch = batch.filter(pc.is_in(batch.column(fields[0]), value_set=exact_values))
                    else:
                        columns = [batch.column(field).to_pylist() for field in fields]
                        mask = [_may_match(_hash_key(key_of(dict(zip(fields, values)), fields)), bitmap)
                                for values in zip(*columns)]
                        batch = batch.filter(pa.array(mask, type=pa.bool_()))
                yield from _rows(batch)
        for row in _apply(rows(), operations):
            key = key_of(row, fields)
            hashed = _hash_key(key)
            if bitmap is not None and not _may_match(hashed, bitmap):
                continue
            part = hashed % partitions
            payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
            buffers[part].append((key, payload))
            counts[part] += 1
            sizes[part] += len(payload) + len(key) * 4 + 128
            if sizes[part] >= buffer_bytes:
                flush(part)
        for part in range(partitions):
            flush(part)
    return TaskOutput({part: Block(path, counts[part], Path(path).stat().st_size) for part, path in paths.items()}, input_rows)


def _fold(records, fields, reducer, initial, *, group_rows=None, output="items", encoded_keys=False):
    # Native ordering already emits canonical keys; reuse them across the Python
    # callback boundary instead of serializing every row key a second time.
    key_fn = (lambda r: r[0]) if encoded_keys else (lambda r: key_of(r, fields))
    for key, group in itertools.groupby(records, key=key_fn):
        if encoded_keys:
            group = (row for _, row in group)
        if group_rows is not None:
            base = dict(zip(fields, json.loads(key)))
            batch, index = [], 0
            for row in group:
                if len(batch) == group_rows:
                    yield key, {**base, output: batch, "group_index": index, "group_last": False}
                    batch, index = [], index + 1
                batch.append(row)
            if batch:
                yield key, {**base, output: batch, "group_index": index, "group_last": True}
        else:
            state = copy.deepcopy(initial)
            for row in group:
                state = reducer(state, row)
            if state is not None:
                yield key, state


def _join_records(left, right, spec, directory, chunk_bytes):
    """Reference join kernel; no executor or task pool inside a worker task."""
    lk, rk, how = spec['on'], spec['right_on'], spec['how']
    a, b = Path(directory)/'left', Path(directory)/'right'
    a.mkdir(); b.mkdir()
    with ExitStack() as stack:
        right = iter(right)
        stack.callback(right.close)
        index, remaining = _right_index(right, rk, chunk_bytes)
        if index is not None and how in {'inner', 'semi'}:
            left = (r for r in left if key_of(r, lk) in index)
        left_rows = sorted_rows(left, lk, a, chunk_bytes, workers=1)
        stack.callback(left_rows.close)
        if index is not None:
            for key, row in left_rows:
                matches = index.get(key, ())
                if how == 'semi':
                    if matches: yield dict(row)
                elif how == 'anti':
                    if not matches: yield dict(row)
                elif matches:
                    for payload in matches:
                        yield _merge_row(row, pickle.loads(payload), lk, rk, spec['suffix'])
                elif how == 'left':
                    yield dict(row)
            return
        right_rows = sorted_rows(remaining, rk, b, chunk_bytes, workers=1)
        stack.callback(right_rows.close)
        groups = iter(itertools.groupby(right_rows, key=lambda pair: pair[0]))
        current = next(groups, None)
        for key, group in itertools.groupby(left_rows, key=lambda pair: pair[0]):
            while current is not None and current[0] < key:
                current = next(groups, None)
            matched = current is not None and current[0] == key and all(v is not None for v in json.loads(key))
            matches = _matched_rows(current[1], directory, chunk_bytes) if matched and how in {'inner','left'} else None
            for _, row in group:
                if how == 'semi':
                    if matched: yield dict(row)
                elif how == 'anti':
                    if not matched: yield dict(row)
                elif matched:
                    for other in matches():
                        yield _merge_row(row, other, lk, rk, spec['suffix'])
                elif how == 'left':
                    yield dict(row)
            if matched:
                current = next(groups, None)


def _key_task(left, right, encoded, path, directory, chunk_bytes):
    """An entire key belongs to exactly one worker, including hot-key spill."""
    import cloudpickle
    spec = cloudpickle.loads(encoded)
    def rows(blocks):
        for block in blocks:
            yield from _rows(block)
    fields = spec["on"]
    with tempfile.TemporaryDirectory(dir=directory, prefix="partition-") as temp:
        if spec["kind"] == "join":
            records = _join_records(rows(left), rows(right), spec, temp, chunk_bytes)
        else:
            records = (r for _, r in sorted_rows(rows(left), fields, temp, chunk_bytes, workers=1))
        with closing(records):
            if spec.get("fold"):
                out = _fold(records, fields, spec["reducer"], spec["initial"],
                            group_rows=spec.get("group_rows"), output=spec.get("output", "items"))
            else:
                out = ((key_of(row, fields), row) for row in records)
            return TaskOutput(_write_block(path, out), sum(b.rows for b in (*left, *right)))


def _worker_call(function, args):
    started = time.monotonic()
    completed = function(*args)
    result = completed.value
    output_rows = result.rows if isinstance(result, Block) else sum(b.rows for b in result.values())
    return result, {"pid": os.getpid(), "thread": threading.get_ident(), "seconds": time.monotonic() - started,
                    "rows_input": completed.rows_input, "rows_output": output_rows}


def _process_init(barrier, preload_lance):
    # One process consumes one scheduler CPU slot; do not create another Arrow
    # CPU pool per process. Thread mode keeps the host's native configuration.
    import pyarrow as pa
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    if preload_lance:
        import lance  # noqa: F401
    barrier.wait(timeout=60)


def _worker_ready():
    return os.getpid()


class TaskScheduler:
    """Action-owned pool, ordered bounded submission, no automatic UDF retry."""
    def __init__(self, options, stats, *, preload_lance=False):
        self.options, self.stats = options, stats
        self.pool = None
        self.pending = set()
        self.preload_lance = preload_lance
        self.started = False

    def __enter__(self):
        if self.options.worker_mode == "process":
            context = multiprocessing.get_context("spawn")
            self.pool = ProcessPoolExecutor(max_workers=self.options.workers, mp_context=context,
                initializer=_process_init, initargs=(context.Barrier(self.options.workers), self.preload_lance))
        else:
            self.pool = ThreadPoolExecutor(max_workers=self.options.workers, thread_name_prefix="demiflow-partition")
        return self

    def run(self, function, arguments, stage):
        iterator = iter(arguments)
        pending = deque()
        width = self.options.workers * 2
        def submit():
            try:
                args = next(iterator)
            except StopIteration:
                return False
            if not self.started:
                self.started = True
                if self.options.worker_mode == 'process':
                    # Start every slot together. Otherwise the first small
                    # build-side task warms one process; it can finish all later
                    # short tasks while the other processes are still importing.
                    ready = [self.pool.submit(_worker_ready) for _ in range(self.options.workers)]
                    for future in ready:
                        future.result()
            self.pending = {f for f in self.pending if not f.done()}
            while len(self.pending) >= width:
                wait(self.pending, return_when=FIRST_COMPLETED)
                self.pending = {f for f in self.pending if not f.done()}
            future = self.pool.submit(_worker_call, function, args)
            self.pending.add(future)
            pending.append(future)
            self.stats["peak_pending_tasks"] = max(self.stats["peak_pending_tasks"], len(self.pending))
            stage["submitted"] += 1
            return True
        try:
            for _ in range(width):
                if not submit():
                    break
            while pending:
                future = pending.popleft()
                value, metrics = future.result()
                self.pending.discard(future)
                stage["completed"] += 1
                stage["worker_seconds"] += metrics["seconds"]
                stage["rows_input"] += metrics["rows_input"]
                stage["rows_output"] += metrics["rows_output"]
                worker = (metrics["pid"], metrics["thread"])
                if worker not in self.stats["workers_used"]:
                    self.stats["workers_used"].append(worker)
                yield value
                submit()
        finally:
            for future in pending:
                future.cancel()
            close = getattr(iterator, "close", None)
            if close:
                close()

    def __exit__(self, kind, error, traceback):
        for future in self.pending:
            future.cancel()
        if kind is not None and self.options.worker_mode == "process":
            # Python <3.14 has no terminate_workers(). Only this action's owned
            # children are terminated; wait before deleting their spill files.
            terminate = getattr(self.pool, "terminate_workers", None)
            if terminate is not None:
                terminate()
            else:
                for process in list((getattr(self.pool, "_processes", None) or {}).values()):
                    process.terminate()
        self.pool.shutdown(wait=True, cancel_futures=True)


class LocalTaskEngine:
    def __init__(self, executor, scheduler, directory, stats):
        self.executor = executor
        self.options = executor._local_kernel
        self.scheduler, self.directory, self.stats = scheduler, Path(directory), stats

    def stage(self, name, **details):
        value = {"name": name, "submitted": 0, "completed": 0, "worker_seconds": 0.0,
                 "rows_input": 0, "rows_output": 0, **details}
        self.stats["stages"].append(value)
        return value

    def path(self):
        return str(self.directory / (uuid.uuid4().hex + ".pkl"))

    def render(self, runs):
        blocks = iter(runs.blocks)
        try:
            if runs.keyed:
                paths = [b.path for b in blocks]
                with closing(_merge_records(paths)) as records:
                    for _, payload in records:
                        yield pickle.loads(payload)
            else:
                for block in blocks:
                    yield from _rows(block)
        finally:
            close = getattr(blocks, "close", None)
            if close:
                close()

    def batches(self, rows, size):
        with closing(iter(rows)) as iterator:
            while True:
                values = tuple(itertools.islice(iterator, size))
                if not values:
                    return
                yield values

    def source_blocks(self, source, *, bounded=False):
        if isinstance(source, UnionSource):
            for item in source.inputs:
                runs = self.evaluate(item.source, item.plan)
                if runs.keyed:
                    # Keyed children need their global merge order before a
                    # downstream order-sensitive map or fold can consume them.
                    yield from self.batches(self.render(runs), self.options.batch_rows)
                else:
                    # Replay already materialized worker blocks directly. A
                    # driver roundtrip would turn each source fragment into
                    # thousands of tiny shuffle files without changing rows.
                    yield from runs.blocks
        elif isinstance(source, LanceSource):
            from ..lance.read import iter_lance_batches, plan_lance_scan_partitions
            from ..lance.model import LanceScanSpec
            from ..lance.storage import open_lance_dataset
            if isinstance(source.query, LanceScanSpec) and source.query.limit is None and not bounded:
                # Freeze latest once. Use contiguous manifest fragment order,
                # so parallel scan tasks preserve within-key source order.
                template = plan_lance_scan_partitions(source.query, target_partitions=1)[0]
                ds = open_lance_dataset(template.query.uri, template.query.version, template.query.storage_options)
                ids = tuple(int(f.fragment_id) for f in ds.get_fragments())
                if len(ids) > 1:
                    size = max(1, (len(ids) + self.options.partitions - 1)//self.options.partitions)
                    groups = [ids[i:i+size] for i in range(0, len(ids), size)]
                    for i, group in enumerate(groups):
                        yield ScanBlock(replace(template, fragment_ids=group, partition_index=i,
                                                partition_count=len(groups)), self.options.batch_rows)
                    return
                source = replace(source, query=template.query)
            # Arrow decoding to Python happens in the worker. The producer only
            # holds bounded native batches for a single-fragment/limited scan.
            yield from iter_lance_batches(source.query, batch_size=self.options.batch_rows)
        elif isinstance(source, RangeSource):
            for start in range(0, source.count, self.options.batch_rows):
                yield range(start, min(start + self.options.batch_rows, source.count))
        else:
            from ..data.constraints import SourceReadConstraints
            rows = self.executor._iter_source(source, constraints=SourceReadConstraints())
            yield from self.batches(rows, self.options.batch_rows)

    def maps(self, blocks, operations):
        import cloudpickle
        encoded = cloudpickle.dumps(tuple(operations))
        def mapped():
            stage = self.stage("map", operators=[type(op).__name__ for op in operations])
            arguments = ((block, encoded, self.path()) for block in blocks)
            yield from self.scheduler.run(_map_task, arguments, stage)
        return Runs(mapped(), map_input=blocks, map_operations=tuple(operations))

    def shuffle(self, runs, fields, filter_path=None):
        import cloudpickle
        parts = [[] for _ in range(self.options.partitions)]
        fused = runs.map_input is not None
        stage = self.stage("map+shuffle" if fused else "shuffle", keys=list(fields), partitions=len(parts),
                           operators=[type(op).__name__ for op in runs.map_operations],
                           key_filter='bloom' if filter_path else None)
        blocks = runs.map_input if fused else (self.batches(self.render(runs), self.options.batch_rows) if runs.keyed else runs.blocks)
        encoded = cloudpickle.dumps(runs.map_operations if fused else ())
        # The bound applies across all partition buffers in each worker.
        buffer_bytes = max(1, self.options.memory_bytes // self.options.workers // len(parts))
        args = ((b, fields, len(parts), str(self.directory), buffer_bytes, encoded, filter_path) for b in blocks)
        for result in self.scheduler.run(_shuffle_task, args, stage):
            for part, block in result.items():
                parts[part].append(block)
        return parts

    def join_filter(self, parts, fields):
        # Fixed working budget, shared read-only through mmap, never duplicate a
        # large Python key set or serialize it with every input task.
        rows = sum(block.rows for group in parts for block in group)
        size = min(max(64, (rows * 10 + 7)//8), self.options.memory_bytes//self.options.workers//8)
        bitmap = bytearray(size)
        exact = set() if len(fields) == 1 else None
        value_type, used = None, 0
        for group in parts:
            for block in group:
                for key, _ in _read_records(block.path):
                    for bit in _bloom_bits(_hash_key(key), size):
                        bitmap[bit >> 3] |= 1 << (bit & 7)
                    if exact is not None:
                        value = json.loads(key)[0]
                        if value is None:
                            continue
                        # Canonical Python keys distinguish int/float/bool;
                        # Arrow membership is safe only for homogeneous exact
                        # scalar types. Float, nested and mixed keys use Bloom.
                        if type(value) not in {str, int, bool} or (value_type is not None and type(value) is not value_type):
                            exact = None
                        elif value not in exact:
                            value_type = type(value)
                            exact.add(value)
                            used += len(key.encode()) + 96
                            if used > self.options.memory_bytes//2:
                                exact = None
        path = str(self.directory / (uuid.uuid4().hex + '.bloom'))
        Path(path).write_bytes(bitmap)
        values_path = None
        if exact:
            import pyarrow as pa
            try:
                values = pa.table({'key': sorted(exact)})
            except (pa.ArrowException, OverflowError):
                pass
            else:
                values_path = str(self.directory / (uuid.uuid4().hex + '.arrow'))
                with pa.OSFile(values_path, 'wb') as sink:
                    with pa.ipc.new_file(sink, values.schema) as writer:
                        writer.write_table(values)
        return KeyFilter(path, values_path)

    def partition_input(self, item, fields, filter_path=None):
        source = item.source
        if (isinstance(source, KeyedSource) and source.kind == 'join'
                and not item.plan.operations and source.on == fields):
            # A direct same-key join preserves both the visible left key and
            # within-key order. Its outputs already belong to these stable
            # hash partitions; no driver decode/merge/repartition is needed.
            runs = self.keyed(source)
            parts = [[] for _ in range(self.options.partitions)]
            row_n = 0
            for block in runs.blocks:
                if not block.rows:
                    continue
                with closing(_read_records(block.path)) as records:
                    key, _ = next(records)
                parts[_hash_key(key) % len(parts)].append(block)
                row_n += block.rows
            self.stage('reuse_key_partitions', keys=list(fields), partitions=len(parts),
                       rows_input=row_n, rows_output=row_n)
            return parts
        return self.shuffle(self.evaluate(source, item.plan), fields, filter_path)

    def keyed(self, source):
        import cloudpickle
        fold = None
        # Preserve co-location across a direct same-key join→reduce/groups. A
        # Python map between them invalidates the proof even if it looks simple.
        if source.kind in {"reduce", "groups"} and isinstance(source.left.source, KeyedSource):
            parent = source.left.source
            if not source.left.plan.operations and parent.kind == "join" and parent.on == source.on:
                fold, source = source, parent
        right = self.partition_input(source.right, source.right_on) if source.right else None
        filter_path = self.join_filter(right, source.right_on) if right is not None and source.how in {'inner','semi'} else None
        left = self.partition_input(source.left, source.on, filter_path)
        if right is None:
            right = [[] for _ in left]
        group = fold or (source if source.kind != "join" else None)
        spec = {"kind": source.kind, "on": source.on, "right_on": source.right_on,
                "how": source.how, "suffix": source.suffix, "fold": group is not None,
                "reducer": group.reducer if group else None, "initial": group.initial if group else None,
                "group_rows": group.max_rows if group and group.kind == "groups" else None,
                "output": group.output if group else "items"}
        encoded = cloudpickle.dumps(spec)
        budget = min(source.chunk_bytes, self.options.memory_bytes // self.options.workers)
        stage = self.stage("join+" + group.kind if fold else source.kind, keys=list(source.on), partitions=len(left))
        args = ((a, b, encoded, self.path(), str(self.directory), budget) for a, b in zip(left, right) if a)
        # Partition results remain key-sorted; final merge restores the existing
        # global canonical order without regrouping or re-running the reducer.
        return Runs(list(self.scheduler.run(_key_task, args, stage)), keyed=True)

    def evaluate(self, source, plan):
        operations = list(plan.operations)
        bounded = any(isinstance(op, ops.LimitOp) for op in operations)
        if isinstance(source, KeyedSource):
            runs = self.keyed(source)
        else:
            prefix = []
            while operations and isinstance(operations[0], NARROW):
                prefix.append(operations.pop(0))
            runs = self.maps(self.source_blocks(source, bounded=bounded), prefix)
        while operations:
            prefix = []
            while operations and isinstance(operations[0], NARROW):
                prefix.append(operations.pop(0))
            if prefix:
                runs = self.maps(self.batches(self.render(runs), self.options.batch_rows), prefix)
                continue
            op = operations.pop(0)
            if isinstance(op, ops.LimitOp):
                rows = self.render(runs)
                def limited(rows=rows, maximum=op.limit):
                    with closing(rows):
                        yield from itertools.islice(rows, maximum)
                # No task at the limit boundary: only rechunk the demanded rows.
                runs = Runs(self.batches(limited(), self.options.batch_rows))
            elif isinstance(op, ops.MapBatchesOp):
                runs = self.maps(self.batches(self.render(runs), op.batch_size or self.options.batch_rows), [op])
            else:
                raise NotImplementedError(f"local partition kernel does not support {type(op).__name__}; use the ordinary local/async path")
        return runs


def execute(executor, source, plan):
    if executor._local_kernel_closed:
        raise RuntimeError("local_execution is closed; execute actions inside its context")
    # Validate the entire graph before scanning or executing any callback.
    uses_lance = False
    def validate(source, plan):
        nonlocal uses_lance
        uses_lance = uses_lance or isinstance(source, LanceSource)
        executor.plan(source, plan, "iter_rows")
        for operation in plan.operations:
            if not isinstance(operation, (*NARROW, ops.MapBatchesOp, ops.LimitOp)):
                raise NotImplementedError(f"local partition kernel does not support {type(operation).__name__}; use the ordinary local/async path")
            if isinstance(operation, ops.MapBatchesOp) and operation.zero_copy_batch:
                raise NotImplementedError('local partition kernel does not support zero_copy_batch=True')
        if isinstance(source, KeyedSource):
            validate(source.left.source, source.left.plan)
            if source.right is not None:
                validate(source.right.source, source.right.plan)
        elif isinstance(source, UnionSource):
            for item in source.inputs:
                validate(item.source, item.plan)
    options = executor._local_kernel
    stats = {"worker_mode": options.worker_mode, "workers": options.workers, "partitions": options.partitions,
             "peak_pending_tasks": 0, "workers_used": [], "stages": [], "status": "running"}
    executor._local_kernel_stats = stats
    started = time.monotonic()
    try:
        validate(source, plan)
        with tempfile.TemporaryDirectory(prefix="demiflow-local-", dir=options.temp_directory) as directory:
            with TaskScheduler(options, stats, preload_lance=uses_lance) as scheduler:
                engine = LocalTaskEngine(executor, scheduler, directory, stats)
                with closing(engine.render(engine.evaluate(source, plan))) as rows:
                    yield from rows
        stats["status"] = "complete"
    except GeneratorExit:
        stats["status"] = "closed_early"
        raise
    except BaseException as error:
        stats.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        stats["seconds"] = time.monotonic() - started
