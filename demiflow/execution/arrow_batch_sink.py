"""Keep a local Lance -> Arrow batch callback -> Lance sink columnar."""
from contextlib import closing, ExitStack
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterator
import tempfile
import time
import io

from ..data.plan import MapBatchesOp, StandardCallable
from ..data.sources import LanceSource
from .local_tasks import Block, TaskOutput, TaskScheduler


def eligible(executor, source, plan, spec):
    return (spec.schema is not None and executor._local_kernel is not None and isinstance(source, LanceSource)
            and source.native_options is None and len(plan.operations) == 1
            and isinstance(plan.operations[0], MapBatchesOp)
            and plan.operations[0].batch_format == 'pyarrow'
            and not plan.operations[0].zero_copy_batch
            and plan.operations[0].native_options is None)


@dataclass(frozen=True)
class ArrowBlock(Block):
    """Task-owned Arrow IPC stream; never consumed by the row spill reader."""


def rechunk(batches, size, *, max_bytes=None, reader_max_bytes=None):
    """Preserve global callback boundaries even across Lance fragments.

    Input buffering borrows the sealed stage inflight byte ceiling. A declared
    callback group that cannot fit fails explicitly; its row scope is never
    silently shrunk to admit it.
    """
    import pyarrow as pa
    from .recovery_contract import RecoveryContractError
    pieces, count, nbytes = [], 0, 0
    for batch in batches:
        if reader_max_bytes is not None and batch.get_total_buffer_size() > reader_max_bytes:
            raise RecoveryContractError('Arrow reader retained buffers exceed its admitted byte budget')
        offset = 0
        while offset < batch.num_rows:
            n = min(size - count, batch.num_rows - offset)
            piece = batch.slice(offset, n)
            # A one-row slice can retain an entire fragment. Detach that backing
            # allocation before handing the group to an independently held task.
            if piece.get_total_buffer_size() > piece.nbytes:
                piece = piece.take(pa.array(range(n), type=pa.int64()))
            retained = piece.get_total_buffer_size()
            if max_bytes is not None and nbytes + retained > max_bytes:
                raise RecoveryContractError('Declared Arrow callback group exceeds the sealed inflight byte budget')
            pieces.append(piece)
            count += n
            nbytes += retained
            offset += n
            if count == size:
                yield pa.Table.from_batches(pieces)
                pieces, count, nbytes = [], 0, 0
    if count:
        yield pa.Table.from_batches(pieces)


class _CappedWriter(io.RawIOBase):
    """Reject each IPC write before it can exceed the task's scratch grant."""
    def __init__(self, stream, ceiling):
        self.stream, self.ceiling, self.written = stream, ceiling, 0

    def writable(self):
        return True

    def tell(self):
        return self.written

    def write(self, value):
        from .recovery_contract import RecoveryContractError
        if self.ceiling is not None and self.written + len(value) > self.ceiling:
            raise RecoveryContractError('Arrow IPC staging exceeds the admitted scratch byte budget')
        count = self.stream.write(value)
        self.written += count
        return count


def apply_batch(batch, encoded, path, schema, max_bytes=None, memory_bytes=None):
    import cloudpickle
    import pyarrow as pa
    from .executors.local import LocalDatasetExecutor
    from .recovery_contract import RecoveryContractError
    operation = cloudpickle.loads(encoded)
    result = StandardCallable(operation.callable)(batch)
    outputs = result if isinstance(result, Iterator) else (result,)
    rows = 0
    written = 0
    writer = None
    stream = None
    memory_bytes = max_bytes if memory_bytes is None else memory_bytes
    failed = True
    try:
        for output in outputs:
            if isinstance(output, pa.RecordBatch):
                output = pa.Table.from_batches([output])
            elif not isinstance(output, pa.Table):
                # Existing callbacks may return dict arrays or row lists.
                from ..data.runtime_contract import check_size
                values, retained = [], 0
                for row in LocalDatasetExecutor(workers=1)._batch_to_rows(output):
                    retained += check_size(row, memory_bytes or 128 * 1024**2) + 64
                    if memory_bytes is not None and retained > memory_bytes:
                        raise RecoveryContractError('Arrow callback conversion exceeds admitted memory bytes')
                    values.append(row)
                output = pa.Table.from_pylist(values)
            if not output.num_rows:
                continue
            if memory_bytes is not None and output.get_total_buffer_size() > memory_bytes:
                raise RecoveryContractError('Arrow callback retained output exceeds the sealed output staging byte budget')
            output = output.cast(schema)
            # Output staging borrows the sealed output byte ceiling; an over-budget
            # callback fails before publishing rather than spilling without limit.
            if memory_bytes is not None and output.get_total_buffer_size() > memory_bytes:
                raise RecoveryContractError('Arrow callback output exceeds the sealed output staging byte budget')
            if writer is None:
                stream = open(path, 'wb')
                writer = pa.ipc.new_stream(pa.PythonFile(_CappedWriter(stream, max_bytes)), output.schema)
            writer.write_table(output)
            written += output.nbytes
            rows += output.num_rows
        if writer is not None:
            writer.close()
            writer = None
        failed = False
    finally:
        try:
            if writer is not None:
                writer.close()
        finally:
            if stream is not None:
                stream.close()
            if failed:
                Path(path).unlink(missing_ok=True)
            close = getattr(outputs, 'close', None)
            if close:
                close()
    return TaskOutput(ArrowBlock(path, rows, Path(path).stat().st_size if rows else 0), batch.num_rows)


def execute(executor, source, plan, schema, *, execution=None):
    import cloudpickle
    import pyarrow as pa
    from dataclasses import replace
    from ..lance.read import iter_lance_batches
    from .native_resources import _sealed_ceilings, dataset_session
    if executor._local_kernel_closed:
        raise RuntimeError('local_execution is closed; execute actions inside its context')
    options = executor._local_kernel
    ceilings = _sealed_ceilings(execution)
    if ceilings is not None:
        # Borrow the sealed placement into a local options copy before the scheduler
        # exists; never mutate the shared executor configuration. CPU tightens real
        # concurrency and inflight_rows tightens the physical reader batch. The
        # declared business batch_size below is preserved separately.
        workers = options.workers
        if ceilings['cpu_millis'] is not None:
            workers = min(workers, max(1, ceilings['cpu_millis'] // 1000))
        batch_rows = options.batch_rows
        if ceilings['inflight_rows'] is not None:
            batch_rows = min(batch_rows, ceilings['inflight_rows'])
        options = replace(options, workers=workers, batch_rows=batch_rows,
                          partitions=min(options.partitions, workers))
    operation = plan.operations[0]
    size = operation.batch_size or options.batch_rows
    stats = dict(worker_mode=options.worker_mode, workers=options.workers, partitions=options.partitions,
                 peak_pending_tasks=0, workers_used=[], stages=[], status='running', arrow_batch_sink=True)
    stage = dict(name='arrow_map_batches', submitted=0, completed=0, worker_seconds=0.0,
                 rows_input=0, rows_output=0)
    stats['stages'].append(stage)
    executor._local_kernel_stats = stats
    started = time.monotonic()
    encoded = cloudpickle.dumps(operation)
    from .run_context import current_run
    run = current_run()
    temporary_root = options.temp_directory
    if run is not None:
        temporary_root = run.directory / 'scratch' / 'arrow-tasks'
        temporary_root.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix='demiflow-arrow-', dir=temporary_root) as directory, ExitStack() as stack:
            import copy
            admitted_executor = copy.copy(executor)
            admitted_executor.resource_root = Path(directory)
            session = stack.enter_context(dataset_session(admitted_executor, directory, execution=execution))
            grant = stack.enter_context(session.borrow_resources())
            # The same admission covers reader, pending inputs, pending outputs
            # and consumer-held output. Scheduler yield retains its slot until
            # the consumer resumes; no replacement task is launched earlier.
            ceiling = ceilings or {}
            memory = min(options.memory_bytes, grant.memory_bytes)
            budget = dict(inflight_bytes=min(ceiling.get('inflight_bytes') or memory, memory // 4),
                          output_bytes=min(ceiling.get('output_bytes') or memory, memory // 4))
            scratch = min(grant.max_scratch_bytes, ceiling.get('scratch_bytes') or grant.max_scratch_bytes)
            owned = set()
            if run is not None:
                def release_owned():
                    for path in owned:
                        path.unlink(missing_ok=True)
                    run.release_temporaries(list(owned))
                stack.callback(release_owned)
            with TaskScheduler(options, stats, budget=budget) as scheduler:
                scratch_per_task = min(scratch // scheduler._width, scheduler.task_output_bytes + 64 * 1024)
                with closing(iter_lance_batches(source.query, batch_size=min(size, options.batch_rows))) as batches:
                    def arguments():
                        with closing(rechunk(batches, size, max_bytes=scheduler.task_inflight_bytes,
                                             reader_max_bytes=memory // 4)) as groups:
                            for i, batch in enumerate(groups):
                                path = Path(directory) / f'{i}.arrow'
                                if run is not None:
                                    # Reserve before dispatch through the same
                                    # run artifact budget used by durable sinks.
                                    run.allocate_artifact(path, size=scratch_per_task, atomic=False)
                                    owned.add(path)
                                yield batch, encoded, str(path), schema, scratch_per_task, scheduler.task_output_bytes
                    args = arguments()
                    with closing(scheduler.run(apply_batch, args, stage)) as results:
                        for result in results:
                            if result.rows:
                                with pa.OSFile(result.path, 'rb') as stream:
                                    yield from pa.ipc.open_stream(stream)
                            path = Path(result.path)
                            path.unlink(missing_ok=True)
                            if run is not None:
                                run.release_temporaries([path])
                                owned.discard(path)
        stats['status'] = 'complete'
    except GeneratorExit:
        stats['status'] = 'closed_early'
        raise
    except BaseException as error:
        stats.update(status='failed', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        stats['seconds'] = time.monotonic() - started
