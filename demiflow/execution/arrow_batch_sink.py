"""Keep a local Lance -> Arrow batch callback -> Lance sink columnar."""
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterator
import tempfile
import time

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


def rechunk(batches, size, *, max_bytes=None):
    """Preserve global callback boundaries even across Lance fragments.

    Input buffering borrows the sealed stage inflight byte ceiling. A declared
    callback group that cannot fit fails explicitly; its row scope is never
    silently shrunk to admit it.
    """
    import pyarrow as pa
    from .recovery_contract import RecoveryContractError
    pieces, count, nbytes = [], 0, 0
    for batch in batches:
        offset = 0
        while offset < batch.num_rows:
            n = min(size - count, batch.num_rows - offset)
            piece = batch.slice(offset, n)
            if max_bytes is not None and nbytes + piece.nbytes > max_bytes:
                raise RecoveryContractError('Declared Arrow callback group exceeds the sealed inflight byte budget')
            pieces.append(piece)
            count += n
            nbytes += piece.nbytes
            offset += n
            if count == size:
                yield pa.Table.from_batches(pieces)
                pieces, count, nbytes = [], 0, 0
    if count:
        yield pa.Table.from_batches(pieces)


def apply_batch(batch, encoded, path, schema, max_bytes=None):
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
    try:
        for output in outputs:
            if isinstance(output, pa.RecordBatch):
                output = pa.Table.from_batches([output])
            elif not isinstance(output, pa.Table):
                # Existing callbacks may return dict arrays or row lists.
                output = pa.Table.from_pylist(list(LocalDatasetExecutor(workers=1)._batch_to_rows(output)))
            if not output.num_rows:
                continue
            output = output.cast(schema)
            # Output staging borrows the sealed output byte ceiling; an over-budget
            # callback fails before publishing rather than spilling without limit.
            if max_bytes is not None and written + output.nbytes > max_bytes:
                raise RecoveryContractError('Arrow callback output exceeds the sealed output staging byte budget')
            if writer is None:
                writer = pa.ipc.new_stream(path, output.schema)
            writer.write_table(output)
            written += output.nbytes
            rows += output.num_rows
    finally:
        if writer is not None:
            writer.close()
        close = getattr(outputs, 'close', None)
        if close:
            close()
    return TaskOutput(ArrowBlock(path, rows, Path(path).stat().st_size if rows else 0), batch.num_rows)


def execute(executor, source, plan, schema, *, execution=None):
    import cloudpickle
    import pyarrow as pa
    from dataclasses import replace
    from ..lance.read import iter_lance_batches
    from .native_resources import _sealed_ceilings
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
    try:
        with tempfile.TemporaryDirectory(prefix='demiflow-arrow-', dir=options.temp_directory) as directory:
            with TaskScheduler(options, stats, budget=ceilings) as scheduler:
                with closing(iter_lance_batches(source.query, batch_size=size)) as batches:
                    args = ((batch, encoded, str(Path(directory) / f'{i}.arrow'), schema, scheduler.task_output_bytes)
                            for i, batch in enumerate(rechunk(batches, size, max_bytes=scheduler.task_inflight_bytes)))
                    with closing(scheduler.run(apply_batch, args, stage)) as results:
                        for result in results:
                            if result.rows:
                                with pa.OSFile(result.path, 'rb') as stream:
                                    yield from pa.ipc.open_stream(stream)
                                Path(result.path).unlink()
        stats['status'] = 'complete'
    except GeneratorExit:
        stats['status'] = 'closed_early'
        raise
    except BaseException as error:
        stats.update(status='failed', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        stats['seconds'] = time.monotonic() - started
