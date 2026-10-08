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


def rechunk(batches, size):
    """Preserve global callback boundaries even across Lance fragments."""
    import pyarrow as pa
    pieces, count = [], 0
    for batch in batches:
        offset = 0
        while offset < batch.num_rows:
            n = min(size - count, batch.num_rows - offset)
            pieces.append(batch.slice(offset, n))
            count += n
            offset += n
            if count == size:
                yield pa.Table.from_batches(pieces)
                pieces, count = [], 0
    if count:
        yield pa.Table.from_batches(pieces)


def apply_batch(batch, encoded, path, schema):
    import cloudpickle
    import pyarrow as pa
    from .executors.local import LocalDatasetExecutor
    operation = cloudpickle.loads(encoded)
    result = StandardCallable(operation.callable)(batch)
    outputs = result if isinstance(result, Iterator) else (result,)
    rows = 0
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
            if writer is None:
                writer = pa.ipc.new_stream(path, output.schema)
            writer.write_table(output)
            rows += output.num_rows
    finally:
        if writer is not None:
            writer.close()
        close = getattr(outputs, 'close', None)
        if close:
            close()
    return TaskOutput(ArrowBlock(path, rows, Path(path).stat().st_size if rows else 0), batch.num_rows)


def execute(executor, source, plan, schema):
    import cloudpickle
    import pyarrow as pa
    from ..lance.read import iter_lance_batches
    if executor._local_kernel_closed:
        raise RuntimeError('local_execution is closed; execute actions inside its context')
    options = executor._local_kernel
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
            with TaskScheduler(options, stats) as scheduler:
                with closing(iter_lance_batches(source.query, batch_size=size)) as batches:
                    args = ((batch, encoded, str(Path(directory) / f'{i}.arrow'), schema)
                            for i, batch in enumerate(rechunk(batches, size)))
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
