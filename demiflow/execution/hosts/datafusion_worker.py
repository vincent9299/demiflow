"""Private native subprocess; query outputs or staged merge files, no commits."""
from __future__ import annotations

import ctypes
import faulthandler
import gc
import hashlib
import os
from pathlib import Path
import signal
import sys
import time


from demiflow.operators.relational_sql import register_key_functions


def _write_error(path, error, managed):
    import cloudpickle
    if not managed:
        path.write_bytes(cloudpickle.dumps(error))
        return
    from demiflow.execution.hosts.datafusion import LimitedWriter
    with path.open('wb') as stream:
        cloudpickle.dump(error, LimitedWriter(stream, 256*1024))


def _region_offsets(request):
    """Execute a read-only region against bounded binary-key tables in this process."""
    import pickle
    import datafusion
    import pyarrow as pa
    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
    from datafusion.context import SQLOptions
    from demiflow.execution.hosts.datafusion import LimitedWriter
    limit = request['wire_limit']
    maximum = request['max_pairs']
    try:
        if datafusion.__version__ != '54.0.0':
            raise RuntimeError('native relation requires datafusion==54.0.0')
        pa.set_cpu_count(1)
        pa.set_io_thread_count(1)
        config = SessionConfig().with_target_partitions(1).with_batch_size(min(1024, maximum + 1))
        context = SessionContext(config, RuntimeEnvBuilder().with_greedy_memory_pool(limit).with_disk_manager_disabled())
        selection = request.get('selection', False)
        for name, rows in request.pop('tables').items():
            if isinstance(rows, pa.Table):
                batches = rows.to_batches(max_chunksize=8192)
                if not batches:
                    batches = [pa.RecordBatch.from_arrays([pa.array([], type=f.type) for f in rows.schema], schema=rows.schema)]
                context.register_record_batches(name, [batches])
                continue
            if selection:
                batches, start = [], 0
                for keys in rows.chunks or [pa.array([], type=rows.type)]:
                    offsets = pa.array(range(start, start + len(keys)), type=pa.int64())
                    batches.append(pa.record_batch([keys, offsets], names=['k', 'off']))
                    start += len(keys)
                context.register_record_batches(name, [batches])
                continue
            table = pa.record_batch([pa.array((key for key, _ in rows), type=pa.binary()),
                                     pa.array((offset for _, offset in rows), type=pa.int64()),
                                     pa.array(range(len(rows)), type=pa.int64())], names=['k', 'off', 'ord'])
            context.register_record_batches(name, [[table]])
        register_key_functions(context, {'keys': request.get('key_functions', ())})
        read_only = SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
        frame = context.sql(request['sql'], options=read_only)
        schema = frame.schema()
        if not len(schema) or any(field.type != pa.int64() for field in schema):
            raise TypeError('native relation must return int64 offset columns')
        rows = bytearray((maximum + 7) // 8) if selection else []
        if selection and len(schema) != 1:
            raise ValueError('native selection requires one offset column')
        status, value = 'complete', rows
        for wrapped in frame.execute_stream():
            batch = wrapped.to_pyarrow()
            if selection:
                for offset in batch.column(0).to_pylist():
                    if type(offset) is not int or not 0 <= offset < maximum:
                        raise ValueError('native selection returned an invalid offset')
                    index, bit = divmod(offset, 8)
                    if rows[index] & (1 << bit):
                        raise ValueError('native selection returned a duplicate offset')
                    rows[index] |= 1 << bit
                continue
            if len(rows) + batch.num_rows > maximum:
                status, value = 'budget', 'native relation produced more offset pairs than admitted'
                break
            if (len(rows) + batch.num_rows) * (128 + 48 * len(schema)) > limit:
                status, value = 'budget', 'native relation output tuples exceed their memory window'
                break
            rows.extend(zip(*(column.to_pylist() for column in batch.columns)))
    except Exception as exc:
        # DataFusion 54 exposes its ResourcesExhausted variant through Python
        # as a plain Exception with this stable category prefix.
        value = str(exc)[:512]
        exhausted = isinstance(exc, MemoryError) or value.startswith('DataFusion error: Resources exhausted:')
        status = 'budget' if exhausted else 'failed'
    with os.fdopen(request['result_fd'], 'wb', closefd=False) as result:
        try:
            pickle.dump((status, value), LimitedWriter(result, limit), protocol=5)
        except ValueError:
            result.seek(0)
            result.truncate()
            pickle.dump(('budget', 'native relation result exceeds its memory window'), result)
        # The query can finish between parent samples; also check the native
        # process high-water mark before acknowledging its result.
        # VmHWM belongs to this exec'd address space. getrusage(ru_maxrss)
        # can still include the much larger worker parent's pre-exec image.
        with open('/proc/self/status') as status_file:
            peak = next(int(line.split()[1]) * 1024 for line in status_file if line.startswith('VmHWM:'))
        if peak + os.stat(sys.argv[1]).st_size + limit > request['rss_bytes']:
            result.seek(0)
            result.truncate()
            pickle.dump(('budget', 'native relation child exceeded its granted peak RSS'), result)
    return 0


def main():
    import cloudpickle
    with open(sys.argv[1], 'rb') as stream:
        request = cloudpickle.load(stream)
    # A parent crash must not leave a native query holding admission forever.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'Unable to set DataFusion parent-death signal')
    if os.getppid() != request['parent_pid']:
        raise RuntimeError('DataFusion parent exited before worker initialization')
    options = request['options']
    allowed = sorted(os.sched_getaffinity(0))
    end = len(allowed) - request['slot'] * options['threads']
    os.sched_setaffinity(0, allowed[end-options['threads']:end])
    faulthandler.enable()
    if request.get('region_offsets'):
        return _region_offsets(request)
    # Periodic traceback dumping reproducibly crashed this runtime during Python
    # transforms. Keep fatal-error diagnostics; the parent enforces deadlines
    # and reports progress/RSS without asynchronously walking Python frames.
    from demiflow.execution.hosts.datafusion import _save
    managed = request.get('managed_diagnostics', False)
    if managed:
        from demiflow.execution.hosts.datafusion import mandatory_json as _save
    directory = Path(request['directory'])
    diagnostic = Path(request['diagnostic'])
    started = time.monotonic()
    result = {'complete': False, 'success': False, 'rows': 0, 'phase': 'initializing'}
    def note(**values):
        result.update(values, seconds=time.monotonic()-started)
        _save(diagnostic / 'worker_result.json', result)
    try:
        if request.get('lance_index') is not None:
            from demiflow.lance.index import _prepare_index
            note(phase='lance_index_prepare')
            try:
                prepared = _prepare_index(request['lance_index'])
            except Exception as error:
                _write_error(diagnostic / 'index_error.pickle', error, managed)
                result['error_origin'] = 'lance_index'
                raise
            (directory / 'prepared_index.pickle').write_bytes(cloudpickle.dumps(prepared))
            note(complete=True, success=True, phase='prepared', rows=prepared['rows'])
            return 0
        if request.get('lance_merge') is not None:
            import lance
            import pyarrow as pa
            from demiflow.lance.read import iter_lance_batches
            from demiflow.lance.mutate import _prepare_merge
            pa.set_cpu_count(options['threads'])
            pa.set_io_thread_count(min(options['threads'], 4))
            spec, query, source_rows = request['lance_merge']
            if getattr(lance, '__build_version__', None) != '12.0.0+demiflow.arrowfix1':
                raise RuntimeError('This merge adapter is validated for pylance==12.0.0+demiflow.arrowfix1')
            note(phase='lance_merge_prepare', rows=source_rows, lance_version=lance.__version__, lance_build_version=lance.__build_version__,
                 lance_memory_pool_bytes=int(os.environ['LANCE_MEM_POOL_SIZE']),
                 lance_cpu_threads=int(os.environ['LANCE_CPU_THREADS']))
            try:
                prepared = _prepare_merge(spec, iter_lance_batches(query, batch_size=options['batch_rows']),
                                          _source_row_count=source_rows)
            except Exception as error:
                _write_error(diagnostic / 'merge_error.pickle', error, managed)
                result['error_origin'] = 'lance_merge'
                raise
            (directory / 'prepared_merge.pickle').write_bytes(cloudpickle.dumps(prepared))
            note(complete=True, success=True, phase='prepared', rows=prepared.input_rows)
            return 0
        import datafusion
        import lance
        import pyarrow as pa
        from demiflow.lance.arrow_batches import fixed_row_tables, LANCE_BATCH_ROWS, ARROW_BATCH_BYTES
        # Requests from already running parents may predate this option.
        max_row_bytes = options.get('max_row_bytes', ARROW_BATCH_BYTES)
        writer_batch_bytes = min(ARROW_BATCH_BYTES, max_row_bytes)
        from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
        from datafusion.context import SQLOptions
        if datafusion.__version__ != '54.0.0' or getattr(lance, '__build_version__', None) != '12.0.0+demiflow.arrowfix1':
            raise RuntimeError('This adapter is validated for datafusion==54.0.0 and pylance==12.0.0+demiflow.arrowfix1')
        pa.set_cpu_count(options['threads'])
        pa.set_io_thread_count(min(options['threads'], 4))
        spill = directory / 'spill'
        spill.mkdir()
        payload_read = (request.get('dataset_spec') or {}).get('payload_read')
        opaque_payloads = (request.get('dataset_spec') or {}).get('opaque_payloads') or []
        # Key sorting and payload hydration have independent batch budgets.
        # A rare wide payload must not create millions of tiny key-sort batches.
        query_batch_rows = 8192 if payload_read else options['batch_rows']
        config = (SessionConfig().with_target_partitions(options['partitions']).with_batch_size(query_batch_rows)
            .set('datafusion.optimizer.prefer_hash_join', str(options['prefer_hash_join']).lower()))
        context = SessionContext(config,
            RuntimeEnvBuilder().with_fair_spill_pool(options['memory_bytes']).with_disk_manager_specified(str(spill)))
        sql_options = SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
        from demiflow.execution.sources.source_readers import register_native_sources
        register_native_sources(context, request['sources'], payload_read=payload_read,
                                with_row_addr=bool(request.get('dataset_spec')))
        if request.get('dataset_spec'):
            register_key_functions(context, request['dataset_spec'])
        for name, sql in request['views'].items():
            context.register_view(name, context.sql(sql, options=sql_options))
        frame = context.sql(request['sql'], options=sql_options)
        if not managed:
            (diagnostic / 'plan.txt').write_text(str(frame.execution_plan()))
        from demiflow.lance.read import payload_schema, hydrate_payloads
        hydrated_schema, opaque_payloads = payload_schema(frame.schema(), opaque_payloads)
        schema = request['schema'] if request['schema'] is not None else hydrated_schema
        if opaque_payloads:
            result['opaque_payload_read'] = {'columns': [ref['field'] for ref in opaque_payloads],
                'rows': 0, 'batches': 0, 'max_batch_bytes': 0, 'max_lookup_bytes': 0,
                'query_payload_columns': 0}
        from demiflow.execution.sources.source_readers import open_native_payload, hydrate_native_payload
        payload_snapshot = open_native_payload(request['sources'], payload_read, result, query_batch_rows)
        note(phase='query', datafusion_version=datafusion.__version__, lance_version=lance.__version__, lance_build_version=lance.__build_version__,
             schema=str(schema), cpu_affinity=sorted(os.sched_getaffinity(0)),
             worker_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        ipc = directory / 'output.arrow'
        last_note = time.monotonic()
        transform = request['batch_transform']
        from demiflow.execution.kernels.dataflow import invoke_native_batch
        from demiflow.lance.write import native_arrow_writer, cast_native_result_batch
        def callback_error(error):
            if request.get('dataset_spec'):
                _write_error(diagnostic / 'callback_error.pickle', error, managed)
                result['error_origin'] = 'python_callback'
        with native_arrow_writer(ipc, schema) as writer:
            native_batches = (item.to_pyarrow() for item in frame.execute_stream())
            if opaque_payloads:
                native_batches = hydrate_payloads(native_batches, opaque_payloads,
                    max_row_bytes=max_row_bytes, stats=result['opaque_payload_read'])
            if payload_read:
                native_batches = hydrate_native_payload(native_batches, payload_snapshot, payload_read,
                    writer_batch_bytes=writer_batch_bytes, max_row_bytes=max_row_bytes, stats=result['payload_read'])
            task_rows = (request.get('dataset_spec') or {}).get('transform_batch_rows')
            if task_rows:
                native_batches = fixed_row_tables(native_batches,
                    payload_snapshot.schema if payload_read else frame.schema(), batch_rows=task_rows)
            for batch in native_batches:
                batch = invoke_native_batch(batch, transform, callback_error)
                batch = cast_native_result_batch(batch, schema)
                writer.write_batch(batch)
                result['rows'] += batch.num_rows
                if time.monotonic() - last_note >= 5:
                    note(phase='query')
                    last_note = time.monotonic()
        # Keep wide-query batches bounded through the private writer too.
        # Batch sizes no longer need to divide the file row count.
        writer_batch_rows = min(LANCE_BATCH_ROWS, options['batch_rows'])
        note(phase='private_write', query_seconds=time.monotonic()-started,
             writer_batch_rows=writer_batch_rows, writer_batch_bytes=writer_batch_bytes,
             writer_max_row_bytes=max_row_bytes)
        from demiflow.execution.sources.source_readers import verify_native_sources
        verify_native_sources(request['sources'])
        # Drain the native stream before entering the Lance writer. The disk
        # boundary prevents an engine callback from being nested inside a sink.
        frame = context = None
        gc.collect()
        # Read one IPC batch into ordinary buffers. Mapping the whole IPC file
        # made its touched pages accumulate in worker RSS during private writes.
        from demiflow.lance.write import write_native_output
        output = write_native_output(ipc, schema, directory, writer_batch_rows=writer_batch_rows,
            writer_batch_bytes=writer_batch_bytes, max_row_bytes=max_row_bytes, rows=result['rows'])
        note(complete=True, success=True, phase='finished', version=output.version)
        return 0
    except Exception as exc:
        note(complete=True, success=False, error_type=type(exc).__name__, message=str(exc)[:4096] if managed else str(exc))
        raise


if __name__ == '__main__':
    sys.exit(main())
