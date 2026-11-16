"""Driver-side stream callbacks over the same sealed Dataset execution."""
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from .contracts.dataflow_errors import DataflowError
from .dataflow import execute_action
from .kernels.async_instance import AsyncInstance
from .request_limits import ServiceStopped
from .stream import StreamStats


def run_stream_action(request, *, on_progress=None, on_drain=None, log_every=0,
                      cancellation=None, stall_timeout=None, stop_file=None):
    """Callbacks stay on the driver; business operators only run in workers."""
    if type(log_every) is not int or log_every < 0:
        raise ValueError('log_every must be a nonnegative integer')
    if stall_timeout is not None and (type(stall_timeout) not in (int, float) or stall_timeout <= 0):
        raise ValueError('stall_timeout must be positive')
    for hook in (on_progress, on_drain):
        if hook is not None and not callable(hook):
            raise TypeError('stream hooks must be callable')
    from .dataflow_api import resolve_action_runtime
    policy, bounds = resolve_action_runtime(request)
    callbacks = AsyncInstance(lambda value: value,
        SimpleNamespace(bounds=bounds, check_stop=request.action.context.check_stop))
    stats, handle, failure = StreamStats(), None, None
    tick = time.monotonic()
    previous, reported = 0, 0

    def observe():
        if handle is None:
            return 0
        execution = handle._execution
        from .dataflow_operator_observations import stream_observations
        stream_observations(execution, stats)
        if getattr(execution, '_stream_lance', None) is not None:
            stats.outputs = execution._stream_lance.outputs
        total = 0
        for node in execution._graph.nodes:
            counters = [value for (occ, _), value in execution._node_metrics.items()
                        if occ == node.occurrence_id]
            if not counters:
                continue
            rows_in = sum(value['rows_input'] for value in counters)
            rows_out = sum(value['rows_output'] for value in counters)
            stats.stages[node.occurrence_id] = {'in': rows_in, 'emitted': rows_out}
            total += rows_in + rows_out
        return total

    try:
        if stop_file is not None and Path(stop_file).exists():
            raise ServiceStopped('operator_stop_file')
        handle = execute_action(request, policy=policy, bounds=bounds)
        execution = handle._execution
        while True:
            if stop_file is not None and Path(stop_file).exists():
                raise ServiceStopped('operator_stop_file')
            if cancellation is not None and cancellation.requested:
                execution.satisfy_terminal()
            status = execution.pump(deadline_ms=50)
            total = observe()
            if total != previous:
                tick, previous = time.monotonic(), total
            elif stall_timeout is not None and time.monotonic() - tick >= stall_timeout:
                from ..errors import StallError
                raise StallError('unified stream made no observed progress')
            if on_progress is not None and (log_every == 0 or total - reported >= log_every):
                callbacks.resolve(on_progress(stats))
                reported = total
            if status.state in ('succeeded', 'failed', 'cancelled'):
                break
        result = handle.result()
        if execution._acquisition is not None:
            stats.metrics['resources'] = execution._acquisition.search_metrics()
        # The committed terminal receipt supplies the final count, including
        # an earlier attempt's prefix; no counter is inferred from the plan.
        stats.stage('committed_output')['emitted'] = result.rows or 0
        return stats
    except BaseException as exc:
        failure = exc
        if isinstance(exc, ServiceStopped) and str(exc) == 'operator_stop_file' and handle is not None:
            from .request_limits import COOPERATIVE_STOP_REASON
            try:
                handle._execution.cancel(COOPERATIVE_STOP_REASON)
            except BaseException as cleanup:
                failure.add_note('cooperative stop: ' + str(cleanup))
        raise
    finally:
        observe()
        if handle is not None:
            from .dataflow_api import record_execution_metadata
            record_execution_metadata(request, handle._execution)
        def finish_observations():
            observe()
            if handle is not None:
                from .dataflow_model_observations import finish_journal_audit
                finish_journal_audit(handle._execution, stats)
        for cleanup in (handle.close if handle is not None else None, finish_observations,
                        (lambda: callbacks.resolve(on_drain(stats), closing=True)) if on_drain else None,
                        callbacks.close):
            if cleanup is None:
                continue
            try:
                cleanup()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    failure.add_note('stream cleanup: ' + str(exc))
        if failure is not None and __import__('sys').exc_info()[0] is None:
            raise failure
