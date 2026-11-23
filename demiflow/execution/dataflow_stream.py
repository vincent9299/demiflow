"""Driver-side stream callbacks over the same sealed Dataset execution."""
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from .contracts.dataflow_errors import DataflowError
from .action_driver.dataflow import execute_action
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
    from .plan_capture import current_capture
    capture = current_capture()
    if capture is not None:
        # Do not enter callback ownership: its finally block invokes on_drain.
        capture.intercept(request, policy=policy, bounds=bounds)
    callbacks = AsyncInstance(lambda value: value,
        SimpleNamespace(bounds=bounds, check_stop=request.action.context.check_stop))
    stats, handle, failure = StreamStats(), None, None
    body_failed = False
    tick = time.monotonic()
    previous, reported = 0, 0
    request.action._observation_rows = log_every if on_progress is not None else 0

    def observe():
        if handle is None:
            return 0
        execution = handle._execution
        from demiflow.execution.journal.dataflow_operator_observations import stream_observations
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
            total += rows_in + rows_out
        return total

    def report_progress():
        nonlocal reported
        observe()
        # Use the actual first business input frontier, not the sum of every
        # physical edge. Worker progress events preserve intermediate samples.
        frontier = next(iter(stats.stages.values()), {'in': 0})['in']
        if on_progress is not None and log_every and frontier - reported >= log_every:
            callbacks.resolve(on_progress(stats))
            reported = frontier

    def note_cleanup_error(label, error):
        if error is failure:
            return
        try:
            failure.add_note(label + str(error))
        except BaseException:
            # Diagnostics are optional. Exceptions may fail to format and Python
            # 3.10 does not provide add_note. All remaining owners must still close.
            pass

    try:
        if stop_file is not None and Path(stop_file).exists():
            raise ServiceStopped('operator_stop_file')
        handle = execute_action(request, policy=policy, bounds=bounds)
        execution = handle._execution
        execution._observe_progress = report_progress
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
            if on_progress is not None and log_every == 0:
                callbacks.resolve(on_progress(stats))
            if status.state in ('succeeded', 'failed', 'cancelled'):
                break
        result = handle.result()
        if execution._acquisition is not None:
            stats.metrics['resources'] = execution._acquisition.search_metrics()
        # The committed terminal receipt supplies the final count, including
        # an earlier attempt's prefix; no counter is inferred from the plan.
        stats._committed_output = result.rows or 0
        stats.metrics['committed_output'] = {'rows': result.rows or 0}
        return stats
    except BaseException as exc:
        body_failed = True
        failure = exc
        if isinstance(exc, ServiceStopped) and str(exc) == 'operator_stop_file' and handle is not None:
            from .request_limits import COOPERATIVE_STOP_REASON
            try:
                handle._execution.cancel(COOPERATIVE_STOP_REASON)
            except BaseException as cleanup:
                note_cleanup_error('cooperative stop: ', cleanup)
        raise
    finally:
        if handle is not None:
            handle._execution._observe_progress = None
        request.action._observation_rows = 0
        def cleanup(operation, *args):
            nonlocal failure
            try:
                operation(*args)
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    note_cleanup_error('stream cleanup: ', exc)

        if handle is not None:
            def record_metadata():
                from .dataflow_api import record_execution_metadata
                record_execution_metadata(request, handle._execution)

            def finish_audit():
                from .dataflow_model_observations import finish_journal_audit
                finish_journal_audit(handle._execution, stats)

            cleanup(observe)
            cleanup(handle.close)
            cleanup(observe)
            cleanup(record_metadata)
            cleanup(finish_audit)
        if on_drain is not None:
            cleanup(lambda: callbacks.resolve(on_drain(stats), closing=True))
        cleanup(callbacks.close)
        if failure is not None and not body_failed:
            raise failure
