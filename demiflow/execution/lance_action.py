"""The public Lance terminal's original checked action and receipt boundary."""
from contextlib import contextmanager
from contextvars import ContextVar

_BINDING = ContextVar('demiflow_internal_lance_action_binding', default=None)


@contextmanager
def bind_lance_action(before_execute, *, max_rows_per_batch):
    """Internal registration hook, before any worker/source activation.

    No public Dataset option, new action, scheduler or commit authority lives
    here. The hook receives the already-created original RunAction once.
    """
    if _BINDING.get() is not None:
        raise RuntimeError('Nested Lance registration binding')
    token = _BINDING.set((before_execute, max_rows_per_batch))
    try:
        yield
    finally:
        _BINDING.reset(token)


def execute_lance_write(dataset, spec, *, native_options=None):
    from dataclasses import asdict
    from .checked import action
    from .run_context import current_action
    from .dataflow_api import make_action_request, run_dataset_action
    from .contracts.dataflow import ActionProgram
    from ..observability import observe_action
    arguments = {} if native_options is None else {'native_options': asdict(native_options)}
    binding = _BINDING.get()
    if binding is not None:
        from .dataflow_native_limits import EXECUTION_LIMITS
        arguments[EXECUTION_LIMITS] = {'inflight_rows': binding[1]}
    with action('write_table', dataset._source, dataset._plan, spec) as checked, observe_action(
        'write_lance', dataset._source, dataset._plan, dataset._executor,
        sink_type='LanceWrite', sink_target=spec.uri,
    ) as observer:
        if binding is not None:
            _BINDING.set(None)  # Never bind a second action in nested callbacks.
            binding[0](current_action(), spec)
        request = make_action_request(dataset._executor, checked.source, checked.plan,
            kind='write_lance', arguments=arguments,
            programs=(ActionProgram(name='write_lance', kind='sink', value=spec),))
        receipt = run_dataset_action(request)
        checked.receipt(receipt)
        if receipt.status == 'indeterminate':
            from ..errors import LanceWriteError
            raise LanceWriteError(receipt)
        observer.rows = int(receipt.written_rows or 0)
        observer.complete(result_target=spec.uri, write_receipt=receipt.to_dict(),
                          reconciliation_required=False)
    return receipt
