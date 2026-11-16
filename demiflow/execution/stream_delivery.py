"""Legacy stream iterator entry using the one native dataflow runtime.

The original action, journal, object publication and terminal own delivery. This
adapter only checks the borrowed identity; it creates no thread, sink or commit
coordinator and never completes an action owned by its caller.
"""
from .recovery_contract import RecoveryContractError


def iter_stream_rows(executor, source, plan, *, limit=None, action=None, execution=None):
    if limit is not None and (type(limit) is not int or limit < 0):
        raise ValueError('Native delivery limit must be a nonnegative integer')
    from .queue_producer import producer_actor
    if producer_actor(plan) is not None:
        raise RecoveryContractError('Incremental enqueue requires run_stream; delivery terminals cannot publish tasks')
    if limit == 0:
        return
    from .run_context import current_action, current_run
    from .recovery_identity import plan_identity, source_identity
    borrowed = action if action is not None else current_action()
    if borrowed is not None:
        if borrowed.context is not current_run():
            raise RecoveryContractError('Delivery action belongs to a different run context')
        if source_identity(source) != borrowed.declaration.get('source'):
            raise RecoveryContractError('Delivery action source differs from the terminal source')
        expected = plan_identity('delivery', source, plan)['operations']
        if expected != borrowed.declaration.get('operations'):
            raise RecoveryContractError('Delivery action plan differs from the terminal plan')
        if borrowed is not current_action():
            raise RecoveryContractError('Delivery action must be the active action authority')
    from .dataset_native import execute
    rows = execute(executor, source, plan, execution=execution, limit=limit)
    try:
        yield from rows
    finally:
        rows.close()
