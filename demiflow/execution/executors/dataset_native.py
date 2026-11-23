"""Bind a native Dataset adapter call to its original v3 action and limits."""
from contextlib import closing, nullcontext

from ...data import plan as ops


def execute(executor, source, plan, *, arrow=False, sink_schema=None, execution=None, limit=None):
    """Bind native delivery, schema and resource limits to the caller's v3 action."""
    from ..dataflow_api import make_action_request, run_dataset_action
    from ..dataflow_native_limits import EXECUTION_LIMITS, DELIVERY_LIMIT, execution_ceilings
    from ..native_resources import _sealed_ceilings

    if limit is not None and (type(limit) is not int or limit < 0):
        raise ValueError('Native delivery limit must be a nonnegative integer')
    if limit == 0:
        return
    original_plan = plan
    kind = "take" if limit is not None else "iter_batches" if arrow else "iter_rows"
    arguments = {"batch_format": "pyarrow"} if arrow else {}
    if limit is not None:
        arguments.update(limit=limit, **{DELIVERY_LIMIT: limit})
    ceilings = execution_ceilings(_sealed_ceilings(execution))
    if ceilings:
        arguments[EXECUTION_LIMITS] = ceilings
    if arrow and (ceilings.get('inflight_rows') is not None or limit is not None):
        arguments['batch_size'] = min(ceilings.get('inflight_rows', 256),
                                      limit if limit is not None else 2**63-1)
    if sink_schema is not None:
        import pyarrow as pa
        from ..contracts.dataflow_errors import DataflowError
        from ..dataflow_api import _PREFLIGHT_BOUNDS
        from ..dataflow_schema import KEY, encode
        from ..dataset_native import _cast_sink_batch
        if not arrow or not isinstance(sink_schema, pa.Schema):
            raise DataflowError('INVALID_CONTRACT', 'sink_schema requires Arrow output and a pyarrow.Schema')
        encoded = encode(sink_schema, bounds=_PREFLIGHT_BOUNDS)
        arguments[KEY] = encoded
        # A real graph node performs and records the conversion before terminal
        # publication. The driver's return adapter never relabels stored data.
        maximum = min(ceilings.get('memory_bytes', 2**63-1),
                      ceilings.get('inflight_bytes', 2**63-1),
                      ceilings.get('output_bytes', 2**63-1))
        plan = plan.append(ops.MapBatchesOp(ops.CallableSpec.create(_cast_sink_batch,
            fn_kwargs={'schema_bytes': encoded, 'max_bytes': maximum}),
            batch_size=min(256, ceilings.get('inflight_rows', 256)), batch_format='pyarrow'))
    from ..checked import action as checked_action
    from ..dataflow_api import declare_action_v3
    from ..run_context import current_action, current_run
    from ..contracts.dataflow_errors import DataflowError
    active = current_action()
    if active is not None:
        if active.context is not current_run():
            raise DataflowError('INVALID_CONTRACT', 'native execute action belongs to a different run context')
        declaration = declare_action_v3(source, original_plan, kind=kind, arguments={},
                                        sink_declaration=None)
        if any(active.declaration.get(key) != declaration[key] for key in ('source', 'operations')):
            raise DataflowError('INVALID_CONTRACT',
                'native execute source/plan differ from the active action declaration')
        # The source/plan still belong to the caller's action. New adapter facts
        # are bound by that action's original graph seal, verified on every
        # subsequent read by validate_execution_binding, never by another run.
    scope = checked_action(kind, source, plan, arguments=arguments) if active is None else nullcontext()
    with scope as checked:
        if checked is not None:
            source, plan = checked.source, checked.plan
        request = make_action_request(executor, source, plan, kind=kind, arguments=arguments)
        try:
            with closing(run_dataset_action(request)) as result:
                if arrow:
                    for table in result:
                        yield from table.to_batches()
                else:
                    yield from result
        except GeneratorExit:
            # A bounded take is still delivered lazily by this compatibility
            # iterator. Only the owner it created may finalize early delivery.
            if active is None and request.action.record['result'] is None:
                request.action.complete({'kind': kind, 'delivery': 'closed'})
            raise
