"""Partition binding for the original aggregation algorithms."""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.streams import materialize_row as _materialize_row
from demiflow.operators.aggregate import resolve_aggregates


def accumulate_partial(records, *, spec, services):
    from demiflow.operators.aggregate import accumulate_partial as accumulate
    aggregates = resolve_aggregates(spec.configuration, services.programs)
    return accumulate(records, aggregates=aggregates, bounds=services.bounds,
        store=services.store, check_stop=services.check_stop,
        read_row=lambda record: _materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None)))


def merge_partials(partials, *, spec, services):
    from demiflow.operators.aggregate import merge_partials as merge
    if not spec.mergeable:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'non-mergeable kernel cannot decompose into partials')
    aggregates = resolve_aggregates(spec.configuration, services.programs)
    return merge(partials, aggregates=aggregates, bounds=services.bounds,
        store=services.store, check_stop=services.check_stop)


def aggregate_records(program, read_records, *, services, workspace, batch_eligible):
    from demiflow.operators.aggregate import aggregate_records as accumulate
    def call_codec(function, *args, **kwargs):
        return function(*args,
            resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),
            **kwargs)
    return accumulate(program, read_records, bounds=services.bounds,
        resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),
        batch_rows=(services.constants.get('native_limits') or {}).get('batch_rows', 1),
        workspace=workspace, batch_eligible=batch_eligible, call_codec=call_codec)
