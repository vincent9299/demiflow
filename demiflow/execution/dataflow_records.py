"""Copy an already validated record without reinterpreting frozen control JSON."""
from dataclasses import replace

from .contracts.dataflow import Record
from .contracts.dataflow_codec import _thaw_json
from .contracts.dataflow_errors import DataflowError


def clone_record(record, **changes):
    """Revalidate a copy; external replacement values keep normal DTO checks.

    Only the original, bounded source-position document is thawed. Its tuples
    are frozen JSON arrays, unlike business payload tuples whose type matters.
    Do not broaden Record's public constructor to accept arbitrary objects.
    """
    if type(record) is not Record:
        raise DataflowError('INVALID_CONTRACT', 'clone_record requires a validated Record')
    if 'source_position' not in changes:
        changes['source_position'] = _thaw_json(record.source_position)
    return replace(record, **changes)
