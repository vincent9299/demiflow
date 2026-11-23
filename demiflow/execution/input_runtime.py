"""Execution-owned bounded input construction and the public summary bridge."""
from __future__ import annotations

from collections.abc import Iterator

from ..data.execution_contract import InputSpec
from demiflow.execution.sources.materialized_input import build_input_handle


def prepare_input(executor, request: InputSpec):
    if request.kind == 'public_items':
        handle, _ = prepare_public_items(request, select_executor=lambda: executor)
        return handle
    if request.kind == 'items':
        if not isinstance(request.value, list) or isinstance(request.value, Iterator):
            raise TypeError('from_items requires a materialized list')
        value = list(request.value)
    else:
        value = request.value
    return build_input_handle(value, format=request.kind,
                              override_num_blocks=request.override_num_blocks)


def prepare_public_items(request: InputSpec, *, select_executor):
    """Keep validation/capture before default-host assembly, as in the public reader.

    The selector is the reader's private assembly callback, never an ActionSpec
    field or a caller-controlled validation policy. It runs only after checked
    admission, preserving the original validation and capture ordering.
    """
    from .checked import action, current
    from ..data.runtime_contract import check_size

    if request.kind != 'public_items':
        raise ValueError('prepare_public_items requires the public items request')
    items = request.value
    if current() is not None:
        if type(items) is not list or not 1 <= len(items) <= 128:
            raise ValueError('Managed from_items requires 1..128 explicit summary rows')
        check_size(items, 256 * 1024)
    with action('from_items', None, None, arguments={'items': items}) as checked:
        executor = select_executor()
        handle = executor.prepare_input(InputSpec('items', items, request.override_num_blocks))
        checked.materialized(handle)
        return handle, executor
