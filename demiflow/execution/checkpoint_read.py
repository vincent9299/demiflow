"""Checkpoint read validation and fixed source binding, without SDK assembly."""
from __future__ import annotations

from ..data.execution_contract import CheckpointReadBinding, InputSpec
from ..data.lance_declaration import make_lance_scan_source
from ..data.plan import LogicalPlan
from ..data.sources import MaterializedSource


def read_checkpoint_binding(checkpoint, stage, *, columns=None, executor):
    """Return the registered source using an already selected input port."""
    binding, _ = _read_checkpoint_binding(
        checkpoint, stage, columns=columns, select_executor=lambda: executor,
    )
    return binding


def _read_checkpoint_binding(checkpoint, stage, *, columns, select_executor):
    """Select a default host only after the original checkpoint admission.

    The private assembly callback is not a request field or a validation hook.
    Both callers share this implementation; the core never imports DataAPI or
    Dataset and never constructs another execution owner.
    """
    from .checked import action, current
    from .stream_checkpoint import StreamCheckpoint, normalize_checkpoint_read
    import lance
    import pyarrow as pa

    if type(checkpoint) is not StreamCheckpoint or checkpoint.declared_outputs is None:
        raise ValueError('Read requires a declared native checkpoint')
    owner = current()
    facts = owner.checkpoint_facts(checkpoint) if owner is not None else checkpoint.contract_arguments
    arguments = normalize_checkpoint_read(dict(checkpoint=facts, stage=stage, columns=columns))
    declared = checkpoint.declared_outputs.get(stage)
    if declared is None:
        raise ValueError('Read stage is absent from declared checkpoint')
    with action('read_checkpoint', None, None, arguments=arguments) as checked:
        with checkpoint.lock:
            checkpoint.validate_ownership()
            if checkpoint.state['pending'] is not None:
                raise ValueError('Checkpoint has an unresolved commit; reopen it for recovery')
            ref = dict(checkpoint.state['outputs'][stage]) if stage in checkpoint.state['outputs'] else None
        executor = select_executor()
        if ref is None:
            schema = declared['schema']
            if columns:
                schema = pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)
            table = pa.Table.from_pylist([], schema=schema)
            source = MaterializedSource(executor.prepare_input(InputSpec('arrow', table)))
        else:
            from ..lance.schema import schema_identity
            if schema_identity(lance.dataset(**ref).schema) != schema_identity(declared['schema']):
                raise ValueError('Checkpoint committed schema differs from its declaration')
            source = make_lance_scan_source(
                ref['uri'], version=ref['version'], columns=columns,
                batch_readahead=1, fragment_readahead=1,
            )
        binding = CheckpointReadBinding(source, LogicalPlan(), ref)
        checked.checkpoint_read(source, ref)
        return binding, executor
