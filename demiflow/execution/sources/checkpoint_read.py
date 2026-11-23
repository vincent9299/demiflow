"""Checkpoint read implementation."""
from __future__ import annotations

from demiflow.data.lance_declaration import make_lance_scan_source


def fixed_checkpoint_source(ref, declared, columns):
    """Validate the original fixed version and construct its read declaration."""
    import lance
    from demiflow.lance.schema import schema_identity
    if schema_identity(lance.dataset(**ref).schema) != schema_identity(declared['schema']):
        raise ValueError('Checkpoint committed schema differs from its declaration')
    source = make_lance_scan_source(
        ref['uri'], version=ref['version'], columns=columns,
        batch_readahead=1, fragment_readahead=1,
    )
    return source

