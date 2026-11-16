"""Explicit local object reads, sharing the platform's original path/SHA guard."""
from ..collect.flow_contract import normalize_object
from ..objects import local_object_uri, read_local_object


def object_read_binding(value):
    if type(value) is not dict or 'root' not in value or set(value) - {'root', 'relocations', 'layout'}:
        raise ValueError('Embedding object_read requires root and optional relocations/layout')
    declared = normalize_object(dict(reference='object', output='raw', error_output=None,
        label='embedding_objects', concurrency=1, queue_depth=1, max_bytes=1,
        root=value['root'], relocations=value.get('relocations', ()), layout=value.get('layout', 'path')))
    return {key: declared[key] for key in ('root', 'relocations', 'layout')}


def read_bound_object(ref, *, binding, max_bytes):
    """No network or alternate authority; SHA/layout/path checks precede use."""
    uri = local_object_uri(ref, **binding)
    return read_local_object(uri, ref['sha256'], root=binding['root'], max_bytes=max_bytes)
