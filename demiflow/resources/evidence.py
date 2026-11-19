"""Bounded references to original provider records, not a new evidence store.

Resolve with the provider's original root and the scope/allocation from its
observation. A reference authenticates those coordinates and the bounded record
bytes; it is never, by itself, evidence that a process or thread has exited.
"""
import hashlib
import json
from pathlib import Path

from .contracts import ResourceError, document, identifier

_PREFIX = 'df-resource-evidence:v1:'
_RECORDS = frozenset({'thread.json', 'identity.json', 'process.json', 'cleanup.json'})


def _read(path):
    # The provider root is canonical; descendants must stay in that exact
    # allocation directory rather than borrowing another file's exit proof.
    try:
        if path.resolve() != path:
            raise ResourceError('INVALID_SPEC', 'resource evidence path contains a symlink')
    except (OSError, RuntimeError) as exc:
        raise ResourceError('INVALID_SPEC', 'resource evidence path cannot be resolved') from exc
    try:
        with path.open('rb') as stream:
            raw = stream.read(8193)
    except FileNotFoundError:
        return None
    if len(raw) > 8192:
        raise ResourceError('INVALID_SPEC', 'resource evidence exceeds its byte budget')
    return raw


def read_evidence_file(path):
    """The bounded, symlink-refusing read of one evidence file: its bytes, or None when absent.

    For a caller outside this package that resolves the original provider path itself, such as
    the engine cleaning up after a stopped run.
    """
    return _read(path)


def _reference(root, scope, allocation, record):
    identifier(scope); identifier(allocation)
    if record not in _RECORDS:
        raise ResourceError('INVALID_SPEC', 'unsupported resource evidence record')
    root = Path(root).resolve()
    directory = root / scope / allocation
    raw = _read(directory / record)
    # cleanup.json contains exit observations; process.json contains the exact
    # original birth/unit identity that those observations were checking.
    process = _read(directory / 'process.json') if record == 'cleanup.json' else None
    digest = lambda data: hashlib.sha256(data).hexdigest() if data is not None else None
    binding = {'root': str(root), 'scope': scope, 'allocation': allocation,
               'record': record, 'bytes': digest(raw), 'process': digest(process)}
    return _PREFIX + hashlib.sha256(document(binding)).hexdigest() + ':' + record, raw


def evidence_reference(root, scope, allocation, record):
    return _reference(root, scope, allocation, record)[0]


def resolve_evidence(root, scope, allocation, reference):
    if type(reference) is not str or not reference.startswith(_PREFIX):
        raise ResourceError('INVALID_SPEC', 'unsupported resource evidence reference')
    record = reference.rsplit(':', 1)[-1]
    actual, raw = _reference(root, scope, allocation, record)
    if reference != actual:
        raise ResourceError('ARTIFACT_MISMATCH', 'resource evidence identity or bytes differ')
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ResourceError('INVALID_SPEC', 'resource evidence is not JSON') from exc
