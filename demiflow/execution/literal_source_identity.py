"""Stream the historical literal-source projection into its original SHA256.

Source contents are payload, not a journal metadata document. Keep describe's
JSON spelling and >1024-item projection exactly, with bounded traversal and
string fragments instead of assembling a potentially large JSON byte string.
The public describe/document/identity contracts remain unchanged.
"""
from dataclasses import fields, is_dataclass
import hashlib
import json
import math
from types import MappingProxyType

from .recovery_contract import RecoveryContractError


def literal_source_digest(value, *, max_bytes=256 * 1024**2):
    if type(max_bytes) is not int or not 0 < max_bytes <= 256 * 1024**2:
        raise RecoveryContractError('Literal identity needs a bounded source byte budget')
    remaining, nodes = max_bytes, 0

    def emit(raw, sink):
        nonlocal remaining
        if len(raw) > remaining:
            raise RecoveryContractError('Literal source identity exceeds its content byte budget')
        remaining -= len(raw)
        sink.update(raw)

    def string(text, sink):
        emit(b'"', sink)
        for offset in range(0, len(text), 8192):
            # Escaping individual code-point slices is byte-identical to the
            # original ensure_ascii=False encoding of their concatenation.
            fragment = json.encoder.encode_basestring(text[offset:offset + 8192])[1:-1]
            emit(fragment.encode('utf-8'), sink)
        emit(b'"', sink)

    def visit(item, depth, sink, *, projected=False):
        nonlocal nodes
        nodes += 1
        if nodes > 1000000 or depth > 32:
            raise RecoveryContractError('Literal source identity exceeds its structural budget')
        kind = type(item)
        if kind is str:
            string(item, sink)
        elif item is None or kind in (int, float, bool):
            if kind is float and not math.isfinite(item):
                raise RecoveryContractError('Nonfinite execution metadata')
            emit(json.dumps(item, allow_nan=False, separators=(',', ':')).encode(), sink)
        elif kind in (tuple, list):
            if len(item) > (16384 if projected else 100000):
                raise RecoveryContractError('Plan declaration exceeds item budget')
            if not projected and len(item) > 1024:
                content = hashlib.sha256()
                for child in item:
                    visit(child, depth + 1, content)
                    emit(b'\n', content)
                visit({'items': len(item), 'sha256': content.hexdigest()}, depth, sink, projected=True)
                return
            emit(b'[', sink)
            for index, child in enumerate(item):
                if index:
                    emit(b',', sink)
                visit(child, depth + 1, sink, projected=projected)
            emit(b']', sink)
        elif kind in (dict, MappingProxyType):
            if len(item) > 16384:
                raise RecoveryContractError('Invalid execution metadata keys')
            # describe historically stringifies keys, with the final duplicate
            # string key winning. Preserve that projection, including sorting.
            members = {str(key): child for key, child in item.items()}
            emit(b'{', sink)
            for index, key in enumerate(sorted(members)):
                if index:
                    emit(b',', sink)
                string(key, sink)
                emit(b':', sink)
                visit(members[key], depth + 1, sink, projected=projected)
            emit(b'}', sink)
        elif not projected and is_dataclass(item):
            # The canonical wrapper has fields before type. Its wrapper is not
            # another semantic depth level in the existing describe function.
            emit(b'{"fields":{', sink)
            members = sorted(f.name for f in fields(item)
                             if f.name not in {'runtime_contract', 'label'})
            for index, name in enumerate(members):
                if index:
                    emit(b',', sink)
                string(name, sink)
                emit(b':', sink)
                visit(getattr(item, name), depth + 1, sink)
            emit(b'},"type":', sink)
            string(type(item).__module__ + '.' + type(item).__qualname__, sink)
            emit(b'}', sink)
        elif not projected:
            from .recovery_identity import describe
            # Typed scalar/bytes/array and declaration identities retain their
            # original authority; do not apply the list projection twice.
            visit(describe(item, depth), depth, sink, projected=True)
        else:
            raise RecoveryContractError('Execution metadata must be strict JSON')

    digest = hashlib.sha256()
    visit(value, 0, digest)
    return digest.hexdigest()
