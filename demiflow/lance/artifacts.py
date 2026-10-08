"""Named evidence metadata with independent object URIs and content hashes.

Logical names are identifiers, never filesystem fallbacks. Exact original
bytes are retained for reproducibility; one blob may serve many evidence roles.
"""
from pathlib import PurePosixPath
import hashlib
import json
import mimetypes
import pyarrow as pa
from .refs import DatasetRef
from ..objects import ObjectRef

ARTIFACTS = pa.schema([
    pa.field('path', pa.string(), nullable=False),
    pa.field('sha256', pa.string(), nullable=False),
    pa.field('byte_size', pa.int64(), nullable=False),
    ('media_type', pa.string()), ('role', pa.string()),
    pa.field('object_uri', pa.string(), nullable=False),
])

# Frozen evidence remains readable; new writers only use ARTIFACTS above.
_LEGACY_ARTIFACTS = pa.schema([
    *list(ARTIFACTS)[:-1],
    pa.field('blob_uri', pa.string(), nullable=False),
    pa.field('blob_version', pa.int64(), nullable=False),
    pa.field('blob_column', pa.string(), nullable=False),
])


def logical_path(path):
    value = str(path)
    p = PurePosixPath(value)
    if not value or p.is_absolute() or '..' in p.parts or str(p) != value:
        raise ValueError('Artifact name must be a normalized relative POSIX path')
    return value


class ArtifactSet:
    def __init__(self, root, reference):
        self.root = root
        self.ref = reference if isinstance(reference, DatasetRef) else DatasetRef.from_dict(reference)
        ds = self.ref.open(root)
        self._legacy = ds.schema.equals(_LEGACY_ARTIFACTS, check_metadata=True)
        if not self._legacy and not ds.schema.equals(ARTIFACTS, check_metadata=True):
            raise ValueError('Unexpected artifact schema')
        entries = ds.to_table().to_pylist()
        self.entries = {r['path']: r for r in entries}
        if len(self.entries) != len(entries):
            raise ValueError('Duplicate artifact names')
        for name in self.entries: logical_path(name)

    def paths(self, prefix=''):
        return sorted(p for p in self.entries if p.startswith(prefix))

    def reference(self, path):
        path = logical_path(path)
        if path not in self.entries: raise KeyError(path)
        return {'artifact_set': self.ref.to_dict(), 'path': path}

    def object_ref(self, path):
        if self._legacy:
            raise ValueError('Migrate legacy artifacts to independent object URIs before publishing references')
        row = self.entries[logical_path(path)]
        return ObjectRef(row['object_uri'], row['sha256'])

    def read_bytes(self, path):
        row = self.entries[logical_path(path)]
        if self._legacy:
            from .blobs import BlobRef
            raw = BlobRef(row['blob_uri'], row['blob_version'], row['sha256'], row['blob_column']).read(self.root)
        else:
            raw = self.object_ref(path).read()
        if len(raw) != row['byte_size']: raise ValueError('Artifact size changed')
        return raw

    def read_text(self, path, encoding='utf-8'):
        return self.read_bytes(path).decode(encoding)

    def json(self, path): return json.loads(self.read_text(path))

    def rows(self, path):
        return [json.loads(line) for line in self.read_text(path).splitlines() if line.strip()]

    def verify(self):
        if self._legacy:
            # Historical audit only; this branch cannot produce new references.
            seen = set()
            for path, row in self.entries.items():
                self.read_bytes(path)
                seen.add((row['blob_uri'], row['blob_version'], row['sha256'], row['blob_column']))
            return {'artifacts': len(self.entries), 'verified_blobs': len(seen)}
        objects = {}
        for row in self.entries.values():
            key = (row['object_uri'], row['sha256'])
            prior = objects.get(key)
            if prior is not None and prior != row['byte_size']:
                raise ValueError('Conflicting artifact size')
            objects[key] = row['byte_size']
        for (uri, sha256), size in objects.items():
            if ObjectRef(uri, sha256).verify() != size:
                raise ValueError('Artifact content or size changed')
        return {'artifacts': len(self.entries), 'verified_objects': len(objects)}


def read_artifact(root, reference):
    return ArtifactSet(root, reference['artifact_set']).read_bytes(reference['path'])


def import_jsonl(root, source):
    """Decode an explicit ArtifactRef or explicitly imported file into rows."""
    from pathlib import Path
    if isinstance(source, dict):
        assets=ArtifactSet(root, source['artifact_set'])
        raw=assets.read_bytes(source['path'])
        identity={'path':source['path'],'sha256':hashlib.sha256(raw).hexdigest(),
                  'artifact_ref':source}
    else:
        path=Path(source).resolve()
        raw=path.read_bytes()
        identity={'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()}
    return {'rows':[json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()],
            'source':identity}
