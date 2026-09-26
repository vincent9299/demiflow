"""Immutable named evidence backed by fixed Lance Blob references.

Logical names are identifiers, never filesystem fallbacks. Exact original
bytes are retained for reproducibility; one blob may serve many evidence roles.
"""
from .storage import resolve_local_uri
from pathlib import PurePosixPath
import hashlib
import json
import mimetypes
import pyarrow as pa
from .refs import DatasetRef
from .blobs import BlobRef

ARTIFACTS = pa.schema([
    pa.field('path', pa.string(), nullable=False),
    pa.field('sha256', pa.string(), nullable=False),
    pa.field('byte_size', pa.int64(), nullable=False),
    ('media_type', pa.string()), ('role', pa.string()),
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
        if not ds.schema.equals(ARTIFACTS, check_metadata=True):
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

    def blob_ref(self, path):
        row = self.entries[logical_path(path)]
        return BlobRef(row['blob_uri'], row['blob_version'], row['sha256'], row['blob_column'])

    def read_bytes(self, path):
        row = self.entries[logical_path(path)]
        raw = self.blob_ref(path).read(self.root)
        if len(raw) != row['byte_size']: raise ValueError('Artifact size changed')
        return raw

    def read_text(self, path, encoding='utf-8'):
        return self.read_bytes(path).decode(encoding)

    def json(self, path): return json.loads(self.read_text(path))

    def rows(self, path):
        return [json.loads(line) for line in self.read_text(path).splitlines() if line.strip()]

    def verify(self):
        import lance
        from collections import defaultdict
        from pathlib import Path
        groups = defaultdict(dict)
        for row in self.entries.values():
            key = (row['blob_uri'], row['blob_version'], row['blob_column'])
            prior = groups[key].get(row['sha256'])
            if prior is not None and prior != row['byte_size']:
                raise ValueError('Conflicting artifact size')
            groups[key][row['sha256']] = row['byte_size']
        count = 0
        for (uri, version, column), wanted in groups.items():
            ds = lance.dataset(str(resolve_local_uri(Path(self.root)/uri)), version=version)
            keys = sorted(wanted)
            for at in range(0, len(keys), 128):
                chunk = keys[at:at+128]
                predicate = 'sha256 IN (' + ','.join("'"+key+"'" for key in chunk) + ')'
                found = ds.scanner(columns=['sha256'], filter=predicate, with_row_id=True).to_table()
                if sorted(found['sha256'].to_pylist()) != chunk:
                    raise ValueError('Missing or duplicate evidence Blob')
                blobs = ds.take_blobs(column, ids=found['_rowid'].to_pylist())
                for key, blob in zip(found['sha256'].to_pylist(), blobs):
                    raw = blob if isinstance(blob, bytes) else blob.read()
                    if len(raw) != wanted[key] or hashlib.sha256(raw).hexdigest() != key:
                        raise ValueError('Artifact content or size changed')
                    count += 1
        return {'artifacts': len(self.entries), 'verified_blobs': count}


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
