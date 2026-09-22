"""Immutable content-addressed Blob writes and fixed-version reads."""
from dataclasses import dataclass, asdict
from pathlib import Path
from .control import control_directory, table_lock_path
import hashlib
import fcntl


@dataclass(frozen=True)
class BlobRef:
    relative_uri: str
    version: int
    sha256: str
    column: str = 'data'

    def to_dict(self): return asdict(self)

    def read(self, root):
        import lance
        relative = Path(self.relative_uri)
        if relative.is_absolute() or '..' in relative.parts: raise ValueError('Blob URI must be relative')
        if len(self.sha256) != 64 or any(c not in '0123456789abcdef' for c in self.sha256):
            raise ValueError('Invalid content SHA')
        ds = lance.dataset(str(Path(root)/relative), version=self.version)
        matches = ds.scanner(columns=['sha256'], filter=f"sha256 = '{self.sha256}'", with_row_id=True).to_table()
        if matches.num_rows != 1: raise ValueError('Blob must resolve exactly one row')
        blob = ds.take_blobs(self.column, ids=[matches['_rowid'][0].as_py()])[0]
        value = blob.read()
        if hashlib.sha256(value).hexdigest() != self.sha256: raise ValueError('Blob content changed')
        return value


class LanceBlobStore:
    def __init__(self, root, relative_uri):
        self.root, self.relative_uri = Path(root), relative_uri
        relative = Path(relative_uri)
        if relative.is_absolute() or '..' in relative.parts: raise ValueError('Blob URI must be relative')
        self.path = self.root/relative

    def put(self, data):
        import lance
        import pyarrow as pa
        sha = hashlib.sha256(data).hexdigest()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with table_lock_path(self.path).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.path.exists():
                ds = lance.dataset(str(self.path))
                existing = ds.to_table(columns=['written_version'], filter=f"sha256 = '{sha}'")
                if existing.num_rows:
                    ref = BlobRef(self.relative_uri, existing['written_version'][0].as_py(), sha)
                    if ref.read(self.root) != data: raise ValueError('Blob hash collision')
                    return ref
                version = ds.version + 1
            else: version = 1
            schema = pa.schema([pa.field('sha256',pa.string(),nullable=False),
                                pa.field('written_version',pa.int64(),nullable=False), lance.blob_field('data')])
            batch = pa.RecordBatch.from_arrays([pa.array([sha]), pa.array([version]), lance.blob_array([data])],schema=schema)
            ds = lance.write_dataset(batch,str(self.path),mode='append' if self.path.exists() else 'create',schema=schema)
            return BlobRef(self.relative_uri,ds.version,sha)
