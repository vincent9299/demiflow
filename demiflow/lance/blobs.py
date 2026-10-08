"""Read-only compatibility for historical table-backed Blob references.

New production references use demiflow.objects.ObjectRef. No new table-backed
asset store is provided; keep this reader for frozen historical previews."""
from dataclasses import dataclass, asdict
from .storage import resolve_local_uri
from pathlib import Path
import hashlib


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
        ds = lance.dataset(str(resolve_local_uri(Path(root)/relative)), version=self.version)
        matches = ds.scanner(columns=['sha256'], filter=f"sha256 = '{self.sha256}'", with_row_id=True).to_table()
        if matches.num_rows != 1: raise ValueError('Blob must resolve exactly one row')
        blob = ds.take_blobs(self.column, ids=[matches['_rowid'][0].as_py()])[0]
        if blob is None: raise ValueError('Historical Blob has no bytes')
        try:
            value = blob.read()
        finally:
            blob.close()
        if hashlib.sha256(value).hexdigest() != self.sha256: raise ValueError('Blob content changed')
        return value
