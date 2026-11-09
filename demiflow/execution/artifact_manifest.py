"""Bounded immutable artifact pages, referenced by the existing commit journal.

Pages contain at most 1024 descriptors and 256 KiB. The small index authenticates
all page bytes in order; it never accumulates one path per output in control JSON.
"""
import hashlib
import json
from pathlib import Path
import uuid

from .recovery_contract import RecoveryContractError, document
from .state_artifacts import atomic_bytes

FORMAT = 'paged-artifacts-v1'
PAGE_BYTES = 256 * 1024


class ArtifactManifest:
    def __init__(self, run, root):
        self.run = run
        self.root = Path(root) / ('pages-' + uuid.uuid4().hex)
        self.pages = self.items = 0
        self.digest = hashlib.sha256()
        self.reference = None

    def append(self, values):
        if self.reference is not None:
            raise RecoveryContractError('Cannot append to a sealed artifact manifest')
        if not values:
            return
        if len(values) > 1024:
            raise RecoveryContractError('Artifact page item ceiling exceeded')
        raw = document(values, PAGE_BYTES)
        if self.pages >= self.run.limits['max_artifacts']:
            raise RecoveryContractError('Artifact page count exceeds run budget')
        self._write(self.root / (str(self.pages) + '.json'), raw)
        self.digest.update(hashlib.sha256(raw).digest())
        self.pages += 1
        self.items += len(values)

    def _write(self, path, raw):
        self.run.allocate_artifact(path, size=len(raw))
        with self.run.journal.publication_guard(self.run.fence):
            atomic_bytes(path, raw)
        self.run.release_temporaries([path.with_name(path.name + ".staging")])

    def seal(self):
        if self.reference is None:
            raw = document(dict(format=FORMAT, pages=self.pages, items=self.items,
                                digest=self.digest.hexdigest()), 4096)
            path = self.root / 'index.json'
            self._write(path, raw)
            self.reference = dict(format=FORMAT, path=str(path.relative_to(self.run.directory)),
                                  bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        return dict(self.reference)


def _read(path, maximum, *, size=None, digest=None):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise RecoveryContractError('Artifact manifest contains a symlink')
    actual = path.stat().st_size
    if actual > maximum or size is not None and actual != size:
        raise RecoveryContractError('Artifact manifest size differs')
    with path.open('rb') as source:
        raw = source.read(maximum + 1)
    if len(raw) != actual or digest is not None and hashlib.sha256(raw).hexdigest() != digest:
        raise RecoveryContractError('Artifact manifest checksum differs')
    return raw


def manifest_pages(directory, reference, *, maximum=65536):
    if type(reference) is not dict or set(reference) != {'format', 'path', 'bytes', 'sha256'} or reference['format'] != FORMAT:
        raise RecoveryContractError('Invalid artifact manifest reference')
    relative = Path(reference['path'])
    if relative.is_absolute() or '..' in relative.parts or relative.parts[0] != 'materializations':
        raise RecoveryContractError('Artifact manifest escapes its run')
    index = Path(directory) / relative
    value = json.loads(_read(index, 4096, size=reference['bytes'], digest=reference['sha256']))
    if (set(value) != {'format', 'pages', 'items', 'digest'} or value['format'] != FORMAT
            or type(value['pages']) is not int or not 0 <= value['pages'] <= maximum
            or type(value['items']) is not int or not 0 <= value['items'] <= maximum * 1024):
        raise RecoveryContractError('Invalid artifact page index')
    digest, count = hashlib.sha256(), 0
    for number in range(value['pages']):
        path = index.parent / (str(number) + '.json')
        raw = _read(path, PAGE_BYTES)
        page = json.loads(raw)
        document(page, PAGE_BYTES)
        if type(page) is not list or not 1 <= len(page) <= 1024:
            raise RecoveryContractError('Artifact manifest page exceeds item ceiling')
        count += len(page)
        digest.update(hashlib.sha256(raw).digest())
        yield str(path.relative_to(directory)), page
    if digest.hexdigest() != value['digest'] or count != value['items']:
        raise RecoveryContractError('Artifact manifest page sequence differs')


def entries(run, value):
    if type(value) is list:
        if len(value) > 1024:
            raise RecoveryContractError('Artifact reference count exceeds page ceiling')
        yield from value
    else:
        for _, page in manifest_pages(run.directory, value, maximum=run.limits['max_artifacts']):
            yield from page
