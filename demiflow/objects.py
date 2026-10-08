"""Independent binary objects addressed by ordinary URIs and SHA256.

References contain no table, row, version or workspace-specific lookup. A URI
can be opened by other tools with the same filesystem/object-store access.
LocalObjectStore owns durable objects; its directory is not a disposable cache.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import io
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise ValueError('sha256 must be 64 lowercase hex characters')
    return value


def _local_path(uri):
    parsed = urlsplit(uri)
    if parsed.scheme != 'file' or parsed.netloc not in {'', 'localhost'}:
        raise ValueError('Expected a local file URI')
    if parsed.query or parsed.fragment:
        raise ValueError('File URI must not contain a query or fragment')
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise ValueError('File URI must be absolute')
    return path


@contextmanager
def open_object(uri):
    """Open an independent object; the caller owns content verification.

    file:// and HTTP(S) use the standard library. Other supported object-store
    schemes use PyArrow and its normal credential configuration. HTTP uses a
    finite timeout. This function never looks inside a Lance table.
    """
    if not isinstance(uri, str) or not urlsplit(uri).scheme:
        raise ValueError('Object URI requires an explicit scheme')
    scheme = urlsplit(uri).scheme
    if scheme == 'file':
        from .execution.artifacts import resolve_local_artifact
        with resolve_local_artifact(_local_path(uri)).open('rb') as stream:
            yield stream
    elif scheme in {'http', 'https'}:
        with urlopen(uri, timeout=60) as stream:
            yield stream
    else:
        from pyarrow.fs import FileSystem
        filesystem, path = FileSystem.from_uri(uri)
        with filesystem.open_input_file(path) as stream:
            yield stream


@dataclass(frozen=True)
class ObjectRef:
    uri: str
    sha256: str

    def __post_init__(self):
        _hash(self.sha256)
        if not isinstance(self.uri, str) or not urlsplit(self.uri).scheme:
            raise ValueError('Object URI requires an explicit scheme')
        if urlsplit(self.uri).scheme == 'file':
            _local_path(self.uri)

    def to_dict(self):
        return asdict(self)

    def read(self, *, max_bytes=None):
        """Read and verify a complete object, optionally rejecting it before full buffering."""
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 1):
            raise ValueError('max_bytes must be a positive integer or None')
        with open_object(self.uri) as stream:
            value = stream.read() if max_bytes is None else stream.read(max_bytes + 1)
        if max_bytes is not None and len(value) > max_bytes:
            raise ValueError('Object exceeds max_bytes: ' + self.uri)
        if hashlib.sha256(value).hexdigest() != self.sha256:
            raise ValueError('Object SHA256 mismatch: ' + self.uri)
        return value

    def verify(self):
        """Verify in bounded chunks; return the byte size without keeping bytes."""
        digest, size = hashlib.sha256(), 0
        with open_object(self.uri) as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        if digest.hexdigest() != self.sha256:
            raise ValueError('Object SHA256 mismatch: ' + self.uri)
        return size


class LocalObjectStore:
    """Publish immutable content-named files without replacing existing objects.

    Each hash maps to one ordinary file. Concurrent identical puts converge on
    that file. A pre-existing corrupt file raises an error instead of silently
    changing bytes beneath references. No table or registry is required to read.
    """
    def __init__(self, directory):
        value = str(directory)
        if urlsplit(value).scheme not in {'', 'file'}:
            raise ValueError('LocalObjectStore requires a local directory or file URI')
        self.directory = (_local_path(value) if value.startswith('file:')
                          else Path(value).absolute())
        from .execution.artifacts import resolve_local_artifact
        self.directory = resolve_local_artifact(self.directory)

    def reference(self, sha256):
        sha256 = _hash(sha256)
        return ObjectRef((self.directory / sha256[:2] / sha256).as_uri(), sha256)

    def put(self, value, *, sha256=None):
        # In-memory bytes already have a cheap, exact identity. Verify existing
        # assets without rewriting them, and stage new ones in their hash shard
        # so concurrent writers do not all mutate the same root directory.
        actual = hashlib.sha256(value).hexdigest()
        if sha256 is not None:
            _hash(sha256)
            if actual != sha256:
                raise ValueError('Object SHA256 mismatch before publication')
        ref = self.reference(actual)
        if _local_path(ref.uri).exists():
            ref.verify()
            # A concurrent writer may have linked the complete file but not
            # yet synced its directory. Preserve durable publication even if
            # that writer is interrupted before finishing its own sync.
            directory_fd = os.open(_local_path(ref.uri).parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return ref
        return self.put_stream(io.BytesIO(value), sha256=actual)

    def put_stream(self, stream, *, sha256=None):
        """Consume a binary stream in bounded chunks, then atomically publish it."""
        if sha256 is not None:
            _hash(sha256)
        # Known hashes can stage beside the final object. Unknown streams still
        # use the root until their content identity has been computed.
        staging = self.directory / sha256[:2] if sha256 is not None else self.directory
        staging.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix='.pending-', dir=staging)
        temporary = Path(temporary)
        digest = hashlib.sha256()
        try:
            with os.fdopen(descriptor, 'wb') as output:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            actual = digest.hexdigest()
            if sha256 is not None and actual != sha256:
                raise ValueError('Object SHA256 mismatch before publication')
            ref = self.reference(actual)
            target = _local_path(ref.uri)
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(temporary, target)
            except FileExistsError:
                ref.verify()
            directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return ref
        finally:
            temporary.unlink(missing_ok=True)


def externalize_data_uris(value, store):
    """Replace inline binary data URLs in JSON values with independent objects.

    The MIME/encoding header is retained so restore_data_uris reproduces the
    exact request value. Ordinary text and table references are unchanged.
    """
    import base64
    if isinstance(value, dict):
        return {key: externalize_data_uris(item, store) for key, item in value.items()}
    if isinstance(value, list):
        return [externalize_data_uris(item, store) for item in value]
    if isinstance(value, str) and value.startswith('data:') and ';base64,' in value:
        header, encoded = value.split(',', 1)
        payload = base64.b64decode(encoded, validate=True)
        if base64.b64encode(payload).decode() != encoded:
            raise ValueError('Non-canonical binary data URL cannot be losslessly externalized')
        return {'_demiflow_data_uri': {'header': header, **store.put(payload).to_dict()}}
    return value


def restore_data_uris(value):
    """Hydrate stored binary URI values only at their consuming boundary."""
    import base64
    if isinstance(value, dict):
        if set(value) == {'_demiflow_data_uri'}:
            saved = value['_demiflow_data_uri']
            return saved['header'] + ',' + base64.b64encode(ObjectRef(saved['uri'], saved['sha256']).read()).decode()
        return {key: restore_data_uris(item) for key, item in value.items()}
    if isinstance(value, list):
        return [restore_data_uris(item) for item in value]
    return value
