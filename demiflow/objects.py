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
import stat
import uuid
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


def local_object_directory(directory, *, base=None):
    """Resolve an object-store root without opening or creating the store."""
    value = str(directory)
    if urlsplit(value).scheme not in {'', 'file'}:
        raise ValueError('LocalObjectStore requires a local directory or file URI')
    if value.startswith('file:'):
        return _local_path(value)
    return ((Path(base) / value) if base is not None else Path(value)).absolute()


def read_local_object(uri, sha256, *, root, max_bytes):
    """Read one exact, bounded local object under an explicit root.

    No implicit relocation, network, prefix guessing or symlink traversal.
    Callers resolve declared historical locations before invoking this reader.
    """
    _hash(sha256)
    if type(max_bytes) is not int or not 1 <= max_bytes <= 128 * 1024**2:
        raise ValueError('Local object byte budget must be 1..128 MiB')
    root, path = Path(root).absolute(), _local_path(uri)
    if '..' in root.parts or '..' in path.parts or not path.is_relative_to(root) or len(path.parts) > 64:
        raise ValueError('Object is outside its declared local root')
    # Walk held directory descriptors: a concurrently replaced parent must not
    # turn a checked local path into a symlink traversal between check and open.
    parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(descriptor, 'rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes:
            raise ValueError('Local object is not a bounded regular file')
        raw = source.read(max_bytes + 1)
        after = os.fstat(source.fileno())
    if len(raw) > max_bytes or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('Local object changed or exceeded its byte budget')
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError('Local object SHA256 mismatch')
    return raw


def local_object_uri(ref, *, root, relocations=(), layout='path'):
    """Resolve only explicitly declared directory relocations, never search."""
    _hash(ref['sha256'])
    path, root = _local_path(ref['uri']), Path(root).absolute()
    if '..' in path.parts:
        raise ValueError('Object reference contains parent traversal')
    matches = [base for base in [root, *(Path(p) for p in relocations)] if path.is_relative_to(base)]
    if len(matches) != 1:
        raise ValueError('Object must match exactly one declared root or relocation')
    suffix = path.relative_to(matches[0])
    if layout == 'sha256_prefix2':
        legacy = suffix == Path(ref['sha256'][:2]) / ref['sha256']
        parts = suffix.parts
        managed = (len(parts) == 6 and parts[0] == '.demiflow-objects'
            and re.fullmatch('[0-9a-f]{32}', parts[1])
            and all(re.fullmatch('[A-Za-z0-9_-]{1,128}', p) for p in parts[2:4])
            and parts[-2:] == (ref['sha256'][:2], ref['sha256']))
        if not legacy and not managed:
            raise ValueError('Object reference differs from declared SHA256 path layout')
    return (root / suffix).as_uri()


class ReadLocalObject:
    """Native row reader; null references propagate without opening files."""
    def __init__(self, **arguments):
        from .collect.flow_contract import normalize_object
        self.arguments = normalize_object(arguments)

    def snapshot_state(self):
        return {}

    def restore_state(self,state):
        if state!={}:raise ValueError('Object reader has no mutable semantic state')

    def __call__(self, row):
        from .collect.flow_contract import row_result
        a = self.arguments
        ref = row[a['reference']]
        if ref is None:
            return row_result(row, a, None)
        try:
            uri = local_object_uri(ref, root=a['root'], relocations=a['relocations'], layout=a['layout'])
            raw = read_local_object(uri, ref['sha256'], root=a['root'], max_bytes=a['max_bytes'])
        except (OSError, ValueError, TypeError, KeyError) as error:
            return row_result(row, a, None, error)
        return row_result(row, a, raw)


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

    def verify(self, *, max_bytes=None):
        """Verify in bounded chunks; return the byte size without keeping bytes."""
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
            raise ValueError('max_bytes must be a nonnegative integer or None')
        digest, size = hashlib.sha256(), 0
        with open_object(self.uri) as stream:
            while chunk := stream.read(1024 * 1024 if max_bytes is None else min(1024 * 1024, max_bytes-size+1)):
                digest.update(chunk)
                size += len(chunk)
                if max_bytes is not None and size > max_bytes:
                    raise ValueError('Object exceeds max_bytes: ' + self.uri)
        if digest.hexdigest() != self.sha256:
            raise ValueError('Object SHA256 mismatch: ' + self.uri)
        return size


class LocalObjectStore:
    """Publish immutable content-named files without replacing existing objects.

    Each hash maps to one ordinary file. Concurrent identical puts converge on
    that file. A pre-existing corrupt file raises an error instead of silently
    changing bytes beneath references. No table or registry is required to read.
    """
    def __init__(self, directory, *, read_domain='host', domain_id=None, store_id='raw'):
        from .execution.recovery_contract import identifier
        if read_domain not in {'host', 'shared'}:
            raise ValueError('read_domain must be host or shared')
        if read_domain == 'shared':
            if domain_id is None:
                raise ValueError('Shared read domain requires an explicit domain_id')
            identifier(domain_id)
        elif domain_id is not None:
            raise ValueError('Host domain identity is assigned by the native resource registry')
        self.read_domain, self.domain_id, self.store_id = read_domain, domain_id, identifier(store_id)
        self.directory = local_object_directory(directory)
        # New explicit domain reads require a direct physical declaration. Keep
        # the original spelling for that check without changing legacy put/read
        # location and artifact-alias behavior.
        self._domain_read_directory = self.directory
        from .execution.artifacts import resolve_local_artifact
        self.directory = resolve_local_artifact(self.directory)

    def recovery_identity(self):
        return dict(directory=str(self.directory), read_domain=self.read_domain,
                    domain_id=self.domain_id, store_id=self.store_id, layout='sha256_prefix2')

    def reference(self, sha256):
        sha256 = _hash(sha256)
        return ObjectRef((self.directory / sha256[:2] / sha256).as_uri(), sha256)

    def resolve(self, reference, *, domain):
        """Bind a native raw reference to this explicit, existing read domain.

        Returns a new URI with the original SHA; never changes the receipt,
        creates a store, searches registrations, moves bytes or downloads. Only
        a shared domain with the same persistent marker can change mount root.
        This checks address/domain, not content or completion. For descriptor-
        guarded bounded reads use read_local_object(ref.uri, ref.sha256,
        root=self.directory, max_bytes=...). Ordinary ObjectRef.read/verify keep
        their existing location/alias semantics and are not no-follow readers.
        """
        from .execution.raw_objects import resolve_raw_reference
        return resolve_raw_reference(self, reference, domain=domain)

    def put(self, value, *, sha256=None, temporary=None, max_bytes=None):
        # In-memory bytes already have a cheap, exact identity. Verify existing
        # assets without rewriting them, and stage new ones in their hash shard
        # so concurrent writers do not all mutate the same root directory.
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0 or len(value) > max_bytes):
            raise ValueError('Object exceeds max_bytes')
        actual = hashlib.sha256(value).hexdigest()
        if sha256 is not None:
            _hash(sha256)
            if actual != sha256:
                raise ValueError('Object SHA256 mismatch before publication')
        ref = self.reference(actual)
        if _local_path(ref.uri).exists():
            ref.verify(max_bytes=max_bytes)
            # A concurrent writer may have linked the complete file but not
            # yet synced its directory. Preserve durable publication even if
            # that writer is interrupted before finishing its own sync.
            directory_fd = os.open(_local_path(ref.uri).parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return ref
        return self.put_stream(io.BytesIO(value), sha256=actual, temporary=temporary, max_bytes=max_bytes)

    def put_stream(self, stream, *, sha256=None, temporary=None, max_bytes=None):
        """Consume a binary stream in bounded chunks, then atomically publish it."""
        if sha256 is not None:
            _hash(sha256)
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
            raise ValueError('max_bytes must be a nonnegative integer or None')
        # Known hashes can stage beside the final object. Unknown streams still
        # use the root until their content identity has been computed.
        staging = self.directory / sha256[:2] if sha256 is not None else self.directory
        staging.mkdir(parents=True, exist_ok=True)
        if temporary is None:
            temporary = staging / ('.pending-' + uuid.uuid4().hex)
        else:
            temporary = Path(temporary).absolute()
            if temporary.parent != staging or not temporary.name.startswith('.pending-'):
                raise ValueError('Object temporary must be in its native staging shard')
        digest = hashlib.sha256()
        size = 0
        created = False
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            created = True
            with os.fdopen(descriptor, 'wb') as output:
                while chunk := stream.read(1024 * 1024 if max_bytes is None else min(1024 * 1024, max_bytes-size+1)):
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        raise ValueError('Object exceeds max_bytes')
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
                ref.verify(max_bytes=max_bytes)
            directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return ref
        finally:
            if created:
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


from .collect.fetch_flow import declare_value as _declare_fetch_value
_declare_fetch_value(LocalObjectStore,'local_store')

__all__ = ['ObjectRef','LocalObjectStore','ReadLocalObject','read_local_object','local_object_uri','open_object',
           'externalize_data_uris','restore_data_uris']
