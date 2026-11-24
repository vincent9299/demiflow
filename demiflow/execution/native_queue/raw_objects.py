"""Raw CAS participant for native commits; no transport or scheduling here.

A content check holds a file descriptor outside the control transaction. Only
this exact participant may use the short guarded recheck/publish protocol.
Existing image and third-party sink publication guards are unchanged.
"""
from contextlib import contextmanager
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from demiflow.data.datasink import Datasink
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity, identifier


class ConcurrentRawPublication(UnresolvedCommit):
    """A content name appeared after outside inspection; retry inspection."""


def _signature(s):
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def _recheck_signature(observed, expected):
    current = _signature(observed)
    if current != expected:
        if current[:4] == expected[:4]:
            # Linking/unlinking a concurrent publisher's staging name changes
            # ctime. A fresh outside hash is required before the next guard.
            raise ConcurrentRawPublication('Raw inode metadata changed; fresh inspection required')
        raise UnresolvedCommit('Raw object changed after inspection')


def _directory(path, create=False):
    path = Path(path).absolute()
    if '..' in path.parts or len(path.parts) > 64:
        raise RecoveryContractError('Invalid raw storage root')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _immutable(path, value):
    """Durable non-replacing small marker, safe against concurrent publishers."""
    raw = document(value, 65536)
    parent = _directory(path.parent, create=True)
    tmp = '.marker-' + uuid.uuid4().hex
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(fd, 'wb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, path.name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
        except FileExistsError:
            pass
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as f:
            st = os.fstat(f.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_size > 65536:
                raise RecoveryContractError('Invalid raw marker')
            existing = f.read(65537)
        os.fsync(parent)
        return json.loads(existing)
    finally:
        os.unlink(tmp, dir_fd=parent)
        os.close(parent)


def same_domain(expected,actual):
    keys=('domain_id','read_domain','declared_domain','store_id','marker_digest')
    if expected['read_domain']=='shared' and actual['read_domain']=='shared':
        return all(expected[k]==actual[k] for k in keys)
    return expected==actual


def _host_identity():
    with Path('/etc/machine-id').open() as stream:machine=stream.read(257).strip()
    if not machine or len(machine)>128:
        raise RecoveryContractError('Native host identity is unavailable')
    return identity(['host-v1',machine])


def _read_domain_marker(base):
    """One finite, descriptor-held marker reader for recovery and read binding."""
    parent=_directory(base)
    try:
        fd=os.open('.domain.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
        with os.fdopen(fd,'rb') as stream:
            st=os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_size>65536:
                raise UnresolvedCommit('Invalid raw domain marker')
            value=json.loads(stream.read(65537))
            after=os.fstat(stream.fileno())
            if (st.st_size,st.st_mtime_ns,st.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns):
                raise UnresolvedCommit('Raw domain marker changed while reading')
            return value
    finally:os.close(parent)


def resolve_raw_reference(store, reference, *, domain):
    """Explicit read-only native namespace binding; no journal or registration."""
    from demiflow.objects import ObjectRef, _local_path
    if type(reference) is dict:
        if (len(reference)!=2 or set(reference)!={'uri','sha256'} or
                type(reference['uri']) is not str or len(reference['uri'])>4096 or
                type(reference['sha256']) is not str or len(reference['sha256'])!=64):
            raise ValueError('Raw reference exceeds its closed scalar bounds')
        reference=ObjectRef(**reference)
    elif type(reference) is not ObjectRef:
        raise ValueError('Domain resolution requires ObjectRef or its closed mapping')
    if len(reference.uri)>4096:
        raise ValueError('Raw reference URI exceeds its byte budget')
    keys={'domain_id','read_domain','declared_domain','host_identity','store_id','physical_root','marker_digest'}
    if type(domain) is not dict or len(domain)!=7 or set(domain)!=keys:
        raise ValueError('Domain resolution requires the native raw domain')
    for name in ('domain_id','marker_digest','host_identity'):
        value=domain[name]
        if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('Invalid raw domain identity')
    for name in ('store_id','declared_domain'):
        value=domain[name]
        if name=='declared_domain' and value is None:continue
        if type(value) is not str or not 1<=len(value)<=128:
            raise ValueError('Raw domain identifier exceeds its scalar bound')
        identifier(value)
    if (type(domain['read_domain']) is not str or domain['read_domain'] not in {'host','shared'} or domain['read_domain']!=store.read_domain or
            domain['declared_domain']!=store.domain_id or domain['store_id']!=store.store_id):
        raise ValueError('Raw reference and explicit store domain differ')
    if type(domain['physical_root']) is not str or len(domain['physical_root'])>4096:
        raise ValueError('Raw namespace root exceeds its physical path bound')
    # All seven values are now bounded scalars matching a validated store, so
    # JSON/path parsing cannot first allocate an unbounded caller declaration.
    document(domain,16384)
    original=Path(domain['physical_root'])
    if (not original.is_absolute() or '..' in original.parts or str(original)!=domain['physical_root'] or
            len(original.parts)>64 or len(str(original))>4096 or
            len(original.parts)<4 or original.parent.parent.name!='.demiflow-raw' or original.name!=store.store_id):
        raise ValueError('Invalid native raw namespace root')
    namespace_id=identifier(original.parent.name)
    expected=original/reference.sha256[:2]/reference.sha256
    if _local_path(reference.uri)!=expected or reference.uri!=expected.as_uri():
        raise ValueError('Raw reference differs from its native domain/hash path')
    if store._domain_read_directory!=store.directory:
        raise ValueError('Domain resolution requires a direct physical store root without aliases or symlinks')
    namespace=RawNamespace(store,namespace_id)
    host=_host_identity()
    if domain['read_domain']=='host' and (domain['host_identity']!=host or original!=namespace.root):
        raise ValueError('Host raw domain cannot change host or physical root')
    marker=_read_domain_marker(namespace.base)
    marker_keys={'format','marker','read_domain','declared_domain','host_identity'}
    if (type(marker) is not dict or set(marker)!=marker_keys or type(marker['format']) is not int or marker['format']!=1 or
            marker['read_domain']!=store.read_domain or marker['declared_domain']!=store.domain_id or
            marker['host_identity']!=(host if store.read_domain=='host' else None)):
        raise ValueError('Explicit raw store marker differs from its declaration')
    identifier(marker['marker'])
    actual=dict(domain_id=identity(marker),read_domain=store.read_domain,declared_domain=store.domain_id,
        host_identity=host,store_id=store.store_id,physical_root=str(namespace.root),marker_digest=identity(marker))
    if not same_domain(domain,actual):
        raise ValueError('Explicit raw store is not the original artifact domain')
    os.close(_directory(namespace.root))
    return ObjectRef(namespace.target(reference.sha256).as_uri(),reference.sha256)


class RawNamespace:
    def __init__(self, store, namespace):
        self.store = store
        identifier(namespace)
        self.base = store.directory / '.demiflow-raw'
        self.root = self.base / namespace / store.store_id
        self.domain = None

    def open(self):
        # Native machine registration plus an independently persistent root UUID.
        host = _host_identity()
        proposed = dict(format=1, marker=uuid.uuid4().hex, read_domain=self.store.read_domain,
                        declared_domain=self.store.domain_id,
                        host_identity=host if self.store.read_domain == 'host' else None)
        marker = _immutable(self.base / '.domain.json', proposed)
        if (set(marker) != set(proposed) or marker['format'] != 1 or
                marker['read_domain'] != proposed['read_domain'] or
                marker['declared_domain'] != proposed['declared_domain'] or
                marker['host_identity'] != proposed['host_identity']):
            raise RecoveryContractError('Raw store domain registration differs')
        identifier(marker['marker'])
        domain_id = identity(marker)
        self.domain = dict(domain_id=domain_id, read_domain=self.store.read_domain,
                           declared_domain=self.store.domain_id, host_identity=host,
                           store_id=self.store.store_id, physical_root=str(self.root),
                           marker_digest=identity(marker))
        self.marker = marker
        for path in (self.root, self.root / '.pending', self.root / '.receipts'):
            os.close(_directory(path, create=True))
        return self.domain

    def recheck(self):
        parent = _directory(self.base)
        try:
            fd = os.open('.domain.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd,'rb') as f:
                st = os.fstat(f.fileno())
                if not stat.S_ISREG(st.st_mode) or st.st_size > 65536 or f.read(65537) != document(self.marker):
                    raise RecoveryContractError('Raw store marker changed')
        finally:
            os.close(parent)

    def target(self, digest):
        if type(digest) is not str or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise RecoveryContractError('Invalid raw object checksum')
        return self.root / digest[:2] / digest

    def stage(self, key, maximum):
        self.recheck()
        return RawWriter(self, identifier(key), maximum)


class RawWriter:
    def __init__(self, namespace, key, maximum):
        self.namespace, self.maximum, self.size = namespace, maximum, 0
        self.digest = hashlib.sha256()
        self.path = namespace.root / '.pending' / (key + '-' + uuid.uuid4().hex)
        parent = _directory(self.path.parent)
        try:
            fd = os.open(self.path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        finally:
            os.close(parent)
        self.file = os.fdopen(fd, 'wb', buffering=0)
        self.retained = False

    def write(self, chunk):
        if self.size + len(chunk) > self.maximum:
            raise RecoveryContractError('Raw writer hard byte bound exceeded')
        remaining=memoryview(chunk)
        while remaining:
            written=self.file.write(remaining)
            if type(written) is not int or not 0<written<=len(remaining):
                raise OSError(errno.EIO,'Raw storage write made no progress')
            remaining=remaining[written:]
        self.digest.update(chunk)
        self.size += len(chunk)

    def finish(self):
        if os.fstat(self.file.fileno()).st_size!=self.size:
            raise OSError(errno.EIO,'Raw storage length differs before preparation')
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        parent = _directory(self.path.parent)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return dict(staging=self.path.name, sha256=self.digest.hexdigest(), bytes=self.size)

    def close(self):
        self.file.close()
        if not self.retained:
            parent = _directory(self.path.parent)
            try:
                try:
                    os.unlink(self.path.name, dir_fd=parent)
                except FileNotFoundError:
                    pass
            finally:
                os.close(parent)


class RawInspection:
    def __init__(self, namespace, fact, check_stop=None):
        self.namespace, self.fact = namespace, fact
        self.fd = self.parent = None
        target = namespace.target(fact['sha256'])
        self.target = target
        # Staging exists before publication; a missing published object is never
        # silently recreated once its native receipt has been recorded.
        for path in (target, namespace.root / '.pending' / fact['staging']):
            try:
                parent = _directory(path.parent)
                try:
                    fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                except BaseException:
                    os.close(parent)
                    raise
            except FileNotFoundError:
                continue
            self.fd, self.parent, self.path = fd, parent, path
            break
        if self.fd is None:
            raise UnresolvedCommit('Prepared raw object is missing or inaccessible')
        try:
            before = os.fstat(self.fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size != fact['bytes']:
                raise UnresolvedCommit('Prepared raw object size differs')
            sha, size = hashlib.sha256(), 0
            checked_at=0
            while chunk := os.read(self.fd, min(65536, fact['bytes'] - size + 1)):
                if check_stop is not None and size>=checked_at:
                    check_stop();checked_at=size+4*1024**2
                size += len(chunk)
                if size > fact['bytes']:
                    raise UnresolvedCommit('Raw object grew during inspection')
                sha.update(chunk)
            self.signature = _signature(before)
            if size != fact['bytes'] or sha.hexdigest() != fact['sha256']:
                raise UnresolvedCommit('Prepared raw object checksum differs')
            _recheck_signature(os.fstat(self.fd), self.signature)
        except BaseException:
            self.close()
            raise

    def recheck(self):
        self.namespace.recheck()
        now = os.stat(self.path.name, dir_fd=self.parent, follow_symlinks=False)
        _recheck_signature(os.fstat(self.fd), self.signature)
        _recheck_signature(now, self.signature)
        # Reopen parents under no-follow; a moved directory is not the current
        # registered namespace even if the held old descriptor remains valid.
        fd = _directory(self.path.parent)
        try:
            if _signature(os.fstat(fd))[:2] != _signature(os.fstat(self.parent))[:2]:
                raise UnresolvedCommit('Raw storage directory was replaced')
        finally:
            os.close(fd)

    def close(self):
        for attr in ('fd','parent'):
            fd = getattr(self, attr, None)
            if fd is not None:
                os.close(fd)
                setattr(self,attr,None)


class RawObjectSink(Datasink):
    sink_id = 'raw'

    def __init__(self, namespace, maximum, *, check_stop=None):
        self.namespace, self.maximum = namespace, maximum
        self.check_stop=check_stop

    def write(self, blocks, context):
        raise RecoveryContractError('Raw objects require native commit')

    def prepare(self, blocks, context):
        value = blocks
        self._validate(value)
        return value

    def _validate(self, prepared):
        document(prepared, 32768)
        if type(prepared) is not dict or set(prepared) != {'format','object','result','domain'} or prepared['format'] != 1:
            raise RecoveryContractError('Invalid raw prepared descriptor')
        fact = prepared['object']
        from demiflow.collect.raw_contract import validate_result
        try:
            validate_result(prepared['result'],fact,prepared['domain'])
        except (TypeError,ValueError,KeyError) as exc:
            raise RecoveryContractError('Invalid prepared raw receipt') from exc
        if fact is not None:
            if type(fact) is not dict or set(fact) != {'staging','sha256','bytes'}:
                raise RecoveryContractError('Invalid raw object descriptor')
            identifier(fact['staging'])
            self.namespace.target(fact['sha256'])
            if type(fact['bytes']) is not int or not 0 <= fact['bytes'] <= self.maximum:
                raise RecoveryContractError('Raw prepared byte bound differs')
            if not same_domain(prepared['domain'],self.namespace.domain):
                raise UnresolvedCommit('Prepared raw artifact domain is unavailable')
        elif prepared['domain'] is not None:
            raise RecoveryContractError('Object-free receipt cannot pin an artifact domain')

    @contextmanager
    def inspection(self, prepared):
        self._validate(prepared)
        inspected = RawInspection(self.namespace, prepared['object'],self.check_stop) if prepared['object'] else None
        try:
            yield inspected
        finally:
            if inspected is not None:
                inspected.close()

    def _receipt(self, prepared, context):
        from demiflow.collect.raw_contract import validate_result
        validate_result(prepared['result'],prepared['object'],prepared['domain'],context=context)
        return dict(commit_id=context.commit_id, sink_id=self.sink_id, status='committed', data=prepared)

    def inspect_guarded(self, prepared, context, inspected):
        if inspected is None:
            return self._receipt(prepared, context)
        inspected.recheck()
        marker = self.namespace.root / '.receipts' / (context.commit_id + '.json')
        try:
            parent = _directory(marker.parent)
            try:
                fd = os.open(marker.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                with os.fdopen(fd,'rb') as f:
                    st = os.fstat(f.fileno())
                    if not stat.S_ISREG(st.st_mode) or st.st_size > 65536:
                        raise UnresolvedCommit('Raw receipt is invalid')
                    value = json.loads(f.read(65537))
            finally:
                os.close(parent)
        except FileNotFoundError:
            return None
        if inspected.path != inspected.target or value != self._receipt(prepared, context):
            raise UnresolvedCommit('Committed raw object or receipt differs')
        return value

    def publish_guarded(self, prepared, context, inspected):
        if inspected is None:
            return self._receipt(prepared, context)
        inspected.recheck()
        if inspected.path != inspected.target:
            parent = _directory(inspected.target.parent, create=True)
            try:
                try:
                    os.link(inspected.path.name, inspected.target.name, src_dir_fd=inspected.parent,
                            dst_dir_fd=parent, follow_symlinks=False)
                except FileExistsError:
                    # Another hash publisher won after the outside inspection.
                    # Reinspect its actual bytes outside the transaction.
                    raise ConcurrentRawPublication('Concurrent CAS publication requires fresh inspection') from None
                os.fsync(parent)
            finally:
                os.close(parent)
        receipt = self._receipt(prepared, context)
        existing = _immutable(self.namespace.root / '.receipts' / (context.commit_id + '.json'), receipt)
        if existing != receipt:
            raise UnresolvedCommit('Raw receipt changed')
        return receipt

    def inspect(self, prepared, context):
        with self.inspection(prepared) as inspected:
            return self.inspect_guarded(prepared, context, inspected)

    def commit(self, prepared, context):
        raise RecoveryContractError('Raw publication requires native inspected guard')

    def abort(self, prepared, context):
        # Committed CAS bytes may be shared by other task phases. No deletion.
        return None


def inspect_acquisition(run, commit_id):
    """Read exactly one referenced native acquisition, without mutation."""
    from types import SimpleNamespace
    from demiflow.execution.checkpoint.commit import CommitCoordinator
    fact = run.journal.execution_commit(identifier(commit_id))
    if fact is None or set(fact['intent']['sinks']) != {'raw'} or fact['prepared'] is None:
        raise UnresolvedCommit('Referenced raw acquisition is unavailable')
    if fact['prepared'].get('raw_acquisitions') or fact['prepared']['state']:
        raise UnresolvedCommit('Raw acquisition cannot contain external state or recursive acquisitions')
    prepared = fact['prepared']['sinks']['raw']
    domain = prepared['domain']
    namespace = None
    if domain is not None:
        from demiflow.execution.queue_contract import ArtifactDomainUnavailable
        host=_host_identity()
        if domain['read_domain']=='host' and domain['host_identity']!=host:
            raise ArtifactDomainUnavailable('Raw artifact belongs to another host domain')
        with run.journal._connection() as db:
            key = identity([domain['store_id'],domain['domain_id'],domain['physical_root']])
            row = db.execute('SELECT document FROM runtime_raw_resources WHERE resource_id=?',(key,)).fetchone()
        if row is None or json.loads(row[0]) != domain:
            raise UnresolvedCommit('Raw acquisition domain is not registered')
        actual=domain
        if domain['read_domain']=='shared':
            delivery=getattr(run.journal,'delivery',None)
            declared=getattr(delivery,'domain',None)
            candidates=[declared] if declared is not None else []
            if hasattr(run.journal,'channel'):
                with run.journal._connection() as db:
                    registered=db.execute('SELECT domain FROM queue_workers WHERE generation=? LIMIT 1025',(delivery.generation,)).fetchall()
                if len(registered)>1024:raise UnresolvedCommit('Shared domain registration count exceeded')
                candidates.extend(json.loads(value[0]) for value in registered)
            for candidate in candidates:
                if candidate['host_identity']==host and same_domain(domain,candidate):
                    try:fd=_directory(candidate['physical_root'])
                    except FileNotFoundError:continue
                    else:os.close(fd)
                    actual=candidate
                    break
        # Reconstruct a read-only handle. Shared members may mount the verified
        # marker at another root; the immutable receipt retains its original URI.
        namespace = object.__new__(RawNamespace)
        namespace.root = Path(actual['physical_root'])
        namespace.base = namespace.root.parent.parent
        namespace.domain = actual
        try:namespace.marker = _read_domain_marker(namespace.base)
        except FileNotFoundError:
            raise ArtifactDomainUnavailable('Raw artifact domain root is unavailable') from None
        if identity(namespace.marker) != domain['marker_digest']:
            raise UnresolvedCommit('Raw domain marker differs')
    sink = RawObjectSink(namespace,run.limits['max_output_bytes'],check_stop=run.check_stop)
    action = SimpleNamespace(context=run,action_id=fact['action_id'])
    CommitCoordinator(action).inspect_completed(fact,{'raw':sink})
    return fact
