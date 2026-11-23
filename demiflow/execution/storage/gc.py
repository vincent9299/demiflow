"""Quiescent published-object reclamation, decided by the original RunJournal.

This never retires a historical cut. Unknown carriers, live physical leases or
an incomplete bounded trace leave both bytes and quota alone. It is deliberately
not a collector for active actions between cuts.
"""
from contextlib import contextmanager
import hashlib
import json
import os
import stat
import time

from ..contracts.dataflow import ArtifactRef
from ..contracts.dataflow_errors import DataflowError
from .lease import exclusive_access, exclusive_here, journal_identity
from .objects import RunArtifactStore, _no_symlink, _sync_directory


class UnprovenGC(Exception):
    pass


def _fault(point):
    """Narrow test seam; no fault is enabled by business/environment input."""


def gc_paths(journal, item):
    from ..recovery_contract import identifier
    for name in ('store_id', 'digest', 'gc_id'):
        identifier(item[name])
    root = journal.directory / '.dataflow-store' / item['store_id']
    target = root / 'objects' / item['digest']
    quarantine = root / 'gc' / item['gc_id']
    _no_symlink(target)
    _no_symlink(quarantine)
    return target, quarantine


def _check_time(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise UnprovenGC('Optional collection time budget exhausted')


def _verify_file(path, item, *, deadline=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        held, visible = os.fstat(fd), path.lstat()
        expected = (item['device'], item['inode'])
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1
                or (held.st_dev, held.st_ino) != expected
                or (visible.st_dev, visible.st_ino) != expected
                or held.st_size != item['bytes']):
            raise UnprovenGC('GC candidate physical identity changed')
        digest = hashlib.sha256()
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            for chunk in iter(lambda: stream.read(65536), b''):
                _check_time(deadline)
                digest.update(chunk)
        if digest.hexdigest() != item['digest']:
            raise UnprovenGC('GC candidate content changed')
    finally:
        os.close(fd)


def _reconcile(journal, fence, item, *, deadline=None):
    _check_time(deadline)
    if not exclusive_here(journal.directory / '.dataflow-store'):
        raise UnprovenGC('GC requires its original exclusive physical inode')
    target, quarantine = gc_paths(journal, item)
    if item['status'] == 'prepared':
        if target.exists():
            if quarantine.exists():
                raise UnprovenGC('GC target and quarantine both exist')
            _verify_file(target, item, deadline=deadline)
            exclusive_here(journal.directory / '.dataflow-store')
            quarantine.parent.mkdir(exist_ok=True)
            _no_symlink(quarantine.parent)
            os.rename(target, quarantine)
            _sync_directory(target.parent)
            _sync_directory(quarantine.parent)
            _fault('after_rename')
        if not quarantine.exists():
            # No verified quarantine fact: absence alone grants no refund.
            raise UnprovenGC('Prepared GC object is missing without physical proof')
        _verify_file(quarantine, item, deadline=deadline)
        journal.confirm_dataflow_gc_quarantine(fence, item['gc_id'])
        _fault('after_quarantine')
    if target.exists():
        raise UnprovenGC('Quarantined GC object was republished')
    if quarantine.exists():
        _verify_file(quarantine, item, deadline=deadline)
        exclusive_here(journal.directory / '.dataflow-store')
        quarantine.unlink()
        _sync_directory(quarantine.parent)
    # After a crash in unlink->finish, the original *verified quarantine* fact,
    # its unique owned path and this exclusive original inode prove removal.
    # This is not an arbitrary missing file being treated as free space.
    _fault('after_unlink')
    released = journal.finish_dataflow_gc(fence, item['gc_id'])
    _fault('after_refund')
    return released


@contextmanager
def _exclusive(journal, fence):
    protocol = journal.dataflow_gc_protocol()
    if protocol is None or not protocol[2]:
        raise UnprovenGC('Store predates physical object lease protocol')
    # Same order as native publication / claim_execution. The exclusive-context
    # marker only permits this thread's bounded internal journal/read operations.
    with journal._publication_lease(0), exclusive_access(
            journal.directory / '.dataflow-store', journal_identity(protocol)):
        with journal.execution_guard(fence):
            pass
        with journal._connection() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='resource_allocations'").fetchone():
                if db.execute("SELECT 1 FROM resource_allocations WHERE cleanup!='CONFIRMED' LIMIT 1").fetchone():
                    raise UnprovenGC('Original resource cleanup is not confirmed for every allocation')
        yield


def recover_published_gc(journal, fence, *, deadline=None):
    result = {'removed': 0, 'bytes': 0, 'blocked': None}
    try:
        _check_time(deadline)
        with _exclusive(journal, fence):
            for item in journal.dataflow_gc_intents():
                _check_time(deadline)
                result['bytes'] += _reconcile(journal, fence, item, deadline=deadline)
                result['removed'] += 1
            if journal.pending_dataflow_gc():
                raise UnprovenGC('GC recovery page exhausted')
            _check_time(deadline)
    except Exception as exc:
        result['blocked'] = str(exc)
    return result


_REF_FIELDS = {'digest', 'size_bytes', 'store_id', 'key', 'media_type'}
_LEAVES = {'application/octet-stream', 'application/vnd.demiflow.arrow-schema.v1',
    'application/vnd.demiflow.arrow-batch.v1', 'application/vnd.demiflow.mapped-arrow-batch.v1',
    'application/x-demiflow-program-payload-v3', 'text/x-python', 'text/plain'}
_STRUCTURED = {'application/json',
    'application/x-demiflow-worker-assignment-v3', 'application/x-demiflow-recovery-image-v3',
    'application/x-demiflow-graph-v3', 'application/x-demiflow-physical-v3',
    'application/x-demiflow-program-v3', 'application/x-demiflow-code-pin-v3',
    'application/x-demiflow-bridge-v3', 'application/x-demiflow-constants-v3',
    'application/vnd.demiflow.source-manifest.v3', 'application/vnd.demiflow.record.v3',
    'application/x-demiflow-record-v3', 'application/x-demiflow-record-v1',
    'application/x-demiflow-kernel-image-v3', 'application/x-demiflow-aggregate-state-v3',
    'application/x-demiflow-aggregate-result-v3', 'application/x-demiflow-sink-manifest-v3',
    'application/x-demiflow-sink-publication-v3', 'application/x-demiflow-terminal-segments.v1',
    'application/vnd.demiflow.segment.v3', 'application/vnd.demiflow.seal.v3',
    'application/vnd.demiflow.channel_state.v3'}
_LINES = {'application/x-demiflow-records-v3', 'application/x-demiflow-finite-records-v3'}


class _Trace:
    def __init__(self, journal, bounds, *, deadline=None):
        self.journal, self.bounds = journal, bounds
        self.maximum = min(journal.recovery_identity()['limits']['max_artifacts'],
                           max(1, min(bounds.max_state_bytes, bounds.max_resident_bytes // 16) // 2048))
        self.refs, self.pending, self.states = {}, [], set()
        self.steps, self.bytes = 0, 0
        self.deadline = deadline if deadline is not None else time.monotonic() + min(5, bounds.timeout_ms / 1000)

    def check(self, size=0):
        self.steps += 1
        self.bytes += size
        if (self.steps > self.maximum * 256 or self.bytes > self.bounds.max_spill_bytes
                or time.monotonic() > self.deadline):
            raise UnprovenGC('Complete root trace exceeds scan budget')

    def decode(self, raw):
        # Bound Python container/string amplification *before* json.loads.
        # This is collector metadata, not the business row/batch admission.
        maximum = self.bounds.max_resident_bytes // 4
        estimate = 8 * len(raw)
        if estimate > maximum:
            raise UnprovenGC('JSON carrier exceeds collector decode workspace')
        depth_limit = min(64, self.bounds.max_depth)
        if type(raw) is bytes and len(raw) <= 65536:
            # Counting punctuation inside strings only overestimates the old
            # workspace/depth bounds. When that conservative bound fits, C
            # byte counts prove admission before the same json.loads call.
            # Ambiguous/large carriers retain the original streaming scan.
            openings = raw.count(b'[') + raw.count(b'{')
            punctuation = openings + raw.count(b',') + raw.count(b':')
            if openings <= depth_limit and estimate + 256 * punctuation <= maximum:
                if raw:
                    self.check()  # Same scan step/deadline as a short carrier.
                return json.loads(raw)
        quoted = escaped = False
        depth = 0
        for index, byte in enumerate(raw):
            if quoted:
                if escaped:
                    escaped = False
                elif byte == 92:
                    escaped = True
                elif byte == 34:
                    quoted = False
            elif byte == 34:
                quoted = True
            elif byte in (91,123,44,58):
                estimate += 256
                if byte in (91,123):
                    depth += 1
            elif byte in (93,125):
                depth -= 1
            if estimate > maximum or depth > depth_limit:
                raise UnprovenGC('JSON structure exceeds collector decode workspace')
            if index % 65536 == 0:
                self.check()
        return json.loads(raw)

    def add(self, value):
        ref = ArtifactRef(**value)
        key = (ref.store_id, ref.key, ref.media_type)
        if ref.key != ref.digest:
            raise UnprovenGC('Unrecognized artifact key domain')
        if key not in self.refs:
            if len(self.refs) >= self.maximum:
                raise UnprovenGC('Complete root trace exceeds reference budget')
            self.refs[key] = ref
            self.pending.append(ref)

    def value(self, value, depth=0):
        self.check()
        if depth > min(64, self.bounds.max_depth):
            raise UnprovenGC('Root nesting exceeds trace budget')
        if type(value) is dict:
            if set(value) == _REF_FIELDS:
                self.add(value)
                return
            if {'state_id','path','bytes','sha256'} <= set(value):
                path = value['path']
                if path != 'state/' + value['sha256'] + '.json':
                    raise UnprovenGC('Unrecognized state carrier')
                if path not in self.states:
                    if len(self.states) >= self.maximum:
                        raise UnprovenGC('State carriers exceed trace budget')
                    self.states.add(path)
                    from ..artifact_manifest import _read
                    raw = _read(self.journal.directory / path,
                                min(self.bounds.max_state_bytes,self.bounds.max_resident_bytes//32),
                                size=value['bytes'], digest=value['sha256'])
                    self.check(len(raw))
                    self.value(self.decode(raw), depth + 1)
            if value.get('format') == 'paged-artifacts-v1':
                from ..artifact_manifest import manifest_pages
                for _, page in manifest_pages(self.journal.directory, value, maximum=self.maximum):
                    self.value(page, depth + 1)
            for field in ('data_b64', 'typed_result'):
                if field in value:
                    import base64
                    raw = base64.b64decode(value[field], validate=True)
                    if len(raw) > max(self.bounds.max_row_bytes, self.bounds.max_state_bytes):
                        raise UnprovenGC('Encoded record carrier exceeds trace bound')
                    self.check(len(raw))
                    self.value(self.decode(raw), depth + 1)
            for child in value.values():
                self.value(child, depth + 1)
        elif type(value) is list:
            for child in value:
                self.value(child, depth + 1)
        elif type(value) is str and value.startswith(('{', '[')):
            # Kernel-state ['record',id,seq,document] and finite record lines
            # embed the original record JSON as a string. Decode data, no code.
            try:
                nested = self.decode(value.encode())
            except ValueError:
                return
            self.value(nested, depth + 1)

    def roots(self):
        columns = {'runtime_actions': ('declaration','snapshot','result'),
            'runtime_sources': ('document',), 'runtime_commits': ('intent','prepared'),
            'runtime_receipts': ('document',), 'runtime_effects': ('document',),
            'runtime_commit_scopes': ('declaration',),
            'calls': ('request_json','response_json','error_json')}
        for table, fields in columns.items():
            after = 0
            with self.journal._connection() as db:
                existing = {row[1] for row in db.execute('PRAGMA table_info(' + table + ')')}
            if not existing:
                continue
            if not set(fields) <= existing:
                raise UnprovenGC('Unrecognized journal root schema: ' + table)
            while True:
                with self.journal._connection() as db:
                    sizes = db.execute('SELECT rowid,' + ','.join('length(CAST('+f+' AS BLOB))' for f in fields) +
                        ' FROM ' + table + ' WHERE rowid>? ORDER BY rowid LIMIT 1', (after,)).fetchone()
                    if sizes is None:
                        break
                    limit = min(self.bounds.max_control_bytes,self.bounds.max_resident_bytes//32)
                    if any(size is not None and size > limit for size in sizes[1:]):
                        raise UnprovenGC('Journal root exceeds collector decode bound')
                    rows = db.execute('SELECT rowid,' + ','.join(fields) + ' FROM ' + table +
                        ' WHERE rowid=?', (sizes[0],)).fetchall()
                if not rows:
                    break
                for row in rows:
                    after = row[0]
                    for raw in row[1:]:
                        if raw is not None:
                            if len(raw.encode()) > self.bounds.max_control_bytes:
                                raise UnprovenGC('Journal root exceeds control bound')
                            self.check(len(raw.encode()))
                            self.value(self.decode(raw.encode()))
        # Owners lack media/size. Recover their exact immutable reference from
        # original publication intent, never assume an owner points to a leaf.
        with self.journal._connection() as db:
            owners = db.execute('SELECT DISTINCT store_id,digest,key FROM artifact_owner LIMIT ?',
                                (self.maximum + 1,)).fetchall()
        if len(owners) > self.maximum:
            raise UnprovenGC('Owner roots exceed trace budget')
        for store, digest, key in owners:
            after, found = '', False
            while True:
                with self.journal._connection() as db:
                    rows = db.execute('SELECT s.reservation_id,s.publication FROM runtime_dataflow_staging s '
                        'JOIN runtime_dataflow_settlements d ON s.reservation_id=d.reservation_id '
                        'WHERE d.store_id=? AND d.digest=? AND s.publication IS NOT NULL '
                        'AND s.reservation_id>? ORDER BY s.reservation_id LIMIT 16', (store,digest,after)).fetchall()
                if not rows:
                    break
                for after, raw in rows:
                    found = True
                    self.value(self.decode(raw.encode()))
            if not found or key != digest:
                raise UnprovenGC('Owner lacks original typed publication proof')

    def walk(self):
        self.roots()
        while self.pending:
            ref = self.pending.pop()
            store = RunArtifactStore(root=self.journal.directory / '.dataflow-store',
                store_id=ref.store_id, journal=None, bounds=self.bounds, readonly=True)
            media = ref.media_type
            from ..dataflow_kernel_pack import PACK_MEDIA, iter_kernel_images
            if media == PACK_MEDIA:
                self.check(ref.size_bytes)
                for _, raw in iter_kernel_images(store, ref, self.bounds):
                    self.value(self.decode(raw))
                    del raw
            elif media in _LEAVES:
                # A protected opaque leaf may be large; authenticate incrementally.
                for chunk in store.read(ref, max_bytes=max(1, ref.size_bytes)):
                    self.check(len(chunk))
            elif media in _LINES:
                pending = b''
                for chunk in store.read(ref, max_bytes=max(1, ref.size_bytes)):
                    self.check(len(chunk))
                    pending += chunk
                    offset = 0
                    while (end := pending.find(b'\n',offset)) >= 0:
                        line = pending[offset:end]
                        offset = end + 1
                        if len(line) > min(self.bounds.max_batch_bytes,self.bounds.max_resident_bytes//16):
                            raise UnprovenGC('Record carrier exceeds line bound')
                        if line:
                            self.value(self.decode(line))
                    pending = pending[offset:]
                    if len(pending) > min(self.bounds.max_batch_bytes,self.bounds.max_resident_bytes//16):
                        raise UnprovenGC('Record carrier exceeds line bound')
                if pending:
                    self.value(self.decode(pending))
            elif media in _STRUCTURED:
                maximum = max(self.bounds.max_control_bytes, self.bounds.max_state_bytes,
                              self.bounds.max_batch_bytes)
                maximum = min(maximum, self.bounds.max_resident_bytes // 32)
                if ref.size_bytes > maximum:
                    raise UnprovenGC('Structured carrier exceeds decode bound')
                raw = b''.join(store.read(ref, max_bytes=max(1, maximum)))
                self.check(len(raw))
                decoded = self.decode(raw)
                if media == 'application/json':
                    schemas = {'demiflow.worker-frontier.v3', 'demiflow.runtime_wire.v3'}
                    if not (type(decoded) is dict and (decoded.get('schema') in schemas
                            or set(decoded) == {'ref','count','bytes'})):
                        raise UnprovenGC('Unknown generic JSON artifact carrier')
                self.value(decoded)
            else:
                raise UnprovenGC('Unknown artifact carrier: ' + media)
        return {(ref.store_id, ref.digest) for ref in self.refs.values()}


def collect_published_orphans(action, *, bounds, max_objects=64):
    """Called only after confirmed physical cleanup and journal thread exit.

    The original store writer leases independently enforce this condition.
    There is no public 'force' flag and no active-cut call site.
    """
    journal, fence = action.context.journal, action.context.fence
    deadline = _maintenance_deadline(action.context, bounds)
    if deadline is None:
        return _insufficient_time()
    return _collect(journal, fence, bounds=bounds, max_objects=max_objects, deadline=deadline)


def _collect(journal, fence, *, bounds, max_objects=64, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + min(5, bounds.timeout_ms / 1000)
    result = {'removed': 0, 'bytes': 0, 'blocked': None}
    if type(max_objects) is not int or not 1 <= max_objects <= 64:
        raise ValueError('GC object page exceeds budget')
    try:
        _check_time(deadline)
        with _exclusive(journal, fence):
            for item in journal.dataflow_gc_intents():
                result['bytes'] += _reconcile(journal, fence, item, deadline=deadline)
                result['removed'] += 1
            if journal.pending_dataflow_gc():
                raise UnprovenGC('Pending reconciliation exceeds page budget')
            # Unknown publication/staging ownership can still refer to settled
            # objects. Reconcile via the existing owner protocol first.
            if journal.pending_dataflow_staging(fence, limit=1):
                raise UnprovenGC('Unresolved staging owner prevents complete root proof')
            with journal._connection() as db:
                legacy = db.execute("SELECT 1 FROM runtime_reservations r LEFT JOIN runtime_dataflow_settlements s ON s.reservation_id=r.reservation_id LEFT JOIN runtime_dataflow_staging t ON t.reservation_id=r.reservation_id WHERE r.name='artifact_staging_bytes' AND s.reservation_id IS NULL AND t.reservation_id IS NULL LIMIT 1").fetchone()
            if legacy:
                raise UnprovenGC('Legacy unowned reservation prevents complete root proof')
            protected = _Trace(journal, bounds, deadline=deadline).walk()
            after = ('', '')
            while result['removed'] < max_objects:
                _check_time(deadline)
                with journal._connection() as db:
                    rows = db.execute('SELECT store_id,digest,bytes,charge_id FROM runtime_dataflow_objects '
                        'WHERE (store_id,digest)>(?,?) ORDER BY store_id,digest LIMIT 64', after).fetchall()
                if not rows:
                    break
                for store_id,digest,size,charge_id in rows:
                    _check_time(deadline)
                    after = (store_id,digest)
                    if after in protected or charge_id is None:
                        continue
                    from ..recovery_contract import identifier
                    identifier(store_id)
                    if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
                        raise UnprovenGC('GC object registry has an invalid digest')
                    path = journal.directory / '.dataflow-store' / store_id / 'objects' / digest
                    _no_symlink(path)
                    st = path.lstat()
                    item = dict(store_id=store_id,digest=digest,bytes=size,charge_id=charge_id,
                                device=st.st_dev,inode=st.st_ino,status='prepared')
                    _verify_file(path, item, deadline=deadline)
                    item['gc_id'] = journal.prepare_dataflow_gc(fence,store_id=store_id,
                        digest=digest,size=size,charge_id=charge_id,inode=(st.st_dev,st.st_ino))
                    _fault('after_prepare')
                    result['bytes'] += _reconcile(journal,fence,item,deadline=deadline)
                    result['removed'] += 1
                    if result['removed'] >= max_objects:
                        break
    except Exception as exc:
        result['blocked'] = str(exc)
    return result


def _insufficient_time():
    return {'removed':0,'bytes':0,'blocked':'Insufficient original run time for optional collection'}


def _maintenance_deadline(run, bounds):
    # Use exactly the two original check_stop clocks. Leave the caller at least
    # one second for its mandatory finalization/check_stop; do not extend either
    # deadline or force optional maintenance into a nearly exhausted run.
    now = time.monotonic()
    remaining = min(run.deadline - time.time(),
                    run.limits['timeout_s'] - (now - run.started))
    allowance = min(5, bounds.timeout_ms / 1000)
    return None if remaining < allowance + 1 else now + allowance


def defer_run_gc(run, bounds):
    """One conservative budget per run, never a growing action/worker list."""
    from dataclasses import fields, replace
    previous = getattr(run, '_published_gc_pending', None)
    run._published_gc_pending = (bounds if previous is None else replace(previous,
        **{field.name: min(getattr(previous,field.name),getattr(bounds,field.name))
           for field in fields(bounds)}))


def finish_run_gc(run):
    """Only called after RunContext's original native journal.close returned."""
    bounds = getattr(run, '_published_gc_pending', None)
    run._published_gc_pending = None
    native = getattr(run, '_native_prompt_journal', None)
    thread = getattr(native, '_batch_thread', None)
    if thread is not None and thread.is_alive():
        run._published_gc_result = {'removed':0,'bytes':0,'blocked':'Original owner exit remains unconfirmed'}
        return
    lease = getattr(native, '_dataflow_object_lease', None)
    if lease is not None:
        try:
            lease.close()
        except Exception as exc:
            run._published_gc_result = {'removed':0,'bytes':0,'blocked':str(exc)}
            return
        native._dataflow_object_lease = None
    if getattr(run,'_published_gc_guard_failed',False):
        run._published_gc_result = {'removed':0,'bytes':0,'blocked':'Original owner exit remains unconfirmed'}
        return
    if bounds is None:
        return
    try:
        deadline = _maintenance_deadline(run,bounds)
        run._published_gc_result = (_insufficient_time() if deadline is None else
            _collect(run.journal,run.fence,bounds=bounds,deadline=deadline))
    except Exception as exc:
        # Cleanup failures never overwrite a business/cancellation exception.
        run._published_gc_result = {'removed':0,'bytes':0,'blocked':str(exc)}
