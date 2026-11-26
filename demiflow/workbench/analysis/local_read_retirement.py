"""Crash-consistent retirement of one proved-clean trusted read control.

The existing read owner and role remain charged until the original directory
and its durable pending intent are retired. This is bounded backend control
metadata, not an execution ledger or a recovery loop. No worker is restarted.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import stat

from demiflow.forge.process import process_identity
from demiflow.resources.process_observation import unit_stopped
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.cleanup import _sync
from demiflow.workbench.analysis.cleanup import remove_tree
from demiflow.workbench.analysis.execution_facts import receipt as validate_receipt
from demiflow.workbench.analysis.job_context import bounded_json
from demiflow.workbench.analysis.oci import job_id

NAME = 'local-retired.json'
NEXT = '.local-retired.next'
STAGE = '.local-read-retiring'
FORMAT = 'demiflow_analysis_local_read_retirements_v1'
MAXIMUM = 56 * 1024
KINDS = {'media-read', 'media-probe', 'case-read'}


def _inode_token(value):
    # Keep the common JSON signed-integer contract. Filesystems may return
    # uint64 ino_t values; only this private stat identity uses decimal text
    # for the upper half, without truncation or floating-point conversion.
    c.integer(value,'original directory inode',2**64-1,1)
    return value if value<2**63 else str(value)


def _directory(path, expected=None):
    path = no_symlinks(Path(path))
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    identity = dict(device=info.st_dev, inode=_inode_token(info.st_ino))
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or expected is not None and identity != expected):
        c.reject('Original local retirement directory changed', code='conflict')
    return identity


def _identity(value):
    c.fields(value, {'device', 'inode'}, label='original retiring directory')
    c.integer(value['device'], 'directory device', 2**63-1)
    inode=value['inode']
    if type(inode) is str:
        if re.fullmatch(r'[1-9][0-9]{18,19}',inode) is None:
            c.reject('Directory inode text is not canonical uint64',code='conflict')
        c.integer(int(inode),'directory inode',2**64-1,2**63)
    else:c.integer(inode,'directory inode',2**63-1,1)


def _read_lease(backend, value):
    identifier = job_id(value.get('job_id') if type(value) is dict else None)
    backend._validate(value, identifier)
    if (value['state'] != 'cleanup_complete' or value['owner_kind'] not in KINDS
            or (value['owner_kind'] == 'case-read') != (value['role'] == 'native')
            or value['role'] not in {'native', 'media'}):
        c.reject('Only completed original trusted read controls may retire', code='conflict')


def _recent(value):
    c.fields(value, {'job_id', 'owner_digest', 'origin_digest'},
        {'lease_digest', 'cleanup_digest', 'observation_digest', 'directory_identity'},
        label='bounded local retirement receipt')
    job_id(value['job_id'])
    for key in ('owner_digest', 'origin_digest'):
        c.sha(value[key])
    extra = {'lease_digest', 'cleanup_digest', 'observation_digest', 'directory_identity'}
    if set(value) & extra:
        if not extra <= set(value):
            c.reject('Local retirement receipt is incomplete', code='conflict')
        for key in extra - {'directory_identity'}:
            c.sha(value[key])
        _identity(value['directory_identity'])
    return value


def _pending(backend, value):
    c.fields(value, {'phase', 'lease', 'lease_digest', 'owner', 'directory_identity',
        'cleanup_receipt', 'cleanup_digest', 'observation', 'observation_digest'},
        label='pending original local retirement')
    c.one_of(value['phase'], {'prepared', 'removing'}, 'local retirement phase')
    lease = value['lease']
    _read_lease(backend, lease)
    _identity(value['directory_identity'])
    for field, body in (('lease_digest', lease), ('cleanup_digest', value['cleanup_receipt']),
                        ('observation_digest', value['observation'])):
        c.sha(value[field])
        if value[field] != c.digest(body):
            c.reject('Original retirement evidence changed', code='conflict')
    owner = read_control.validate(value['owner'])
    if (c.digest(owner) != lease['owner_digest'] or owner['kind'] != lease['owner_kind']
            or owner['read_request_id'] != lease['execution_id'] or owner['project'] != lease['project']
            or owner['controller'] != lease['controller'] or owner['web_parent'] != lease['web_parent']):
        c.reject('Pending local retirement owner differs', code='conflict')
    proof = validate_receipt(value['cleanup_receipt'], mode='trusted_local', job_id=lease['job_id'])
    if proof['state'] != 'complete':
        c.reject('Retirement requires completed scoped cleanup', code='conflict')
    observed = value['observation']
    c.fields(observed, {'state', 'reason', 'status'}, {'startup','legacy_wav'}, label='original retirement process witness')
    if observed['state'] != 'exited':
        c.reject('Retirement lacks the original process witness', code='conflict')
    status = observed['status']
    if 'legacy_wav' in observed and (status is not None or observed['reason']!='legacy_closed_wav_unit_stopped' or 'startup' in observed):
        c.reject('Unexpected legacy WAV proof',code='conflict')
    if 'startup' in observed and (status is not None or observed['reason']!='startup_not_released'):
        c.reject('Unexpected local startup proof',code='conflict')
    if status is None:
        if observed['reason']=='legacy_closed_wav_unit_stopped':
            from demiflow.workbench.analysis.legacy_wav_recovery import verify
            verify(observed.get('legacy_wav'),backend.root,lease,controller_fd=backend.lock_fd)
        elif observed['reason']=='startup_not_released':
            from demiflow.workbench.analysis.local_startup import validate
            startup=validate(observed.get('startup'),lease)
            if startup['phase']!='held':c.reject('Original local startup was released',code='conflict')
        elif lease['init_identity'] is not None or observed['reason'] != 'not_started':
            c.reject('Retirement lacks its original empty-start witness', code='conflict')
    else:
        c.fields(status, {'job_id', 'context_sha256', 'wrapper', 'worker', 'state', 'reason',
            'peak_sampled_rss_bytes', 'descendants_exited'},
            {'sampled_processes', 'sampled_scratch_bytes', 'returncode', 'diagnostic'}, label='retiring original status')
        if (status['job_id'] != lease['job_id'] or status['context_sha256'] != lease['context_sha256']
                or status['wrapper'] != lease['init_identity'] or status['state'] != 'exited'
                or status['descendants_exited'] is not True):
            c.reject('Retirement original process identity differs', code='conflict')
        if status['worker'] is not None:
            from demiflow.workbench.analysis.trusted_local import _identity as process_fields
            process_fields(status['worker'])
    return value


def _load(backend):
    path = no_symlinks(backend.root/NAME)
    try:
        raw = bounded_json(path, MAXIMUM)
    except FileNotFoundError:
        return dict(format=FORMAT, pending=None, recent=[])
    value = c.decode(raw, maximum=MAXIMUM)
    if type(value) is list:
        # Keep old compact receipts. They are not proof for a new deletion or
        # an idempotent success; a pre-upgrade partial deletion stays unknown.
        rows = c.sequence(value, 'legacy local retirement receipts', 32)
        unique = {}
        for row in rows:
            c.fields(row, {'job_id', 'owner_digest', 'origin_digest'}, label='legacy local retirement')
            _recent(row)
            if row['job_id'] in unique and unique[row['job_id']] != row:
                c.reject('Legacy original retirement differs', code='conflict')
            unique[row['job_id']] = row
        return dict(format=FORMAT, pending=None, recent=list(unique.values()))
    c.fields(value, {'format', 'pending', 'recent'}, label='local read retirement control')
    if value['format'] != FORMAT:
        c.reject('Unknown local read retirement format', code='conflict')
    seen = set()
    for row in c.sequence(value['recent'], 'terminal local read controls', 32):
        _recent(row)
        if row['job_id'] in seen:
            c.reject('Duplicate local retirement identity', code='conflict')
        seen.add(row['job_id'])
    if value['pending'] is not None:
        pending = _pending(backend, value['pending'])
        if pending['lease']['job_id'] in seen:
            c.reject('Original read is both retiring and terminal', code='conflict')
    return value


def _discard_next(root):
    path = no_symlinks(root/NEXT)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_nlink != 1 or info.st_size > MAXIMUM):
        c.reject('Original local retirement write stage changed', code='conflict')
    path.unlink()
    _sync(root)


def _save(backend, value):
    # The fixed write stage can be reconciled after process death. An orphaned
    # random atomic-write temporary must not make the runtime unopenable.
    raw = c.encoded(value, maximum=MAXIMUM)
    _discard_next(backend.root)
    path = backend.root/NEXT
    descriptor = os.open(no_symlinks(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(path, backend.root/NAME)
    _sync(backend.root)


def _paths(backend, pending):
    identifier = pending['lease']['job_id']
    expected = pending['directory_identity']
    original = _directory(backend.root/identifier, expected)
    staged = _directory(backend.root/STAGE, expected)
    if original is not None and staged is not None:
        c.reject('Original local retirement has conflicting directories', code='conflict')
    if pending['phase'] == 'prepared' and original is None and staged is None:
        c.reject('Prepared original retirement directory is missing', code='conflict')
    if pending['phase'] == 'removing' and original is not None:
        c.reject('Original control reappeared after retirement staging', code='conflict')
    return original, staged


def _stopped(backend, pending):
    _pending(backend, pending)
    lease = pending['lease']
    if read_control.load(backend.root) != pending['owner']:
        c.reject('Original local retirement lost its durable owner', code='conflict')
    identities = [lease['init_identity']]
    if pending['observation']['status'] is not None:
        identities.append(pending['observation']['status']['worker'])
    for original in identities:
        if original is not None and process_identity(original['pid']) == original:
            c.reject('Original retiring local process remains live', code='busy')
    original, staged = _paths(backend, pending)
    if pending['phase'] == 'prepared':
        control = backend.root/lease['job_id'] if original is not None else backend.root/STAGE
        if not unit_stopped(control/'startup.lock', lease['unit_identity']):
            c.reject('Original local startup unit remains active', code='busy')
        stored = c.decode(bounded_json(control/'lease.json'))
        if stored != lease:
            c.reject('Original local lease changed before retirement', code='conflict')
        if 'startup' in pending['observation']:
            from demiflow.workbench.analysis.local_startup import load
            if load(control,lease)!=pending['observation']['startup']:
                c.reject('Original local startup proof changed',code='conflict')
        if 'legacy_wav' in pending['observation']:
            from demiflow.workbench.analysis.legacy_wav_recovery import load
            if load(backend,lease,owned=control)!=pending['observation']['legacy_wav']:
                c.reject('Original legacy WAV proof changed',code='conflict')
    return original, staged


def recover(backend):
    """Restore bounded charge/evidence only. The common caller advances it."""
    audit = _load(backend)
    pending = audit['pending']
    if pending is None:
        if _directory(backend.root/STAGE) is not None:
            c.reject('Unknown local retirement stage; no adoption', code='conflict')
        for row in audit['recent']:
            if 'lease_digest' in row and _directory(backend.root/row['job_id']) is not None:
                c.reject('Terminal original read control reappeared', code='conflict')
    else:
        _stopped(backend, pending)
        lease = pending['lease']
        known = backend.leases.get(lease['job_id'])
        if known is not None and known != lease:
            c.reject('Pending retirement differs from the original loaded lease', code='conflict')
        backend.leases[lease['job_id']] = dict(lease)
        backend.retiring_reads.add(lease['job_id'])
    backend.pending_retirement = pending
    _discard_next(backend.root)
    if (backend.root/NAME).exists():
        # Re-acknowledge a rename whose previous directory fsync may have
        # failed, including a terminal record observed on a rebuilt helper.
        _sync(backend.root)
    return audit


def observation(backend, identifier):
    pending = backend.pending_retirement
    if pending is None or pending['lease']['job_id'] != identifier:
        return None
    _stopped(backend, pending)
    return dict(pending['observation'])


def cleaned(backend, identifier):
    pending = backend.pending_retirement
    if pending is None or pending['lease']['job_id'] != identifier:
        return None
    _stopped(backend, pending)
    return dict(pending['cleanup_receipt'])


def _forget(backend, identifier):
    pipe = backend.pipes.pop(identifier, None)
    if pipe is not None:
        os.close(pipe)
    backend.leases.pop(identifier, None)
    backend.processes.pop(identifier, None)
    backend.worker_identities.pop(identifier, None)
    backend.stop_fences.discard(identifier)
    backend.retiring_reads.discard(identifier)
    backend.pending_retirement = None


def _answer(identifier, row, audit):
    return dict(job_id=identifier, retired=True, history_removed=False, backend_mode='trusted_local',
        owner_digest=row['owner_digest'], origin_digest=row['origin_digest'],
        lease_digest=row['lease_digest'], retirement_digest=c.digest(audit))


def retire(backend, identifier):
    identifier = job_id(identifier)
    audit = recover(backend)
    pending = audit['pending']
    if pending is not None and pending['lease']['job_id'] != identifier:
        c.reject('Another original local read retirement is pending', code='busy')
    original = backend.root/identifier
    if pending is None:
        previous = next((row for row in audit['recent'] if row['job_id'] == identifier and 'lease_digest' in row), None)
        if previous is not None:
            if _directory(original) is not None:
                c.reject('Terminal original control is still present', code='conflict')
            _forget(backend, identifier)
            return _answer(identifier, previous, audit)
        lease = backend.leases.get(identifier)
        if lease is None:
            c.reject('Original trusted read lease is unavailable', code='not_found')
        _read_lease(backend, lease)
        observed = backend.observe(identifier)
        if observed['state'] != 'exited':
            c.reject('Original local read retirement is unconfirmed', code='busy')
        identity = _directory(original)
        if identity is None:
            c.reject('Original local read control is missing without an intent', code='conflict')
        proof = c.decode(bounded_json(original/'cleanup.json'))
        pending = dict(phase='prepared', lease=dict(lease), lease_digest=c.digest(lease),
            owner=read_control.load(backend.root), directory_identity=identity,
            cleanup_receipt=proof, cleanup_digest=c.digest(proof),
            observation=observed, observation_digest=c.digest(observed))
        _stopped(backend, pending)
        backend.retiring_reads.add(identifier)
        audit = dict(audit, pending=pending, recent=[row for row in audit['recent'] if row['job_id'] != identifier])
        _save(backend, audit)  # No destructive action before durable intent.
        backend.pending_retirement = pending
    present, staged = _stopped(backend, pending)
    stage = backend.root/STAGE
    if present is not None:
        os.rename(original, stage)
        _sync(backend.root)
    if pending['phase'] == 'prepared':
        # _stopped required an original exact directory. Persist this phase
        # before the first unlink; absence alone is never an empty-set proof.
        pending = dict(pending, phase='removing')
        audit = dict(audit, pending=pending)
        _save(backend, audit)
        backend.pending_retirement = pending
    _, staged = _stopped(backend, pending)
    if staged is not None:
        def witness():
            _stopped(backend, pending)
            return _directory(stage, pending['directory_identity']) is not None
        remove_tree(stage, maximum_entries=16, maximum_bytes=256*1024, witness=witness)
    _sync(backend.root)
    _stopped(backend, pending)
    if _directory(original) is not None or _directory(stage) is not None:
        c.reject('Original local retirement directories remain', code='busy')
    row = dict(job_id=identifier, owner_digest=pending['lease']['owner_digest'],
        origin_digest=pending['lease']['origin_digest'], lease_digest=pending['lease_digest'],
        cleanup_digest=pending['cleanup_digest'], observation_digest=pending['observation_digest'],
        directory_identity=pending['directory_identity'])
    complete = dict(format=FORMAT, pending=None, recent=[*audit['recent'], row][-32:])
    _save(backend, complete)
    _forget(backend, identifier)
    return _answer(identifier, row, complete)
