"""Explicit bounded retirement of proved-clean read controls, never run history.

One durable pending identity bridges rename/unlink/fsync failure. The rolling32
terminal receipts contain only read control facts; committed artifacts and
Notebook/generation records are untouched. Recovery never replays a job.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat

from demiflow.workbench.process import process_identity
from demiflow.forge.io import atomic_write_text
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.cleanup import _sync
from demiflow.workbench.analysis.cleanup import remove_tree
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.oci import job_id

NAME='read-retirements.json'
STAGE='.read-retiring'
FORMAT='demiflow_analysis_read_retirements_v1'
KINDS={'media-read','case-read','media-probe'}


def read_lease(value):
    leases.validate(value)
    if value['format']!=leases.MEDIA_FORMAT or value.get('owner_kind') not in KINDS or value['state']!='cleanup_complete':
        c.reject('Only proved-clean independent read leases may retire',code='conflict')
    return value


def load(root):
    path=no_symlinks(Path(root)/NAME)
    if not path.exists():return dict(format=FORMAT,pending=None,recent=[],total=0)
    value=c.decode(_read(path),maximum=56*1024)
    c.fields(value,{'format','pending','recent','total'},label='bounded read retirement audit')
    if value['format']!=FORMAT:c.reject('Unknown read retirement audit')
    c.integer(value['total'],'retired read count',2**63-1)
    seen=set()
    for item in c.sequence(value['recent'],'terminal read receipts',32):
        c.fields(item,{'job_id','kind','role','project','execution_id','owner_digest','lease_digest'},label='terminal read receipt')
        job_id(item['job_id']);c.one_of(item['kind'],KINDS,'retired read kind')
        c.one_of(item['role'],{'media','native'},'retired read role')
        if (item['kind']=='case-read')!=(item['role']=='native'):c.reject('Read receipt kind/role differs')
        from demiflow.workbench.projects.registry import identity
        identity(item['project']);c.identity(item['execution_id']);c.sha(item['owner_digest']);c.sha(item['lease_digest'])
        if item['job_id'] in seen:c.reject('Duplicate retired read receipt')
        seen.add(item['job_id'])
    if value['total']<len(seen):c.reject('Read retirement audit count differs')
    pending=value['pending']
    if pending is not None:
        c.fields(pending,{'lease','directory_identity'},label='pending read retirement')
        read_lease(pending['lease'])
        c.fields(pending['directory_identity'],{'device','inode'},label='retiring control directory identity')
        c.integer(pending['directory_identity']['device'],'retiring device',2**63-1)
        c.integer(pending['directory_identity']['inode'],'retiring inode',2**63-1,1)
        if pending['lease']['job_id'] in seen:c.reject('Read retirement is both pending and terminal')
    return value


def save(root,value):
    atomic_write_text(Path(root)/NAME,c.encoded(value,maximum=56*1024).decode())


def directory(path,expected=None):
    path=no_symlinks(path)
    if not path.exists():return None
    info=path.stat()
    actual=dict(device=info.st_dev,inode=info.st_ino)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or expected is not None and actual!=expected:
        c.reject('Original retiring control directory changed',code='conflict')
    return actual


def recover(runtime):
    audit=load(runtime.root);pending=audit['pending'];stage=runtime.root/STAGE
    if not hasattr(runtime,'retiring_reads'):runtime.retiring_reads=set()
    if pending is None:
        if directory(stage) is not None:c.reject('Unknown staged read retirement; no adoption',code='conflict')
        return
    value=pending['lease'];identifier=value['job_id'];expected=pending['directory_identity']
    original=directory(runtime.root/identifier,expected);staged=directory(stage,expected)
    if original is not None and staged is not None:c.reject('Read retirement has conflicting control directories',code='conflict')
    known=runtime.leases.get(identifier)
    if known is not None and known!=value:c.reject('Pending retirement differs from original lease',code='conflict')
    runtime.leases[identifier]=dict(value)
    runtime.retiring_reads.add(identifier)


def charged(runtime,role):
    return any(runtime.leases.get(identifier,{}).get('role')==role for identifier in getattr(runtime,'retiring_reads',set()))


def _absent(runtime,value):
    runtime.delegation(value)
    if not runtime.groups.absent(value['job_id']):c.reject('Read retirement requires the absent original leaf',code='conflict')
    original=value['init_identity']
    if original is not None and process_identity(original['pid'])==original:
        c.reject('Read init is still live',code='busy')
    if value['job_id'] in runtime.pidfds:c.reject('Read cleanup retained an unresolved init pidfd',code='busy')
    controller=value['controller']
    if controller!=process_identity(os.getpid()) and process_identity(controller['pid'])==controller:
        c.reject('Original read controller remains live',code='conflict')
    return True


def retire(runtime,identifier):
    from demiflow.execution.process_observation import unit_stopped
    job_id(identifier);recover(runtime)
    audit=load(runtime.root);pending=audit['pending']
    if pending is not None and pending['lease']['job_id']!=identifier:
        c.reject('Another read retirement remains unresolved',code='busy')
    value=runtime.leases.get(identifier)
    if value is None:
        previous=next((item for item in audit['recent'] if item['job_id']==identifier),None)
        if previous is None:c.reject('Original read lease is unavailable',code='not_found')
        return dict(job_id=identifier,retired=True,cleanup_proven=True,owner_digest=previous['owner_digest'],audit_digest=c.digest(audit))
    read_lease(value);_absent(runtime,value)
    original=runtime.root/identifier;stage=runtime.root/STAGE
    if pending is None:
        # Keep the immutable read owner until retirement succeeds. Clearing its
        # watchdog binding first is harmless; unlinking it first is not.
        owner=read_control.load(runtime.root)
        if c.digest(owner)!=value['owner_digest'] or owner['kind']!=value['owner_kind']:
            c.reject('Read retirement lost its original durable owner',code='conflict')
        identity=directory(original)
        if identity is None or not unit_stopped(original/'startup.lock',value['unit_identity']):
            c.reject('Read startup/control identity is unavailable or active',code='busy')
        if runtime._listed(value):c.reject('Read runtime inventory is not empty',code='busy')
    # Existing clean registration removes the exact watch only after original
    # delegation and absent-leaf checks. True is the trusted WatchClient result.
    if runtime.register_watch(value) is not True:
        c.reject('Read watchdog unregister is unconfirmed',code='operation_failed')
    runtime.retiring_reads.add(identifier)
    if pending is None:
        pending=dict(lease=dict(value),directory_identity=identity)
        audit={**audit,'pending':pending}
        save(runtime.root,audit)  # a blocked/failed write keeps role and owner
    expected=pending['directory_identity']
    if directory(original,expected) is not None:
        if directory(stage) is not None:c.reject('Retirement stage already exists',code='conflict')
        os.rename(original,stage);_sync(runtime.root)
    if directory(stage,expected) is not None:
        def witness():
            return _absent(runtime,value) and directory(stage,expected) is not None
        remove_tree(stage,maximum_entries=64,maximum_bytes=2*1024*1024,witness=witness)
    _sync(runtime.root)
    receipt=dict(job_id=identifier,kind=value['owner_kind'],role=value['role'],project=value['project'],
        execution_id=value['execution_id'],owner_digest=value['owner_digest'],lease_digest=c.digest(value))
    total=c.integer(audit['total']+1,'retired read count',2**63-1)
    complete=dict(format=FORMAT,pending=None,recent=[*audit['recent'],receipt][-32:],total=total)
    save(runtime.root,complete)
    runtime.leases.pop(identifier,None);runtime.retiring_reads.discard(identifier)
    runtime.stop_fences.discard(identifier)
    return dict(job_id=identifier,retired=True,cleanup_proven=True,owner_digest=value['owner_digest'],audit_digest=c.digest(complete))
