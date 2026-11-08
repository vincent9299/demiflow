"""Web-owned bounded source-export outbox in the admitted analysis volume.

Administrator maps exactly this directory into Web and helper namespaces. It
is owned by Web, shared only with the dedicated helper for reads/owned cleanup, never mounted into a
kernel. At most3x64 MiB data files coexist with one64 MiB helper ingress; input
metadata/FS allocation overhead is charged to the separate64 MiB control area.
An inherited real unit lock fences exporter/parent lifetime before deletion.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import stat
import time
import threading

from demiflow.workbench.process import process_identity
from demiflow.execution.process_observation import unit_stopped
from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import no_symlinks
from demiflow.workbench.worker import operation
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.cleanup import remove_tree
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.sources import locator
from demiflow.workbench.analysis.transfers import input_id
from demiflow.workbench.analysis.filesystem_identity import identity as fs_identity
from demiflow.workbench.analysis.filesystem_identity import parse as native_identity


class Outbox:
    def __init__(self, root):
        self.root = no_symlinks(Path(root))
        info = self.root.stat()
        if (not self.root.is_absolute() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o2770):
            c.reject("Web transfer mount must be its administrator-bound owned setgid2770 directory")
        if not 512 <= os.statvfs(self.root).f_frsize <= 65536:
            c.reject("Transfer volume allocation unit exceeds the candidate profile", code="operation_rejected")
        self.identity = dict(device=info.st_dev, inode=info.st_ino, gid=info.st_gid)
        self._lock=threading.RLock();self._retired=set()

    def _check(self):
        info = no_symlinks(self.root).stat()
        if dict(device=info.st_dev,inode=info.st_ino,gid=info.st_gid) != self.identity or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o2770:
            c.reject("Original Web transfer mount identity/permissions changed", code="conflict")

    def _record(self, identifier):
        self._check();input_id(identifier)
        root = no_symlinks(self.root/identifier)
        info = root.stat()
        if info.st_uid != os.geteuid() or info.st_gid != self.identity["gid"] or stat.S_IMODE(info.st_mode) != 0o2770:
            c.reject("Transfer directory owner/group/mode differs", code="conflict")
        value = c.decode(_read(root/"manifest.json"))
        metadata=(root/"manifest.json").stat()
        if metadata.st_uid!=os.geteuid() or metadata.st_gid!=self.identity["gid"] or stat.S_IMODE(metadata.st_mode)!=0o640:
            c.reject("Source outbox manifest owner/group/mode differs",code="conflict")
        c.fields(value, {"format","id","project","request_digest","source_snapshot","state","result","unit_identity","web_parent","created"}, label="source export intent")
        if value["format"] not in {"demiflow_analysis_source_export_v1","demiflow_analysis_media_export_v1"} or value["id"] != identifier:
            c.reject("Source outbox intent identity differs", code="conflict")
        c.one_of(value["state"],{"pending","complete"},"source export state")
        c.sha(value["request_digest"]);c.sha(value["source_snapshot"])
        native_identity(value["unit_identity"])
        return value

    def _save(self, value):
        path = self.root/value["id"]/"manifest.json"
        atomic_write_json(path,value)
        path.chmod(0o640)

    def export(self,workspace,*,project,export_id,source_locator,source_snapshot,authorize):
        return self._export(workspace,project=project,export_id=export_id,source_locator=source_locator,
            source_snapshot=source_snapshot,authorize=authorize)

    def export_media(self,workspace,*,project,export_id,source_locator,source_snapshot,case_key,field,authorize):
        c.text(case_key,'fixed media Case',20);c.text(field,'fixed media field',256)
        if not case_key.isascii() or not case_key.isdecimal() or str(int(case_key))!=case_key or int(case_key)>=2**64:c.reject('Media Case must be canonical uint64')
        return self._export(workspace,project=project,export_id=export_id,source_locator=source_locator,
            source_snapshot=source_snapshot,authorize=authorize,media=dict(case_key=case_key,field=field))

    def _export(self,workspace,**kwargs):
        # The existing reader lane already serializes native exports. Hold the
        # same bounded local lock through completion so cleanup cannot race a
        # callback that has not created its durable intent yet.
        with self._lock:
            if kwargs['export_id'] in self._retired:c.reject('Original export was retired; no automatic replay',code='conflict')
            return self._export_owned(workspace,**kwargs)

    def _export_owned(self, workspace, *, project, export_id, source_locator, source_snapshot, authorize,media=None):
        self._check();project_identity(project);input_id(export_id);locator(source_locator);c.sha(source_snapshot)
        if not callable(authorize) or authorize() is not True:
            c.reject("Source export is not currently authorized",code="operation_rejected")
        request_digest = c.digest(dict(project=project,locator=source_locator,source_snapshot=source_snapshot,**({"media":media} if media is not None else {})))
        directory = no_symlinks(self.root/export_id)
        if directory.exists():
            old = self._record(export_id)
            if old["project"] != project or old["request_digest"] != request_digest:
                c.reject("Export ID already belongs to a different fixed source",code="conflict")
            if old["state"] != "complete":
                c.reject("Original source export is incomplete/unknown; no automatic replay",code="conflict")
            if authorize() is not True:
                c.reject("Source export permission was revoked",code="operation_rejected")
            return old["result"]
        with os.scandir(self.root) as entries:
            for count,entry in enumerate(entries):
                if count >= 2:
                    c.reject("Source outbox is full; reconcile its original owned exports",code="resource_limit")
                input_id(entry.name)
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    c.reject("Unexpected source outbox entry",code="conflict")
        directory.mkdir(mode=0o2770)
        # Set permissions on this exclusively created directory explicitly;
        # never change the Web process's global umask.
        directory.chmod(0o2770)
        info=directory.stat()
        if info.st_gid != self.identity["gid"] or stat.S_IMODE(info.st_mode) != 0o2770:
            c.reject("Source export directory did not inherit its admitted shared group/mode")
        unit = os.open(directory/"unit.lock",os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o660)
        fd = None
        try:
            os.fchmod(unit,0o660);fcntl.flock(unit,fcntl.LOCK_SH)
            identity=os.fstat(unit)
            value=dict(format="demiflow_analysis_source_export_v1" if media is None else "demiflow_analysis_media_export_v1",id=export_id,project=project,request_digest=request_digest,
                source_snapshot=source_snapshot,state="pending",result=None,unit_identity=fs_identity(identity),
                web_parent=process_identity(os.getpid()),created=time.time())
            self._save(value)  # durable intent before any exporter child starts
            fd=os.open(directory/"data.bin",os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600)
            result=operation('analysis_export' if media is None else 'analysis_media_export',workspace,
                source_locator=source_locator,source_snapshot=source_snapshot,output_fd=fd,_pass_fds=(fd,unit),**(media or {}))
            if media is None:
                c.fields(result,{"source_snapshot","schema_sha256","stream_sha256","encoded_bytes","chunks","rows"},label="trusted source export receipt")
                c.sha(result["schema_sha256"]);c.sha(result["stream_sha256"])
                c.integer(result["encoded_bytes"],"export bytes",64*1024*1024,1);c.integer(result["chunks"],"export chunks",4096,1)
                c.integer(result["rows"],"export rows",100000)
                expected_size=result['encoded_bytes'];actual_source=result['source_snapshot']
            else:
                from demiflow.workbench.analysis.registered_media import source_witness
                from demiflow.workbench.analysis.media_artifact_worker import content
                c.fields(result,{'source','mime','bytes','sha256'},label='trusted declared media export receipt')
                source_witness(result['source']);content({key:result[key] for key in ('mime','bytes','sha256')})
                if (any(result['source'][key]!=media[key] for key in ('case_key','field'))
                        or result['source']['declared_sha256']!=result['sha256']):c.reject('Media export receipt belongs to another resource',code='conflict')
                expected_size=result['bytes'];actual_source=result['source']['source_snapshot']
            if actual_source != source_snapshot or os.fstat(fd).st_size != expected_size:
                c.reject("Source exporter output receipt differs",code="conflict")
            if authorize() is not True:
                c.reject("Source export permission was revoked before publication",code="operation_rejected")
            os.fsync(fd);os.fchmod(fd,0o440)
            value["state"],value["result"]="complete",result
            self._save(value)
            return result
        finally:
            if fd is not None:os.close(fd)
            # Do not explicitly LOCK_UN: the inherited SAME open description
            # must remain locked if a child survived a lost/failed parent call.
            os.close(unit)

    def release(self,*,project,export_id,source_snapshot):
        project_identity(project);input_id(export_id);c.sha(source_snapshot)
        with self._lock:
            if export_id not in self._retired and len(self._retired)>=2048:c.reject('Original export retirement budget is full',code='resource_limit')
            self._retired.add(export_id)
            return self._release_owned(project=project,export_id=export_id,source_snapshot=source_snapshot)

    def _release_owned(self, *, project, export_id, source_snapshot):
        self._check();project_identity(project);input_id(export_id);c.sha(source_snapshot)
        directory=no_symlinks(self.root/export_id)
        if not directory.exists():return dict(released=True)
        record=self._record(export_id)
        if record["project"] != project or record["source_snapshot"] != source_snapshot:
            c.reject("Cannot clean another source export identity",code="conflict")
        with os.scandir(directory) as entries:
            for count,item in enumerate(entries):
                if count>=3 or item.name not in {"manifest.json","unit.lock","data.bin"} or item.is_symlink() or not item.is_file(follow_symlinks=False):
                    c.reject("Source export has unknown files/links; cleanup retained",code="conflict")
        witness=lambda:unit_stopped(directory/"unit.lock",native_identity(record["unit_identity"]))
        # This operation returns no source bytes and remains available for an
        # already owned export after project/grant revocation. The live helper
        # peer/principal and exact original export identity still authorize it.
        remove_tree(directory,maximum_entries=3,maximum_bytes=64*1024*1024+128*1024,witness=witness)
        return dict(released=not directory.exists())


def release_from_helper(root, *, project, export_id, source_snapshot, allowed_web_uids, helper_gid, witness):
    """Recovery cleanup after Web death, without a new grant or identity adoption.

    This fixed outbox is shared with the helper only for controlled temporary
    IO. Validate the Web owner's sealed intent, exact project/source/nonce and
    real inherited unit before unlinking its three known files. No chmod or
    user-directory traversal, source data read, new job or DB action occurs.
    """
    root=no_symlinks(Path(root));project_identity(project);input_id(export_id);c.sha(source_snapshot)
    info=root.stat()
    if info.st_uid not in allowed_web_uids or info.st_gid!=helper_gid or stat.S_IMODE(info.st_mode)!=0o2770:
        c.reject("Admitted shared Web outbox owner/group/mode differs",code="conflict")
    directory=no_symlinks(root/export_id)
    try:owned=directory.stat()
    except FileNotFoundError:return True
    if (not stat.S_ISDIR(owned.st_mode) or owned.st_uid!=info.st_uid or owned.st_gid!=helper_gid
            or stat.S_IMODE(owned.st_mode)!=0o2770):
        c.reject("Source export cleanup directory identity differs",code="conflict")
    manifest=no_symlinks(directory/"manifest.json")
    meta=manifest.stat()
    if meta.st_uid!=info.st_uid or meta.st_gid!=helper_gid or stat.S_IMODE(meta.st_mode)!=0o640:
        c.reject("Source export cleanup manifest identity differs",code="conflict")
    record=c.decode(_read(manifest))
    c.fields(record,{"format","id","project","request_digest","source_snapshot","state","result","unit_identity","web_parent","created"},label="owned source cleanup intent")
    if (record["format"] not in {"demiflow_analysis_source_export_v1","demiflow_analysis_media_export_v1"} or record["id"]!=export_id
            or record["project"]!=project or record["source_snapshot"]!=source_snapshot):
        c.reject("Cannot reconcile another export's owned data",code="conflict")
    native_identity(record["unit_identity"])
    def stopped():
        return callable(witness) and witness() is True and unit_stopped(directory/"unit.lock",native_identity(record["unit_identity"]))
    if not stopped():c.reject("Owned source exporter/copy still has an active IO unit",code="busy")
    names=[];total=0
    with os.scandir(directory) as entries:
        for count,item in enumerate(entries):
            value=item.stat(follow_symlinks=False)
            if (count>=3 or item.name not in {"manifest.json","unit.lock","data.bin"} or not stat.S_ISREG(value.st_mode)
                    or value.st_nlink!=1 or value.st_uid!=info.st_uid or value.st_gid!=helper_gid):
                c.reject("Unknown source export files/ownership; cleanup retained",code="conflict")
            total+=max(value.st_size,value.st_blocks*512)
            if total>64*1024*1024+128*1024:c.reject("Source cleanup byte bound exceeded",code="resource_limit")
            names.append(item.name)
    if not stopped():c.reject("Source exporter became active before cleanup",code="busy")
    # Never chmod the foreign Web owner. Group write applies only to this
    # admitted temporary directory; file data remains sealed read-only.
    for name in names:(directory/name).unlink()
    fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
    directory.rmdir()
    fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
    return not directory.exists()
