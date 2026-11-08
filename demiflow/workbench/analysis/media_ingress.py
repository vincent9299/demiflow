"""Opaque pending media in the existing reserved object staging directory.

The writer closes and freezes before an isolated probe. Its verified metadata
is added exactly once, after probe retirement, using the original Ingress seal
and atomic rename. No codec is imported by this controller-side module.
"""
import hashlib
import os
from pathlib import Path
import stat

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.object_files import Ingress
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.object_files import _fsync
from demiflow.workbench.analysis.transfers import input_id

MIB=1024*1024


class MediaIngress(Ingress):
    def __init__(self,database,state_root,outbox_root,*,export_id,reservation,receipt,web_uid,helper_gid):
        from demiflow.workbench.analysis.media_artifact_worker import configuration
        self.config=dict(version=1,operation='probe-media',reservation=reservation,
            content={key:receipt[key] for key in ('sha256','bytes','mime')},source=receipt['source'])
        configuration(self.config)
        self.database=database;self.root=private_directory(state_root)
        self.staging_root=private_directory(self.root/'.staging');self.objects_root=private_directory(self.root/'objects')
        self.publish_id=reservation['publish_id'];self.stage=no_symlinks(self.staging_root/self.publish_id)
        self.stage.mkdir(mode=0o700,exist_ok=False);self.data=self.stage/'payload';self.data.mkdir(mode=0o700)
        self.current=None;self.finished=set();self.sealed=False;self.frozen=False;self.manifest=None;self.digest=None
        self.source_fd=self.output_fd=None;self.copied=0;self.hash=hashlib.sha256()
        try:
            path=no_symlinks(Path(outbox_root)/input_id(export_id)/'data.bin')
            self.source_fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            self.before=os.fstat(self.source_fd)
            if (not stat.S_ISREG(self.before.st_mode) or self.before.st_uid!=web_uid or self.before.st_gid!=helper_gid
                    or self.before.st_nlink!=1 or stat.S_IMODE(self.before.st_mode)!=0o440 or self.before.st_size!=receipt['bytes']):
                c.reject('Sealed media export identity/size differs',code='conflict')
            self.output_fd=os.open(self.data/'media.bin',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        except BaseException:self.close();raise

    def step(self):
        if self.frozen or self.source_fd is None or self.output_fd is None:c.reject('Original media copy is closed',code='conflict')
        size=self.config['content']['bytes'];raw=os.read(self.source_fd,min(MIB,size-self.copied+1))
        if raw:
            if self.copied+len(raw)>size:c.reject('Media export exceeds its admitted commitment',code='conflict')
            with self.database.guard.object_write():
                remaining=memoryview(raw)
                while remaining:
                    count=os.write(self.output_fd,remaining)
                    if count<=0:c.reject('Media ingress write made no progress',code='operation_failed')
                    remaining=remaining[count:]
            self.copied+=len(raw);self.hash.update(raw);return False
        after=os.fstat(self.source_fd)
        facts=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        if facts(after)!=facts(self.before) or self.copied!=size or self.hash.hexdigest()!=self.config['content']['sha256']:
            c.reject('Media export changed or its content commitment differs',code='conflict')
        os.fsync(self.output_fd);os.fchmod(self.output_fd,0o400)
        os.close(self.output_fd);self.output_fd=None;os.close(self.source_fd);self.source_fd=None
        _fsync(self.data);self.data.chmod(0o500);_fsync(self.stage);self.stage.chmod(0o500);_fsync(self.staging_root)
        self.frozen=True;return True

    def prepare_after_probe(self,reply):
        if not self.frozen or self.source_fd is not None or self.output_fd is not None or self.manifest is not None:
            c.reject('Media pending ingress is not the closed original copy',code='conflict')
        value=dict(format='media',**self.config['content'],complete=True,source=self.config['source'],
            descriptor=reply['descriptor'],chunk_sha256=reply['chunk_sha256'])
        self.manifest=candidate_manifest(self.config['reservation']['seed'],value,{'media.bin':{key:value[key] for key in ('bytes','sha256')}})
        self.digest=c.digest(self.manifest);self.finished={'media.bin'}
        # The probe's full process/mount retirement is a precondition enforced
        # by the caller before making this private stage writable for sealing.
        self.stage.chmod(0o700);self.data.chmod(0o700)
        return self.seal()

    def close(self):
        for key in ('source_fd','output_fd'):
            descriptor=getattr(self,key,None)
            if descriptor is not None:os.close(descriptor);setattr(self,key,None)
        super().close()
