"""Ordinary same-account processes behind the existing analysis controller.

No cgroup substitute, OS project boundary, second execution ledger or replay.
The local lease is only bounded job ownership/recovery metadata.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
import fcntl
import hashlib
import logging
import os
from pathlib import Path
import resource
import signal
import stat
import subprocess
import time
import threading
import uuid

from demiflow.workbench.process import process_identity
from demiflow.execution.process_observation import _pidfd
from demiflow.execution.process_observation import _signal_fd
from demiflow.execution.process_observation import unit_stopped
from demiflow.execution.process_observation import require_pidfds
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import atomic_write_text
from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import local_read_retirement
from demiflow.workbench.analysis import local_startup
from demiflow.workbench.analysis import legacy_wav_recovery
from demiflow.workbench.analysis.cleanup import remove_tree
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.execution_backend import capabilities
from demiflow.workbench.analysis.execution_backend import cleanup_receipt
from demiflow.workbench.analysis.job_context import FORMAT as CONTEXT_FORMAT
from demiflow.workbench.analysis.job_context import bounded_json
from demiflow.workbench.analysis.oci import job_id

LEASE = 'demiflow_analysis_local_lease_v1'


def file_sha(path, maximum=128*1024**2):
    digest=hashlib.sha256();total=0
    fd=os.open(no_symlinks(Path(path)),os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as stream:
        for chunk in iter(lambda:stream.read(1024**2),b''):
            total+=len(chunk)
            if total>maximum:c.reject('Release file exceeds hashing bound',code='resource_limit')
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class LocalSite:
    state: Path
    python: Path
    code_root: Path
    code_manifest: Path
    code_digest: str
    python_digest: str
    environment_digest: str
    environment_manifest: Path
    workspace: Path
    original_fsize: tuple
    limits: dict
    media_tool: dict | None = None
    _verified: dict = field(default_factory=dict,compare=False,repr=False)

    def check(self):
        try:return self._check()
        except ForgeError:raise
        except (OSError,KeyError,TypeError,ValueError):
            c.reject('Fixed runtime/dependency metadata is unavailable or changed',code='conflict')

    def _check(self):
        parents=set()
        def verify(path,digest,maximum=128*1024**2):
            path=Path(path)
            if path.parent not in parents:
                no_symlinks(path.parent);parents.add(path.parent)
            info=path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size>maximum:return False
            token=(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns,digest)
            if self._verified.get(str(path))==token:return True
            if file_sha(path,maximum)!=digest:return False
            self._verified[str(path)]=token
            return True
        for path in (self.state,self.python,self.code_root,self.code_manifest,self.environment_manifest,self.workspace):
            if not isinstance(path,Path) or not path.is_absolute() or '..' in path.parts:
                c.reject('Local site requires fixed physical absolute paths')
            no_symlinks(path)
        for value in (self.code_digest,self.python_digest,self.environment_digest):c.sha(value)
        if not verify(self.environment_manifest,self.environment_digest,1024**2):
            c.reject('Selected dependency manifest drifted',code='conflict')
        environment=c.decode(bounded_json(self.environment_manifest,1024**2),maximum=1024**2,nodes=32768)
        c.fields(environment,{'format','files'},label='fixed dependency metadata')
        if environment['format']!='demiflow_analysis_environment_manifest_v1' or not 1<=len(environment['files'])<=128:
            c.reject('Bounded dependency fingerprints required')
        for name,digest in environment['files'].items():
            if not Path(name).is_relative_to(self.python.parent.parent) or not verify(name,digest,1024**2):
                c.reject('Installed dependency metadata drifted',code='conflict')
        if not verify(self.python,self.python_digest) or not verify(self.code_manifest,self.code_digest,1024**2):
            c.reject('Selected interpreter/release drifted',code='conflict')
        release=c.decode(bounded_json(self.code_manifest,1024**2),maximum=1024**2,nodes=32768)
        c.fields(release,{'format','files'},label='fixed local platform release')
        if release['format']!='demiflow_analysis_source_release_v1' or not 1<=len(release['files'])<=2048:
            c.reject('Explicit bounded release manifest required')
        total=0
        for name,row in release['files'].items():
            path=Path(name)
            if path.is_absolute() or '..' in path.parts or str(path)!=name:c.reject('Invalid release file')
            total+=row['bytes']
            if total>128*1024**2 or not verify(self.code_root/path,row['sha256'],16*1024**2):
                c.reject('Selected platform source drifted',code='conflict')
        if len(self.original_fsize)!=2 or any(type(value) is not int for value in self.original_fsize):
            c.reject('Original account file limits must be captured before Database guard')
        expected={'cell_seconds','kernel_seconds','native_seconds','media_seconds','scratch_bytes','sample_rss_bytes'}
        if set(self.limits)!=expected or any(type(value) not in {int,float} or not 0<value<=2**40 for value in self.limits.values()):
            c.reject('Explicit bounded local execution policy required')
        if self.media_tool is not None:
            c.fields(self.media_tool,{'path','sha256','version'},label='fixed local probe tool')
            c.sha(self.media_tool['sha256']);c.text(self.media_tool['version'],'probe version',64)
            if not Path(self.media_tool['path']).is_absolute() or not verify(self.media_tool['path'],self.media_tool['sha256'],64*1024**2):
                c.reject('Fixed local probe tool drifted',code='conflict')
        return self

    def origin(self,workspace=None):
        workspace=self.workspace if workspace is None else Path(workspace)
        if not workspace.is_absolute() or '..' in workspace.parts or not workspace.is_dir():
            c.reject('Registered project workspace is unavailable',code='conflict')
        no_symlinks(workspace)
        return dict(format='demiflow_analysis_local_origin_v1',mode='trusted_local',
            code_digest=self.code_digest,python_digest=self.python_digest,python=str(self.python),
            environment_digest=self.environment_digest,uid=os.geteuid(),gid=os.getegid(),
            cwd=str(workspace),original_fsize=list(self.original_fsize),limits=dict(self.limits),
            media_tool=self.media_tool,capabilities=capabilities('trusted_local'))


def default_limits():
    return dict(cell_seconds=120,kernel_seconds=3600,native_seconds=60,media_seconds=5,
        scratch_bytes=256*1024**2,sample_rss_bytes=2*1024**3)


def _identity(value):
    c.fields(value,{'pid','boot','start'},label='original local process')
    c.integer(value['pid'],'original PID',2**31-1,1)
    c.text(value['boot'],'boot',64);c.text(value['start'],'start',32)


class TrustedLocalBackend:
    mode='trusted_local'

    def __init__(self,site):
        if not isinstance(site,LocalSite):c.reject('Explicit local site is required')
        require_pidfds()
        self.site=site.check();self.root=private_directory(site.state/'runtime')
        self.lock_fd=os.open(no_symlinks(self.root/'controller.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        self.leases={};self.processes={};self.pipes={};self.stop_fences=set();self.closed=False
        self.owner_controls={};self.worker_identities={}
        self.control_locks=tuple(threading.RLock() for _ in range(32))
        self.retiring_reads=set();self.pending_retirement=None
        try:
            fcntl.flock(self.lock_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            local_read_retirement.recover(self)
            with os.scandir(self.root) as scan:
                for count,item in enumerate(scan):
                    if count>=66:c.reject('Local control history exceeds bounded maintenance limit',code='resource_limit')
                    if item.name in {'controller.lock','priority-owner.json','media-read-owner.json','local-retired.json',
                            local_read_retirement.NEXT,local_read_retirement.STAGE,legacy_wav_recovery.NAME}:continue
                    job_id(item.name)
                    if item.name in self.retiring_reads:continue
                    value=c.decode(bounded_json(self.root/item.name/'lease.json'))
                    self._validate(value,item.name);self.leases[item.name]=value
            for kind,name in (('generation','priority-owner.json'),('read','media-read-owner.json')):
                path=self.root/name
                if path.exists():
                    from demiflow.workbench.analysis.priority_control import validate as generation_validate
                    from demiflow.workbench.analysis.read_control import validate as read_validate
                    record=(generation_validate if kind=='generation' else read_validate)(c.decode(bounded_json(path)))
                    self.owner_controls[kind]=dict(digest=c.digest(record),record=record,stop=True)
        except BaseException:
            os.close(self.lock_fd);raise

    def describe(self):return capabilities(self.mode)

    def owner(self,operation,digest,*,record=None):
        return self._owner_control('generation',operation,digest,record)

    def read_owner(self,operation,digest,*,record=None):
        return self._owner_control('read',operation,digest,record)

    def _owner_control(self,kind,operation,digest,record):
        c.sha(digest)
        actions={'bind-owner':'bind','owner-status':'status','clear-owner':'clear','stop-owner':'stop'} if kind=='generation' else {
            'bind-read-owner':'bind','read-owner-status':'status','clear-read-owner':'clear','stop-read-owner':'stop'}
        if operation not in actions:c.reject('Unsupported local owner action')
        action=actions[operation];current=self.owner_controls.get(kind)
        if action=='bind':
            if kind=='generation':
                from demiflow.workbench.analysis.priority_control import validate
            else:
                from demiflow.workbench.analysis.read_control import validate
            validate(record)
            if c.digest(record)!=digest or record['controller']!=process_identity(os.getpid()):
                c.reject('Local owner belongs to another controller',code='conflict')
            if current is not None and current['digest']!=digest:c.reject('Original local owner is occupied',code='busy')
            if current is None:self.owner_controls[kind]=dict(digest=digest,record=dict(record),stop=False)
            current=self.owner_controls[kind]
        elif current is None or current['digest']!=digest:
            if action=='clear':return dict(owner_digest=digest,bound=False,stop_requested=False)
            c.reject('Original local owner is unavailable',code='conflict')
        if kind=='read' and time.monotonic()>=current['record']['deadline']:current['stop']=True
        if action=='stop':current['stop']=True
        if current['stop']:
            for identifier,value in list(self.leases.items()):
                if value['owner_digest']==digest and value['state']!='cleanup_complete':self.stop(identifier,reason='owner_stopping')
        if action=='clear':
            if any(v['owner_digest']==digest and (v['state']!='cleanup_complete'
                    or kind=='read' and v['owner_kind'] is not None) for v in self.leases.values()):
                c.reject('Local owner cleanup remains unresolved',code='busy')
            del self.owner_controls[kind]
            return dict(owner_digest=digest,bound=False,stop_requested=False)
        return dict(owner_digest=digest,bound=True,stop_requested=current['stop'])

    @staticmethod
    def _validate(value,identifier):
        c.fields(value,{'format','job_id','role','state','project','execution_id','controller','web_parent',
            'owner_digest','origin_digest','context_sha256','unit_identity','init_identity','reason','owner_kind','broker_inodes'},label='local lease')
        if value['format']!=LEASE or value['job_id']!=identifier:c.reject('Local lease differs; no adoption',code='conflict')
        job_id(identifier);c.one_of(value['role'],{'kernel','native','media'},'local role')
        c.one_of(value['state'],{'created','started','cleanup_pending','cleanup_complete'},'local lease state')
        for name in ('owner_digest','origin_digest','context_sha256'):c.sha(value[name])
        for name in ('controller','web_parent'):_identity(value[name])
        if value['init_identity'] is not None:_identity(value['init_identity'])
        if type(value['broker_inodes']) is not dict or len(value['broker_inodes'])>8:c.reject('Local bridge ownership exceeds bound')
        for name,identity in value['broker_inodes'].items():
            if name not in {'kernel.sock','data.sock','output.sock','result.sock','arrow.sock','output-read.sock','case-read.sock','media-probe.sock'}:
                c.reject('Local bridge name differs')
            c.fields(identity,{'device','inode'},label='original local bridge identity')
            c.integer(identity['device'],'bridge device',2**63-1)
            c.integer(identity['inode'],'bridge inode',2**63-1,1)
        return value

    def register_bridge(self,identifier,path):
        value=dict(self.leases[job_id(identifier)]);path=Path(path)
        if path.parent!=self.root/identifier/'broker' or path.name not in {'kernel.sock','data.sock','output.sock',
                'result.sock','arrow.sock','output-read.sock','case-read.sock','media-probe.sock'}:
            c.reject('Bridge is outside its fixed job')
        info=path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid!=os.geteuid():c.reject('Original owned bridge required')
        identity=dict(device=info.st_dev,inode=info.st_ino)
        old=value['broker_inodes'].get(path.name)
        if old is not None and old!=identity:c.reject('Bridge cannot be replaced in the original job',code='conflict')
        value['broker_inodes']={**value['broker_inodes'],path.name:identity};self._save(value)
        return dict(registration='lease_owned',backend_mode=self.mode)

    def _save(self,value):
        self._validate(value,value['job_id'])
        atomic_write_json(self.root/value['job_id']/'lease.json',value)
        self.leases[value['job_id']]=dict(value)

    def create(self,*,project,execution_id,role,config,web_parent,owner_digest,authorize,origin_digest,read_owner=None,workspace=None):
        from demiflow.workbench.projects.registry import identity as project_identity
        from demiflow.workbench.analysis.runtime import Runtime
        project_identity(project);c.identity(execution_id);c.sha(owner_digest);c.sha(origin_digest)
        origin=self.site.origin(workspace)
        if origin_digest!=c.digest(origin):c.reject('Generation environment differs from selected backend',code='conflict')
        Runtime._config(role,config);_identity(web_parent)
        if self.closed or not callable(authorize) or authorize() is not True:c.reject('Current job permission is absent',code='operation_rejected')
        if process_identity(web_parent['pid'])!=web_parent:c.reject('Original Web lifetime ended',code='conflict')
        if len(self.leases)>=32:c.reject('Local control history requires bounded maintenance',code='resource_limit')
        if any(v['role']==role and (v['state']!='cleanup_complete' or v['owner_kind'] is not None)
                for v in self.leases.values()):c.reject('Original role is still occupied',code='busy')
        purpose=('media-probe' if config.get('operation')=='probe-media' else 'media-read') if role=='media' else 'case-read' if config.get('operation')=='case-page' else None
        if purpose:
            from demiflow.workbench.analysis.read_control import validate
            from demiflow.workbench.analysis.read_control import seconds
            validate(read_owner)
            if (read_owner['kind']!=purpose or read_owner['project']!=project or read_owner['read_request_id']!=execution_id
                    or read_owner['controller']!=process_identity(os.getpid()) or read_owner['web_parent']!=web_parent
                    or c.digest(read_owner)!=owner_digest or not time.monotonic()<read_owner['deadline']<=time.monotonic()+seconds(read_owner)):
                c.reject('Read owner differs or expired',code='conflict')
        elif read_owner is not None:c.reject('Generation job cannot borrow a read owner')
        if purpose=='media-read':
            if config.get('operation')=='artifact-media':
                from demiflow.workbench.analysis.media_artifact_worker import owner_artifact
                fixed_project=config['artifact']['project'];fixed_artifact=owner_artifact(config)
            else:fixed_project=config['witness']['provenance']['project'];fixed_artifact=config['witness']['artifact']
            if fixed_project!=project or read_owner['artifact']!=fixed_artifact:c.reject('Media read owner binding differs',code='conflict')
        elif purpose=='media-probe':
            from demiflow.workbench.analysis.media_artifact_worker import probe_binding
            if config['reservation']['seed']['project']!=project or read_owner['probe']!=probe_binding(config):
                c.reject('Pending probe binding differs',code='conflict')
        elif purpose=='case-read' and (config['witness']['project']!=project or read_owner['case']!=dict(
                object=config['witness']['artifact'],source_digest=config['witness']['source']['digest'],config_digest=c.digest(config))):
            c.reject('Case read owner binding differs',code='conflict')
        control=self.owner_controls.get('read' if purpose else 'generation')
        if control is None or control['digest']!=owner_digest or control['stop']:
            c.reject('Original local owner is absent or stopping',code='operation_rejected')
        if control['record']['project']!=project or control['record']['web_parent']!=web_parent:
            c.reject('Local job owner belongs to another project/Web lifetime',code='conflict')
        if role=='kernel' and (control['record']['session_id']!=config['session_id'] or control['record']['generation']!=config['generation']):
            c.reject('Kernel configuration differs from original owner',code='conflict')
        with os.scandir(self.root) as scan:
            for count,item in enumerate(scan):
                if count>=66:c.reject('Local runtime entry bound exceeded',code='resource_limit')
                if item.name in {'controller.lock','priority-owner.json','media-read-owner.json','local-retired.json',
                        local_read_retirement.NEXT,legacy_wav_recovery.NAME}:continue
                if item.name==local_read_retirement.STAGE:
                    if self.pending_retirement is None:
                        c.reject('Unknown local retirement stage',code='conflict')
                    local_read_retirement.observation(self,self.pending_retirement['lease']['job_id'])
                    continue
                if item.name not in self.leases:c.reject('Unindexed local control needs reconciliation',code='conflict')
        self.site.check()
        identifier='j'+uuid.uuid4().hex;owned=self.root/identifier
        # The descriptor number is filled only in start(), before child release.
        paths={'/broker':str(owned/'broker'),'/work':str(owned/'work'),'/scratch':str(owned/'scratch'),'/job':str(owned/'job'),
            '/sources':str(self.site.state/'objects'),'/objects':str(self.site.state/'objects'),
            '/pending':str(self.site.state/'.staging'/config['reservation']['publish_id']/'payload') if purpose=='media-probe' else str(owned/'job'),
            '/input':str(self.site.state/'transfer/ingress'/config['input_id']) if 'input_id' in config else str(owned/'job'),
            '/var/lib/demiflow-analysis/native-slots':str(self.site.state/'native-slots')}
        deadline=read_owner['deadline'] if purpose else time.monotonic()+self.site.limits[role+'_seconds']
        context=dict(format=CONTEXT_FORMAT,mode=self.mode,job_id=identifier,role=role,config=config,paths=paths,
            cwd=origin['cwd'],python=str(self.site.python),code_root=str(self.site.code_root),controller=process_identity(os.getpid()),
            web_parent=web_parent,origin_digest=origin_digest,owner_digest=owner_digest,deadline=deadline,
            original_fsize=list(self.site.original_fsize),limits=dict(self.site.limits),unit_fd=-1)
        if self.site.media_tool is not None:context['media_tool']=self.site.media_tool
        # Reserve space for the descriptor number filled at start. Validate
        # bytes/nodes BEFORE any job directory or lease can be allocated.
        encoded_context=c.encoded(context,maximum=65536-256)
        encoded_media=c.encoded(config,maximum=56*1024) if role=='media' else None
        owned.mkdir(mode=0o700)
        for name in ('broker','work','scratch','job'):private_directory(owned/name)
        private_directory(self.site.state/'native-slots')
        unit=os.open(owned/'startup.lock',os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:info=os.fstat(unit)
        finally:os.close(unit)
        atomic_write_text(owned/'job/context.json',encoded_context.decode('utf-8'))
        if encoded_media is not None:atomic_write_text(owned/'job/output-request.json',encoded_media.decode('utf-8'))
        value=dict(format=LEASE,job_id=identifier,role=role,state='created',project=project,execution_id=execution_id,
            controller=context['controller'],web_parent=web_parent,owner_digest=owner_digest,origin_digest=origin_digest,
            context_sha256=file_sha(owned/'job/context.json'),unit_identity=dict(device=info.st_dev,inode=info.st_ino),
            init_identity=None,reason=None,owner_kind=purpose,broker_inodes={})
        self._save(value)
        return dict(job_id=identifier,state='created',execution_mode=self.mode,origin_digest=origin_digest)

    def _control_lock(self,identifier):
        # Fixed storage, independent of retired history. A job always maps to
        # the same stripe; readiness waits never hold this lock.
        return self.control_locks[int(job_id(identifier)[1:],16)%len(self.control_locks)]

    def start(self,identifier,*,authorize):
        identifier=job_id(identifier)
        with self._control_lock(identifier):
            value=dict(self.leases[identifier])
            if self.closed or value['state']!='created' or identifier in self.stop_fences:
                c.reject('Original local job cannot restart',code='conflict')
            if not callable(authorize) or authorize() is not True:
                c.reject('Job permission was revoked',code='operation_rejected')
            control=self.owner_controls.get('read' if value['owner_kind'] else 'generation')
            if control is None or control['digest']!=value['owner_digest'] or control['stop']:
                c.reject('Original local owner is absent or stopping',code='operation_rejected')
            if value['owner_kind'] and time.monotonic()>=control['record']['deadline']:
                self.stop(identifier,reason='read_owner_expired')
                c.reject('Original read owner expired',code='operation_rejected')
            if process_identity(value['web_parent']['pid'])!=value['web_parent']:
                c.reject('Original Web lifetime ended',code='operation_rejected')
        owned=self.root/identifier;context=c.decode(bounded_json(owned/'job/context.json'))
        if file_sha(owned/'job/context.json')!=value['context_sha256']:
            c.reject('Original local context changed',code='conflict')
        self.site.check()
        reader=writer=unit=None;popen_attempted=False;context_rewritten=False
        try:
            with self._control_lock(identifier):
                value=dict(self.leases[identifier])
                if self.closed or identifier in self.stop_fences or value['state']!='created':
                    c.reject('Original local startup was cancelled',code='operation_rejected')
                reader,writer=os.pipe2(os.O_CLOEXEC)
                unit=os.open(owned/'startup.lock',os.O_RDWR|os.O_NOFOLLOW);fcntl.flock(unit,fcntl.LOCK_SH)
                context['unit_fd']=unit
                atomic_write_text(owned/'job/context.json',c.encoded(context).decode('utf-8'))
                context_rewritten=True
                value['context_sha256']=file_sha(owned/'job/context.json');self._save(value)
                local_startup.write(owned,value,'held')
                entry=self.site.code_root/'demiflow/workbench/analysis/trusted_entry.py'
                argv=[str(self.site.python),'-I','-B',str(entry),'--code-root',str(self.site.code_root),'--operation','wrapper',
                    '--context',str(owned/'job/context.json'),'--context-sha256',value['context_sha256'],'--control-fd',str(reader)]
                env=dict(PATH=str(self.site.python.parent)+':/usr/bin:/bin',PYTHONDONTWRITEBYTECODE='1',
                    OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',
                    RAYON_NUM_THREADS='1',TOKIO_WORKER_THREADS='1',LANCE_CPU_THREADS='1')
                popen_attempted=True
                process=subprocess.Popen(argv,cwd=owned,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,close_fds=True,pass_fds=(reader,unit),start_new_session=True)
                self.processes[identifier]=process;self.pipes[identifier]=writer
                writer=None  # Ownership transferred; never close its stale number locally.
                value['init_identity']=process_identity(process.pid)
                if value['init_identity'] is None:c.reject('Local wrapper exited before identity capture',code='operation_failed')
                self._save(value)
            ready_deadline=min(context['deadline'],time.monotonic()+10)
            while True:
                status=self._status(identifier)
                if status is not None:
                    if status['state']=='running' and status['worker'] is None and status['reason']=='wrapper_ready':break
                    c.reject('Original local wrapper did not become ready',code='operation_failed')
                if process.poll() is not None:c.reject('Original local wrapper failed before ready',code='operation_failed')
                if time.monotonic()>=ready_deadline:c.reject('Original local wrapper readiness deadline',code='resource_limit')
                time.sleep(.01)
            with self._control_lock(identifier):
                current=dict(self.leases[identifier])
                control=self.owner_controls.get('read' if current['owner_kind'] else 'generation')
                if (self.closed or identifier in self.stop_fences or current['state']!='created'
                        or control is None or control['stop'] or identifier not in self.pipes):
                    c.reject('Original local startup was cancelled',code='operation_rejected')
                if time.monotonic()>=context['deadline'] or authorize() is not True or process_identity(value['web_parent']['pid'])!=value['web_parent']:
                    c.reject('Original local startup permission or deadline ended',code='operation_rejected')
                local_startup.write(owned,current,'released')
                os.write(self.pipes[identifier],b'S')
                current['state']='started';self._save(current)
        except BaseException:
            with self._control_lock(identifier):
                self.stop_fences.add(identifier)
                try:
                    if context_rewritten and not popen_attempted and not (owned/local_startup.NAME).exists():
                        context['unit_fd']=-1
                        atomic_write_text(owned/'job/context.json',c.encoded(context).decode('utf-8'))
                        current=dict(self.leases[identifier])
                        current['context_sha256']=file_sha(owned/'job/context.json');self._save(current)
                finally:
                    pipe=self.pipes.pop(identifier,None)
                    if pipe is not None:os.close(pipe)
                    if writer is not None:os.close(writer);writer=None
            raise
        finally:
            if reader is not None:os.close(reader)
            if unit is not None:os.close(unit)
        return dict(job_id=identifier,state='started')

    def _status(self,identifier):
        path=self.root/identifier/'status.json'
        # A status file can be absent before first publication or observed
        # missing across a hot replacement. Absence is not process death or
        # cleanup proof; observe still checks the original wrapper/unit.
        try:raw=bounded_json(path)
        except FileNotFoundError:return None
        result=c.decode(raw)
        c.fields(result,{'job_id','context_sha256','wrapper','worker','state','reason','peak_sampled_rss_bytes','descendants_exited'},
            {'sampled_processes','sampled_scratch_bytes','returncode','diagnostic'},label='original local process status')
        c.one_of(result['state'],{'running','exited','unknown'},'local process status')
        if type(result['descendants_exited']) is not bool:c.reject('Local process status needs explicit cleanup fact')
        if result['reason'] is not None:c.text(result['reason'],'local stop reason',256)
        lease=self.leases[identifier]
        if (result.get('job_id')!=identifier or result.get('context_sha256')!=lease['context_sha256']
                or result.get('wrapper')!=lease['init_identity']):c.reject('Local status belongs to another original job',code='conflict')
        if result['worker'] is not None:
            _identity(result['worker'])
            previous=self.worker_identities.get(identifier)
            if previous is not None and previous!=result['worker']:
                c.reject('Original local worker identity changed',code='conflict')
            self.worker_identities[identifier]=dict(result['worker'])
        return result

    def observe(self,identifier):
        value=self.leases[job_id(identifier)];process=self.processes.get(identifier)
        if process is not None:process.poll()
        try:
            retiring=local_read_retirement.observation(self,identifier)
            if retiring is not None:return retiring
            wrapper=value['init_identity'];live=wrapper is not None and process_identity(wrapper['pid'])==wrapper
            stopped=unit_stopped(self.root/identifier/'startup.lock',value['unit_identity'])
            startup=local_startup.load(self.root/identifier,value)
            if not live and stopped and startup is not None and startup['phase']=='held':
                return dict(state='exited',reason='startup_not_released',status=None,startup=startup)
            if not live and stopped:
                legacy=legacy_wav_recovery.load(self,value)
                if legacy is not None:return dict(state='exited',reason='legacy_closed_wav_unit_stopped',status=None,legacy_wav=legacy)
            status=self._status(identifier)
            if live:return dict(state='alive',reason=None,status=status)
            if wrapper is None and stopped:
                raw=bounded_json(self.root/identifier/'job/context.json')
                if hashlib.sha256(raw).hexdigest()==value['context_sha256']:
                    context=c.decode(raw)
                    # The fixed context changes this sentinel BEFORE any
                    # Popen attempt. Stop can change the lease state without
                    # turning a never-started job into an unknown process.
                    if type(context) is dict and context.get('unit_fd')==-1:
                        return dict(state='exited',reason='not_started',status=None)
            if status and status.get('state')=='exited' and status.get('descendants_exited') is True and stopped:
                return dict(state='exited',reason=status.get('reason'),status=status)
            if getattr(self,'_observation_warnings',0)<16:
                self._observation_warnings=getattr(self,'_observation_warnings',0)+1
                logging.getLogger(__name__).warning('Local original job unconfirmed job=%s wrapper=%s stopped=%s status=%s',
                    identifier,wrapper,stopped,status)
            return dict(state='unknown',reason='original_local_cleanup_unconfirmed',status=status)
        except (OSError,ForgeError) as error:
            if getattr(self,'_observation_warnings',0)<16:
                self._observation_warnings=getattr(self,'_observation_warnings',0)+1
                logging.getLogger(__name__).warning('Local original observation unavailable job=%s detail=%s',
                    identifier,str(error)[:512],exc_info=True)
            return dict(state='unknown',reason='local_observation_unavailable',status=None)

    def accept_peer(self,identifier,peer,*,init_only=False):
        value=self.leases[job_id(identifier)]
        if value['state']!='started' or identifier in self.stop_fences or peer.uid!=os.geteuid() or not peer.live():return False
        status=self._status(identifier)
        worker=self.worker_identities.get(identifier) if status is None else status.get('worker')
        if worker is None:return False
        wrapper=value['init_identity']
        live_wrapper=wrapper is not None and process_identity(wrapper['pid'])==wrapper
        if worker==peer.identity and live_wrapper:return True
        if init_only:return False
        if not live_wrapper:return False
        from demiflow.execution.process_observation import descendants
        try:
            item=descendants(wrapper['pid']).get(peer.pid)
            return item is not None and item['start']==peer.identity['start'] and peer.live()
        except (OSError,RuntimeError):return False

    def stop(self,identifier,*,reason):
        with self._control_lock(identifier):
            return self._stop(identifier,reason=reason)

    def _stop(self,identifier,*,reason):
        value=dict(self.leases[job_id(identifier)]);self.stop_fences.add(identifier)
        if value['state']=='cleanup_complete':return dict(job_id=identifier,accepted=True,cleanup_complete=True,cleanup_proven=False)
        value.update(state='cleanup_pending',reason=str(reason)[:256]);self._save(value)
        pipe=self.pipes.pop(identifier,None)
        if pipe is not None:
            try:os.write(pipe,b'T')
            except (BrokenPipeError,OSError):pass
            finally:os.close(pipe)
        else:
            original=value['init_identity']
            # Repeated stop and helper rebuild must not send default SIGTERM
            # into Python before the original wrapper installs its handlers.
            # The first pipe stop/EOF and fixed startup deadline remain live.
            try:ready=self._status(identifier)
            except (OSError,ForgeError):ready=None
            if ready is not None and original is not None and process_identity(original['pid'])==original:
                try:
                    fd=_pidfd(original['pid'])
                    try:
                        if process_identity(original['pid'])==original:_signal_fd(fd,signal.SIGTERM)
                    finally:os.close(fd)
                except ProcessLookupError:pass
        return dict(job_id=identifier,accepted=True,cleanup_complete=False,cleanup_proven=False)

    def cleanup(self,identifier,*,close_bridges,clean_private_io=None):
        with self._control_lock(identifier):
            return self._cleanup(identifier,close_bridges=close_bridges,clean_private_io=clean_private_io)

    def _cleanup(self,identifier,*,close_bridges,clean_private_io=None):
        identifier=job_id(identifier)
        pending=local_read_retirement.cleaned(self,identifier)
        if pending is not None:
            if not callable(close_bridges) or close_bridges(identifier) is not True:
                c.reject('Original retiring read channels remain open',code='busy')
            local_read_retirement.cleaned(self,identifier)
            return dict(job_id=identifier,cleanup_complete=True,cleanup_proven=False,cleanup_receipt=pending)
        value=dict(self.leases[identifier]);observed=self.observe(identifier)
        exited=observed['state']=='exited'
        if exited:
            # Normal short reads do not call stop(). Release the original
            # controller write end once the process witness has exited.
            pipe=self.pipes.pop(identifier,None)
            if pipe is not None:os.close(pipe)
        channels=exited and callable(close_bridges) and close_bridges(identifier) is True
        temporary=False
        if channels:
            owned=self.root/identifier
            witness=lambda:self.observe(identifier)['state']=='exited'
            # Listener owns socket unlinking by exact inode; never blindly unlink.
            broker=owned/'broker'
            for name,identity in value['broker_inodes'].items():
                path=broker/name
                try:info=path.lstat()
                except FileNotFoundError:continue
                if not stat.S_ISSOCK(info.st_mode) or dict(device=info.st_dev,inode=info.st_ino)!=identity:
                    c.reject('Original bridge inode changed',code='conflict')
                if witness() is not True:c.reject('Original job cleanup changed before bridge unlink',code='busy')
                path.unlink()
            if broker.exists() and next(broker.iterdir(),None) is not None:
                c.reject('Local broker has unresolved entries',code='conflict')
            for name in ('scratch','work','broker'):
                remove_tree(owned/name,maximum_entries=8192,maximum_bytes=int(self.site.limits['scratch_bytes'])+16*1024**2,witness=witness)
            temporary=True
        state='complete' if exited and channels and temporary else 'unknown' if observed['state']=='unknown' else 'pending'
        receipt=cleanup_receipt(job_id=identifier,mode=self.mode,state=state,processes=exited,
            descendants=exited,channels=bool(channels),temporary=temporary,reason=observed['reason'])
        atomic_write_json(self.root/identifier/'cleanup.json',receipt)
        if state=='complete':value['state']='cleanup_complete';self._save(value)
        return dict(job_id=identifier,cleanup_complete=state=='complete',cleanup_proven=False,cleanup_receipt=receipt)

    def retire_read(self,identifier):
        return local_read_retirement.retire(self,identifier)

    def close(self):
        if self.closed:return
        self.closed=True
        for identifier in list(self.pipes):
            with self._control_lock(identifier):
                pipe=self.pipes.pop(identifier,None)
                if pipe is not None:os.close(pipe)
        os.close(self.lock_fd)
