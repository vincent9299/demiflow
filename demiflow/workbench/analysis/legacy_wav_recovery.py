"""Internal one-target maintenance witness for a closed Source33 WAV read.

No public request consumes an authorization path. The operator publishes one
review-bound witness while both original locks are exclusive. Consumption
rechecks immutable bindings and the original kernel unit; it never starts work
or creates a historical wrapper status. Other operations remain unknown.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import stat
import time
from datetime import datetime
from datetime import timezone

from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import no_symlinks
from demiflow.forge.io import write_exclusive
from demiflow.forge.io import fsync_directory
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis.job_context import bounded_json

NAME='legacy-wav-recovery.json'
FORMAT='demiflow_analysis_legacy_wav_unit_witness_v2'
SCOPE='reviewed_closed_wav_unit_domain'
AUTH='demiflow_analysis_legacy_wav_authorization_v2'
SOURCE33='1ddf5f2789981a2630973bc10d5eb4029d42192c82e3b574034a4f164b00b181'
CLOSURE=('trusted_local.py','trusted_entry.py','trusted_runner.py','job_context.py',
         'media_worker.py','output_worker.py','output_reader.py','media.py')
KEYS=('job_id','role','project','execution_id','controller','web_parent','owner_digest',
      'origin_digest','context_sha256','unit_identity','init_identity','owner_kind','broker_inodes')


def sha(path, maximum=1024**2):
    path=no_symlinks(Path(path));raw=bounded_json(path,maximum)
    return hashlib.sha256(raw).hexdigest()


def reference(value,maximum=1024**2):
    c.fields(value,{'path','sha256'},label='reviewed recovery reference')
    c.sha(value['sha256']);path=no_symlinks(Path(value['path']))
    if not path.is_absolute():c.reject('Fixed recovery reference required',code='conflict')
    info=path.stat()
    if info.st_uid!=os.geteuid() or info.st_mode&0o022 or sha(path,maximum)!=value['sha256']:
        c.reject('Recovery reference changed',code='conflict')
    return path


def lock_path(fd,path,identity=None):
    path=no_symlinks(Path(path));info=os.fstat(fd);current=path.lstat()
    actual=dict(device=info.st_dev,inode=info.st_ino)
    if (not stat.S_ISREG(info.st_mode) or not stat.S_ISREG(current.st_mode)
            or info.st_uid!=os.geteuid() or (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino)
            or identity is not None and identity!=actual):
        c.reject('Original recovery lock/path differs',code='conflict')
    return actual


@contextmanager
def exclusive(path,identity=None):
    fd=os.open(no_symlinks(Path(path)),os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        lock_path(fd,path,identity)
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:c.reject('Original recovery unit remains occupied',code='busy')
        lock_path(fd,path,identity)
        yield fd
        lock_path(fd,path,identity)
    finally:os.close(fd)


def known_process(identity):
    if identity is None:return None
    c.fields(identity,{'pid','start','boot'},label='known original lifetime')
    c.integer(identity['pid'],'known original PID',2**31-1,1)
    boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if identity['boot']!=boot:c.reject('Original recovery boot changed',code='conflict')
    try:
        with open('/proc/'+str(identity['pid'])+'/stat','rb') as stream:raw=stream.read(32769)
    except (FileNotFoundError,ProcessLookupError):
        return dict(original=identity,state='absent',observed=None)
    except PermissionError:c.reject('Known original process is unreadable',code='operation_rejected')
    if len(raw)>32768:c.reject('Known process byte limit exceeded',code='resource_limit')
    try:
        fields=raw.rsplit(b')',1)[1].split();start=fields[19].decode('ascii')
        if not start.isdecimal():raise ValueError('non-numeric start')
    except (IndexError,UnicodeError,ValueError):
        c.reject('Known original process evidence is malformed',code='operation_rejected')
    observed=dict(pid=identity['pid'],start=start,boot=boot)
    if fields[0]==b'Z':return dict(original=identity,state='zombie',observed=observed)
    if observed!=identity:return dict(original=identity,state='reused',observed=observed)
    c.reject('Original recovery lifetime remains active',code='busy')


def mount_for(path):
    raw=bounded_json(Path('/proc/'+str(os.getpid())+'/mountinfo'),1024**2).decode()
    selected=None;path=str(no_symlinks(Path(path)))
    for line in raw.splitlines():
        left,right=line.split(' - ',1);fields=left.split();point=fields[4]
        if (path==point or path.startswith(point.rstrip('/')+'/')) and (selected is None or len(point)>len(selected['mountpoint'])):
            selected=dict(id=fields[0],device=fields[2],root=fields[3],mountpoint=point,type=right.split()[0])
    if selected is None:c.reject('Original unit filesystem cannot be determined',code='conflict')
    return selected


def filesystem(auth,root):
    proof=c.decode(bounded_json(reference(auth['filesystem_test']),16384),maximum=16384)
    c.fields(proof,{'format','device','unit_identity','mount','boot','parent_identity','child_identity',
        'parent_and_child_live_exclusive_refused','parent_exited_child_live_exclusive_refused',
        'all_holders_exited_exclusive_succeeded','no_explicit_unlock_or_conversion',
        'test_script_sha256','private_unit','original_unit_content_or_job_changed'},label='same-filesystem unit attestation')
    c.sha(proof['test_script_sha256'])
    if (proof.get('format')!='demiflow_inherited_unit_filesystem_v1'
            or proof.get('device')!=auth['unit_identity']['device'] or root.stat().st_dev!=proof['device']
            or proof.get('boot')!=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            or proof.get('mount')!=mount_for(root) or proof['original_unit_content_or_job_changed'] is not False
            or any(proof.get(key) is not True for key in ('parent_and_child_live_exclusive_refused',
                'parent_exited_child_live_exclusive_refused','all_holders_exited_exclusive_succeeded',
                'no_explicit_unlock_or_conversion'))):
        c.reject('Same-filesystem inherited unit proof differs',code='conflict')
    return proof['mount']


def authorization(path,digest):
    path=reference(dict(path=str(path),sha256=digest))
    value=c.decode(bounded_json(path,32768),maximum=32768)
    c.fields(value,{'format','state','job_id','read_request_id','lease_sha256','context_sha256',
        'owner_sha256','origin_digest','unit_identity','artifact','source_root','source_manifest',
        'installation_receipt','review','maintenance_allowed','proof_scope','controller_lock_identity','filesystem_test',
        'original_lease_snapshot','cleanup_sha256'},label='one-target WAV authorization')
    if value['format']!=AUTH:c.reject('Unsupported recovery authorization',code='conflict')
    if value['proof_scope']!=SCOPE:c.reject('Unreviewed recovery proof scope',code='conflict')
    if value['maintenance_allowed'] is not True:
        c.reject('Final one-target maintenance review is required',code='operation_rejected')
    reference(value['review']);reference(value['installation_receipt']);reference(value['original_lease_snapshot'])
    if value['cleanup_sha256'] is not None:c.sha(value['cleanup_sha256'])
    manifest=reference(value['source_manifest'])
    if value['source_manifest']['sha256']!=SOURCE33:c.reject('Only reviewed Source33 recovery is supported',code='conflict')
    source=no_symlinks(Path(value['source_root']))
    files=c.decode(bounded_json(manifest,1024**2),maximum=1024**2,nodes=32768)['files']
    closure={}
    for name in CLOSURE:
        # This verifies the immutable Source33 snapshot, not the installed package.
        relative='demiflow/forge/analysis/'+name
        actual=sha(source/relative)
        if actual!=files[relative]['sha256']:c.reject('Original Source33 recovery closure changed',code='conflict')
        closure[relative]=actual
    return value,closure


def bindings(root,lease,auth,*,initial=False,owned=None):
    root=no_symlinks(Path(root))
    if str(root.parent)!=auth['state'] or lease['job_id']!=auth['job_id'] or lease['execution_id']!=auth['read_request_id']:
        c.reject('Recovery target differs',code='conflict')
    owned=root/lease['job_id'] if owned is None else owned;context=c.decode(bounded_json(owned/'job/context.json'))
    owner=read_control.load(root)
    if owner is None or sha(root/read_control.NAME)!=auth['owner_sha256'] or c.digest(owner)!=lease['owner_digest']:
        c.reject('Original recovery owner changed',code='conflict')
    if sha(owned/'job/context.json')!=auth['context_sha256'] or lease['context_sha256']!=auth['context_sha256']:
        c.reject('Original recovery context changed',code='conflict')
    if initial and sha(owned/'lease.json')!=auth['lease_sha256']:
        c.reject('Original recovery lease changed',code='conflict')
    original=c.decode(bounded_json(reference(auth['original_lease_snapshot'])))
    if (original.get('format')!='demiflow_analysis_local_lease_v1' or lease.get('format')!=original['format']
            or original.get('state')!='cleanup_pending'
            or any(original.get(key)!=lease.get(key) for key in KEYS)):
        c.reject('Original immutable recovery lease binding changed',code='conflict')
    if initial:
        if lease['state']!='cleanup_pending':c.reject('Original recovery lease is not pending',code='conflict')
        cleanup=owned/'cleanup.json'
        if (auth['cleanup_sha256'] is None and cleanup.exists()
                or auth['cleanup_sha256'] is not None and sha(cleanup)!=auth['cleanup_sha256']):
            c.reject('Quiescent original cleanup evidence changed',code='conflict')
        if auth['cleanup_sha256'] is not None:
            from demiflow.workbench.analysis.execution_facts import receipt as validate_receipt
            prior=validate_receipt(c.decode(bounded_json(cleanup)),mode='trusted_local',job_id=lease['job_id'])
            if prior['state']=='complete':c.reject('Legacy recovery cannot relabel a completed cleanup',code='conflict')
    config=context['config']
    if (lease['role']!='media' or lease['owner_kind']!='media-read' or context['role']!='media'
            or config.get('operation')!='output-media' or config.get('mime')!='audio/wav'
            or config.get('output_index')!=0 or config.get('range')!='bytes=0-0'
            or config['witness']['artifact']!=auth['artifact'] or owner['artifact']!=auth['artifact']
            or lease['origin_digest']!=auth['origin_digest'] or context['origin_digest']!=auth['origin_digest']
            or lease['unit_identity']!=auth['unit_identity'] or context['unit_fd']<3
            or context['job_id']!=lease['job_id'] or context['controller']!=lease['controller']
            or context['owner_digest']!=lease['owner_digest'] or context['web_parent']!=lease['web_parent']
            or context['mode']!='trusted_local'):
        c.reject('Recovery is outside the exact closed WAV operation',code='conflict')
    if lease['state'] not in {'cleanup_pending','cleanup_complete'} or (owned/'status.json').exists() or (owned/'startup.json').exists():
        c.reject('Legacy recovery cannot replace existing/new startup evidence',code='conflict')
    for identity in (lease['controller'],lease['web_parent'],lease['init_identity']):
        known_process(identity)
    receipt=c.decode(bounded_json(reference(auth['installation_receipt'])))
    if (receipt.get('format')!='demiflow_source33_installed_release_receipt_v1'
            or receipt['source_manifest']['sha256']!=SOURCE33
            or receipt['code_root']!=context['code_root'] or receipt['python']!=context['python']):
        c.reject('Original installed Source33 provenance differs',code='conflict')
    from demiflow.workbench.analysis.execution_backend import capabilities
    origin=dict(format='demiflow_analysis_local_origin_v1',mode='trusted_local',
        code_digest=receipt['installed_code_manifest']['sha256'],python_digest=receipt['python_sha256'],
        python=context['python'],environment_digest=receipt['environment_manifest']['sha256'],
        uid=owner['web_uid'],gid=owner['web_gid'],cwd=context['cwd'],
        original_fsize=context['original_fsize'],limits=context['limits'],
        media_tool=context.get('media_tool'),capabilities=capabilities('trusted_local'))
    if c.digest(origin)!=lease['origin_digest']:
        c.reject('Original installed generation origin differs',code='conflict')
    return context


def publish(root,auth_path,auth_digest,snapshot):
    """Operator-only preparation. Returns only after both locks/proof are durable."""
    root=no_symlinks(Path(root));auth,closure=authorization(auth_path,auth_digest)
    if (root/NAME).exists():c.reject('An original recovery witness already exists',code='conflict')
    with exclusive(root/'controller.lock',auth['controller_lock_identity']) as controller_fd:
        if (root/NAME).exists():c.reject('An original recovery witness already exists',code='conflict')
        lease=c.decode(bounded_json(root/auth['job_id']/'lease.json'))
        context=bindings(root,lease,auth,initial=True)
        # Preparation still checks the original physical installed closure.
        for relative,digest in closure.items():
            if sha(Path(context['code_root'])/relative)!=digest:
                c.reject('Original physical Source33 closure changed before maintenance',code='conflict')
        with exclusive(root/lease['job_id']/'startup.lock',lease['unit_identity']) as fd:
            mount=filesystem(auth,root)
            known=[known_process(lease[key]) for key in ('controller','web_parent','init_identity')]
            bindings(root,lease,auth,initial=True)
            lock_path(controller_fd,root/'controller.lock',auth['controller_lock_identity'])
            lock_path(fd,root/lease['job_id']/'startup.lock',lease['unit_identity'])
            domain=dict(scope=SCOPE,boot=lease['controller']['boot'],mount=mount,
                known_participants=known,controller_lock_identity=auth['controller_lock_identity'],
                unit_identity=lease['unit_identity'],controller_exclusive=True,unit_exclusive=True,
                filesystem_test=auth['filesystem_test'],global_process_enumeration=False,
                observed_at_monotonic=time.monotonic(),observed_at_utc=datetime.now(timezone.utc).isoformat())
            snapshot=no_symlinks(Path(snapshot))
            if not snapshot.is_absolute() or snapshot.is_relative_to(root.parent):
                c.reject('Independent original failure snapshot required',code='conflict')
            snapshot.mkdir(mode=0o700)
            records={}
            offered=[('lease.json',root/lease['job_id']/'lease.json'),
                              ('context.json',root/lease['job_id']/'job/context.json'),
                              ('owner.json',root/read_control.NAME),('authorization.json',Path(auth_path)),
                              ('original-unknown-lease.json',reference(auth['original_lease_snapshot']))]
            if auth['cleanup_sha256'] is not None:offered.append(('preexisting-cleanup.json',root/lease['job_id']/'cleanup.json'))
            for name,path in offered:
                raw=bounded_json(path)
                # UTF-8 round-trip preserves the original JSON bytes; the
                # existing helper uses O_EXCL,flush and fsync for each file.
                write_exclusive(snapshot/name,raw.decode('utf-8'))
                records[name]=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
            atomic_write_json(snapshot/'manifest.json',dict(files=records,status_absent=True,cleanup_history_not_rewritten=True))
            fsync_directory(snapshot)
            fsync_directory(snapshot.parent)
            witness=dict(format=FORMAT,job_id=lease['job_id'],authorization=dict(path=str(Path(auth_path)),sha256=auth_digest),
                lease_binding={key:lease[key] for key in KEYS},source_closure=closure,
                original_failure_snapshot=dict(path=str(snapshot/'manifest.json'),sha256=sha(snapshot/'manifest.json')),
                proof=domain,reason='legacy_closed_wav_unit_stopped')
            c.encoded(witness,maximum=16384)
            atomic_write_json(root/NAME,witness)
            return witness


def verify(witness,root,lease,*,controller_fd):
    c.fields(witness,{'format','job_id','authorization','lease_binding','source_closure',
        'original_failure_snapshot','proof','reason'},label='original WAV maintenance witness')
    if witness['format']!=FORMAT or witness['reason']!='legacy_closed_wav_unit_stopped':
        c.reject('Unknown legacy recovery witness',code='conflict')
    auth,closure=authorization(witness['authorization']['path'],witness['authorization']['sha256'])
    lock_path(controller_fd,root/'controller.lock',auth['controller_lock_identity'])
    manifest=reference(witness['original_failure_snapshot'])
    snapshot=c.decode(bounded_json(manifest))
    c.fields(snapshot,{'files','status_absent','cleanup_history_not_rewritten'},label='original failure snapshot')
    expected={'lease.json':auth['lease_sha256'],'context.json':auth['context_sha256'],
        'owner.json':auth['owner_sha256'],'authorization.json':witness['authorization']['sha256'],
        'original-unknown-lease.json':auth['original_lease_snapshot']['sha256']}
    if auth['cleanup_sha256'] is not None:expected['preexisting-cleanup.json']=auth['cleanup_sha256']
    if set(snapshot['files'])!=set(expected) or snapshot['status_absent'] is not True or snapshot['cleanup_history_not_rewritten'] is not True:
        c.reject('Original failure snapshot differs',code='conflict')
    for name,digest in expected.items():
        item=snapshot['files'][name]
        c.fields(item,{'bytes','sha256'},label='original snapshot file')
        c.integer(item['bytes'],'original snapshot bytes',65536)
        if item['sha256']!=digest or (manifest.parent/name).stat().st_size!=item['bytes'] or sha(manifest.parent/name)!=digest:
            c.reject('Original failure snapshot bytes changed',code='conflict')
    proof=witness['proof']
    c.fields(proof,{'scope','boot','mount','known_participants','controller_lock_identity','unit_identity',
        'controller_exclusive','unit_exclusive','filesystem_test','global_process_enumeration',
        'observed_at_monotonic','observed_at_utc'},label='closed unit proof')
    c.text(proof['observed_at_utc'],'maintenance observation time',64)
    if type(proof['observed_at_monotonic']) not in {int,float} or not 0<proof['observed_at_monotonic']<=time.monotonic():
        c.reject('Maintenance observation time differs',code='conflict')
    if (witness['job_id']!=lease['job_id'] or witness['lease_binding']!={key:lease[key] for key in KEYS}
            or witness['source_closure']!=closure or str(root.parent)!=auth['state']
            or proof['scope']!=SCOPE or proof['boot']!=lease['controller']['boot']
            or proof['unit_identity']!=lease['unit_identity']
            or proof['controller_lock_identity']!=auth['controller_lock_identity']
            or proof['controller_exclusive'] is not True or proof['unit_exclusive'] is not True
            or proof['filesystem_test']!=auth['filesystem_test'] or proof['mount']!=filesystem(auth,root)
            or proof['global_process_enumeration'] is not False):
        c.reject('Original WAV maintenance binding changed',code='conflict')
    c.sequence(proof['known_participants'],'known original participants',3,minimum=3)
    for original,observed in zip((lease[key] for key in ('controller','web_parent','init_identity')),proof['known_participants']):
        if original is None:
            if observed is not None:c.reject('Unknown participant was invented',code='conflict')
        else:
            c.fields(observed,{'original','state','observed'},label='original participant observation')
            if observed['original']!=original or observed['state'] not in {'absent','reused','zombie'}:
                c.reject('Original participant proof differs',code='conflict')
            current=observed['observed']
            if observed['state']=='absent':
                if current is not None:c.reject('Absent participant has a live observation',code='conflict')
            else:
                c.fields(current,{'pid','start','boot'},label='observed original PID')
                if current['pid']!=original['pid'] or current['boot']!=original['boot'] or observed['state']=='reused' and current['start']==original['start']:
                    c.reject('Observed PID reuse proof differs',code='conflict')
    # This fixed file is an internal maintenance intent, not a public override.
    if c.decode(bounded_json(root/NAME,16384),maximum=16384)!=witness:
        c.reject('Original WAV maintenance witness changed',code='conflict')
    return auth


def load(backend,lease,*,owned=None):
    path=backend.root/NAME
    try:raw=bounded_json(path,16384)
    except FileNotFoundError:return None
    witness=c.decode(raw,maximum=16384)
    if type(witness) is not dict:c.reject('Invalid original WAV recovery witness',code='conflict')
    if witness.get('job_id')!=lease['job_id']:return None
    if backend.closed:c.reject('Original recovery controller is closed',code='conflict')
    auth=verify(witness,backend.root,lease,controller_fd=backend.lock_fd)
    lock_path(backend.lock_fd,backend.root/'controller.lock',auth['controller_lock_identity'])
    bindings(backend.root,lease,auth,owned=owned)
    owned=backend.root/lease['job_id'] if owned is None else owned
    with exclusive(owned/'startup.lock',lease['unit_identity']) as fd:
        filesystem(auth,backend.root)
        bindings(backend.root,lease,auth,owned=owned)
        lock_path(backend.lock_fd,backend.root/'controller.lock',auth['controller_lock_identity'])
        lock_path(fd,owned/'startup.lock',lease['unit_identity'])
    return witness
