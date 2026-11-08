"""Closed pending media probe and sealed ready range operations.

No storage allocator, publication, host path/URL or permission fallback. Only
the already admitted media job may execute this fixed reader/prober.
"""
from contextlib import closing
import hashlib
import os
from pathlib import Path
import re
import socket
import stat

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.sandbox import _read
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current
from demiflow.forge.schema import ForgeError

MIB=1024*1024
MIMES={'image/png':'image','image/jpeg':'image','image/gif':'image','image/webp':'image',
    'audio/wav':'audio','audio/mpeg':'audio','audio/ogg':'audio','audio/webm':'audio',
    'video/mp4':'video','video/webm':'video','video/ogg':'video'}


def primitives():
    try:from demiflow.workbench.analysis import media
    except ImportError:c.reject('Media primitives are absent from this intermediate release')
    return media


def content(value):
    c.fields(value,{'sha256','bytes','mime'},label='declared media content')
    c.sha(value['sha256']);c.one_of(value['mime'],MIMES,'media MIME')
    c.integer(value['bytes'],'fixed media bytes',{'image':8*MIB,'audio':16*MIB,'video':64*MIB}[MIMES[value['mime']]],1)
    return value


def source(value):
    try:from demiflow.workbench.analysis.registered_media import source_witness
    except ImportError:c.reject('Registered media source contract is absent from this intermediate release')
    return source_witness(value)


def chunks(value,size):
    items=c.sequence(value,'sealed media chunk hashes',64)
    if len(items)!=(size+MIB-1)//MIB:c.reject('Media chunk proof count differs',code='conflict')
    for sha in items:c.sha(sha)


def configuration(value):
    if type(value) is not dict:c.reject('Fixed media configuration must be an object')
    probe=value.get('operation')=='probe-media'
    c.fields(value,{'version','operation','reservation','content','source'} if probe else {'version','operation','artifact','range'},label='fixed media artifact operation')
    if type(value['version']) is not int or value['version']!=1 or value['operation'] not in {'probe-media','artifact-media'}:
        c.reject('Unsupported media artifact operation')
    from demiflow.workbench.projects.registry import identity
    if probe:
        reservation=value['reservation']
        c.fields(reservation,{'publish_id','seed','seed_digest','reserved_bytes','reserved_entries'},label='owned media reservation')
        if type(reservation['publish_id']) is not str or not re.fullmatch('p[0-9a-f]{32}',reservation['publish_id']):
            c.reject('Media probe requires the original fixed publication ID')
        seed=reservation['seed']
        c.fields(seed,{'project','object_id','generation','retention','execution_id','parents','source_binding'},label='original media allocation seed')
        identity(seed['project']);object_id(seed['object_id']);c.integer(seed['generation'],'artifact generation',2**31-1,1)
        c.identity(seed['execution_id']);c.sha(seed['source_binding']);c.sha(reservation['seed_digest'])
        if seed['retention']!='pinned' or seed['parents']!=[] or c.digest(seed)!=reservation['seed_digest']:
            c.reject('Media capture requires its exact pinned allocation seed',code='conflict')
        declared=content(value['content']);witness=source(value['source'])
        if seed['source_binding']!=witness['source_snapshot'] or declared['sha256']!=witness['declared_sha256']:
            c.reject('Media declaration differs from its original source binding',code='conflict')
        c.integer(reservation['reserved_bytes'],'media held bytes',128*MIB,declared['bytes'])
        c.integer(reservation['reserved_entries'],'media held entries',128,4)
    else:
        artifact=value['artifact']
        c.fields(artifact,{'project','id','generation','digest','sha256','bytes','mime','source_digest','descriptor','chunk_sha256'},label='sealed media artifact')
        identity(artifact['project']);object_id(artifact['id']);c.integer(artifact['generation'],'artifact generation',2**31-1,1)
        c.sha(artifact['digest']);c.sha(artifact['source_digest'])
        content({key:artifact[key] for key in ('sha256','bytes','mime')});chunks(artifact['chunk_sha256'],artifact['bytes'])
        media=primitives();descriptor=media.verified_metadata(artifact['descriptor'],MIMES[artifact['mime']])
        if any(descriptor[key]!=artifact[key] for key in ('sha256','bytes','mime')) or descriptor['artifact_id']!=artifact['id']:
            c.reject('Ready media descriptor differs from registered content',code='conflict')
        media.byte_range(value['range'],artifact['bytes'])
    c.encoded(value,maximum=56*1024)
    return value


def owner_artifact(value):
    configuration(value)
    if value['operation']!='artifact-media':c.reject('Pending media probe is not a ready artifact')
    return {key:value['artifact'][key] for key in ('id','generation','digest','sha256','bytes')}


def probe_binding(value):
    configuration(value)
    if value['operation']!='probe-media':c.reject('Only pending media may use the capture control')
    reservation=value['reservation']
    return dict(publish_id=reservation['publish_id'],object_id=reservation['seed']['object_id'],
        seed_digest=reservation['seed_digest'],source_digest=c.digest(value['source']),
        content_sha256=value['content']['sha256'],config_digest=c.digest(value))


def open_fixed(root,parts):
    descriptor=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        for name in parts[:-1]:
            following=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=descriptor)
            os.close(descriptor);descriptor=following
        return os.open(parts[-1],os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW,dir_fd=descriptor)
    finally:os.close(descriptor)


def file_identity(fd,size):
    info=os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_nlink!=1 or stat.S_IMODE(info.st_mode)!=0o400 or info.st_size!=size:
        c.reject('Fixed media file metadata differs',code='conflict')
    return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)


def probe(value,*,pending_root='/pending',tool=None):
    configuration(value)
    if value['operation']!='probe-media':c.reject('Only pending probe configuration is allowed')
    declared=value['content'];identifier=value['reservation']['seed']['object_id'];media=primitives()
    fd=open_fixed(pending_root,['media.bin'])
    try:
        before=file_identity(fd,declared['bytes']);digest=hashlib.sha256();parts=[]
        for offset in range(0,declared['bytes'],MIB):
            amount=min(MIB,declared['bytes']-offset);raw=os.pread(fd,amount,offset)
            if len(raw)!=amount:c.reject('Pending media is truncated',code='conflict')
            digest.update(raw);parts.append(hashlib.sha256(raw).hexdigest())
        if digest.hexdigest()!=declared['sha256']:c.reject('Pending media content hash differs',code='conflict')
        kind=MIMES[declared['mime']]
        if kind=='image' or declared['mime']=='audio/wav':
            # Existing per-kind encoded caps8/16MiB apply; no64MiB video copy.
            raw=os.pread(fd,declared['bytes'],0)
            descriptor=media.probe_image(raw,artifact_id=identifier,mime=declared['mime']) if kind=='image' else media.probe_wav(raw,artifact_id=identifier)
        else:
            descriptor=media.probe_av_fd(fd,artifact_id=identifier,mime=declared['mime'],kind=kind,tool=tool)
        if file_identity(fd,declared['bytes'])!=before:c.reject('Pending media changed during probe',code='conflict')
        if descriptor['sha256']!=declared['sha256']:c.reject('Probe content differs from original declaration',code='conflict')
        return dict(operation='media-probe',publish_id=value['reservation']['publish_id'],object_id=identifier,
            seed_digest=value['reservation']['seed_digest'],**declared,source_digest=c.digest(value['source']),
            descriptor=descriptor,chunk_sha256=parts)
    finally:os.close(fd)


def ready_range(value,*,objects_root='/objects'):
    configuration(value)
    if value['operation']!='artifact-media':c.reject('Only sealed ready media may be ranged')
    artifact=value['artifact'];root=Path(objects_root)/artifact['id']
    manifest_fd=open_fixed(root,['manifest.json'])
    try:
        info=os.fstat(manifest_fd)
        if not 1<=info.st_size<=56*1024:c.reject('Media manifest size differs',code='conflict')
        before=file_identity(manifest_fd,info.st_size)
        raw=os.read(manifest_fd,56*1024+1)
        if len(raw)!=info.st_size or file_identity(manifest_fd,info.st_size)!=before:
            c.reject('Media manifest changed while reading',code='conflict')
        manifest=c.decode(raw,maximum=56*1024)
    finally:os.close(manifest_fd)
    c.fields(manifest,{'format','project','object_id','generation','retention','execution_id','parents','source_binding',
        'payload','files','encoded_bytes','filesystem_entries'},label='sealed media object manifest')
    from demiflow.workbench.analysis.object_files import candidate_manifest
    seed={key:manifest[key] for key in ('project','object_id','generation','retention','execution_id','parents','source_binding')}
    canonical=candidate_manifest(seed,manifest['payload'],manifest['files'])
    payload=manifest['payload']
    if (canonical!=manifest or c.digest(manifest)!=artifact['digest'] or manifest['project']!=artifact['project']
            or manifest['object_id']!=artifact['id'] or manifest['generation']!=artifact['generation']
            or payload.get('format')!='media' or payload.get('complete') is not True):
        c.reject('Media ready manifest differs',code='conflict')
    if (any(payload.get(key)!=artifact[key] for key in ('sha256','bytes','mime','descriptor','chunk_sha256'))
            or c.digest(source(payload['source']))!=artifact['source_digest']
            or manifest['source_binding']!=payload['source']['source_snapshot']
            or manifest['files']!={'media.bin':dict(bytes=artifact['bytes'],sha256=artifact['sha256'])}):
        c.reject('Media sealed facts differ from registration',code='conflict')
    return _read_chunks(value,root)


def _read_chunks(value,root):
    """Only called after canonical registration checks; bounded file reader."""
    artifact=value['artifact']
    start,end,status=primitives().byte_range(value['range'],artifact['bytes'])
    fd=open_fixed(root,['payload','media.bin'])
    try:
        before=file_identity(fd,artifact['bytes']);selected=[]
        for index in range(start//MIB,end//MIB+1):
            offset=index*MIB;size=min(MIB,artifact['bytes']-offset);raw=os.pread(fd,size,offset)
            if len(raw)!=size or hashlib.sha256(raw).hexdigest()!=artifact['chunk_sha256'][index]:
                c.reject('Media range content differs from sealed chunk proof',code='conflict')
            selected.append(raw[max(start-offset,0):min(end-offset+1,size)])
        data=b''.join(selected)
        if len(data)!=end-start+1 or len(data)>MIB or file_identity(fd,artifact['bytes'])!=before:
            c.reject('Media range changed while reading',code='conflict')
        return dict(data=data,metadata=dict(status=status,mime=artifact['mime'],start=start,end=end,total=artifact['bytes'],
            sha256=artifact['sha256'],source_digest=artifact['source_digest'],descriptor=artifact['descriptor']))
    finally:os.close(fd)


def response(value):
    """Known failures cross the worker wire without paths or tracebacks."""
    pending=value['operation']=='probe-media';tool=None;data=None
    identity=dict(operation='media-probe',publish_id=value['reservation']['publish_id'],
        object_id=value['reservation']['seed']['object_id'],seed_digest=value['reservation']['seed_digest']) if pending else dict(
        operation='artifact-media',artifact={key:value['artifact'][key] for key in ('id','generation','digest')})
    try:
        if pending and current() is not None and current().value.get('media_tool') is not None:
            tool=primitives().ProbeTool(**current().value['media_tool'])
        elif pending and current() is None and Path('/opt/demiflow/media-tools.json').exists():
            configured=c.decode(_read('/opt/demiflow/media-tools.json',4096),maximum=4096)
            c.fields(configured,{'ffprobe'},label='pinned image probe tool')
            c.fields(configured['ffprobe'],{'path','sha256','version'},label='pinned ffprobe')
            tool=primitives().ProbeTool(**configured['ffprobe'])
        if current() is None:
            result=probe(value,tool=tool) if pending else ready_range(value)
        else:
            result=probe(value,pending_root=job_path('/pending'),tool=tool) if pending else ready_range(value,objects_root=job_path('/objects'))
        message=result if pending else dict(**identity,metadata=result['metadata'])
        if not pending:data=result['data']
    except (ForgeError,OSError) as exc:
        message=dict(**identity,error=dict(code=getattr(exc,'code','operation_failed'),message='Fixed media validation or read failed; no media was returned'))
    c.encoded(message,maximum=56*1024)
    return message,data


def run(value):
    configuration(value)
    pending=value['operation']=='probe-media';message,data=response(value)
    connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.settimeout(5)
    with closing(connection):
        connection.connect(job_path('/broker/media-probe.sock' if pending else '/broker/output-read.sock'))
        frames=Frames(connection);frames.send('json',message)
        if data is not None:frames.send('blob',data)
        frames.end_read()
