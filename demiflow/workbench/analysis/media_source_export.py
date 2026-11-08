"""Trusted workspace-worker export of one explicitly declared fixed media row.

No codec and no browser path. Bytes stream only to an inherited exclusive FD;
the private pending probe must succeed before any artifact becomes ready.
"""
import hashlib
import os
from pathlib import Path
import resource
import stat
from urllib.parse import urlsplit

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import catalog as table_view
from demiflow.forge.io import no_symlinks
from demiflow.objects import local_object_uri
from demiflow.objects import _local_path
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.source_export import fixed_input
from demiflow.workbench.analysis.registered_media import source_witness
from demiflow.workbench.analysis.filesystem_identity import identity

MIB=1024*1024
CONTROL={'.forge','.git','.codex','.codex-home','.ssh','.config'}


def _public_path(root,path):
    # Include the declared base itself and every ancestor within this project,
    # not just the selected file's suffix under that base. For an external
    # resource root, its whole explicit path is checked.
    parts=path.relative_to(root).parts if path.is_relative_to(root) else path.parts
    if any(part in CONTROL for part in parts):c.reject('Media declaration cannot use a control directory or descendant',code='operation_rejected')


def _open(path):
    """Hold every directory FD so link swaps cannot redirect the final open."""
    path=Path(path)
    if not path.is_absolute() or '..' in path.parts or len(path.parts)>64:c.reject('Declared media path is invalid')
    parent=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent);os.close(parent);parent=child
        return os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    finally:os.close(parent)


def export(root,**kwargs):
    from demiflow.forge.schema import ForgeError
    try:return _export(root,**kwargs)
    except ForgeError:raise
    except (OSError,ValueError,KeyError,TypeError):
        c.reject('Declared fixed media is unavailable or changed; no bytes were published',code='operation_rejected')


def _selection(root,*,source_locator,source_snapshot,case_key,field):
    c.sha(source_snapshot);c.text(field,'declared media field',256);c.text(case_key,'fixed media Case key',20)
    if not case_key.isascii() or not case_key.isdecimal() or str(int(case_key))!=case_key or int(case_key)>=2**64:c.reject('Media row must be a canonical uint64 key')
    if source_locator.get('kind')!='workspace_table':c.reject('This source has no supported explicit public resource-root declaration',code='operation_rejected')
    root,workspace,_=ws.load_workspace(root)
    uri,version,source=fixed_input(root,source_locator)
    if source['digest']!=source_snapshot or field not in {item['name'] for item in source['schema']}:c.reject('Media field/source differs from its registered fixed projection',code='conflict')
    control,ref,current_uri=ws.table_binding(root,workspace,source_locator['alias'])
    if Path(current_uri)!=uri or ref.lance_version!=version:c.reject('Media table authority changed',code='conflict')
    from demiflow.lance import preview
    snapshot=preview._open(uri,version)
    path=[field] if field in snapshot.schema.names else field.split('.')
    choices=[item for item in control.get('display',{}).get('fields',[]) if item['kind'] in {'image','audio','video'} and item['path']==path]
    if len(choices)!=1:c.reject('Media field requires one explicit public resource declaration',code='operation_rejected')
    declared=choices[0]
    base=no_symlinks(ws.location_root(root,workspace['locations'][declared['location_id']]))
    _public_path(root,base)
    # A selected table field does not authorize the project's control tree.
    if root.is_relative_to(base) or base==root:c.reject('Media needs a dedicated declared public data/resource root',code='operation_rejected')
    root_before=base.stat();root_identity=identity(root_before);hashes=declared['sha256_path']
    if any(type(part) is not str for part in path+hashes):c.reject('Nested list media needs an explicit concrete field declaration',code='operation_rejected')
    columns=list(dict.fromkeys([path[0],hashes[0]]))
    rows=preview.sample_table(uri,ref.to_dict(),columns=columns,row_id=int(case_key),limit=1)['rows']
    if len(rows)!=1:c.reject('Fixed media Case is unavailable',code='not_found')
    value,expected=table_view.at(rows[0],path),table_view.at(rows[0],hashes)
    c.text(value,'declared media path',4096);c.sha(expected,'declared media content')
    if not urlsplit(value).scheme:value=(base/value).as_uri()
    target=_local_path(local_object_uri(dict(uri=value,sha256=expected),root=base,relocations=declared.get('relocations',[])))
    _public_path(root,target)
    mime=declared.get('mime')
    if mime is None:mime={'.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.gif':'image/gif','.webp':'image/webp'}.get(target.suffix.lower())
    if mime is None:c.reject('Media MIME must be explicitly declared',code='operation_rejected')
    maximum=min(declared['max_bytes'],{'image':8,'audio':16,'video':64}[declared['kind']]*MIB)
    witness=source_witness(dict(version=1,source_snapshot=source_snapshot,case_key=case_key,field=field,declared_sha256=expected,
        resource_binding=c.digest(dict(declaration=declared,location=workspace['locations'][declared['location_id']],root_identity=root_identity))))
    return dict(root=root,workspace=workspace,source_locator=source_locator,source=source,base=base,root_identity=root_identity,
        path=path,declared=declared,target=target,mime=mime,maximum=maximum,witness=witness)


def _recheck(selected):
    root,workspace,source_locator,source,base,root_identity,path,declared=(selected[key] for key in
        ('root','workspace','source_locator','source','base','root_identity','path','declared'))
    current=no_symlinks(base).stat()
    if identity(current)!=root_identity:c.reject('Original public resource root changed',code='conflict')
    _,_,latest=fixed_input(root,source_locator)
    if latest!=source:c.reject('Source binding changed during media export',code='conflict')
    # Display/resource declarations intentionally do not change a Lance table
    # binding. Recheck them separately instead of treating the table digest as
    # a permission witness for external files.
    _,workspace_now,_=ws.load_workspace(root)
    control_now,_,_=ws.table_binding(root,workspace_now,source_locator['alias'])
    fields_now=[item for item in control_now.get('display',{}).get('fields',[]) if item['kind'] in {'image','audio','video'} and item['path']==path]
    if (fields_now!=[declared] or workspace_now['locations'].get(declared['location_id'])!=workspace['locations'][declared['location_id']]
            or no_symlinks(ws.location_root(root,workspace_now['locations'][declared['location_id']]))!=base):
        c.reject('Public media declaration or resource authorization changed during export',code='conflict')
    return True


def resolve_fields(root,*,source_locator,source_snapshot,case_key,fields):
    """Current fixed row/declaration/root proof; never read media bytes or pin."""
    from demiflow.forge.schema import ForgeError
    try:
        c.sequence(fields,'current declared media fields',16,minimum=1)
        if len(set(fields))!=len(fields):c.reject('Repeated media resource field')
        values=[]
        for field in fields:
            selected=_selection(root,source_locator=source_locator,source_snapshot=source_snapshot,case_key=case_key,field=field)
            _recheck(selected);values.append(dict(source=selected['witness'],mime=selected['mime']))
        return dict(source_snapshot=source_snapshot,case_key=case_key,fields=values)
    except ForgeError:raise
    except (OSError,ValueError,KeyError,TypeError):
        c.reject('Current fixed media declaration or public resource root is unavailable',code='operation_rejected')


def _export(root,*,source_locator,source_snapshot,case_key,field,output_fd):
    c.sha(source_snapshot);c.text(field,'declared media field',256);c.text(case_key,'fixed media Case key',20)
    if not case_key.isascii() or not case_key.isdecimal() or str(int(case_key))!=case_key or int(case_key)>=2**64:c.reject('Media row must be a canonical uint64 key')
    if source_locator.get('kind')!='workspace_table':c.reject('This source has no supported explicit public resource-root declaration',code='operation_rejected')
    c.integer(output_fd,'exclusive inherited media FD',127,3);before=os.fstat(output_fd)
    if not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or before.st_nlink!=1 or before.st_size!=0 or os.lseek(output_fd,0,os.SEEK_CUR)!=0:c.reject('Media export FD is not the exclusive empty parent file')
    _,hard=resource.getrlimit(resource.RLIMIT_FSIZE)
    if hard!=resource.RLIM_INFINITY and hard<64*MIB:c.reject('Media export lacks its finite file reservation',code='resource_limit')
    resource.setrlimit(resource.RLIMIT_FSIZE,(64*MIB,64*MIB))
    selected=_selection(root,source_locator=source_locator,source_snapshot=source_snapshot,case_key=case_key,field=field)
    target,maximum,expected,mime=(selected['target'],selected['maximum'],selected['witness']['declared_sha256'],selected['mime'])
    descriptor=_open(target);digest=hashlib.sha256();size=0
    try:
        initial=os.fstat(descriptor)
        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink!=1 or not 0<initial.st_size<=maximum:c.reject('Declared media is not a bounded exclusive regular file',code='resource_limit')
        while True:
            chunk=os.read(descriptor,MIB)
            if not chunk:break
            if size+len(chunk)>maximum:c.reject('Declared media grew beyond its bound',code='resource_limit')
            digest.update(chunk);size+=len(chunk);remaining=memoryview(chunk)
            while remaining:
                written=os.write(output_fd,remaining)
                if written<=0:c.reject('Media export write made no progress',code='operation_failed')
                remaining=remaining[written:]
        after=os.fstat(descriptor)
    finally:os.close(descriptor)
    facts=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
    if facts(initial)!=facts(after) or size!=initial.st_size or digest.hexdigest()!=expected:c.reject('Declared media content changed or its hash differs',code='conflict')
    _recheck(selected)
    os.fsync(output_fd);end=os.fstat(output_fd)
    if (end.st_dev,end.st_ino,end.st_size)!=(before.st_dev,before.st_ino,size):c.reject('Exclusive media export identity differs',code='conflict')
    return dict(source=selected['witness'],mime=mime,bytes=size,sha256=expected)
