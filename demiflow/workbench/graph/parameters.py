"""Registered public parameter projection and cooperating-writer digest CAS.

There is no parameter receipt ledger. A readback proves current content only.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.io import fsync_directory
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError

MAX_BYTES = 65536
SERIALIZATION = 'parameters-json-v1'


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _pointer(parts):
    return '/'+'/'.join(str(p).replace('~','~0').replace('/','~1') for p in parts)


def _decode(raw):
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:
                raise ValueError('Duplicate parameter field')
            value[key]=item
        return value
    def constant(value):
        raise ValueError('Nonfinite parameter number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
    count=0
    def visit(item,depth=0):
        nonlocal count
        count+=1
        if depth>16 or count>4096:
            raise ValueError('Parameter structure exceeds budget')
        if type(item) is float and not math.isfinite(item):
            raise ValueError('Nonfinite parameter number')
        if type(item) is int and not -(2**63)<=item<2**63:
            raise ValueError('Parameter integer exceeds 64 bits')
        if type(item) is dict:
            for child in item.values():visit(child,depth+1)
        elif type(item) is list:
            for child in item:visit(child,depth+1)
    visit(value)
    if (type(value) is not dict or set(value)!={'format','arguments'}
            or value['format'] not in {'demiflow_forge_flow_parameters_v1','demiflow_forge_flow_parameters_v2'}
            or type(value['arguments']) is not dict):
        raise ValueError('Unsupported parameter document')
    return value


def _read_registered_bytes(root,path):
    root,control,_=ws.load_workspace(root)
    if path not in {p for paths in control.get('entries',{}).values() for p in paths}:
        raise ForgeError('Parameter file is not registered',code='not_found')
    target=ws.validate_parameter_path(root,path)
    raw=read_bounded(target,MAX_BYTES)
    return root,control,raw


def read_file(root,path):
    root,control,raw=_read_registered_bytes(root,path)
    try:value=_decode(raw)
    except (ValueError,RecursionError) as exc:
        raise ForgeError('Invalid registered parameters: '+str(exc)[:512],code='invalid_request') from exc
    return root,control,raw,value


def _entry_arguments(root, *, entry, names):
    """AST work runs only inside the existing bounded read worker."""
    import ast
    from demiflow.forge.source import SourceTree
    from demiflow.forge.source import Definition
    root,_,_=ws.load_workspace(root)
    if type(names) is not list or len(names)>4096 or any(type(n) is not str for n in names):
        raise ForgeError('Invalid argument projection',code='invalid_request')
    target,symbol=ws.validate_entry(root,entry)
    module=str(target.relative_to(root))[:-3].replace('/','.')
    tree=SourceTree(root,{'files':16,'total_bytes':4*1024**2})
    definition=tree.resolve(tree.load(module).get(symbol))
    if not isinstance(definition,Definition) or not isinstance(definition.node,(ast.FunctionDef,ast.AsyncFunctionDef)):
        raise ValueError('Entry signature unavailable')
    arguments=definition.node.args
    accepted={p.arg for p in [*arguments.args,*arguments.kwonlyargs] if p.arg in names}
    tree.unchanged()
    return sorted(accepted)


def public_model(root,control,entry,path,nodes,*,readonly=False,sealed=None,expected_document=None):
    result=dict(path=path,sha256=None,serialization_version=SERIALIZATION,fields=[],truncated=False,readonly=readonly)
    if sealed is not None:
        document=sealed
        result['readonly']=True
    elif not path or path not in control.get('entries',{}).get(entry,[]):
        return {**result,'available':False,'reason':'no_registered_parameters'}
    else:
        try:
            _,_,raw=_read_registered_bytes(root,path)
            result['sha256']=_sha(raw)
            # The file identity exists even when the editor cannot project its
            # format. Keep that identity so graph reads can distinguish an
            # unsupported editor document from a real concurrent modification.
            try:document=_decode(raw)
            except (ValueError,RecursionError) as exc:
                return {**result,'available':False,'readonly':True,
                        'reason':'Invalid registered parameters: '+str(exc)[:512]}
        except (ForgeError,OSError) as exc:
            return {**result,'available':False,'reason':str(exc)[:512]}
    if expected_document is not None and content_digest(document)!=content_digest(expected_document):
        raise ForgeError('Parameters changed after report verification',code='conflict')
    result['available']=True
    argument_names=None
    if not result['readonly']:
        from demiflow.workbench.worker import operation
        try:
            argument_names=set(operation('workbench_entry_arguments',root,entry=entry,names=list(document['arguments'])))
        except (ValueError,OSError,ForgeError):
            result['readonly']=True
            result['reason']='entry_signature_unavailable'
    uses={}
    for node in nodes:
        for origin in node.get('parameter_origins',{}).values():
            if (type(origin) is dict and type(origin.get('argument')) is str
                    and type(origin.get('path')) is list and all(type(p) in {str,int} for p in origin['path'])):
                uses.setdefault(origin['argument'],{}).setdefault(tuple(origin['path']),set()).add(node['id'])
    projection_bytes=0
    full=False
    def field(parts,value,kind='literal',writable=True):
        nonlocal projection_bytes,full
        if full or len(result['fields'])>=256:
            result['truncated']=True
            return
        editable=(writable and not result['readonly'] and str(parts[1]) in (argument_names or set()) and value is not None
                  and (type(value) is not str or len(value.encode())<=16384)
                  and (type(value) is not int or abs(value)<=2**53-1))
        public_value=str(value) if type(value) is int and abs(value)>2**53-1 else value
        if kind=='resource_reference':
            resource_kind=value.get('kind') if type(value) is dict else None
            public_value={'kind':resource_kind if resource_kind in {'config','file','location'} else 'unsupported'}
            if resource_kind in {'config','file','location'}:
                public_value.update({key:item for key,item in value.items() if key in {'path','location_id','relative_uri'} and type(item) is str and len(item.encode())<=4096})
        affected=set();field_path=tuple(parts[2:])
        for path,identities in uses.get(str(parts[1]),{}).items():
            # Aliases/output paths resolve to native uri/version members. A
            # literal child is correlated only with its proven container path.
            if (kind in {'table_alias','output_path'} or path==field_path[:len(path)]
                    or field_path==path[:len(field_path)]):
                affected.update(identities)
        affected=sorted(affected)
        projected=dict(pointer=_pointer(parts),parts=parts,value=public_value,kind=kind,
                                     value_type=type(value).__name__,writable=editable,
                                     affected_nodes=affected[:128],affected_node_count=len(affected),
                                     affected_nodes_truncated=len(affected)>128,
                                     reason=None if editable else 'historical_or_unproven_type_or_size')
        size=len(bounded_json(projected,128*1024))+1
        if projection_bytes+size>256*1024:
            full=True;result['truncated']=True;return
        projection_bytes+=size
        result['fields'].append(projected)
    def visit(parts,value,depth=0):
        if full or depth>16 or len(result['fields'])>=256:
            result['truncated']=True
            return
        if type(value) is dict:
            if 'kind' in value:
                if value['kind']=='table' and set(value)=={'kind','alias'}:
                    field(parts+['alias'],value['alias'],'table_alias')
                elif value['kind']=='output' and 'relative_uri' in value:
                    field(parts+['relative_uri'],value['relative_uri'],'output_path')
                else:
                    field(parts,value,'resource_reference',False)
            else:
                for key,child in value.items():visit(parts+[key],child,depth+1)
        elif type(value) is list:
            for index,child in enumerate(value):visit(parts+[index],child,depth+1)
        else:field(parts,value)
    for name,value in document.get('arguments',{}).items():
        visit(['arguments',name],value)
    return result


def state(root,*,path):
    root,control,raw,_=read_file(root,path)
    entry=next(e for e,paths in control['entries'].items() if path in paths)
    projection=public_model(root,control,entry,path,[])
    # Do not combine fields from a second read with the first read's digest.
    if projection['sha256']!=_sha(raw):
        raise ForgeError('Parameters changed while reading',code='conflict')
    return dict(path=path,observed_sha256=_sha(raw),fields=projection['fields'],
                truncated=projection['truncated'],attribution='unknown',durability='not_proven_by_readback')


def _candidate(root,*, table_resolver,path,expected_sha256,graph_binding,replace,proposed_sha256=None):
    from demiflow.workbench.graph.model import from_binding
    if not isinstance(expected_sha256,str) or not re.fullmatch('[0-9a-f]{64}',expected_sha256):
        raise ForgeError('Expected file SHA256 is required',code='invalid_request')
    if type(replace) is not list or not 1<=len(replace)<=32:
        raise ForgeError('Expected 1..32 replace operations',code='invalid_request')
    graph=from_binding(root,graph_binding)
    if graph['binding']['basis_kind']=='run' or not graph['freshness']['current']:
        raise ForgeError('Historical or stale graph is read-only',code='conflict')
    projection=graph['parameters']
    if projection['path']!=path or not projection.get('available'):
        raise ForgeError('Parameters are outside this graph',code='conflict')
    root,control,original,document=read_file(root,path)
    if _sha(original)!=expected_sha256 or projection['sha256']!=expected_sha256:
        raise ForgeError('Parameters changed; keep the draft and read back',code='conflict')
    fields={f['pointer']:f for f in projection['fields'] if f['writable']}
    value=copy.deepcopy(document);seen=set();diff=[]
    for operation in replace:
        if (type(operation) is not dict or set(operation)!={'op','path','value'}
                or operation['op']!='replace' or type(operation['path']) is not str):
            raise ForgeError('Only explicit replace operations are accepted',code='invalid_request')
        pointer=operation['path']
        if pointer in seen or pointer not in fields:
            raise ForgeError('Parameter field is not writable or repeated',code='invalid_request')
        seen.add(pointer);field=fields[pointer];candidate=operation['value'];before=field['value']
        if type(candidate) is not type(before):
            raise ForgeError('Parameter type must match the registered field',code='invalid_request')
        if field['kind']=='table_alias':
            table_resolver(root,control,candidate)
        if field['kind']=='output_path':
            owner=document
            for part in field['parts'][:-1]:owner=owner[part]
            location=control['locations'].get(owner.get('location_id'))
            if not location or not location.get('writable'):
                raise ForgeError('Output location is not writable',code='operation_rejected')
            target=ws.relative_path(ws.location_root(root,location),candidate)
            if target.suffix!='.lance':
                raise ForgeError('Output must name a relative Lance table',code='invalid_request')
        owner=value
        for part in field['parts'][:-1]:owner=owner[part]
        owner[field['parts'][-1]]=candidate
        diff.append(dict(path=pointer,before=before,after=candidate))
    try:
        encoded=(json.dumps(value,ensure_ascii=False,allow_nan=False,indent=2,sort_keys=True)+'\n').encode()
        if len(encoded)>MAX_BYTES:raise ForgeError('Parameter file exceeds 64KiB',code='resource_limit')
        _decode(encoded)
    except (ValueError,RecursionError) as exc:
        raise ForgeError('Invalid replacement parameters',code='invalid_request') from exc
    proposal=_sha(encoded)
    if proposed_sha256 is not None and proposed_sha256!=proposal:
        raise ForgeError('Candidate SHA differs from preview',code='conflict')
    return root,original,encoded,dict(path=path,base_sha256=expected_sha256,proposed_sha256=proposal,
                                    serialization_version=SERIALIZATION,public_diff=diff,
                                    no_change=all(d['before']==d['after'] for d in diff))


def preview(root,**request):
    return _candidate(root,table_resolver=_table_resolver(),**request)[3]


def update(root,*,authorize,**request):
    root,_,_=ws.load_workspace(root)
    with ws.control_lock(root):
        authorize()
        root,original,encoded,result=_candidate(root,table_resolver=_table_resolver(),**request)
        if result['no_change']:
            return dict(applied=False,no_change=True,new_sha256=_sha(original),needs_check=False)
        path=ws.validate_parameter_path(root,request['path'])
        info=path.stat();renamed=False;temporary=None
        def io_failure(exc):
            raise ForgeError('Parameter I/O failed; outcome/durability '+('unknown after replace' if renamed else 'not confirmed')+
                             '. Read back the original path; keep the draft.',code='operation_failed') from exc
        try:
            try:
                fd,temporary=tempfile.mkstemp(prefix='.forge-parameters-',dir=path.parent)
                with os.fdopen(fd,'wb') as stream:
                    current=os.fstat(stream.fileno())
                    if (current.st_uid,current.st_gid)!=(info.st_uid,info.st_gid):
                        os.fchown(stream.fileno(),info.st_uid,info.st_gid)
                    os.fchmod(stream.fileno(),info.st_mode & 0o777)
                    stream.write(encoded);stream.flush();os.fsync(stream.fileno())
            except OSError as exc:
                io_failure(exc)
            from demiflow.workbench.graph.model import from_binding
            confirmed=from_binding(root,request['graph_binding'])
            if not confirmed['freshness']['current']:
                raise ForgeError('Graph sources changed before parameter replace',code='conflict')
            authorize()
            try:
                ws.no_symlinks(path)
                latest=path.stat()
                if (latest.st_dev,latest.st_ino)!=(info.st_dev,info.st_ino) or _sha(read_bounded(path,MAX_BYTES))!=_sha(original):
                    raise ForgeError('Parameters changed before replace',code='conflict')
                os.replace(temporary,path);renamed=True
                fsync_directory(path.parent)
            except OSError as exc:
                io_failure(exc)
        finally:
            if temporary is not None:Path(temporary).unlink(missing_ok=True)
        return dict(applied=True,new_sha256=result['proposed_sha256'],needs_check=True)


def _table_resolver():
    from demiflow.workbench.table_bindings import make_resolver
    from demiflow.workbench.runs import manager as runs
    return make_resolver(ws.table_binding, runs.result_table)
