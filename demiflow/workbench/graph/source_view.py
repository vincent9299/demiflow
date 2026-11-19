"""Graph-bound author source excerpts and explicit public prompt projections."""
from __future__ import annotations

import hashlib
import io
import json
import re

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph.model import bound_source


def _excerpt(text,line,end_line):
    start=max(1,line or 1);end=min(end_line or start+299,start+299)
    chunks=[];size=0;last=start-1;truncated=False
    for number,part in enumerate(io.StringIO(text),1):
        if number<start:continue
        if number>end:
            truncated=end_line is None or end_line>end
            break
        raw=part.encode()
        if size+len(raw)>60000:
            chunks.append(raw[:60000-size].decode('utf-8',errors='ignore'))
            last=number;truncated=True;break
        chunks.append(part);size+=len(raw);last=number
    return dict(text=''.join(chunks),start_line=start,end_line=last,truncated=truncated)


def _public_prompt(raw, *, prompt=None):
    # Compose before safe construction to bound aliases/depth/node traversal.
    # Only named public template/model identifiers leave this projection.
    import yaml
    if len(raw)>65536:
        raise ForgeError('Prompt preview source exceeds 64KiB',code='resource_limit')
    try:
        tree=yaml.compose(raw)
    except yaml.YAMLError as exc:
        raise ForgeError('Invalid prompt YAML',code='operation_rejected') from exc
    stack=[(tree,0)];seen=set();count=0
    while stack:
        node,depth=stack.pop();count+=1
        if node is None:continue
        if depth>16 or count>4096 or id(node) in seen:
            raise ForgeError('Prompt YAML aliases/depth exceed preview contract',code='operation_rejected')
        seen.add(id(node))
        if isinstance(node,yaml.MappingNode):
            keys=[]
            for key,value in node.value:
                if not isinstance(key,yaml.ScalarNode) or key.value in keys:
                    raise ForgeError('Prompt mapping keys must be unique scalars',code='operation_rejected')
                keys.append(key.value);stack.extend(((key,depth+1),(value,depth+1)))
        elif isinstance(node,yaml.SequenceNode):stack.extend((v,depth+1) for v in node.value)
    try:
        value=yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ForgeError('Unsupported prompt YAML values',code='operation_rejected') from exc
    if type(value) is not dict or value.get('schema_version') not in {'demiflow_prompt_pack_v2','demiflow_agent_v2'}:
        raise ForgeError('No supported public prompt projection for this format',code='operation_rejected')
    result={'schema_version':value['schema_version']}
    def text(item):
        if type(item) is str:
            if len(item.encode())>16384:raise ForgeError('Prompt text exceeds preview budget',code='resource_limit')
            return item
        return None
    def model(item):
        if type(item) is not dict:return {}
        projected={k:v for k,v in item.items() if k in {'id','name','provider','transport'} and text(v) is not None}
        for key in ('api_key_env','base_url_env'):
            name=item.get(key)
            if type(name) is str and re.fullmatch('[A-Z_][A-Z0-9_]{0,127}',name):projected[key]=name
        return projected
    def task(item):
        if type(item) is not dict:raise ForgeError('Invalid public prompt task',code='operation_rejected')
        projected={k:v for k,v in item.items() if k in {'version','system','user','template','instructions','input_mode','response_format'} and text(v) is not None}
        if 'model' in item:projected['model']=model(item['model'])
        if type(item.get('schema_retries')) is int:projected['schema_retries']=item['schema_retries']
        if type(item.get('message_limits')) is dict:
            from .plan_details import safe_json
            bounded_json(item['message_limits'],16384)
            projected['message_limits']=safe_json(item['message_limits'])
        messages=item.get('messages')
        if type(messages) is list:
            if len(messages)>32:raise ForgeError('Too many prompt messages for preview',code='resource_limit')
            projected['messages']=[{k:v for k,v in m.items() if k in {'role','content'} and text(v) is not None}
                                   for m in messages if type(m) is dict]
        if type(item.get('response_schema')) is dict:
            bounded_json(item['response_schema'],16384);projected['response_schema']=item['response_schema']
        systemone=item.get('systemone')
        if type(systemone) is dict:
            projected['systemone']={k:v for k,v in systemone.items() if k in {'type','instructions'} and text(v) is not None}
            each=systemone.get('for_each')
            if type(each) is dict and text(each.get('input')) is not None:
                projected['systemone']['for_each']={'input':each['input']}
        return projected
    container='prompts' if value['schema_version']=='demiflow_prompt_pack_v2' else 'tasks'
    definitions=value.get(container)
    if type(definitions) is not dict:
        raise ForgeError('Native prompt configuration has no task definitions',code='operation_rejected')
    if prompt is not None:
        if type(prompt) is not str or prompt not in definitions:
            raise ForgeError('Node prompt is absent from its bound configuration',code='operation_rejected')
        definitions={prompt:definitions[prompt]}
    if len(definitions)>32:raise ForgeError('Too many tasks for unselected preview',code='resource_limit')
    result[container]={name:task(item) for name,item in definitions.items() if type(name) is str and len(name.encode())<=128}
    if container=='tasks':
        result['model']=model(value.get('model'))
        if value.get('runtime') in {'demiflow','codex'}:result['runtime']=value['runtime']
    bounded_json(result,60000)
    return result


def get(root,*,graph_binding,node_id,kind,resource=None,logical_node_id=None,occurrence_id=None):
    if kind not in {'implementation','callsite','configuration','prompt'}:
        raise ForgeError('Unknown source projection',code='invalid_request')
    graph=bound_source(root,graph_binding,node_id,include_parameters=kind=='configuration',
                       logical_node_id=logical_node_id,occurrence_id=occurrence_id)
    node=graph['nodes'][0] if graph['nodes'] else None
    if node is None:
        raise ForgeError('Select a visible node in this graph scope',code='not_found')
    node_id=node['id']
    historical=graph_binding['basis_kind']=='run'
    result=dict(graph_binding=graph_binding,node_id=node_id,requested_kind=kind,
                historical=historical,read_only=True,availability='available',
                source_basis='sealed' if historical else 'report_bound' if graph_binding['basis_kind']=='report' else 'current')
    if graph.get('native_details') is not None:
        contract = graph['native_contract']
        native_node = (contract['outputs'][0]['occurrence_id'] if node_id == contract['action_id'] else node_id)
        details = graph['native_details'].get(native_node, {})
        detail = details.get(kind, {})
        if kind == 'configuration':
            from .plan_details import safe_json
            return {**result, 'kind': kind, 'fields': [],
                'wiring': safe_json(dict(api=node.get('api'), parameters=node.get('parameters', {}), output_fields={})),
                'resources': []}
        metadata = detail.get('metadata', {})
        definitions = [item.get('definition') for item in metadata.get('routes', [metadata]) if item.get('definition')]
        return {**detail, **result, 'kind': kind,
            'availability': detail.get('availability', 'unavailable'),
            'reason': detail.get('reason', 'no_sealed_material'),
            'definition': metadata.get('definition'), 'definitions': definitions}
    if kind=='configuration':
        from .plan_details import safe_json
        fields=[f for f in graph['parameters']['fields'] if node_id in f['affected_nodes']]
        return {**result,'kind':'configuration','fields':fields,
                'wiring':safe_json(dict(api=node.get('api'), parameters=node.get('parameters', {}),
                              parameter_origins=node.get('parameter_origins', {}), output_fields=node.get('fields', {}))),
                'resources':node.get('source_resources',[]),
                'note':'Public configuration fields; template configuration is not an actual model request.'}
    if kind=='prompt':
        resources=node.get('source_resources',[])
        if len(resources)>16:
            raise ForgeError('Too many node prompt sources',code='resource_limit')
        chosen=next((r for r in resources if resource is None or r['parameter']==resource),None)
        if chosen is None:
            return {**result,'availability':'unavailable','reason':'no_bound_prompt_source','kind':'prompt'}
        path,expected=chosen['path'],chosen['sha256']
        if historical:
            from demiflow.workbench.runs.manager import source
            raw=source(root,graph_binding['run_id'],path)['text'].encode()
        else:
            raw=read_bounded(ws.relative_path(root,path),1024**2)
        if hashlib.sha256(raw).hexdigest()!=expected:
            raise ForgeError('Prompt source differs from graph',code='conflict')
        try:projection=_public_prompt(raw,prompt=node.get('parameters',{}).get('prompt'))
        except (ValueError,RecursionError) as exc:
            raise ForgeError('Prompt cannot be projected safely',code='operation_rejected') from exc
        from demiflow.workbench.definition_contracts import prompt_definition
        definitions = [prompt_definition(name, item) for name, item in projection.get('prompts', projection.get('tasks', {})).items()]
        return {**result,'kind':'prompt','path':path,'sha256':expected,'public_projection':projection, 'definitions':definitions,
                'note':'Prompt template only; runtime credentials and concrete model requests are not included.'}
    target=node.get('operator') if kind=='implementation' else None
    actual_kind='implementation' if target else 'callsite'
    path=target['module'].replace('.','/')+'.py' if target else node['source']
    line=target.get('line') if target else node.get('line')
    end=target.get('end_line') if target else node.get('end_line')
    expected=graph['sources'].get(path)
    if expected is None:
        return {**result,'kind':actual_kind,'availability':'unavailable',
                'reason':'implementation_outside_bound_author_closure'}
    if historical:
        from demiflow.workbench.runs.manager import source
        text=source(root,graph_binding['run_id'],path)['text']
        if hashlib.sha256(text.encode()).hexdigest()!=expected:
            raise ForgeError('Sealed source digest differs',code='conflict')
    else:
        text=ws.source_text(root,path,expected_sha256=expected)['text']
    definition = None
    if actual_kind == 'callsite' and node.get('api'):
        from .api_definition import source_address, from_source
        address = source_address(node['api'])
        # A historical API is read from authenticated, sealed platform bytes.
        # Never fill missing historical contracts with the current SDK.
        if historical and address is not None:
            from demiflow.workbench.runs import manager as runs
            target_dir, value = runs._load(root, graph_binding['run_id'], detail='identity', attempt=graph_binding['attempt'])
            facts = value['manifest'].get('platform', {})
            evidence = next((item for item in facts.get('files', []) if item['path'] == address[0]), None)
            if evidence:
                sealed = target_dir / 'runtime/demiflow' / address[0]
                ws.no_symlinks(sealed)
                raw = read_bounded(sealed, min(facts['limits']['file_bytes'], 2 * 1024**2))
                if hashlib.sha256(raw).hexdigest() != evidence['sha256']:
                    raise ForgeError('Sealed API source differs', code='conflict')
                definition = from_source(node['api'], raw)
    elif actual_kind == 'implementation':
        import ast
        from demiflow.workbench.definition_contracts import code_definition
        candidates = [n for n in ast.walk(ast.parse(text)) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                      and min([n.lineno, *(d.lineno for d in n.decorator_list)]) == line]
        if len(candidates) == 1:
            definition = code_definition(candidates[0], '')
    return {**result,'kind':actual_kind,'path':path,'sha256':expected,'definition':definition,
            'reason':('source_callsite_only' if node['kind']=='source_call' else 'builtin_callsite_only') if kind=='implementation' and not target else None,
            **_excerpt(text,line,end)}


def pipeline(root, *, graph_binding):
    """Read the entry file from the same current/report/run source authority."""
    from .model import from_binding
    from .plan_details import material
    graph = from_binding(root, graph_binding)
    if graph_binding['basis_kind'] == 'run':
        from demiflow.workbench.runs import manager as runs
        target, value = runs._load(root, graph_binding['run_id'], detail='identity', attempt=graph_binding['attempt'])
        report = runs._report(target, value['manifest'])
        if report.get('pipeline_detail') is not None:
            return {**report['pipeline_detail'], 'format': 'demiflow.pipeline_source.v1',
                    'graph_binding': graph_binding, 'read_only': True, 'source_basis': 'sealed'}
    entry = graph_binding.get('entry')
    if type(entry) is not str or ':' not in entry:
        raise ForgeError('This graph has no bound pipeline entry', code='not_found')
    path = entry.split(':', 1)[0]
    expected = graph['sources'].get(path)
    if expected is None:
        raise ForgeError('Pipeline entry is outside this graph source closure', code='not_found')
    historical = graph_binding['basis_kind'] == 'run'
    if historical:
        from demiflow.workbench.runs.manager import source
        text = source(root, graph_binding['run_id'], path)['text']
    else:
        text = ws.source_text(root, path, expected_sha256=expected)['text']
    if hashlib.sha256(text.encode()).hexdigest() != expected:
        raise ForgeError('Pipeline source differs from graph', code='conflict')
    result = material(text, language='python', path=path, sha256=expected,
                      start_line=1, end_line=len(text.splitlines()))
    return dict(result, format='demiflow.pipeline_source.v1', graph_binding=graph_binding,
                read_only=True, source_basis='sealed' if historical else
                'report_bound' if graph_binding['basis_kind']=='report' else 'current')
