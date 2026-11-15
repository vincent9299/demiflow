"""Immutable bounded graph projections; neither a checker nor an execution plan."""
from __future__ import annotations

import ast
import hashlib

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.forge.source import Definition
from demiflow.forge.source import InspectionError
from demiflow.forge.source import SourceTree
from demiflow.forge.platform_environment import read_bounded

MAX_NODES = 256
MAX_EDGES = 512
MAX_BYTES = 1024**2
DEFAULT_NODES = 128
DEFAULT_EDGES = 256


def _scope_nodes(items):
    """Observe entry syntax without treating nested scopes as executed calls."""
    stack=list(reversed(items))
    while stack:
        node=stack.pop()
        yield node
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef,ast.Lambda)):
            continue
        stack.extend(reversed(list(ast.iter_child_nodes(node))))


def _declared(root, control, entry, params):
    ws.validate_entry(root, entry)
    if entry in control.get('entries', {}):
        if params is not None and params not in control['entries'][entry]:
            raise ForgeError('Parameters are not registered for this entry', code='conflict')
        return []
    from demiflow.workbench.projects.discovery import discover
    project = discover(root, control)
    entries = project['entries']
    if not any(e['entry'] == entry for e in entries):
        raise ForgeError('Entry is not declared by this project', code='not_found')
    if params is not None:
        raise ForgeError('Original entry has no native parameter registration', code='operation_rejected')
    path=next(e['path'] for e in entries if e['entry']==entry)
    return [d for d in project['diagnostics'] if d.get('path')==path or not d.get('path')]


def _source(root, control, entry, params):
    from demiflow.workbench.worker import operation
    return operation('workbench_source',root,entry=entry,params=params,expected_workspace=content_digest(control))


def _entry_definition(tree, module, path, symbol, diagnostics):
    try:
        namespace = tree.load(module)
        return namespace, tree.resolve(namespace.get(symbol))
    except InspectionError as exc:
        # Compute admission can reject an import/default/decorator before it
        # reaches the declared function. Observing that function's syntax does
        # not require compute admission. Never fall back after a read, parse,
        # resource or namespace failure that did not bind the entry bytes.
        if exc.diagnostic['code'] in {'FL-01', 'FL-10'} or path not in tree.files:
            raise
        failure = exc.diagnostic
    # A local dependency can fail after load has cached the entry namespace.
    # Reuse that exact AST; clearing the exception alone does not release it.
    # With no entry cache, leaving except releases the failed partial entry AST
    # before replacing it from the same bytes. Its nodes were already charged
    # to tree.nodes, alongside every retained dependency (cap: ast_nodes).
    tree.guard()
    if module in tree.modules:
        definition = tree.modules[module].get(symbol)
        if (not isinstance(definition, Definition) or definition.module != module
                or not isinstance(definition.node, (ast.FunctionDef, ast.AsyncFunctionDef))
                or definition.node.name != symbol):
            tree.fail('FL-02', 'Declared function has no unique source definition', path=path, incomplete=True)
    else:
        raw = read_bounded(tree.root / path, tree.limits['file_bytes'])
        if hashlib.sha256(raw).hexdigest() != tree.files[path]:
            raise ForgeError('Entry source changed while observing syntax', code='conflict')
        syntax = ast.parse(raw, filename=path)
        count = sum(1 for _ in ast.walk(syntax))
        if count > tree.limits['file_nodes'] or count > tree.limits['ast_nodes']:
            tree.fail('FL-10', 'Entry AST budget exceeded', path=path, incomplete=True)
        definitions = [node for node in syntax.body
                       if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == symbol]
        if len(definitions) != 1:
            tree.fail('FL-02', 'Declared function has no unique source definition', path=path, incomplete=True)
        definition = Definition(module, definitions[0])
    tree.guard()
    diagnostics.extend((failure, dict(cause='unknown', code='source_syntax_only', scope='flow',
        detail='仅观察已绑定入口的调用语法；依赖未获计算准入，不推定算子实现、输入输出类型或执行关系。')))
    # Keep resolution empty: a syntax observation cannot establish that a
    # global/imported name refers to its apparent definition at runtime.
    return {}, definition


def _source_tree(root, *, entry, params, expected_workspace):
    root,control,_ = ws.load_workspace(root)
    if content_digest(control)!=expected_workspace:
        raise ForgeError('Workspace changed while reading the graph',code='conflict')
    declared_diagnostics = _declared(root, control, entry, params)
    tree = SourceTree(root, {'files':16, 'total_bytes':4*1024**2, 'nodes':256, 'edges':512})
    path, symbol = entry.split(':')
    module = path[:-3].replace('/', '.')
    nodes, edges, diagnostics = [], [], list(declared_diagnostics)
    node_count=edge_count=0
    try:
        namespace, definition = _entry_definition(tree, module, path, symbol, diagnostics)
        if not isinstance(definition, Definition) or not isinstance(definition.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            raise ForgeError('Declared function is unavailable', code='not_found')
        function = definition.node
        scope=list(_scope_nodes(function.body))
        calls = sorted((n for n in scope if isinstance(n, ast.Call)),
                       key=lambda n:(n.lineno, n.col_offset, n.end_lineno, n.end_col_offset))
        node_count=len(calls)
        ids = {id(n):'s'+content_digest([entry,n.lineno,n.col_offset,n.end_lineno,n.end_col_offset])[:16] for n in calls[:MAX_NODES]}
        stores={}
        for item in scope:
            if isinstance(item,ast.Name) and isinstance(item.ctx,ast.Store):stores[item.id]=stores.get(item.id,0)+1
        local_names=set(stores)|{arg.arg for arg in (*function.args.posonlyargs,*function.args.args,*function.args.kwonlyargs)}
        for arg in (function.args.vararg,function.args.kwarg):
            if arg:local_names.add(arg.arg)
        for item in scope:
            if isinstance(item,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):local_names.add(item.name)
            elif isinstance(item,(ast.Import,ast.ImportFrom)):
                local_names.update(a.asname or a.name.split('.')[0] for a in item.names)
        assigned = {}
        for statement in function.body:
            if isinstance(statement, (ast.Assign, ast.AnnAssign)) and isinstance(statement.value, ast.Call):
                targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                for target in targets:
                    if isinstance(target, ast.Name) and stores.get(target.id)==1:
                        assigned.setdefault(target.id, []).append((statement.lineno, statement.col_offset, ids.get(id(statement.value))))
        if any(count>1 for count in stores.values()) or any(isinstance(n,(ast.If,ast.For,ast.AsyncFor,ast.While,ast.Try,ast.With,ast.AsyncWith,ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef,ast.Lambda)) for n in scope):
            diagnostics.append(dict(cause='unknown',code='source_control_flow_unchecked',scope='flow',
                                    detail='仅显示可证明的语法引用；分支、循环、重赋值与嵌套作用域未建立运行关系。'))
        for call in calls[:MAX_NODES]:
            node_id = ids[id(call)]
            callbacks = []
            implementation=None
            if isinstance(call.func,ast.Name) and call.func.id not in local_names:
                try:
                    target=tree.resolve(namespace.get(call.func.id))
                    if isinstance(target,Definition):
                        implementation=dict(module=target.module,name=target.node.name,line=target.node.lineno,
                                            end_line=target.node.end_lineno,column=target.node.col_offset)
                except InspectionError as exc:diagnostics.append(exc.diagnostic)
            for argument in list(call.args) + [k.value for k in call.keywords]:
                if len(callbacks)>=16:
                    break
                if not isinstance(argument, ast.Name) or argument.id in local_names:
                    continue
                try:
                    target = tree.resolve(namespace.get(argument.id))
                    if isinstance(target, Definition):
                        callbacks.append(dict(module=target.module, name=target.node.name, line=target.node.lineno,
                                              end_line=target.node.end_lineno, column=target.node.col_offset))
                except InspectionError as exc:
                    diagnostics.append(exc.diagnostic)
            nodes.append(dict(id=node_id, api=ast.unparse(call.func)[:256], kind='source_call',
                              role='unknown', result='unknown', source=path, line=call.lineno,
                              column=call.col_offset, end_line=call.end_lineno,
                              operator=implementation or (callbacks[0] if callbacks else None),
                              callbacks={str(i):v for i,v in enumerate(callbacks)},
                              parameters={}, fields={}, parameter_origins={}, runtime='not_observed'))
            references = set()
            for item in _scope_nodes([call]):
                if isinstance(item, ast.Call) and item is not call and id(item) in ids:
                    references.add(ids[id(item)])
                elif isinstance(item, ast.Name):
                    prior = sorted(v for v in assigned.get(item.id, [])
                                   if v[2] and (v[0],v[1]) < (call.lineno,call.col_offset))
                    if prior:
                        references.add(prior[-1][2])
            for origin in sorted(references - {node_id}):
                edge_count+=1
                if len(edges) < MAX_EDGES:
                    edges.append({'from':origin,'to':node_id,'type':'reference'})
        if len(calls) > MAX_NODES or edge_count>MAX_EDGES:
            diagnostics.append(dict(cause='resource',code='source_display_limit',scope='flow',
                                    detail='Source graph is clipped; semantic relationships remain unchecked'))
        tree.unchanged()
    except InspectionError as exc:
        diagnostics.append(exc.diagnostic)
    parameter_sha = None
    if params:
        try:
            raw = read_bounded(ws.validate_parameter_path(root, params), 65536)
            parameter_sha = hashlib.sha256(raw).hexdigest()
        except (OSError,ValueError) as exc:
            diagnostics.append(dict(cause='unknown',code='parameters_unavailable',scope='parameters',detail=str(exc)[:512]))
    basis = dict(root=str(root),entry=entry, sources=dict(tree.files), workspace=content_digest(control),
                 params=params, parameter_sha256=parameter_sha)
    return dict(nodes=nodes, edges=edges, diagnostics=diagnostics, sources=dict(tree.files),
                basis_digest=content_digest(basis), semantic='unchecked', status='not_checked',
                total_nodes=node_count,total_edges=edge_count,parameter_sha256=parameter_sha)


def _join_sealed_observations(root, run_id, attempt, logical_view):
    """Annotate sealed actions with one bounded observation page (availability only).

    The full per-node records stay with the existing run-observations query loaded
    on selection; here each bridge entry only gains observed/not_in_page for the
    selected attempt. A page absence never implies the run never observed a node.
    Any single action's observation failure degrades honestly, not the whole graph.
    """
    from demiflow.workbench.runs import facts
    from demiflow.workbench.runs import view as run_view
    for entry in logical_view.get('actions', []):
        bridge = entry.get('identity_bridge')
        if entry.get('status') != 'sealed' or not bridge or not bridge.get('available'):
            continue
        action_id = entry['action_id']
        # A v3 entry already carries the build_plan_view per-occurrence projection
        # (observed/not_observed, never inferred). Keep it as the plan-level authority
        # so the attempt-scoped bridge join below never replaces or re-derives it.
        if entry.get('version') == 'v3' and 'node_observations' in entry:
            entry['plan_node_observations'] = entry['node_observations']
        v3 = entry.get('version') == 'v3'
        scope = (dict(plan_id=entry['plan_id'], dag_id=entry['dag']['dag_id'],
                      plan_revision=entry['physical']['plan_revision']) if v3 else {})
        try:
            page = run_view.observations(root, run_id, attempt=attempt, action_id=action_id, limit=64)
            if page.get('availability') == 'unsupported':
                entry['node_observations'] = dict(available=False, reason='historical_observation_contract')
                continue
            joined = facts.join_node_observations(bridge['bridge'], page, action_id=action_id, attempt=attempt, **scope)
            wrapper = dict(
                available=True, attempt=attempt, has_more=bool(page.get('has_more')),
                entries=[dict(logical_node_id=e['logical_node_id'], occurrence_id=e['occurrence_id'],
                              report_node_id=e['report_node_id'], report_occurrence=e['report_occurrence'],
                              availability=e['availability']) for e in joined])
            if v3:
                # This-run scope credentials: the server verified the page belongs to
                # this sealed action/dag/plan/revision before promoting any observation.
                wrapper.update(action_id=action_id, dag_id=scope['dag_id'],
                               plan_id=scope['plan_id'], plan_revision=scope['plan_revision'])
            entry['node_observations'] = wrapper
            bounded_json(entry, facts.ACTION_BYTES)
        except ForgeError as exc:
            unavailable = dict(available=False,
                               reason='observation_plan_scope_unverified' if v3 else 'observation_page_unavailable',
                               detail=str(exc)[:512])
            if v3:
                unavailable.update(action_id=action_id, dag_id=scope['dag_id'],
                                   plan_id=scope['plan_id'], plan_revision=scope['plan_revision'])
            entry['node_observations'] = unavailable


def get(root, *, basis_kind, entry=None, params=None, report_digest=None,
        run_id=None, attempt=None, node_id=None, node_offset=0):
    root, control, _ = ws.load_workspace(root)
    key = ws.workspace_id(root, control)
    if basis_kind not in {'source','report','run'}:
        raise ForgeError('Unknown graph basis', code='invalid_request')
    if type(node_offset) is not int or not 0 <= node_offset <= 65536:
        raise ForgeError('Invalid node index offset', code='invalid_request')
    report, run, changes = None, None, []
    if basis_kind == 'source':
        if not entry or report_digest is not None or run_id is not None or attempt is not None:
            raise ForgeError('Source graph requires only an entry and optional parameters', code='invalid_request')
        observed = _source(root, control, entry, params)
        nodes, edges = observed['nodes'], observed['edges']
        sources, diagnostics = observed['sources'], observed['diagnostics']
        digest, semantic, status, fresh = observed['basis_digest'], 'unchecked', 'not_checked', True
        bound_parameters = None
    else:
        if entry is not None or params is not None:
            raise ForgeError('Report and run graph identities cannot use current entry/parameters', code='invalid_request')
        if basis_kind == 'report':
            if not report_digest or run_id is not None or attempt is not None:
                raise ForgeError('Report graph requires its digest', code='invalid_request')
            result = ws.report_status(root, report_digest)
            report, changes = result['report'], result['changes']
            fresh = result['verification']['complete'] and not changes
            status = result['status']
            digest = report['digest']
        else:
            if not run_id or report_digest is not None or type(attempt) is not int:
                raise ForgeError('Run graph requires an exact run and attempt', code='invalid_request')
            from demiflow.workbench.runs import manager as runs
            run = runs.show(root, run_id, include_report=True, attempt=attempt,compact=True)
            if run['attempt'] != attempt:
                raise ForgeError('Run attempt differs', code='conflict')
            report = run['report']
            digest, status, fresh = run['digest'], run['status'], True
        entry = report.get('entry', report.get('binding',{}).get('entry'))
        params = report.get('binding',{}).get('parameter_path')
        nodes = report.get('nodes', [])
        edges = [{**e,'type':'data'} for e in report.get('edges', [])]
        edges += [{**e,'type':'value'} for e in report.get('value_edges', [])]
        edges += [{**e,'type':'action'} for e in report.get('action_edges', [])]
        sources = report.get('binding',{}).get('sources', {})
        diagnostics = report.get('diagnostics', [])
        semantic = 'complete' if report['status']=='checked' else 'partial' if nodes else 'unavailable'
        bound_parameters = report.get('binding',{}).get('parameters', {})
    from demiflow.workbench.graph import logical
    if basis_kind == 'source':
        logical_view = logical.unchecked()
    else:
        logical_view = logical.project(nodes, report.get('edges', []), report.get('actions', []))
        if basis_kind == 'run':
            from demiflow.workbench.runs import facts
            logical_view = facts.overlay_sealed(root, run_id, logical_view, report_digest=report['digest'])
            # Surface which sealed projection versions are present (v3 standard
            # plan-view vs historical v1/v2) without re-deriving any v3 field as v1.
            logical_view['sealed_versions'] = sorted({
                e.get('version', 'v1') for e in logical_view.get('actions', [])
                if e.get('binding_level') == 'sealed'})
            _join_sealed_observations(root, run_id, attempt, logical_view)
    binding = dict(workspace_id=key, basis_kind=basis_kind, basis_digest=digest, entry=entry,
                   params=params, run_id=run_id, attempt=attempt)
    total_nodes, total_edges = len(nodes), len(edges)
    index_nodes = nodes[node_offset:node_offset+64]
    node_index = dict(offset=node_offset, total=len(nodes),
                      next_offset=node_offset+64 if node_offset+64<len(nodes) else None,
                      nodes=[dict(id=n['id'], label=str((n.get('operator') or {}).get('name') or n['api'])[:80],
                                  source=str(n['source'])[:160], line=n.get('line')) for n in index_nodes])
    if basis_kind=='source':
        total_nodes,total_edges=observed['total_nodes'],observed['total_edges']
    if node_id is not None:
        if not any(n['id']==node_id for n in nodes):
            raise ForgeError('Node is outside this graph', code='not_found')
        scope = {node_id}
        for edge in edges:
            if edge['from']==node_id or edge['to']==node_id:
                scope.update((edge['from'],edge['to']))
        nodes = [n for n in nodes if n['id'] in scope]
    display_nodes = MAX_NODES if node_id else DEFAULT_NODES
    display_edges = MAX_EDGES if node_id else DEFAULT_EDGES
    clipped = total_nodes>display_nodes or total_edges>display_edges or node_id is not None and len(nodes)<total_nodes
    visible = nodes[:display_nodes]
    ids = {n['id'] for n in visible}
    projected_edges = [e for e in edges if e['from'] in ids and e['to'] in ids][:display_edges]
    from demiflow.workbench.graph.parameters import public_model
    parameters = public_model(root, control, entry, params, nodes,
                              readonly=basis_kind=='run' or not fresh,
                              sealed=bound_parameters if basis_kind=='run' or basis_kind=='report' and not fresh else None,
                              expected_document=bound_parameters if basis_kind=='report' and fresh else None)
    if basis_kind=='source' and parameters['sha256']!=observed['parameter_sha256']:
        raise ForgeError('Parameters changed while projecting the graph',code='conflict')
    public_diagnostics=[]
    diagnostic_bytes=0
    for diagnostic in diagnostics[:256]:
        size=len(bounded_json(diagnostic,8*1024**2))+1
        if diagnostic_bytes+size>128*1024:break
        public_diagnostics.append(diagnostic);diagnostic_bytes+=size
    result = dict(format='demiflow_workbench_graph_v1', binding=binding, status=status,
                  report_digest=report['digest'] if report is not None else None,
                  freshness=dict(current=fresh,changes=changes),
                  completeness=dict(semantic=semantic,display='clipped' if clipped else 'complete',
                                    total_nodes=total_nodes,total_edges=total_edges,scope_node=node_id),
                  nodes=visible,edges=projected_edges,node_index=node_index,diagnostics=public_diagnostics,
                  diagnostics_truncated=len(public_diagnostics)<len(diagnostics),
                  diagnostic_count=len(diagnostics),sources=sources,parameters=parameters,
                  logical=logical_view,
                  coverage=report.get('coverage',{}) if report else {'wiring':'not_checked'})
    # Account for each record once. Repeatedly serializing the whole graph while
    # dropping one node would turn a display budget into quadratic work.
    result['nodes']=[];result['edges']=[]
    used=len(bounded_json(result,MAX_BYTES))
    for node in visible:
        size=len(bounded_json(node,8*1024**2))+1
        if used+size>MAX_BYTES-65536:
            result['completeness']['display']='clipped'
            continue
        result['nodes'].append(node);used+=size
    ids={n['id'] for n in result['nodes']}
    for edge in projected_edges:
        size=len(bounded_json(edge,MAX_BYTES))+1
        if edge['from'] in ids and edge['to'] in ids and used+size<MAX_BYTES-1024:
            result['edges'].append(edge);used+=size
        else:
            result['completeness']['display']='clipped'
    bounded_json(result,MAX_BYTES)
    return result


def _arguments(binding):
    required={'workspace_id','basis_kind','basis_digest','entry','params','run_id','attempt'}
    if type(binding) is not dict or set(binding)!=required:
        raise ForgeError('Invalid graph binding',code='invalid_request')
    kind=binding['basis_kind']
    args={'basis_kind':kind}
    if kind=='source':args.update(entry=binding['entry'],params=binding['params'])
    elif kind=='report':args['report_digest']=binding['basis_digest']
    elif kind=='run':args.update(run_id=binding['run_id'],attempt=binding['attempt'])
    else:raise ForgeError('Unknown graph basis',code='invalid_request')
    return args


def _match(binding, observed):
    if observed != binding:
        raise ForgeError('Graph basis changed; keep the draft and reload',code='conflict')


def from_binding(root, binding):
    result=get(root,**_arguments(binding))
    _match(binding, result['binding'])
    return result


def _resolve_bridge_node(target, report, logical_node_id, occurrence_id):
    """Locate the sealed report node for an execution node via the identity bridge.

    Never guesses: an unbridged node is not_found, and a logical node with several
    bridged instances requires an explicit occurrence rather than picking the first.
    """
    from demiflow.workbench.runs import facts
    matches = []
    for action_id in report.get('actions', []):
        if type(action_id) is not str:
            continue
        sealed = facts.read_sealed_plan(target, action_id)
        if not sealed.get('available'):
            continue
        bridge = facts.read_identity_bridge(target, action_id, dag_id=sealed['dag_id'], report_digest=report['digest'])
        if not bridge.get('available'):
            continue
        for entry in bridge['bridge']['entries']:
            if entry['logical_node_id'] == logical_node_id and (occurrence_id is None or entry['occurrence_id'] == occurrence_id):
                matches.append(entry['report_node_id'])
    unique = set(matches)
    if len(unique) == 1:
        return next(iter(unique))
    if len(unique) > 1:
        raise ForgeError('Logical node has multiple bridged instances; select an occurrence', code='conflict')
    raise ForgeError('Execution node is not bridged to a sealed report node', code='not_found')


def _resolve_bridge_action(target, report, logical_node_id, occurrence_id):
    """Return the unique sealed action that owns a logical/occurrence pair.

    Source/code provenance must bind to the action that actually contains the
    selected occurrence, never the first v3 action encountered while traversing
    the report. Ambiguous membership (several actions) requires an explicit
    occurrence; an unbridged pair is not_found. Never guesses.
    """
    from demiflow.workbench.runs import facts
    actions = set()
    for action_id in report.get('actions', []):
        if type(action_id) is not str:
            continue
        sealed = facts.read_sealed_plan(target, action_id)
        if not sealed.get('available'):
            continue
        bridge = facts.read_identity_bridge(target, action_id, dag_id=sealed['dag_id'], report_digest=report['digest'])
        if not bridge.get('available'):
            continue
        for entry in bridge['bridge']['entries']:
            if entry['logical_node_id'] == logical_node_id and (occurrence_id is None or entry['occurrence_id'] == occurrence_id):
                actions.add(action_id)
    if len(actions) == 1:
        return next(iter(actions))
    if len(actions) > 1:
        raise ForgeError('Logical node belongs to several sealed actions; select an occurrence', code='conflict')
    raise ForgeError('Execution node is not bridged to a sealed report node', code='not_found')


def bound_source(root, binding, node_id, *, include_parameters=True, logical_node_id=None, occurrence_id=None):
    """Read one immutable node's provenance without rechecking the project.

    A report is evidence of its own revision, not a freshness assertion. Source
    bytes are separately SHA-checked by source_view. Parameter mutations still
    use from_binding(), which performs the current native verification. For a run,
    an execution (logical) node reference is resolved to its sealed report node
    through the identity bridge before reusing the original sealed-source read;
    current project files never replace historical code.
    """
    _arguments(binding)
    root, control, _ = ws.load_workspace(root)
    if binding['basis_kind'] == 'source':
        observed=_source(root,control,binding['entry'],binding['params'])
        current=dict(workspace_id=ws.workspace_id(root,control),basis_kind='source',
                     basis_digest=observed['basis_digest'],entry=binding['entry'],params=binding['params'],run_id=None,attempt=None)
        _match(binding,current)
        node=next((n for n in observed['nodes'] if n['id']==node_id),None)
        if node is None:raise ForgeError('Node is outside this graph',code='not_found')
        parameters={'fields':[]}
        if include_parameters:
            from demiflow.workbench.graph.parameters import public_model
            parameters=public_model(root,control,binding['entry'],binding['params'],[node],readonly=True)
            if parameters['sha256']!=observed['parameter_sha256']:
                raise ForgeError('Parameters changed while reading node configuration',code='conflict')
        return dict(binding=current,nodes=[node],sources=observed['sources'],parameters=parameters)
    key = ws.workspace_id(root, control)
    sealed_code_pin_ref = None
    if binding['basis_kind'] == 'report':
        report = ws.load_report(root, binding['basis_digest'])
        digest = report['digest']
    else:
        from demiflow.workbench.runs import manager as runs
        target, value = runs._load(root, binding['run_id'], detail='identity', attempt=binding['attempt'])
        if value['state']['attempt'] != binding['attempt']:
            raise ForgeError('Run attempt differs', code='conflict')
        report = runs._report(target, value['manifest'])
        digest = value['manifest']['digest']
        if logical_node_id is not None:
            node_id = _resolve_bridge_node(target, report, logical_node_id, occurrence_id)
        # Surface the original v3 sealed code identity (never current source) from
        # the unique sealed action that owns the selected occurrence — not the first
        # v3 action traversed. v3 fields are read, not recomputed; an unbridged node
        # is not_found above, so source provenance is never guessed.
        from demiflow.workbench.runs import facts
        sealed_code_pin_ref = None
        if logical_node_id is not None:
            owner = _resolve_bridge_action(target, report, logical_node_id, occurrence_id)
            sealed = facts.read_sealed_plan(target, owner)
            if sealed.get('available') and sealed.get('version') == 'v3':
                sealed_code_pin_ref = sealed['plan_view'].get('code_pin_ref')
    evidence = report.get('binding', {})
    observed = dict(workspace_id=key, basis_kind=binding['basis_kind'], basis_digest=digest,
                    entry=report.get('entry', evidence.get('entry')), params=evidence.get('parameter_path'),
                    run_id=binding['run_id'], attempt=binding['attempt'])
    _match(binding, observed)
    node = next((n for n in report.get('nodes', []) if n['id'] == node_id), None)
    if node is None:
        raise ForgeError('Node is outside this graph', code='not_found')
    from demiflow.workbench.graph.parameters import public_model
    return dict(binding=observed, nodes=[node], sources=evidence.get('sources', {}),
                sealed_code_pin_ref=sealed_code_pin_ref,
                parameters=public_model(root, control, observed['entry'], observed['params'], [node],
                                        readonly=True, sealed=evidence.get('parameters', {})))
