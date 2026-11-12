"""Read-only E2 logical-DAG projection over bounded graph report data.

Pure functions over already-fetched report nodes/edges: no engine writes, no
business-code imports, no live native objects. Each declared action projects
independently; any single failure degrades to that action's structured
{code, reason, retriable} diagnostic and never fails the graph response.
Absence (empty graph, no actions, unchecked source syntax) stays explicit.
"""
from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_workbench_logical_v1'
MAX_ACTIONS = 32
ACTION_BYTES = 96 * 1024
TOTAL_BYTES = 192 * 1024


def _base(*, available, reason, detail, semantic_unchecked=False, actions=(), action_count=0, truncated=False):
    return dict(format=FORMAT, available=available, reason=reason, detail=detail,
                semantic_unchecked=semantic_unchecked, actions=list(actions),
                action_count=action_count, truncated=truncated)


def unchecked():
    """Source observation has no checked semantics: explicit absence, never a guess."""
    return _base(available=False, reason='source_semantics_unchecked', semantic_unchecked=True,
                 detail='语义未检查：source 依据仅观察入口调用语法，未建立算子端口与执行关系，不投影声明图。')


def _descriptor(node):
    return dict(kind=node['kind'], api=node.get('api'), operator=node.get('operator'),
                parameters=node.get('parameters', {}), source=node.get('source'), line=node.get('line'))


def _error(action_id, code, exc):
    return dict(action_id=action_id, status='error',
                error=dict(code=code, reason=str(exc)[:512], retriable=False))


def _project_action(action_id, nodes, edges):
    from demiflow.execution.plan_facts import execution_instances
    from demiflow.execution.recovery_contract import identity
    from demiflow.planning.dag import compile_declared_dag

    skeleton = [dict(id=n['id'], role=n.get('role')) for n in nodes]
    if action_id not in {n['id'] for n in skeleton}:
        raise ValueError('action is not a declared node: ' + action_id)
    instances = execution_instances(skeleton, edges, [action_id])
    closure = {item['node_id'] for item in instances}
    sub_nodes = [dict(id=n['id'], kind=n['kind'], role=n['role']) for n in nodes if n['id'] in closure]
    sub_edges = [e for e in edges if e['from'] in closure and e['to'] in closure]
    descriptors = {n['id']: _descriptor(n) for n in nodes if n['id'] in closure}
    dag = compile_declared_dag(action_id=action_id, nodes=sub_nodes, edges=sub_edges,
                               descriptors=descriptors)
    node_map = {identity(descriptors[n['id']]): n['id'] for n in sub_nodes}
    return dict(action_id=action_id, status='projected', dag=dag, node_map=node_map)


def _project(nodes, edges, actions):
    if not nodes:
        return _base(available=False, reason='empty_graph', detail='报告没有声明节点，无法投影声明图。')
    if not actions:
        return _base(available=False, reason='no_declared_actions',
                     detail='报告未声明 action（终止操作），没有可投影的声明图。')
    known = {n['id'] for n in nodes}
    data_edges = [e for e in edges if e.get('from') in known and e.get('to') in known and 'port' in e]
    entries, used, truncated = [], 0, len(actions) > MAX_ACTIONS
    for action_id in actions[:MAX_ACTIONS]:
        if type(action_id) is not str or not 1 <= len(action_id) <= 128:
            entry = _error(None, 'invalid_action_id', 'action id is not a bounded string')
        else:
            try:
                entry = _project_action(action_id, nodes, data_edges)
                bounded_json(entry, ACTION_BYTES)
            except ForgeError as exc:
                entry = _error(action_id, 'logical_projection_budget' if exc.code == 'resource_limit'
                               else 'logical_projection_failed', exc)
            except Exception as exc:
                entry = _error(action_id, 'logical_projection_failed', exc)
        size = len(bounded_json(entry, ACTION_BYTES)) + 1
        if used + size > TOTAL_BYTES:
            truncated = True
            break
        entries.append(entry)
        used += size
    return _base(available=True, reason=None, detail=None, actions=entries,
                 action_count=len(actions), truncated=truncated)


def project(nodes, edges, actions):
    """Project per declared action over data edges; this call itself never raises."""
    try:
        return _project(nodes, edges, actions)
    except Exception as exc:
        return _base(available=False, reason='logical_projection_failed', detail=str(exc)[:512])