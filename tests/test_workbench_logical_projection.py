"""Read-only E2 logical projection: real reports, per-action isolation, absence."""
import pytest

from demiflow.planning.dag import validate_logical_dag
from demiflow.workbench.graph import logical
from demiflow.workbench.graph import model
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from test_forge_workspace import demo, isolated

ENTRY = 'flows/questions.py:select'
PARAMS = 'flows/select_a.json'


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))


def node(identity, kind, role, **extra):
    return dict(id=identity, kind=kind, role=role, api='demo.' + kind, operator=None,
                parameters={'i': identity}, source='flows/x.py', line=int(identity[1:]), **extra)


def synthetic():
    nodes = [node('n0', 'read_table', 'source'), node('n1', 'map', 'transform'),
             node('n2', 'join', 'bridge'), node('n3', 'save', 'action'),
             node('n4', 'save', 'action'), node('n5', 'map', 'transform')]
    edges = [{'from': 'n0', 'to': 'n1', 'port': 0}, {'from': 'n1', 'to': 'n2', 'port': 0},
             {'from': 'n0', 'to': 'n5', 'port': 0}, {'from': 'n2', 'to': 'n3', 'port': 0},
             {'from': 'n5', 'to': 'n3', 'port': 1}, {'from': 'n1', 'to': 'n4', 'port': 0}]
    return nodes, edges


def test_report_graph_projects_validated_logical_dag(isolated):
    report = ws.inspect_flow(isolated, ENTRY, params=PARAMS)
    assert report['status'] == 'checked'
    g = model.get(isolated, basis_kind='report', report_digest=report['digest'])
    view = g['logical']
    assert view['format'] == 'demiflow_workbench_logical_v1'
    assert view['available'] and not view['semantic_unchecked'] and not view['truncated']
    assert view['action_count'] == len(report['actions']) == len(view['actions'])
    declared = {n['id'] for n in report['nodes']}
    for entry in view['actions']:
        assert entry['status'] == 'projected'
        dag = validate_logical_dag(entry['dag'])
        assert dag['action_id'] == entry['action_id']
        assert dag['schema_version'] == 'demiflow.logical_dag.v1'
        assert dag['terminals'] and dag['sources']
        assert set(entry['node_map']) == {n['logical_node_id'] for n in dag['nodes']}
        assert set(entry['node_map'].values()) <= declared
    again = model.get(isolated, basis_kind='report', report_digest=report['digest'])
    assert again['logical'] == view


def test_source_graph_projection_is_explicit_unchecked_absence(isolated):
    g = model.get(isolated, basis_kind='source', entry=ENTRY, params=PARAMS)
    view = g['logical']
    assert not view['available'] and view['semantic_unchecked']
    assert view['reason'] == 'source_semantics_unchecked'
    assert '语义未检查' in view['detail']
    assert view['actions'] == []


def test_run_graph_projection_uses_sealed_report(isolated):
    run = runs.prepare(isolated, ENTRY, params=PARAMS)
    g = model.get(isolated, basis_kind='run', run_id=run['run_id'], attempt=1)
    view = g['logical']
    assert view['available']
    assert all(e['status'] == 'projected' for e in view['actions'])
    report_digest = runs.show(isolated, run['run_id'], attempt=1, include_report=True)['report']['digest']
    report = model.get(isolated, basis_kind='report', report_digest=report_digest)
    assert [e['dag']['dag_id'] for e in view['actions']] == [e['dag']['dag_id'] for e in report['logical']['actions']]


def test_multi_action_closure_bridge_reuse_and_shared_ancestors():
    nodes, edges = synthetic()
    view = logical.project(nodes, edges, ['n3', 'n4'])
    assert view['available'] and view['action_count'] == 2
    first, second = view['actions']
    assert first['status'] == second['status'] == 'projected'
    # n2 is a bridge: reused, its ancestors belong to its own action, not n3's.
    assert set(first['node_map'].values()) == {'n0', 'n2', 'n3', 'n5'}
    assert set(second['node_map'].values()) == {'n0', 'n1', 'n4'}
    assert any(s['logical_node_id'] == next(k for k, v in first['node_map'].items() if v == 'n2')
               for s in first['dag']['shared_ancestors'])
    assert [t['logical_node_id'] for t in second['dag']['terminals']] == [
        k for k, v in second['node_map'].items() if v == 'n4']
    assert logical.project(nodes, edges, ['n3', 'n4']) == view


def test_single_action_failure_is_isolated_to_its_diagnostic():
    nodes, edges = synthetic()
    nodes[1] = {k: v for k, v in nodes[1].items() if k != 'kind'}
    view = logical.project(nodes, edges, ['n3', 'n4'])
    assert view['available']
    by_action = {e['action_id']: e for e in view['actions']}
    assert by_action['n3']['status'] == 'projected'
    assert by_action['n4']['status'] == 'error'
    error = by_action['n4']['error']
    assert set(error) == {'code', 'reason', 'retriable'}
    assert error['code'] == 'logical_projection_failed' and error['retriable'] is False
    assert 'kind' in error['reason']


def test_unknown_and_malformed_action_ids_are_diagnosed():
    nodes, edges = synthetic()
    view = logical.project(nodes, edges, ['missing', 42, 'n3'])
    assert view['available'] and view['action_count'] == 3
    missing, malformed, ok = view['actions']
    assert missing['status'] == 'error' and 'not a declared node' in missing['error']['reason']
    assert malformed['action_id'] is None and malformed['error']['code'] == 'invalid_action_id'
    assert ok['status'] == 'projected'


def test_absence_is_explicit_for_empty_and_actionless_graphs():
    empty = logical.project([], [], [])
    assert not empty['available'] and empty['reason'] == 'empty_graph'
    nodes, edges = synthetic()
    actionless = logical.project(nodes, edges, [])
    assert not actionless['available'] and actionless['reason'] == 'no_declared_actions'
    broken = logical.project(nodes, edges, 42)
    assert not broken['available'] and broken['reason'] == 'logical_projection_failed'


def test_dangling_edges_do_not_fail_projection():
    nodes, edges = synthetic()
    view = logical.project(nodes, edges + [{'from': 'n0', 'to': 'ghost', 'port': 0}], ['n3'])
    assert view['actions'][0]['status'] == 'projected'


def test_oversized_projection_degrades_to_budget_diagnostic():
    nodes, edges = synthetic()
    nodes[0]['parameters'] = {'blob': 'x' * (256 * 1024)}
    view = logical.project(nodes, edges, ['n3', 'n4'])
    assert view['available']
    assert all(e['status'] == 'error' and e['error']['code'] == 'logical_projection_budget'
               for e in view['actions'])


def test_action_list_truncation_is_explicit():
    nodes = [node('n0', 'read_table', 'source')]
    edges = []
    actions = []
    for index in range(logical.MAX_ACTIONS + 1):
        identity = 'a' + str(index)
        nodes.append(node(identity, 'save', 'action'))
        edges.append({'from': 'n0', 'to': identity, 'port': 0})
        actions.append(identity)
    view = logical.project(nodes, edges, actions)
    assert view['available'] and view['truncated']
    assert view['action_count'] == logical.MAX_ACTIONS + 1
    assert len(view['actions']) == logical.MAX_ACTIONS
    assert all(e['status'] == 'projected' for e in view['actions'])