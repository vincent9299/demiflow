"""Real legacy projection regression plus explicitly synthetic v3 scope negatives.

The real gate executes the candidate's native pipeline and reads its HTTP graph.
The v3 wire sample only checks consumer rejection; it is not Runtime v3 evidence.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest
from playwright.sync_api import expect

from d19_fixtures import TREE, WEB, assert_tree_imports, owned_fixture
from test_forge_markdown_reading import browser


def project(payload, expression):
    node = os.environ.get('FORGE_NODE') or shutil.which('node')
    if not node:
        pytest.skip('FORGE_NODE must name Node for the production JS projection')
    source = """
const fs = require('fs'), vm = require('vm');
vm.runInThisContext(fs.readFileSync(process.argv[1], 'utf8'));
const P = globalThis.ForgeWorkbenchPresentation;
const payload = JSON.parse(fs.readFileSync(0, 'utf8'));
""" + expression
    result = subprocess.run(
        [node, '-e', source, str(WEB / 'workbench-presentation.js')],
        input=json.dumps(payload), text=True, capture_output=True, check=True,
        cwd=TREE, timeout=20)
    return json.loads(result.stdout)


MODEL = """
const choice = P.chooseGraphView(payload.graph);
const model = P.buildGraphModel(payload.graph, {choice, snapshot: payload.snapshot});
console.log(JSON.stringify(model));
"""


@pytest.mark.parametrize('owned_fixture', [{'execution_format': 'legacy'}], indirect=True)
def test_real_legacy_graph_and_sealed_time(owned_fixture, browser):
    fx = owned_fixture
    assert_tree_imports()
    run = fx.run_facts
    assert run['status'] == 'succeeded'
    assert run['outputs'][0]['result']['ref']['row_count'] == 2
    code, graph = fx.api('/api/graph?basis_kind=run&run_id=' + run['run_id']
                         + '&attempt=' + str(run['attempt']))
    assert code == 200, graph
    entry = graph['logical']['actions'][0]
    assert entry['dag']['schema_version'] == 'demiflow.logical_dag.v1'
    assert len(entry['dag']['nodes']) == 7 and len(entry['dag']['edges']) == 6
    # Native linear-chain source identity intentionally differs from node ID.
    assert entry['dag']['sources'][0]['source_id'] not in {
        item['logical_node_id'] for item in entry['dag']['nodes']}
    projection = project({'graph': graph}, MODEL)
    assert projection['execution']['format'] == 'legacy', projection
    assert len(projection['nodes']) == 7 and len(projection['edges']) == 6
    fx.record('projection-real-legacy', dict(
        graph=graph, model=projection, import_roots=fx.import_roots,
        evidence='Native legacy run and real HTTP graph; not Runtime v3'))

    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.get_by_role('button', name='运行记录', exact=True).click()
        page.locator('.wb-run-record[data-run="' + run['run_id'] + '"]').click()
        expect(page.locator('.wb-node')).to_have_count(7)
        stamp = page.locator('.wb-action-bar .wb-sealed-time')
        expect(stamp).to_be_visible()
        expect(stamp).to_contain_text('封存于 ')
        assert stamp.evaluate('(el) => Date.parse(el.dateTime)') == entry['sealed_at']
        expect(stamp).not_to_contain_text('Invalid Date')
        # Legacy terminal still consumes its original action-level commit fact.
        expect(page.locator('.wb-node').filter(has_text='已提交并核验')).to_have_count(1)
        fx.shot(page, 'projection-real-legacy-restored')
        assert not log['errors'], log['errors']


def v3_payload(*, terminal=False):
    sample = json.loads((TREE / 'docs/workbench-demo-alignment-20261114/samples/runtime-v3-wire.json').read_text())
    assert sample['not_runtime_evidence'] is True
    entry, binding = sample['entry'], sample['binding']
    report_digest = 'd' * 64  # A run's manifest digest is a different identity.
    node = (next(item for item in entry['dag']['nodes']
                 if item['occurrence_id'] == entry['dag']['terminals'][0])
            if terminal else entry['dag']['nodes'][1])
    report = 'a' * 16
    bridge = dict(logical_node_id=node['logical_node_id'], occurrence_id=node['occurrence_id'],
                  report_node_id=report, report_occurrence=0, mode='execute')
    scope = dict(action_id=entry['action_id'], dag_id=entry['dag']['dag_id'],
                 plan_id=entry['plan_id'], plan_revision=entry['physical']['plan_revision'])
    entry['identity_bridge'] = dict(available=True, bridge=dict(
        schema='demiflow.identity_bridge.v1', action_id=entry['action_id'],
        dag_id=entry['dag']['dag_id'], report_digest=report_digest, entries=[bridge]))
    entry['node_observations'] = dict(available=True, attempt=binding['attempt'],
                                      **scope, entries=[dict(bridge, availability='observed')])
    row = dict(**scope, occurrence_id=node['occurrence_id'], node_id=report,
               occurrence=0, state='succeeded', timing_scope='node')
    snapshot = dict(format='demiflow_forge_runtime_v1', availability='available',
                    run_id=binding['run_id'], attempt=binding['attempt'],
                    observations=[row], outputs=[])
    if terminal:
        snapshot['observations'] = []
        snapshot['outputs'] = [dict(**scope, occurrence_id=node['occurrence_id'],
                                    status='committed', result={'verified': True})]
    return dict(graph=dict(binding=binding, report_digest=report_digest, logical=dict(actions=[entry]), nodes=[], edges=[]),
                snapshot=snapshot, occurrence_id=node['occurrence_id'])


def selected_state(payload):
    model = project(payload, MODEL)
    return next(card['state'] for card in model['nodes']
                if card['selection']['occurrenceId'] == payload['occurrence_id'])


@pytest.mark.parametrize('terminal', [False, True], ids=['observation', 'terminal-output'])
@pytest.mark.parametrize('field,value', [
    ('plan_id', 'f' * 64), ('dag_id', 'e' * 64), ('plan_revision', 99),
    ('occurrence_id', 'd' * 64), ('action_id', 'other-action'),
    ('attempt', 9), ('run_id', 'another-run'),
    ('plan_id', None), ('dag_id', None), ('plan_revision', None), ('occurrence_id', None),
])
def test_v3_node_state_rejects_unverified_rows(terminal, field, value):
    payload = v3_payload(terminal=terminal)
    assert selected_state(payload)['tone'] == 'success'
    row = payload['snapshot']['outputs' if terminal else 'observations'][0]
    if value is None:
        del row[field]
    else:
        row[field] = value
    assert selected_state(payload)['tone'] == 'neutral'


def test_v3_current_wrapper_does_not_certify_old_rows_or_retained_output():
    payload = v3_payload()
    row = payload['snapshot']['observations'][0]
    for field in ('dag_id', 'plan_id', 'plan_revision', 'occurrence_id'):
        del row[field]
    assert selected_state(payload) == {'label': '观测凭据不完整', 'tone': 'neutral'}

    payload = v3_payload(terminal=True)
    payload['snapshot']['outputs'][0]['retained_from_attempt'] = 0
    assert selected_state(payload)['tone'] == 'neutral'

    payload = v3_payload()
    payload['graph']['logical']['actions'][0]['node_observations']['entries'][0]['report_occurrence'] = 1
    assert selected_state(payload)['tone'] == 'neutral'


@pytest.mark.parametrize('version', ['v3', 'v4'])
def test_occurrence_scoped_endpoint_validation_remains_strict(version):
    payload = v3_payload()
    entry = payload['graph']['logical']['actions'][0]
    assert len(project(payload, MODEL)['nodes']) == len(entry['dag']['nodes'])
    entry['version'] = version
    entry['dag']['schema_version'] = 'demiflow.dataflow_graph.' + version
    payload['graph']['logical']['actions'].insert(0, {'status': 'projected', 'action_id': 'earlier'})
    rendered = project(payload, MODEL)
    assert rendered['execution']['format'] == version
    assert rendered['execution']['actionId'] == entry['action_id']
    del entry['dag']['edges'][0]['from']['occurrence_id']
    model = project(payload, MODEL)
    assert model['nodes'] == []
    assert model['choice']['reason'] == 'v3_endpoint_identity_missing'


def test_fit_control_node_in_v4_preview_has_no_row_traits():
    payload = v3_payload()['graph']
    dag = payload['logical']['actions'][0]['dag']
    dag['schema_version'] = 'demiflow.dataflow_graph.v4'
    node = dag['nodes'][1]
    node.pop('traits', None)
    preview = dict(format='demiflow.plan_preview.v1', available=True, scope='first_action',
                   entry_completed=False, basis={'digest': 'a' * 64}, dag=dag)
    expression = """
try { console.log(JSON.stringify(P.buildPlanPreviewModel(payload).nodes.length)); }
catch (error) { console.log(JSON.stringify(error.message)); }
"""
    node['op_kind'] = 'fit'
    assert project(preview, expression) == len(dag['nodes'])
    node['op_kind'] = 'MapBatchesOp'
    assert '节点身份、端口或声明无效' in project(preview, expression)


@pytest.mark.parametrize('edge_fields', [{'step': 4}, {'kind': 'model_manifest', 'step': 0},
                                         {'kind': 'fixed_dataset', 'step': 1}])
def test_v4_projection_rejects_invalid_step_edges(edge_fields):
    payload = v3_payload()
    entry = payload['graph']['logical']['actions'][0]
    entry['version'] = 'v4'
    entry['dag']['schema_version'] = 'demiflow.dataflow_graph.v4'
    entry['dag']['edges'][0].update(edge_fields)
    rendered = project(payload, MODEL)
    assert not rendered['nodes'] and rendered['choice']['reason'] == 'invalid_topology'


@pytest.mark.parametrize('digest', [None, 'missing', 'f' * 64, 'manifest'])
def test_bridge_requires_the_original_report_digest(digest):
    payload = v3_payload()
    assert selected_state(payload)['tone'] == 'success'
    graph = payload['graph']
    if digest == 'missing':
        del graph['report_digest']
    else:
        graph['report_digest'] = graph['binding']['basis_digest'] if digest == 'manifest' else digest
    rendered = project(payload, MODEL)
    card = next(card for card in rendered['nodes']
                if card['selection']['occurrenceId'] == payload['occurrence_id'])
    assert card['state'] == {'label': '未桥接 · 无观测关联', 'tone': 'neutral'}
    details = project(dict(payload, selection=card['selection']),
                      'console.log(JSON.stringify(P.buildNodeDetails(payload.graph, payload.selection)));')
    assert details['sourceAvailable'] is False
