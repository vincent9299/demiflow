"""One genuine Workbench run: repeated operators, union, two v3 actions.

No engine, seal, bridge, observation, or source double. The final JS projection
consumes the real bounded Workbench DTO and snapshot. All data is test-owned.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

from demiflow.workbench.graph import model
from demiflow.workbench.runs import manager as runs, view

from test_forge_runs import custom, execute


TREE = Path(__file__).resolve().parents[1]
FLOW = '''
from operators.question_rules import normalize_question

def run(*, source, target):
    # historical-multiaction-original
    rows = data.read_lance(source['uri'], version=source['version'], columns=['case_id', 'question'])
    first = rows.map(normalize_question, inputs={'text': 'question'}, output='normalized_question', fn_kwargs={'max_chars': 4096})
    repeated = first.map(normalize_question, inputs={'text': 'question'}, output='normalized_question', fn_kwargs={'max_chars': 4096})
    fixed = repeated.union(repeated).materialize()
    return fixed.select_columns(['case_id', 'normalized_question']).write_lance(target['uri'], mode='create', return_receipt=True)
'''


def _assert_imports():
    import demiflow
    import demiflow.execution.dataflow as runtime
    import demiflow.execution.dataflow_seal as seals
    imports = {}
    for module in (demiflow, runtime, seals, runs, view, model):
        path = Path(module.__file__).resolve()
        assert path.is_relative_to(TREE), (module.__name__, str(path))
        imports[module.__name__] = str(path)
    return imports


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _project(graph, snapshot):
    node = os.environ.get('FORGE_NODE') or shutil.which('node')
    assert node, 'FORGE_NODE must name the production JS projection runtime'
    script = r'''
const fs = require('fs'), vm = require('vm');
vm.runInThisContext(fs.readFileSync(process.argv[1], 'utf8'));
const P = globalThis.ForgeWorkbenchPresentation;
const {graph, snapshot} = JSON.parse(fs.readFileSync(0, 'utf8'));
const models = [];
let selection = null;
for (const action of graph.logical.actions) {
  const choice = P.chooseGraphView(graph, {actionId: action.action_id});
  const model = P.buildGraphModel(graph, {choice, snapshot, selection});
  models.push(model);
  selection = model.nodes.find(card => card.state.tone === 'success')?.selection || model.nodes[0]?.selection;
}
// Return to the first action carrying the second action's original selection.
const first = graph.logical.actions[0];
models.push(P.buildGraphModel(graph, {
  choice: P.chooseGraphView(graph, {actionId: first.action_id}), snapshot, selection
}));
console.log(JSON.stringify(models));
'''
    done = subprocess.run([node, '-e', script, str(TREE / 'demiflow/workbench/web/workbench-presentation.js')],
                          cwd=TREE, input=json.dumps(dict(graph=graph, snapshot=snapshot)),
                          capture_output=True, text=True, check=True, timeout=20)
    return json.loads(done.stdout)


def test_real_v3_multiaction_history_and_exact_projection(tmp_path, monkeypatch):
    assert tmp_path.resolve().is_relative_to(Path('/tmp'))
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setenv('PYTHONDONTWRITEBYTECODE', '1')
    imports = _assert_imports()
    spec = importlib.util.spec_from_file_location('multiaction_private_demo', TREE / 'examples/forge_workspace/make_demo.py')
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    root = tmp_path / 'workspace'
    demo.create(root)
    # Three short text rows need only a 64 KiB row/batch ceiling. Keep the
    # default 256 MiB run memory budget; oversized payloads must still fail.
    prepared = custom(root, FLOW, limits={'max_row_bytes': 64 * 1024, 'max_batch_bytes': 64 * 1024})
    finished = execute(root, prepared)
    assert finished['status'] == 'succeeded', (finished, runs.logs(root, finished['run_id']))
    assert finished['outputs'][0]['result']['ref']['row_count'] == 6
    _verify_history(root, finished, imports, tmp_path)


def _verify_history(root, finished, imports, tmp_path):
    """Read the same real run repeatedly while diagnosing consumer assertions."""
    run_id, attempt = finished['run_id'], finished['attempt']
    target = runs.directory(root, run_id)
    for relative in ('demiflow/execution/dataflow.py', 'demiflow/execution/dataflow_seal.py'):
        assert _digest(target / 'runtime' / relative) == _digest(TREE / relative)
    graph = model.get(root, basis_kind='run', run_id=run_id, attempt=attempt)
    assert graph['report_digest'] != graph['binding']['basis_digest']
    snapshot = view.snapshot(root, run_id, attempt=attempt)
    actions = graph['logical']['actions']
    pages = {entry['action_id']: view.observations(root, run_id, attempt=attempt,
                                                 action_id=entry['action_id'], limit=64)
             for entry in actions}
    evidence = dict(tree=str(TREE), imports=imports, run_id=run_id, attempt=attempt,
                    output=finished['outputs'][0], graph=graph, snapshot=snapshot, pages=pages)
    evidence_path = tmp_path / 'multiaction-history-evidence.json'
    evidence_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2))

    assert len(actions) == 2, [(e['action_id'], e['status']) for e in actions]
    assert len({entry['plan_id'] for entry in actions}) == 2
    assert len({json.dumps(entry['code_pin_ref'], sort_keys=True) for entry in actions}) == 2
    # Original progress documents carry scope; the Workbench reader cannot
    # manufacture these fields from whichever plan is currently selected.
    with sqlite3.connect((target / 'run.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        persisted = {(a, n, o): json.loads(raw) for a, n, o, raw in db.execute(
            'SELECT action_id,node_id,occurrence,document FROM node_observations WHERE attempt=?', (attempt,))}
    repeated_actions = []
    sources_before = {}
    for entry in actions:
        action_id = entry['action_id']
        assert entry['status'] == 'sealed' and entry['version'] == 'v3'
        assert entry['dag']['schema_version'] == 'demiflow.dataflow_graph.v3'
        dag = entry['dag']
        occurrences = {node['occurrence_id'] for node in dag['nodes']}
        assert len(occurrences) == len(dag['nodes'])
        duplicate_semantics = [logical for logical, count in Counter(
            node['logical_node_id'] for node in dag['nodes'] if node['op_kind'] == 'BoundMapOp').items() if count > 1]
        if duplicate_semantics:
            repeated_actions.append(action_id)
            assert len([node for node in dag['nodes'] if node['op_kind'] == 'union']) == 1
            union = next(node for node in dag['nodes'] if node['op_kind'] == 'union')
            incoming = [edge for edge in dag['edges'] if edge['to']['occurrence_id'] == union['occurrence_id']]
            assert len(incoming) == 2
            # Union has two original inputs. The seal decides whether these
            # share an occurrence; equal semantics never imply one execution.
            assert len({edge['to']['port'] for edge in incoming}) == 2
        for edge in dag['edges']:
            assert edge['from']['occurrence_id'] in occurrences
            assert edge['to']['occurrence_id'] in occurrences
            assert edge['from']['occurrence_id'] != edge['to']['occurrence_id']
        assert entry['identity_bridge']['available'] is True
        bridge = entry['identity_bridge']['bridge']
        assert bridge['action_id'] == action_id and bridge['dag_id'] == dag['dag_id']
        assert bridge['report_digest'] == graph['report_digest']
        bridge_by_occurrence = {row['occurrence_id']: row for row in bridge['entries']}
        scope = dict(action_id=action_id, dag_id=dag['dag_id'], plan_id=entry['plan_id'],
                     plan_revision=entry['physical']['plan_revision'])
        page = pages[action_id]
        assert page['attempt'] == attempt and page['run_id'] == run_id
        assert page['has_more'] is False
        assert {key: page.get(key) for key in scope} == scope
        assert entry['node_observations']['available'] is True
        assert {key: entry['node_observations'].get(key) for key in scope} == scope
        assert page['observations']
        for row in page['observations']:
            assert {key: row.get(key) for key in scope} == scope
            assert row['occurrence_id'] in occurrences
            link = bridge_by_occurrence[row['occurrence_id']]
            assert (row['node_id'], row['occurrence']) == (link['report_node_id'], link['report_occurrence'])
            original = persisted[(action_id, row['node_id'], row['occurrence'])]
            for field in ('plan_id', 'dag_id', 'plan_revision', 'occurrence_id'):
                assert row[field] == original[field]
        for link in bridge['entries']:
            sources_before[(action_id, link['occurrence_id'])] = model.bound_source(
                root, graph['binding'], link['report_node_id'],
                logical_node_id=link['logical_node_id'], occurrence_id=link['occurrence_id'])
            assert sources_before[(action_id, link['occurrence_id'])]['sealed_code_pin_ref'] == entry['code_pin_ref']
    assert len(repeated_actions) == 1, 'The real sealed DAG must include two equal-semantic map occurrences'

    projected = _project(graph, snapshot)
    missing_states = []
    for entry, rendered in zip(actions, projected):
        assert rendered['execution']['actionId'] == entry['action_id']
        assert rendered['execution']['planId'] == entry['plan_id']
        assert len(rendered['nodes']) == len(entry['dag']['nodes'])
        assert len(rendered['edges']) == len(entry['dag']['edges'])
        assert {edge['edgeId'] for edge in rendered['edges']} == {edge['edge_id'] for edge in entry['dag']['edges']}
        if not any(card['state']['tone'] == 'success' for card in rendered['nodes']):
            missing_states.append(entry['action_id'])
        for card in rendered['nodes']:
            assert card['selection']['execution']['actionId'] == entry['action_id']
            assert card['selection']['execution']['planId'] == entry['plan_id']
            if card['state']['tone'] == 'success':
                assert any(row['action_id'] == entry['action_id']
                           and row.get('plan_id') == entry['plan_id']
                           and row.get('occurrence_id') == card['selection']['occurrenceId']
                           and row['state'] in ('succeeded', 'reused')
                           and row['timing_scope'] != 'group'
                           for row in snapshot['observations'])
    assert projected[1]['selected'] is None and projected[2]['selected'] is None
    assert projected[2]['execution']['actionId'] == actions[0]['action_id']
    # Consumer fault only: keep real seals/observations untouched and present
    # the manifest digest as the report credential. The bridge must be refused.
    rejected = _project(dict(graph, report_digest=graph['binding']['basis_digest']), snapshot)
    assert all(card['state']['tone'] == 'neutral' for rendered in rejected for card in rendered['nodes'])
    assert all(any('尚无节点身份桥' in notice for notice in rendered['notices']) for rendered in rejected)

    # Editing only this test's current business source must not alter any
    # historical graph, action code-pin, or sealed source bytes.
    sealed_files = {str(path.relative_to(target)): _digest(path)
                    for path in (target / 'source').rglob('*') if path.is_file()}
    original_flow = runs.source(root, run_id, 'flows/custom.py')
    original_operator = runs.source(root, run_id, 'operators/question_rules.py')
    for relative in ('flows/custom.py', 'operators/question_rules.py'):
        path = root / relative
        path.write_text(path.read_text() + '\n# current-workspace-edit-after-run\n')
    assert runs.source(root, run_id, 'flows/custom.py') == original_flow
    assert runs.source(root, run_id, 'operators/question_rules.py') == original_operator
    assert 'current-workspace-edit-after-run' not in original_flow['text']
    after = model.get(root, basis_kind='run', run_id=run_id, attempt=attempt)
    assert after['binding'] == graph['binding']
    assert after['logical'] == graph['logical']
    for entry in actions:
        for link in entry['identity_bridge']['bridge']['entries']:
            old = sources_before[(entry['action_id'], link['occurrence_id'])]
            assert model.bound_source(root, graph['binding'], link['report_node_id'],
                                      logical_node_id=link['logical_node_id'], occurrence_id=link['occurrence_id']) == old
    assert {str(path.relative_to(target)): _digest(path)
            for path in (target / 'source').rglob('*') if path.is_file()} == sealed_files
    evidence.update(projected=projected, repeated_actions=repeated_actions,
                    sealed_source_digests=sealed_files, history_unchanged=True,
                    wrong_report_digest_rejected=True,
                    actions_without_verified_node_states=missing_states)
    evidence_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    assert not missing_states, ('Genuine scoped observations were not rendered', missing_states, str(evidence_path))
    print(json.dumps(dict(run_id=run_id, attempt=attempt, actions=[e['action_id'] for e in actions],
                          repeated_actions=repeated_actions, evidence=str(evidence_path))))
