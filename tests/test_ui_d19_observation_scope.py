"""Original journal observations retain the actual producer's execution scope."""
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from demiflow.execution.run_journal import _node_document
from demiflow.workbench.runs import facts, manager, view
from demiflow.workbench.graph import model

SCOPE = ('plan_id', 'dag_id', 'plan_revision', 'occurrence_id')
TREE = Path(__file__).resolve().parents[1]


def test_native_observation_scope_is_optional_but_atomic():
    old = {'sequence': 1, 'state': 'running', 'rows_output': 2}
    assert json.loads(_node_document(old)) == old
    scope = dict(plan_id='a'*64, dag_id='b'*64, plan_revision=0, occurrence_id='c'*64)
    assert json.loads(_node_document({**old, **scope})) == {**old, **scope}
    for key in SCOPE:
        with pytest.raises(ValueError):
            _node_document({**old, **{k:v for k,v in scope.items() if k != key}})
    with pytest.raises(ValueError):
        _node_document({**old, **scope, 'plan_revision': True})


def test_real_v3_workbench_persists_scope_and_never_relabels_legacy(tmp_path, monkeypatch):
    import demiflow
    from d19_fixtures import _create_real_run
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE)
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'domain'))
    spec=importlib.util.spec_from_file_location('scope_owned_demo',TREE/'examples/forge_workspace/make_demo.py')
    demo=importlib.util.module_from_spec(spec);spec.loader.exec_module(demo)
    root=tmp_path/'workspace';demo.create(root)
    run=_create_real_run(root)
    graph=model.get(root,basis_kind='run',run_id=run['run_id'],attempt=run['attempt'])
    action=next(a for a in graph['logical']['actions'] if a.get('status')=='sealed')
    assert action['version']=='v3'
    assert action['dag']['schema_version']=='demiflow.dataflow_graph.v3'
    aid=action['action_id'];plan_id=action['plan_id'];dag_id=action['dag']['dag_id']
    revision=action['physical']['plan_revision'];bridge=action['identity_bridge']['bridge']
    assert len(action['dag']['nodes'])==7 and len(bridge['entries'])==7
    target=manager.directory(root,run['run_id'])
    page=view.observations(root,run['run_id'],attempt=1,action_id=aid,limit=64)
    assert page['plan_id']==plan_id and page['dag_id']==dag_id and page['plan_revision']==revision
    keys={(e['report_node_id'],e['report_occurrence']):e['occurrence_id'] for e in bridge['entries']}
    assert len(page['observations'])==7
    for row in page['observations']:
        assert {k:row[k] for k in SCOPE}==dict(plan_id=plan_id,dag_id=dag_id,
            plan_revision=revision,occurrence_id=keys[row['node_id'],row['occurrence']])
    snapshot=view.snapshot(root,run['run_id'],attempt=1)
    assert len(snapshot['observations'])==7
    assert all(row['plan_id']==plan_id for row in snapshot['observations'])
    joined=facts.join_node_observations(bridge,page,action_id=aid,attempt=1,
        plan_id=plan_id,dag_id=dag_id,plan_revision=revision)
    assert all(row['availability']=='observed' for row in joined)
    dbpath=target/'run.sqlite'
    with sqlite3.connect(dbpath) as db:
        raw=db.execute('SELECT document FROM node_observations WHERE attempt=1 AND action_id=?',(aid,)).fetchall()
        assert len(raw)==7 and all(json.loads(row[0])['plan_id']==plan_id for row in raw)
        # Simulate a genuinely older persisted observation beside a sealed v3
        # carrier. The reader must not convert the selected seal into provenance.
        first=page['observations'][0]
        doc=json.loads(db.execute('SELECT document FROM node_observations WHERE attempt=1 AND action_id=? AND node_id=? AND occurrence=?',
            (aid,first['node_id'],first['occurrence'])).fetchone()[0])
        for key in SCOPE:doc.pop(key)
        db.execute('UPDATE node_observations SET document=? WHERE attempt=1 AND action_id=? AND node_id=? AND occurrence=?',
            (json.dumps(doc),aid,first['node_id'],first['occurrence']))
    unscoped=view.observations(root,run['run_id'],attempt=1,action_id=aid,limit=64)
    assert 'plan_id' not in unscoped
    original=next(row for row in unscoped['observations'] if row['node_id']==first['node_id'] and row['occurrence']==first['occurrence'])
    assert not set(SCOPE).intersection(original)
    with pytest.raises(Exception, match='sealed plan scope'):
        facts.join_node_observations(bridge,unscoped,action_id=aid,attempt=1,
            plan_id=plan_id,dag_id=dag_id,plan_revision=revision)
    (tmp_path/'scope-proof.json').write_text(json.dumps(dict(run=run,action=aid,plan_id=plan_id,dag_id=dag_id,
        plan_revision=revision,observations=page['observations'],scope_less_not_promoted=True),indent=2))
