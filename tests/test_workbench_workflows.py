"""End-to-end cross-project multi-pipeline workflow tests (orchestrator, permissions, resume, reuse, models, CLI)."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time

from click.testing import CliRunner
import pytest

from demiflow import data as df_data
from demiflow.cli import main as cli_main
from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.journal.run_reader import read_run
from demiflow.forge.platform import content_digest
from demiflow.forge.project import initialize_project, register_flow
from demiflow.forge.workflow import WORKFLOW_FORMAT
from demiflow.workbench.projects import registry as console
from demiflow.forge.runs import manager as runs

from demiflow.workbench.workflows import manager as wf_mgr
from demiflow.workbench.workflows import store as wf_store


def _wait_workflow(host_root: Path, wf_run_id: str, *, attempt: int | None = None, timeout: float = 150.0) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = wf_mgr.show_workflow_run(host_root, wf_run_id, attempt=attempt)
        if last['status'] in wf_store.TERMINAL_WORKFLOW_STATUSES and not last['orchestrator_alive']:
            return last
        time.sleep(0.15)
    raise AssertionError(f'Workflow {wf_run_id} did not reach terminal state in {timeout}s: {last}')


def _setup_two_projects(tmp_path: Path):
    """Create two independent DemiFlow projects: proj_a (producer) and proj_b (consumer/orchestrator)."""
    proj_a = tmp_path / 'proj_a'
    proj_b = tmp_path / 'proj_b'

    # --- Project A: synthetic dataset producer ---
    initialize_project(proj_a, name='proj_a_synth')
    (proj_a / 'flows').mkdir()
    (proj_a / 'configs').mkdir()
    (proj_a / 'data').mkdir()
    (proj_a / 'flows' / 'generate.py').write_text(
        'from demiflow import data\n'
        'def run(*, output_table, multiplier=2):\n'
        '    rows = [{"id": i, "value": i * multiplier} for i in range(1, 5)]\n'
        '    return data.from_items(rows).write_lance(output_table["uri"], mode=output_table["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    (proj_a / 'configs' / 'generate.json').write_text(
        json.dumps({'output_table': {'kind': 'output', 'uri': 'data/gen.lance'}, 'multiplier': 2}),
        encoding='utf-8',
    )
    register_flow(proj_a, 'flows/generate.py:run', params='configs/generate.json')

    # --- Project B: transform (multi-instance) + fan-in compare + optional fail flow ---
    initialize_project(proj_b, name='proj_b_eval')
    (proj_b / 'flows').mkdir()
    (proj_b / 'operators').mkdir()
    (proj_b / 'configs').mkdir()
    (proj_b / 'data').mkdir()

    # Seed Lance tables so static flow registration succeeds cleanly.
    df_data.from_items([{'id': 1, 'value': 10}]).write_lance(str(proj_b / 'data' / 'seed_in.lance'), mode='create')
    df_data.from_items([{'id': 1, 'score': 10, 'tag': 'a'}]).write_lance(str(proj_b / 'data' / 'seed_left.lance'), mode='create')
    df_data.from_items([{'id': 1, 'score': 20, 'tag': 'b'}]).write_lance(str(proj_b / 'data' / 'seed_right.lance'), mode='create')

    (proj_b / 'operators' / 'ops.py').write_text(
        'from typing import TypedDict\n\n'
        '__demiflow_operators__ = ["shift_score", "merge_scores"]\n\n'
        'class ValueRow(TypedDict):\n'
        '    id: int | None\n'
        '    value: int | None\n\n'
        'class ScoreRow(TypedDict):\n'
        '    id: int\n'
        '    score: int\n'
        '    tag: str\n\n'
        'class JoinedScoreRow(TypedDict):\n'
        '    id: int | None\n'
        '    score: int | None\n'
        '    score_right: int | None\n\n'
        'class ComparedRow(TypedDict):\n'
        '    id: int\n'
        '    base_score: int\n'
        '    tuned_score: int\n'
        '    gain: int\n\n'
        'def shift_score(row: ValueRow, delta: int = 0, tag: str = "base") -> ScoreRow:\n'
        '    """按给定偏置计算得分并打标签。\n\n'
        '    Args:\n'
        '        delta: 得分偏移量\n'
        '        tag: 分支标签名称\n'
        '    Example:\n'
        '        输入: {"id": 1, "value": 10}\n'
        '        输出: {"id": 1, "score": 15, "tag": "base"}\n'
        '    """\n'
        '    rid = row["id"]\n'
        '    if rid is None:\n'
        '        rid = 0\n'
        '    val = row["value"]\n'
        '    if val is None:\n'
        '        val = 0\n'
        '    return {"id": int(rid), "score": int(val) + delta, "tag": tag}\n\n'
        'def merge_scores(row: JoinedScoreRow) -> ComparedRow:\n'
        '    """合并左右分支得分并计算差值。\n\n'
        '    Example:\n'
        '        输入: {"id": 1, "score": 10, "score_right": 20}\n'
        '        输出: {"id": 1, "base_score": 10, "tuned_score": 20, "gain": 10}\n'
        '    """\n'
        '    rid = row["id"]\n'
        '    if rid is None:\n'
        '        rid = 0\n'
        '    left = row["score"]\n'
        '    if left is None:\n'
        '        left = 0\n'
        '    right = row["score_right"]\n'
        '    if right is None:\n'
        '        right = 0\n'
        '    return {"id": int(rid), "base_score": int(left), "tuned_score": int(right), "gain": int(right) - int(left)}\n',
        encoding='utf-8',
    )
    (proj_b / 'flows' / 'transform.py').write_text(
        'from demiflow import data\n'
        'from operators import ops\n'
        'def run(*, input_table, output_table, delta=0, tag="base"):\n'
        '    ds = data.read_lance(input_table["uri"], version=input_table.get("version"))\n'
        '    return ds.map(ops.shift_score, fn_kwargs={"delta": delta, "tag": tag}).write_lance(output_table["uri"], mode=output_table["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    (proj_b / 'configs' / 'transform.json').write_text(
        json.dumps({
            'input_table': {'uri': 'data/seed_in.lance', 'version': 1},
            'output_table': {'kind': 'output', 'uri': 'data/transform.lance'},
            'delta': 0,
            'tag': 'base',
        }),
        encoding='utf-8',
    )
    (proj_b / 'flows' / 'compare.py').write_text(
        'from demiflow import data\n'
        'from operators import ops\n'
        'def run(*, left_table, right_table, output_table):\n'
        '    left = data.read_lance(left_table["uri"], version=left_table.get("version"))\n'
        '    right = data.read_lance(right_table["uri"], version=right_table.get("version"))\n'
        '    joined = left.join(right, on="id", suffix="_right")\n'
        '    return joined.map(ops.merge_scores).write_lance(output_table["uri"], mode=output_table["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    (proj_b / 'configs' / 'compare.json').write_text(
        json.dumps({
            'left_table': {'uri': 'data/seed_left.lance', 'version': 1},
            'right_table': {'uri': 'data/seed_right.lance', 'version': 1},
            'output_table': {'kind': 'output', 'uri': 'data/compare.lance'},
        }),
        encoding='utf-8',
    )
    register_flow(proj_b, 'flows/transform.py:run', params='configs/transform.json')
    register_flow(proj_b, 'flows/compare.py:run', params='configs/compare.json')

    # Registry registering both projects (v3 format with explicit permissions).
    reg_path = tmp_path / 'registry.json'
    r_a = console.add(reg_path, proj_a, allow_runs=False)
    r_b = console.add(reg_path, proj_b, allow_runs=False)
    inv = console.load_registry(reg_path)
    console.migrate(reg_path, expected=content_digest(inv))
    inv = console.load_registry(reg_path)
    res_b = console.set_permissions(reg_path, r_b['workspace_id'], expected=content_digest(inv),
                                    run_submit=True, run_control=True, attach=False)
    console.set_permissions(reg_path, r_a['workspace_id'], expected=res_b['digest'],
                            run_submit=True, run_control=True, attach=False)

    return proj_a, proj_b, reg_path, r_a['workspace_id'], r_b['workspace_id']


def _write_cross_project_workflow(proj_b: Path) -> str:
    (proj_b / 'workflows').mkdir(exist_ok=True)
    wf = {
        'format': WORKFLOW_FORMAT,
        'name': '跨项目合成与双分支对比评测',
        'description': 'proj_a 合成数据 -> proj_b 复用同一 Flow 跑 base/tuned -> fan-in 汇总',
        'projects': {
            'synth': 'proj_a_synth',
        },
        'inputs': {
            'multiplier': {'type': 'integer', 'default': 10, 'description': '合成数据倍率'},
        },
        'policy': {'max_parallel': 2, 'on_failure': 'fail_fast'},
        'nodes': [
            {
                'id': 'gen_synth',
                'title': '跨项目生成数据',
                'project': 'synth',
                'entry': 'flows/generate.py:run',
                'params': 'configs/generate.json',
                'overrides': {'multiplier': {'$input': 'multiplier'}},
            },
            {
                'id': 'branch_base',
                'title': 'Base 分支评分',
                'project': 'self',
                'entry': 'flows/transform.py:run',
                'params': 'configs/transform.json',
                'overrides': {
                    'input_table': {'$from': 'gen_synth.outputs.output_table'},
                    'delta': 5,
                    'tag': 'base',
                },
            },
            {
                'id': 'branch_tuned',
                'title': 'Tuned 分支评分',
                'project': 'self',
                'entry': 'flows/transform.py:run',
                'params': 'configs/transform.json',
                'overrides': {
                    'input_table': {'$from': 'gen_synth.outputs.output_table'},
                    'delta': 50,
                    'tag': 'tuned',
                },
            },
            {
                'id': 'compare',
                'title': '双分支效果汇总',
                'project': 'self',
                'entry': 'flows/compare.py:run',
                'params': 'configs/compare.json',
                'overrides': {
                    'left_table': {'$from': 'branch_base.outputs.output_table'},
                    'right_table': {'$from': 'branch_tuned.outputs.output_table'},
                },
            },
        ],
    }
    rel = 'workflows/cross_eval.json'
    (proj_b / rel).write_text(json.dumps(wf, ensure_ascii=False, indent=2), encoding='utf-8')
    return rel


def test_cross_project_dag_fanout_fanin_multi_instance_and_rerun_reuse(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    proj_a, proj_b, reg_path, ws_a, ws_b = _setup_two_projects(tmp_path)
    wf_rel = _write_cross_project_workflow(proj_b)
    ceiling = {'run_submit': True, 'run_control': True, 'attach': True}

    # 1. Permission guard (WF-05): revoking proj_a run_submit must block start_workflow BEFORE creating any run.
    inv = console.load_registry(reg_path)
    console.set_permissions(reg_path, ws_a, expected=content_digest(inv), run_submit=False, run_control=False, attach=False)
    insp_denied = wf_mgr.inspect_workflow(proj_b, wf_rel, registry_path=reg_path, permission_ceiling=ceiling)
    assert insp_denied['can_run'] is False
    assert any(d['rule'] == 'WF-05' for d in insp_denied['permission_diagnostics'])
    with pytest.raises(PermissionError, match='跨项目权限不足'):
        wf_mgr.start_workflow(proj_b, wf_rel, registry_path=reg_path, permission_ceiling=ceiling)

    # Restore proj_a permission.
    inv = console.load_registry(reg_path)
    console.set_permissions(reg_path, ws_a, expected=content_digest(inv), run_submit=True, run_control=True, attach=False)

    # Record original configs/transform.json bytes to prove zero config mutation.
    orig_cfg = (proj_b / 'configs' / 'transform.json').read_bytes()

    # 2. Launch real cross-project DAG workflow!
    started = wf_mgr.start_workflow(
        proj_b, wf_rel,
        inputs={'multiplier': 10},
        registry_path=reg_path,
        permission_ceiling=ceiling,
    )
    done = _wait_workflow(proj_b, started['wf_run_id'])
    assert done['status'] == 'succeeded', (
        done.get('error'),
        {k: (v.get('status'), v.get('error')) for k, v in (done.get('nodes') or {}).items()},
        done.get('events')[-8:],
    )
    assert (proj_b / 'configs' / 'transform.json').read_bytes() == orig_cfg

    # Verify every node succeeded and produced isolated non-colliding Lance outputs!
    for nid in ('gen_synth', 'branch_base', 'branch_tuned', 'compare'):
        nr = done['nodes'][nid]
        assert nr['status'] == 'succeeded', (nid, nr)
        assert nr['child_run_id']
        assert 'output_table' in (nr['artifacts'] or {}).get('outputs', {})

    base_uri = done['nodes']['branch_base']['artifacts']['outputs']['output_table']['uri']
    tuned_uri = done['nodes']['branch_tuned']['artifacts']['outputs']['output_table']['uri']
    assert base_uri != tuned_uri
    assert 'a1_branch_base' in base_uri and 'a1_branch_tuned' in tuned_uri

    # Read actual final Lance table rows to verify end-to-end math:
    # proj_a: id=1..4, value = id*10 -> base: value+5, tuned: value+50 -> gain = 45!
    cmp_out = done['nodes']['compare']['artifacts']['outputs']['output_table']
    rows = sorted(df_data.read_lance(cmp_out['uri'], version=cmp_out['version']).take_all(), key=lambda r: r['id'])
    assert len(rows) == 4
    assert [r['gain'] for r in rows] == [45, 45, 45, 45]
    assert [r['base_score'] for r in rows] == [15, 25, 35, 45]
    assert [r['tuned_score'] for r in rows] == [60, 70, 80, 90]

    # 3. Partial rerun (`from_node="branch_tuned"`):
    #    - gen_synth & branch_base MUST keep their exact attempt-1 child_run_id (zero re-execution!)
    #    - branch_tuned & compare MUST run fresh child runs into a2_* isolated output paths!
    gen_r1 = done['nodes']['gen_synth']['child_run_id']
    base_r1 = done['nodes']['branch_base']['child_run_id']
    tuned_r1 = done['nodes']['branch_tuned']['child_run_id']
    cmp_r1 = done['nodes']['compare']['child_run_id']

    resumed = wf_mgr.resume_workflow_run(
        proj_b, done['wf_run_id'],
        from_node='branch_tuned',
        registry_path=reg_path,
        permission_ceiling=ceiling,
    )
    assert resumed['attempt'] == 2
    done2 = _wait_workflow(proj_b, done['wf_run_id'], attempt=2)
    assert done2['status'] == 'succeeded', done2
    assert done2['nodes']['gen_synth']['child_run_id'] == gen_r1
    assert done2['nodes']['branch_base']['child_run_id'] == base_r1
    assert done2['nodes']['branch_tuned']['child_run_id'] != tuned_r1
    assert done2['nodes']['compare']['child_run_id'] != cmp_r1
    assert 'a2_branch_tuned' in done2['nodes']['branch_tuned']['artifacts']['outputs']['output_table']['uri']

    # 4. Explicit `reuse_runs` on a brand new workflow run skips proj_a execution completely!
    started_reused = wf_mgr.start_workflow(
        proj_b, wf_rel,
        inputs={'multiplier': 10},
        reuse_runs={'gen_synth': gen_r1},
        registry_path=reg_path,
        permission_ceiling=ceiling,
    )
    done_reused = _wait_workflow(proj_b, started_reused['wf_run_id'])
    assert done_reused['status'] == 'succeeded', done_reused
    assert done_reused['nodes']['gen_synth']['status'] == 'reused'
    assert done_reused['nodes']['gen_synth']['child_run_id'] == gen_r1


def test_orchestrator_crash_reconcile_and_cli_commands(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    proj_a, proj_b, reg_path, _, _ = _setup_two_projects(tmp_path)
    wf_rel = _write_cross_project_workflow(proj_b)

    # First run via CLI (`demiflow workbench workflow validate / list / run / status / runs`)
    runner = CliRunner()
    v_res = runner.invoke(cli_main, [
        'workbench', 'workflow', 'validate', str(proj_b), wf_rel,
        '--project', f'synth={proj_a}', '--json',
    ])
    assert v_res.exit_code == 0, v_res.output
    v_data = json.loads(v_res.output)
    assert v_data['status'] == 'passed' and v_data['can_run'] is True

    # Simulate an orchestrator crash after gen_synth's child run already succeeded:
    # Prepare & execute a real child run on proj_a, bind it into a workflow ledger in 'running' state without a live orchestrator,
    # and confirm `reconcile_workflow_run` detects it succeeded and settles without re-running!
    child_prep = runs.prepare(proj_a, 'flows/generate.py:run', params='configs/generate.json', new_outputs=True)
    child_started = runs.start(proj_a, child_prep['run_id'], expected=child_prep['digest'], expected_attempt=child_prep['attempt'])
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        st = runs.show(proj_a, child_started['run_id'])
        if st['status'] == 'succeeded':
            break
        time.sleep(0.1)
    assert runs.show(proj_a, child_started['run_id'])['status'] == 'succeeded'

    # Create a single-node workflow manifest + ledger simulating orchestrator death mid-node.
    single_wf = {
        'format': WORKFLOW_FORMAT,
        'name': '崩溃对账验证',
        'projects': {'synth': 'proj_a_synth'},
        'nodes': [
            {'id': 'gen', 'title': '生成', 'project': 'synth', 'entry': 'flows/generate.py:run', 'params': 'configs/generate.json'},
        ],
    }
    (proj_b / 'workflows' / 'crash_test.json').write_text(json.dumps(single_wf), encoding='utf-8')
    insp = wf_mgr.inspect_workflow(proj_b, 'workflows/crash_test.json', project_map={'synth': str(proj_a)})
    wf_run_id = wf_store.new_workflow_run_id()
    run_dir = wf_store.run_directory(proj_b, wf_run_id)
    wf_store.write_manifest(run_dir, {
        'format': wf_store.MANIFEST_FORMAT,
        'wf_run_id': wf_run_id,
        'host_root': str(proj_b),
        'host_workspace_id': 'w' + '0' * 32,
        'workflow_path': 'workflows/crash_test.json',
        'workflow_name': '崩溃对账验证',
        'workflow_digest': insp['spec']['digest'],
        'spec': insp['spec'],
        'inputs': {},
        'resolved_projects': {'synth': {'alias': 'synth', 'name': 'proj_a_synth', 'root': str(proj_a), 'is_self': False}},
        'authority_mode': 'trusted_local',
        'registry_path': None,
        'permission_ceiling': None,
        'created_at': time.time() - 10,
    })
    wf_store.initialize_ledger(run_dir, wf_run_id=wf_run_id, nodes_init=[{
        'node_id': 'gen',
        'status': 'running',
        'project_alias': 'synth',
        'project_root': str(proj_a),
        'entry': 'flows/generate.py:run',
        'params': 'configs/generate.json',
        'child_run_id': child_started['run_id'],
        'child_attempt': 1,
    }])
    wf_store.update_workflow_status(run_dir, 1, 'running')

    # Reconcile settles the completed child run directly as succeeded with verified outputs!
    rec = wf_mgr.reconcile_workflow_run(proj_b, wf_run_id)
    assert rec['status'] == 'succeeded'
    assert rec['nodes']['gen']['status'] == 'succeeded'
    assert 'output_table' in rec['nodes']['gen']['artifacts']['outputs']


def test_published_models_reader_verifies_model_manifest_sha256(tmp_path):
    """RunReader.published_models reads & sha256-verifies committed model manifests without opening weights."""
    model_dir = tmp_path / 'models' / 'objects'
    model_dir.mkdir(parents=True)
    weights_file = model_dir / 'weights.safetensors'
    weights_bytes = b'fake-lora-weights-bytes'
    weights_file.write_bytes(weights_bytes)
    weights_sha = hashlib.sha256(weights_bytes).hexdigest()

    snap = TrainingSnapshotRef(
        step=1000,
        model_format='lora_safetensors',
        files={'adapter_model.safetensors': ProjectObject(str(weights_file), weights_sha, len(weights_bytes))},
    )
    fit_res = FitResult(
        instance_id='a' * 64,
        fit_key='b' * 64,
        snapshots=(snap,),
        final_step=1000,
        metrics={'loss': 0.12},
    )
    manifest_bytes = json.dumps(fit_res.to_dict(), sort_keys=True, separators=(',', ':')).encode('utf-8')
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_file = model_dir / manifest_sha
    manifest_file.write_bytes(manifest_bytes)

    run_dir = tmp_path / 'fake_run'
    run_dir.mkdir()
    j = RunJournal(run_dir)
    j.initialize({'format': 'test', 'run_id': 'r' + '1' * 24, 'root': str(tmp_path)}, attempts=True)
    import sqlite3
    with sqlite3.connect(str(run_dir / 'run.sqlite')) as db:
        db.execute(
            'CREATE TABLE IF NOT EXISTS runtime_sources (action_id TEXT PRIMARY KEY, document TEXT NOT NULL)'
        )
        rec = {
            'schema': 'demiflow.fit_consumption.v1',
            'action': 'act_fit_1',
            'occurrence': 'fit_occ_1',
            'instance': 'a' * 64,
            'fit_key': 'b' * 64,
            'manifest': {'uri': str(manifest_file), 'sha256': manifest_sha, 'size_bytes': len(manifest_bytes)},
        }
        db.execute('INSERT INTO runtime_sources (action_id, document) VALUES (?, ?)',
                   ('fit-confirmed-1', json.dumps(rec)))

    with read_run(run_dir) as reader:
        models = reader.published_models()
    assert len(models) == 1
    m = models[0]
    assert m['manifest_digest'] == manifest_sha
    assert m['final_step'] == 1000
    assert m['steps']['1000']['files']['adapter_model.safetensors']['uri'] == str(weights_file)
    assert m['files']['adapter_model.safetensors']['sha256'] == weights_sha
