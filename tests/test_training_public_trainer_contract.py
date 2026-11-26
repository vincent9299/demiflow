"""End-to-end and contract tests for the zero-internal-import public trainer API."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import time

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.training import PredictorSpec, TrainingRecipe
from demiflow.execution.contracts.training import FitRequest, FitResult, TrainingSnapshotRef
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.pipeline.training_deployment import TrainingDeployment
from demiflow.execution.worker.training_job import ManagedFitContext, execute_training_job
from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import initialize_project, register_flow
from demiflow.forge.runs import manager as runs

from test_forge_runs import execute, rows
from test_training_domain import setup_request
from training_action_programs import (
    SealedWeightReaderPredictor,
    public_final_step_outside_save_steps_trainer,
    public_reconcile_crash_trainer,
    public_report_and_resume_trainer,
    public_return_only_trainer,
)


def _public_recipe(trainer, *, save_steps=(1, 3), optimizer_updates=3, parameters=None):
    return TrainingRecipe(
        trainer=trainer,
        predictor=PredictorSpec(
            factory=SealedWeightReaderPredictor,
            inputs={'x': 'x'},
            input_schema={'x': 'int64'},
            output_schema={'x': 'int64', 'step': 'int64', 'weight': 'string'},
            batch_format='pyarrow',
        ),
        train_schema={'x': 'int64'},
        base_model_digest='a' * 64,
        environment_digest='b' * 64,
        model_format='native.test',
        budget={'optimizer_updates': optimizer_updates},
        save_steps=save_steps,
        topology={'nodes': 1, 'world_size': 1, 'gpus_per_node': 0},
        parameter_schema={'type': 'object', 'additionalProperties': True} if parameters is not None else None,
        parameters=parameters or {},
    )


def test_zero_internal_import_trainer_passes_forge_check_and_predictor_reads_sealed_weights(tmp_path, monkeypatch):
    """Acceptance chain #1: zero-internal-import trainer passes full DF-02 Forge check,
    runs through native workbench execution, and Predictor reads sealed weights from ModelStore."""
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'private-resources'))
    proj = tmp_path / 'clean_train_proj'
    initialize_project(proj, name='public-trainer-e2e')
    for d in ('flows', 'operators', 'configs', 'data'):
        (proj / d).mkdir(exist_ok=True)

    (proj / 'operators' / 'training.py').write_text(
        '"""公开零内部导入训练与预测算子。"""\n'
        'from pathlib import Path\n'
        'import pyarrow as pa\n\n'
        '__demiflow_operators__ = ["public_trainer", "SealedPredictor"]\n\n\n'
        'def public_trainer(request, context):\n'
        '    """按声明步数写入权重到 model_dir 并直接返回路径映射。\n\n'
        '    Args:\n'
        '        request: 原生训练请求，包含固定输入、超参与保存步数。\n'
        '        context: 原生训练上下文，提供 model_dir 与取消检查。\n\n'
        '    Example:\n'
        '        输入: save_steps=(400, 800)\n'
        '        输出: {"checkpoints": {400: {"weights": ...}, 800: {"weights": ...}}, "metrics": {"steps": 800}}\n'
        '    """\n'
        '    checkpoints = {}\n'
        '    for step in request.semantics["save_steps"]:\n'
        '        context.check_cancelled()\n'
        '        path = context.model_dir / f"weights_{step}.txt"\n'
        '        path.write_text(f"sealed-weights-{step}", encoding="utf-8")\n'
        '        context.report_snapshot(step, files={"weights": path})\n'
        '        checkpoints[step] = {"weights": path}\n'
        '    return {"checkpoints": checkpoints, "metrics": {"steps": step}}\n\n\n'
        'class SealedPredictor:\n'
        '    """从受管模型库读取封存后的真实权重文件内容参与推理。\n\n'
        '    Args:\n'
        '        model: 引擎注入的已封存模型元数据与文件路径清单。\n'
        '        inputs: 输入列名映射。\n'
        '        parameters: 预测参数。\n\n'
        '    Example:\n'
        '        输入: {"x": 5}\n'
        '        输出: {"x": 5, "step": 400, "weight": "sealed-weights-400", "from_store": True}\n'
        '    """\n\n'
        '    def __init__(self, *, model, inputs, parameters):\n'
        '        self.step = model["step"]\n'
        '        self.column = inputs["x"]\n'
        '        uri = model["snapshot"]["files"]["weights"]["uri"]\n'
        '        self.from_store = "/objects/" in uri\n'
        '        self.weight_text = Path(uri).read_text(encoding="utf-8")\n\n'
        '    def __call__(self, batch):\n'
        '        values = batch[self.column].to_pylist()\n'
        '        return pa.table({\n'
        '            "x": values,\n'
        '            "step": [self.step] * len(values),\n'
        '            "weight": [self.weight_text] * len(values),\n'
        '            "from_store": [self.from_store] * len(values),\n'
        '        })\n',
        encoding='utf-8',
    )

    (proj / 'flows' / 'train.py').write_text(
        '"""零内部导入的原生训练与封存权重评测流程。"""\n'
        'from demiflow import data\n'
        'from operators.training import SealedPredictor, public_trainer\n\n\n'
        'def run(digest: str, target: dict):\n'
        '    predictor = data.PredictorSpec(\n'
        '        factory=SealedPredictor,\n'
        '        inputs={"x": "x"},\n'
        '        input_schema={"x": "int64"},\n'
        '        output_schema={"x": "int64", "step": "int64", "weight": "string", "from_store": "bool"},\n'
        '        batch_format="pyarrow",\n'
        '    )\n'
        '    recipe = data.TrainingRecipe(\n'
        '        trainer=public_trainer,\n'
        '        predictor=predictor,\n'
        '        train_schema={"x": "int64"},\n'
        '        base_model_digest=digest,\n'
        '        environment_digest=digest,\n'
        '        model_format="native.test",\n'
        '        budget={"optimizer_updates": 800},\n'
        '        save_steps=[400, 800],\n'
        '        topology={"nodes": 1, "world_size": 1, "gpus_per_node": 0},\n'
        '    )\n'
        '    fitted = data.from_items([{"x": 1}]).fit(recipe=recipe)\n'
        '    holdout = data.from_items([{"x": 5}, {"x": 7}])\n'
        '    scores = fitted.checkpoints.select(steps=[400, 800]).predict(on=holdout)\n'
        '    return scores.write_lance(target["uri"], mode=target.get("mode", "create"), return_receipt=True)\n',
        encoding='utf-8',
    )

    (proj / 'configs' / 'train.json').write_text(
        json.dumps({
            'digest': 'a' * 64,
            'target': {'kind': 'output', 'uri': 'data/scores.lance', 'mode': 'create'},
        }),
        encoding='utf-8',
    )

    register_flow(proj, 'flows/train.py:run', params='configs/train.json')

    # Full project Forge check (including DF-02 internal SDK import & hygiene rules) MUST pass cleanly.
    report = check_project(proj)
    assert report['status'] == 'passed', report['diagnostics']
    assert report['native_flow']['status'] == 'passed'
    assert report['project_layout']['status'] == 'passed'
    assert report['diagnostic_count'] == 0

    # Execute through real native Workbench run and verify Predictor actually read sealed weights from ModelStore.
    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    deployment = tmp_path / 'deployment.json'
    deployment.write_text(json.dumps(dict(project_id='project', project_root=str(proj), binding=binding.to_dict())))
    monkeypatch.setenv('DEMIFLOW_TRAINING_DEPLOYMENT', str(deployment))

    prepared = runs.prepare(proj, 'flows/train.py:run', params='configs/train.json')
    assert prepared['status'] == 'prepared'
    monkeypatch.delenv('DEMIFLOW_TRAINING_DEPLOYMENT')
    result = execute(proj, prepared)
    assert result['status'] == 'succeeded', (result, runs.logs(proj, prepared['run_id']))

    out_rows = rows(proj, result)
    assert sorted((r['step'], r['x'], r['weight'], r['from_store']) for r in out_rows) == [
        (400, 5, 'sealed-weights-400', True),
        (400, 7, 'sealed-weights-400', True),
        (800, 5, 'sealed-weights-800', True),
        (800, 7, 'sealed-weights-800', True),
    ]


def test_public_return_only_without_report_snapshot_and_interrupted_resume_chains(tmp_path):
    """Acceptance chain #2:
    (a) return-only trainer without report_snapshot works end-to-end;
    (b) interrupted launch 1 reports step 1 -> launch 2 delivers ONLY step 3 -> both steps predict;
    (c) interrupted launch 1 crashes before report -> reconcile dict recovers step 1 -> launch 2 reports step 3 & returns None -> both steps predict.
    """
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [1]}), str(source))

    # (a) Return-only trainer (never calls report_snapshot)
    run_a = tmp_path / 'run_return_only'
    run_a.mkdir()
    _, binding_a = setup_request(run_a, managed=True)
    binding_a = replace(binding_a, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with RunContext.start(run_a / 'run', training=TrainingDeployment('project', binding_a)):
        train = data.read_lance(str(source), version=1)
        scores = train.fit(recipe=_public_recipe(public_return_only_trainer)).checkpoints.select(
            steps=[1, 3]).predict(on=data.from_items([{'x': 9}]))
        observed = scores.take_all()
    assert sorted((r['step'], r['x'], r['weight']) for r in observed) == [
        (1, 9, 'public-return-1'),
        (3, 9, 'public-return-3'),
    ]

    # (b) Interrupted resume via report_snapshot(1, files=..., resume_files=...); launch 2 returns ONLY {3: ...}
    run_b = tmp_path / 'run_report_resume'
    run_b.mkdir()
    _, binding_b = setup_request(run_b, managed=True)
    binding_b = replace(binding_b, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with RunContext.start(run_b / 'run', training=TrainingDeployment('project', binding_b)):
        train = data.read_lance(str(source), version=1)
        scores = train.fit(recipe=_public_recipe(public_report_and_resume_trainer)).checkpoints.select(
            steps=[1, 3]).predict(on=data.from_items([{'x': 4}]))
        observed_b = scores.take_all()
    assert sorted((r['step'], r['x'], r['weight']) for r in observed_b) == [
        (1, 4, 'resumed-weight-1'),
        (3, 4, 'resumed-weight-3-from-opt-state-1'),
    ]

    # (c) Interrupted resume via plain-dict reconcile; launch 2 calls report_snapshot(3, ...) and returns None
    run_c = tmp_path / 'run_reconcile_resume'
    run_c.mkdir()
    _, binding_c = setup_request(run_c, managed=True)
    binding_c = replace(binding_c, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with RunContext.start(run_c / 'run', training=TrainingDeployment('project', binding_c)):
        train = data.read_lance(str(source), version=1)
        scores = train.fit(recipe=_public_recipe(public_reconcile_crash_trainer)).checkpoints.select(
            steps=[1, 3]).predict(on=data.from_items([{'x': 6}]))
        observed_c = scores.take_all()
    assert sorted((r['step'], r['x'], r['weight']) for r in observed_c) == [
        (1, 6, 'reconciled-weight-1'),
        (3, 6, 'reconciled-weight-3'),
    ]


def test_final_step_outside_save_steps_requires_explicit_return_and_succeeds_when_returned(tmp_path):
    """When save_steps=(1,) and optimizer_updates=3, reporting step 1 and returning None must fail,
    while returning {3: ...} succeeds and allows selecting step 1."""
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [1]}), str(source))

    # Missing final step 3 -> must fail cleanly
    run_fail = tmp_path / 'run_missing_final'
    run_fail.mkdir()
    _, binding_fail = setup_request(run_fail, managed=True)
    binding_fail = replace(binding_fail, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with pytest.raises(Exception):
        with RunContext.start(run_fail / 'run', training=TrainingDeployment('project', binding_fail)):
            train = data.read_lance(str(source), version=1)
            train.fit(
                recipe=_public_recipe(
                    public_final_step_outside_save_steps_trainer,
                    save_steps=(1,),
                    optimizer_updates=3,
                    parameters={'deliver_final': False},
                )
            ).checkpoints.select(steps=[1]).predict(on=data.from_items([{'x': 2}])).take_all()

    # Explicitly returning final step 3 -> succeeds
    run_ok = tmp_path / 'run_with_final'
    run_ok.mkdir()
    _, binding_ok = setup_request(run_ok, managed=True)
    binding_ok = replace(binding_ok, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with RunContext.start(run_ok / 'run', training=TrainingDeployment('project', binding_ok)):
        train = data.read_lance(str(source), version=1)
        observed = train.fit(
            recipe=_public_recipe(
                public_final_step_outside_save_steps_trainer,
                save_steps=(1,),
                optimizer_updates=3,
                parameters={'deliver_final': True},
            )
        ).checkpoints.select(steps=[1]).predict(on=data.from_items([{'x': 2}])).take_all()
    assert observed == [{'x': 2, 'step': 1, 'weight': 'step-1-saved'}]


def test_public_trainer_return_disambiguation_conflicts_and_security_rails(tmp_path):
    """Unit tests covering every shape disambiguation, conflict, and path boundary rule in training_job."""
    request, binding = setup_request(tmp_path, managed=True)

    def _make_ctx(key='req-unit-1', req=request):
        work_dir = tmp_path / 'work' / key
        work_dir.mkdir(parents=True, exist_ok=True)
        launch = {
            'request_key': key,
            'work_dir': str(work_dir),
            'deadline_unix': time.time() + 60,
        }
        return launch, ManagedFitContext(launch, req, binding)

    class _Code:
        def __init__(self, fn):
            self.value = self
            self.target = fn

        def instantiate(self):
            return self.target

    # 1. Plain {"files": path} is a literal file named "files", NOT extended structure.
    launch, ctx = _make_ctx('k-literal-files')
    f1 = ctx.model_dir / 'f1.bin'
    f3 = ctx.model_dir / 'f3.bin'
    f1.write_bytes(b'one')
    f3.write_bytes(b'three')
    res = execute_training_job(
        launch, request,
        _Code(lambda req, c: {1: {'files': f1}, 3: {'files': f3}}),
        ctx,
    )
    parsed = FitResult.from_dict(res)
    assert set(parsed.snapshots[0].files) == {'files'}
    assert parsed.snapshots[0].resume_files == {}
    assert parsed.snapshots[1].files['files'].size_bytes == 5

    # 2. Extended {"files": {...}, "resume_files": {...}} and relative path inside model_dir.
    launch, ctx = _make_ctx('k-extended')
    (ctx.model_dir / 'w1.bin').write_bytes(b'w1')
    (ctx.model_dir / 'o1.bin').write_bytes(b'o1')
    (ctx.model_dir / 'w3.bin').write_bytes(b'w3')
    res = execute_training_job(
        launch, request,
        _Code(lambda req, c: {
            1: {'files': {'adapter': 'w1.bin'}, 'resume_files': {'state': 'o1.bin'}},
            3: {'files': {'adapter': c.model_dir / 'w3.bin'}},
        }),
        ctx,
    )
    parsed = FitResult.from_dict(res)
    assert set(parsed.snapshots[0].resume_files) == {'state'}
    assert parsed.snapshots[1].final_step if hasattr(parsed.snapshots[1], 'final_step') else parsed.final_step == 3

    # 3. Mixing extended and plain fields reports the exact step and offending field.
    launch, ctx = _make_ctx('k-mixed-step')
    (ctx.model_dir / 'a.bin').write_bytes(b'a')
    with pytest.raises(ValueError, match=r"Step 3:.*'extra'"):
        execute_training_job(
            launch, request,
            _Code(lambda req, c: {3: {'files': {'a': 'a.bin'}, 'extra': 'a.bin'}}),
            ctx,
        )
    with pytest.raises(ValueError, match=r"Step 3:.*'files'"):
        execute_training_job(
            launch, request,
            _Code(lambda req, c: {3: {'files': 'a.bin', 'resume_files': {'s': 'a.bin'}}}),
            ctx,
        )

    # 4. Single-step unwrapped return {"adapter": path} and mixed top-level keys are rejected.
    with pytest.raises(ValueError, match="integer step keys"):
        execute_training_job(launch, request, _Code(lambda req, c: {'adapter': 'a.bin'}), ctx)
    with pytest.raises(ValueError, match="cannot mix integer step keys"):
        execute_training_job(launch, request, _Code(lambda req, c: {3: {'a': 'a.bin'}, 'metrics': {}}), ctx)

    # 5. Contradicting an already-reported snapshot (mutating file between report_snapshot and return) fails.
    launch, ctx = _make_ctx('k-conflict')
    p3 = ctx.model_dir / 'w3.bin'
    p3.write_bytes(b'original')
    def _contradicting(req, c):
        c.report_snapshot(3, files={'w': p3})
        p3.write_bytes(b'mutated')
        return {3: {'w': p3}}
    with pytest.raises(ValueError, match=r"Step 3:.*contradicts"):
        execute_training_job(launch, request, _Code(_contradicting), ctx)

    # 6. Reporting the same step twice with different content fails immutability check.
    launch, ctx = _make_ctx('k-immutable')
    pa_file = ctx.model_dir / 'a.bin'
    pb_file = ctx.model_dir / 'b.bin'
    pa_file.write_bytes(b'aaa')
    pb_file.write_bytes(b'bbb')
    ctx.report_snapshot(1, files={'w': pa_file})
    # Identical re-report is idempotent
    ctx.report_snapshot(step=1, files={'w': pa_file})
    with pytest.raises(ValueError, match="immutable within a launch"):
        ctx.report_snapshot(1, files={'w': pb_file})

    # 7. Security rails: outside model_dir, '..' traversal, symlink escape, and directory path are rejected.
    launch, ctx = _make_ctx('k-sec')
    outside = tmp_path / 'outside_secret.txt'
    outside.write_text('secret', encoding='utf-8')
    symlink = ctx.model_dir / 'escape.bin'
    symlink.symlink_to(outside)
    subdir = ctx.model_dir / 'nested_dir'
    subdir.mkdir()

    for bad_path in (outside, '../outside_secret.txt', symlink, subdir):
        with pytest.raises(Exception):
            ctx.report_snapshot(1, files={'w': bad_path})

    # 8. Reconcile rejects unexpected fields, undeclared steps, and paths outside model_dir.
    orig_dir = tmp_path / 'work' / 'k-rec'
    orig_dir.mkdir(parents=True, exist_ok=True)
    orig_launch_bytes = json.dumps({'request': request.to_dict()}).encode()
    (orig_dir / 'launch.json').write_bytes(orig_launch_bytes)
    inspect_dir = tmp_path / 'work' / 'inspect-k-rec'
    inspect_dir.mkdir(parents=True, exist_ok=True)
    import hashlib
    inspect_launch = {
        'request_key': 'k-rec',
        'work_dir': str(inspect_dir),
        'deadline_unix': time.time() + 60,
        'inspect_source': {'key': 'k-rec', 'launch_digest': hashlib.sha256(orig_launch_bytes).hexdigest()},
    }
    inspect_ctx = ManagedFitContext(inspect_launch, request, binding)

    def _bad_reconcile_extra(req, *, model_dir, work_dir):
        return {'step': 1, 'files': {'w': 'x'}, 'bogus': 1}
    _fn1 = lambda req, c: None
    _fn1.reconcile = _bad_reconcile_extra
    with pytest.raises(ValueError, match="unexpected fields"):
        execute_training_job(inspect_launch, request, _Code(_fn1), inspect_ctx)

    def _bad_reconcile_step(req, *, model_dir, work_dir):
        return {'step': 999, 'files': {'w': 'x'}}
    _fn2 = lambda req, c: None
    _fn2.reconcile = _bad_reconcile_step
    with pytest.raises(ValueError, match="undeclared step"):
        execute_training_job(inspect_launch, request, _Code(_fn2), inspect_ctx)

    def _bad_reconcile_outside(req, *, model_dir, work_dir):
        return {'step': 1, 'files': {'w': outside}}
    _fn3 = lambda req, c: None
    _fn3.reconcile = _bad_reconcile_outside
    with pytest.raises(ValueError, match="must stay inside context.model_dir"):
        execute_training_job(inspect_launch, request, _Code(_fn3), inspect_ctx)
