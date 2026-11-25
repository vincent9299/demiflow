"""Sealed business programs for end-to-end Dataset training action tests."""
import pyarrow as pa


def trainer(request, context):
    raise AssertionError("The provider owns this training job")


class StepPredictor:
    def __init__(self, *, model, inputs, parameters):
        self.step = model['step']
        self.column = inputs['x']

    def __call__(self, batch):
        values = batch[self.column].to_pylist()
        return pa.table({'x': values, 'step': [self.step] * len(values)})


def managed_trainer(request, context):
    from hashlib import sha256
    import os
    from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef
    snapshot_refs = []
    for step in request.semantics['save_steps']:
        context.check_cancelled()
        data = f"{step}-{os.getpid()}".encode()
        path = context.model_dir / str(step)
        path.write_bytes(data)
        weight = ProjectObject(str(path), sha256(data).hexdigest(), len(data))
        snap = TrainingSnapshotRef(step, 'native.test', {'weights': weight})
        context.report_snapshot(snap)
        snapshot_refs.append(snap)
    return FitResult(request.instance_id, request.fit_key, tuple(snapshot_refs), step)



def waiting_trainer(request, context):
    """A real managed process survives its submitting driver during the test."""
    from pathlib import Path
    import time
    root = Path(context.binding.project_dir)
    (root / 'started').write_text(context.request_key)
    while not (root / 'proceed').exists():
        context.check_cancelled()
        time.sleep(.05)
    return managed_trainer(request, context)


class BatchedPredictor:
    def __init__(self, *, model, inputs, parameters):
        self.mode = parameters['mode']

    def __call__(self, batch):
        if self.mode == 'drop':
            return batch.slice(0, 0)
        if self.mode == 'reverse':
            return batch.take(list(reversed(range(batch.num_rows))))
        return batch


def inspect_native_step(request, *, work_dir, model_dir):
    from demiflow.execution.contracts.training import TrainingSnapshotRef
    from demiflow.resources.process_observation import inherited_unit
    import json
    import os
    assert inherited_unit()
    (work_dir / 'inspection-pid').write_text(str(os.getpid()))
    return TrainingSnapshotRef.from_dict(json.loads((model_dir / 'native-step.json').read_text()))


def native_checkpoint_crash(request, context):
    """Commit native optimizer state, die before report, and resume a new writer."""
    from hashlib import sha256
    import json
    import os
    from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef
    step = 3 if request.restart_snapshot else 1
    raw = f'optimizer-{step}'.encode()
    path = context.model_dir / 'payload'
    path.write_bytes(raw)
    obj = ProjectObject(path.as_uri(), sha256(raw).hexdigest(), len(raw))
    snap = TrainingSnapshotRef(step, 'native.test', {'weights': obj}, {'optimizer': obj})
    if step == 1:
        (context.model_dir / 'native-step.json').write_text(json.dumps(snap.to_dict()))
        os._exit(77)
    assert request.restart_snapshot.step == 1
    return FitResult(request.instance_id, request.fit_key, (snap,), 3)


native_checkpoint_crash.reconcile = inspect_native_step


class SealedWeightReaderPredictor:
    """Reads actual sealed weight bytes from the managed model store during prediction."""

    def __init__(self, *, model, inputs, parameters):
        from pathlib import Path
        self.step = model['step']
        self.column = inputs['x']
        weights_uri = model['snapshot']['files']['weights']['uri']
        self.weights_uri = weights_uri
        self.weight_text = Path(weights_uri).read_text(encoding='utf-8')

    def __call__(self, batch):
        values = batch[self.column].to_pylist()
        return pa.table({
            'x': values,
            'step': [self.step] * len(values),
            'weight': [self.weight_text] * len(values),
        })


def public_return_only_trainer(request, context):
    """Zero internal imports: returns {step: {name: path}} directly without report_snapshot."""
    checkpoints = {}
    for step in request.semantics['save_steps']:
        context.check_cancelled()
        path = context.model_dir / f'weights_step_{step}.txt'
        path.write_text(f'public-return-{step}', encoding='utf-8')
        checkpoints[step] = {'weights': path}
    return {'checkpoints': checkpoints, 'metrics': {'mode': 'public_return'}}


def public_report_and_resume_trainer(request, context):
    """Zero internal imports: reports step 1 then crashes; launch 2 delivers ONLY step 3."""
    import os
    from pathlib import Path
    if request.restart_snapshot is None:
        w1 = context.model_dir / 'weights_step_1.txt'
        w1.write_text('resumed-weight-1', encoding='utf-8')
        opt1 = context.model_dir / 'optimizer_step_1.txt'
        opt1.write_text('opt-state-1', encoding='utf-8')
        context.report_snapshot(1, files={'weights': w1}, resume_files={'optimizer': opt1})
        os._exit(77)
    assert request.restart_snapshot.step == 1
    opt_text = Path(request.restart_snapshot.resume_files['optimizer'].uri).read_text(encoding='utf-8')
    assert opt_text == 'opt-state-1'
    w3 = context.model_dir / 'weights_step_3.txt'
    w3.write_text('resumed-weight-3-from-' + opt_text, encoding='utf-8')
    # Second launch delivers ONLY the new step 3; driver merges historical step 1.
    return {3: {'weights': w3}}


def _public_reconcile_dict(request, *, work_dir, model_dir):
    """Zero internal imports: returns plain dict {step, files, resume_files}."""
    import json
    marker = model_dir / 'unreported_step.json'
    if not marker.exists():
        return None
    info = json.loads(marker.read_text(encoding='utf-8'))
    return {
        'step': info['step'],
        'files': {'weights': model_dir / info['weights']},
        'resume_files': {'optimizer': model_dir / info['optimizer']},
    }


def public_reconcile_crash_trainer(request, context):
    """Zero internal imports: crashes before report_snapshot; reconcile recovers step 1; launch 2 reports step 3 & returns None."""
    import json
    import os
    from pathlib import Path
    if request.restart_snapshot is None:
        (context.model_dir / 'w1.txt').write_text('reconciled-weight-1', encoding='utf-8')
        (context.model_dir / 'opt1.txt').write_text('reconciled-opt-1', encoding='utf-8')
        (context.model_dir / 'unreported_step.json').write_text(
            json.dumps({'step': 1, 'weights': 'w1.txt', 'optimizer': 'opt1.txt'}),
            encoding='utf-8',
        )
        os._exit(77)
    assert request.restart_snapshot.step == 1
    opt_text = Path(request.restart_snapshot.resume_files['optimizer'].uri).read_text(encoding='utf-8')
    assert opt_text == 'reconciled-opt-1'
    w3 = context.model_dir / 'w3.txt'
    w3.write_text('reconciled-weight-3', encoding='utf-8')
    context.report_snapshot(3, files={'weights': w3})
    return None


public_reconcile_crash_trainer.reconcile = _public_reconcile_dict


def public_final_step_outside_save_steps_trainer(request, context):
    """save_steps=(1,), optimizer_updates=3: reports step 1; delivers step 3 only when parameters['deliver_final'] is True."""
    w1 = context.model_dir / 'w1.txt'
    w1.write_text('step-1-saved', encoding='utf-8')
    context.report_snapshot(1, files={'weights': w1})
    if not request.parameters.get('deliver_final'):
        return None
    w3 = context.model_dir / 'w3.txt'
    w3.write_text('step-3-final', encoding='utf-8')
    return {3: {'weights': w3}}
