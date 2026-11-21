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
