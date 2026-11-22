"""A registered project flow runs through the native Workbench worker and history readers."""
from dataclasses import replace
import json

import lance
import pytest

from demiflow.forge.project import register_flow
from demiflow.workbench.graph.model import get
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.runs.view import snapshot
from test_forge_runs import base, execute, root
from test_training_domain import setup_request


@pytest.mark.parametrize(('native', 'gpu'), [(False, 0), (True, 0), (False, 1), (True, 1)],
                         ids=['checked-cpu', 'native-cpu', 'checked-gpu', 'native-gpu'])
def test_registered_native_training_run_seals_and_reports_model_evidence(root, tmp_path, monkeypatch, native, gpu):
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'private-resources'))
    (root / 'operators' / 'training.py').write_text('''from hashlib import sha256
from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef

def managed_trainer(request, context):
    import os
    if request.semantics['topology']['gpus_per_node']:
        assert os.environ['CUDA_VISIBLE_DEVICES'] == 'gpu-test'
    snapshots = []
    for step in request.semantics['save_steps']:
        path = context.model_dir / str(step)
        payload = str(step).encode()
        path.write_bytes(payload)
        obj = ProjectObject(str(path), sha256(payload).hexdigest(), len(payload))
        snapshot = TrainingSnapshotRef(step, 'native.test', {'weights':obj})
        context.report_snapshot(snapshot)
        snapshots.append(snapshot)
    return FitResult(request.instance_id, request.fit_key, tuple(snapshots), step)

class StepPredictor:
    def __init__(self, *, model, inputs, parameters):
        self.step = model['step']
    def __call__(self, batch):
        import pyarrow as pa
        return batch.append_column('step', pa.array([self.step]*len(batch)))
''')
    flow = '''from demiflow import data
from operators.training import managed_trainer, StepPredictor

def run(*, digest, target):
    predictor = data.PredictorSpec(factory=StepPredictor, inputs={'x':'x'}, input_schema={'x':'int64'},
        output_schema={'x':'int64','step':'int64'}, batch_format='pyarrow')
    recipe = data.TrainingRecipe(trainer=managed_trainer, predictor=predictor,
        train_schema={'x':'int64'}, base_model_digest=digest, environment_digest=digest,
        model_format='native.test', budget={'optimizer_updates':800}, save_steps=[400,800],
        topology={'nodes':1,'world_size':1,'gpus_per_node':0})
    fitted = data.from_items([{'x':1}]).fit(recipe=recipe)
    holdout = data.from_items([{'x':5},{'x':7}])
    scores = fitted.checkpoints.select(steps=[400,800]).predict(on=holdout)
    return scores.write_lance(target['uri'], mode='create', return_receipt=True)
'''
    if gpu:
        from test_workbench_native_gpu import deployment as resource_deployment
        resources = resource_deployment(tmp_path)
        inventory = tmp_path / 'deployment-inventory.json'
        inventory.write_text(json.dumps(resources['device_inventory']))
        monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY', str(inventory))
        monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', resources['resource_root'])
        monkeypatch.setenv('DEMIFLOW_WORKBENCH_GPU_VIRTUAL_BYTES', str(resources['virtual_address_bytes']))
        flow = flow.replace("'gpus_per_node':0", "'gpus_per_node':1")
    if native:
        flow = flow.replace('from demiflow import data', 'from demiflow import data\nimport pyarrow as pa').replace(
            "mode='create'", "mode='create', schema=pa.schema([('x', pa.int64()), ('step', pa.int64())])")
    (root / 'flows' / 'train.py').write_text(flow)
    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    deployment = tmp_path / 'deployment.json'
    deployment.write_text(json.dumps(dict(project_id='project', project_root=str(root), binding=binding.to_dict())))
    monkeypatch.setenv('DEMIFLOW_TRAINING_DEPLOYMENT', str(deployment))
    params = 'flows/train.json'
    target = {'uri': str(root / 'native-scores.lance')} if native else {
        'kind':'output','location_id':'local','relative_uri':'scores.lance'}
    (root / params).write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',
        arguments={'digest':'a'*64, 'target':target})))
    entry = 'flows/train.py:run'
    if native:
        from _forge_project_fixtures import declare_existing_flow
        declare_existing_flow(root, entry, params=params)
    else:
        register_flow(root, entry, params=params)
    prepared = runs.prepare(root, entry, params=params)
    assert prepared['status'] == 'prepared'
    # Start must use the already sealed deployment and inventory. The parent
    # may have since been restarted with an entirely different environment.
    monkeypatch.delenv('DEMIFLOW_TRAINING_DEPLOYMENT')
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY', raising=False)
    result = execute(root, prepared)
    run_id = prepared['run_id']
    assert result['status'] == 'succeeded', (result, runs.logs(root, run_id))
    if gpu:
        from pathlib import Path
        records = [json.loads(p.read_bytes()) for p in
                   (Path(resources['resource_root']) / 'devices' / 'allocations').glob('*.json')]
        assert len(records) == 1 and records[0]['state'] == 'RELEASED'
    output, = result['outputs']
    _, ref, location = runs.result_table(root, run_id, output['name'], attempt=1)
    assert ref.row_count == 4
    rows = lance.dataset(str(location), version=ref.lance_version).to_table().to_pylist()
    assert sorted((r['step'], r['x']) for r in rows) == [(400,5),(400,7),(800,5),(800,7)]
    graph = get(root, basis_kind='run', run_id=run_id, attempt=1)
    action, = [a for a in graph['logical']['actions'] if a['action_id'] == output['action_id']]
    assert action['status'] == 'sealed' and action['version'] == 'v4'
    output_history, = snapshot(root, run_id, attempt=1)['outputs']
    provenance, = output_history['training']['models']
    assert not output_history['training']['has_more']
    assert provenance['selected_steps'] == [400,800]
    assert (provenance['uri'], provenance['version']) == (str(location), ref.lance_version)
    assert len(provenance['manifest_digest']) == 64
    assert 'configuration schema:' not in runs.logs(root,run_id)['text']
    assert provenance['action_id'] == output_history['action_id'] and provenance['attempt'] == 1
