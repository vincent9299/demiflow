"""A registered project flow runs through the native Workbench worker and history readers."""
from dataclasses import replace
import json
from pathlib import Path

import lance

from demiflow.forge.project import register_flow
from demiflow.workbench.graph.model import get
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.runs.view import snapshot
from test_forge_runs import base, execute, root
from test_training_domain import setup_request


def test_registered_native_training_run_seals_and_reports_model_evidence(root, tmp_path, monkeypatch):
    programs = Path(__file__).with_name('training_action_programs.py')
    (root / 'operators' / 'training.py').write_bytes(programs.read_bytes())
    (root / 'flows' / 'train.py').write_text('''from demiflow import data
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
''')
    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    deployment = tmp_path / 'deployment.json'
    deployment.write_text(json.dumps(dict(project_id='project', project_root=str(root), binding=binding.to_dict())))
    monkeypatch.setenv('DEMIFLOW_TRAINING_DEPLOYMENT', str(deployment))
    params = 'flows/train.json'
    (root / params).write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',
        arguments={'digest':'a'*64, 'target':{'kind':'output','location_id':'local','relative_uri':'scores.lance'}})))
    entry = 'flows/train.py:run'
    register_flow(root, entry, params=params)
    prepared = runs.prepare(root, entry, params=params)
    assert prepared['status'] == 'prepared'
    result = execute(root, prepared)
    run_id = prepared['run_id']
    assert result['status'] == 'succeeded', (result, runs.logs(root, run_id))
    output, = result['outputs']
    _, ref, location = runs.result_table(root, run_id, output['name'], attempt=1)
    assert ref.row_count == 4
    rows = lance.dataset(str(location), version=ref.version).to_table().to_pylist()
    assert sorted((r['step'], r['x']) for r in rows) == [(400,5),(400,7),(800,5),(800,7)]
    graph = get(root, basis_kind='run', run_id=run_id, attempt=1)
    action, = graph['logical']['actions']
    assert action['status'] == 'sealed' and action['version'] == 'v4'
    output_history, = snapshot(root, run_id, attempt=1)['outputs']
    provenance, = output_history['training']['models']
    assert not output_history['training']['has_more']
    assert provenance['action_id'] == output_history['action_id'] and provenance['attempt'] == 1
