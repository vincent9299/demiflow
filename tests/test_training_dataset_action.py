"""Exercise job value edges through public Dataset actions and real Lance writes."""
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.training import PredictorSpec, TrainingRecipe
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.pipeline.training_deployment import TrainingDeployment
from test_training_domain import Provider, setup_request


from training_action_programs import StepPredictor, managed_trainer, trainer
from demiflow.execution.contracts.dataflow_errors import DataflowError


def recipe():
    return TrainingRecipe(trainer=trainer, predictor=PredictorSpec(factory=StepPredictor,
             inputs={'x': 'x'}, input_schema={'x':'int64'}, output_schema={'x': 'int64', 'step': 'int64'}, batch_format='pyarrow'),
        train_schema={'x': 'int64'}, base_model_digest='a'*64, environment_digest='b'*64,
        model_format='native.test', budget={'optimizer_updates': 3}, save_steps=(1, 3),
        topology={'nodes': 1, 'world_size': 1, 'gpus_per_node': 0})


def positive(row):
    return row['x'] > 0


@pytest.mark.parametrize('derived', [False, True])
def test_two_model_steps_share_one_fit_across_dataset_actions(tmp_path, derived):
    import lance
    source = tmp_path / 'training.lance'
    lance.write_dataset(pa.table({'x': [0, 1, 2]}), str(source))
    _, binding = setup_request(tmp_path)
    provider = Provider()
    deployment = TrainingDeployment('project', binding, provider)
    with RunContext.start(tmp_path / 'run', training=deployment):
        train = data.read_lance(str(source), version=1)
        if derived:
            train = train.filter(positive)
        holdout = data.from_items([{'x': 7}, {'x': 8}])
        predictions = train.fit(recipe=recipe()).checkpoints.select(steps=[1, 3]).predict(on=holdout)
        uri = str(tmp_path / 'scores.lance')
        predictions.write_lance(uri, mode='create')
        assert predictions.count() == 4
    assert len(provider.sends) == 1
    assert next(iter(provider.jobs.values()))[0].inputs['train'].rows == (2 if derived else 3)
    observed = lance.dataset(uri).to_table().to_pylist()
    assert sorted((r['step'], r['x']) for r in observed) == [(1,7), (1,8), (3,7), (3,8)]


def test_managed_trainer_runs_in_an_allocated_process(tmp_path):
    from dataclasses import replace
    import lance
    from pathlib import Path
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [1]}), str(source))
    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    deployment = TrainingDeployment('project', binding)
    with RunContext.start(tmp_path / 'run', training=deployment):
        train = data.read_lance(str(source), version=1)
        scores = train.fit(recipe=replace(recipe(), trainer=managed_trainer)).checkpoints.select(
            steps=[1, 3]).predict(on=data.from_items([{'x': 9}]))
        assert scores.count() == 2
        assert scores.count() == 2
    launch, = (tmp_path / 'run' / 'training').iterdir()
    result = __import__('json').loads((launch / 'result.json').read_bytes())
    pid = Path(result['snapshots'][0]['files']['weights']['uri']).read_text().split('-')[1]
    assert int(pid) != __import__('os').getpid()
    units = tmp_path / 'run-driver-resources' / 'units'
    scope, = units.glob('fit-*')
    receipt, = scope.glob('*/cleanup.json')
    proof = __import__('json').loads(receipt.read_bytes())
    assert all(proof[k] for k in ('birth_identity_gone', 'unit_stopped', 'descendants_empty'))


@pytest.mark.parametrize('mode', ['valid', 'drop', 'reverse'])
def test_batched_predictions_preserve_duplicate_input_occurrences(tmp_path, mode):
    from dataclasses import replace
    import lance
    from training_action_programs import BatchedPredictor
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [7]}), str(source))
    _, binding = setup_request(tmp_path)
    deployment = TrainingDeployment('project', binding, Provider())
    spec = replace(recipe().predictor, factory=BatchedPredictor, batch_size=2,
                   output_schema={'x': 'int64'}, parameters={'mode': mode},
                   parameter_schema={'type': 'object', 'properties': {'mode': {'type': 'string'}}})
    def execute():
        with RunContext.start(tmp_path / 'run', training=deployment):
            fit = data.read_lance(str(source), version=1).fit(recipe=replace(recipe(), predictor=spec))
            scores = fit.checkpoints.select(steps=[1]).predict(on=data.from_items([{'x': 4}, {'x': 4}]))
            return scores.take_all()

    if mode == 'valid':
        assert execute() == [{'x': 4}, {'x': 4}]
    else:
        with pytest.raises(DataflowError, match='Predictor'):
            execute()


def test_unreachable_budget_is_rejected_before_managed_allocation(tmp_path):
    from dataclasses import replace
    import lance
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [1]}), str(source))
    _, binding = setup_request(tmp_path, managed=True)
    deployment = TrainingDeployment('project', binding)
    with pytest.raises(ValueError, match='budget is unreachable'), RunContext.start(
            tmp_path / 'run', training=deployment):
        budget = {'optimizer_updates': 3, 'max_epochs': 1}
        fit = data.read_lance(str(source), version=1).fit(recipe=replace(recipe(), budget=budget,
                        topology={'nodes': 1, 'gpus_per_node': 1, 'effective_batch': 4}))
        fit.checkpoints.select(steps=[1]).predict(on=data.from_items([{'x': 2}])).count()
    assert not (tmp_path / 'run' / 'training').exists()
    assert not list((tmp_path / 'run-driver-resources' / 'units').glob('fit-*'))


def test_managed_native_checkpoint_reconciliation_runs_in_cpu_unit(tmp_path):
    from dataclasses import replace
    import json
    import os
    import lance
    from training_action_programs import native_checkpoint_crash
    source = tmp_path / 'train.lance'
    lance.write_dataset(pa.table({'x': [1]}), str(source))
    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    with RunContext.start(tmp_path / 'run', training=TrainingDeployment('project', binding)):
        train = data.read_lance(str(source), version=1)
        scores = train.fit(recipe=replace(recipe(), trainer=native_checkpoint_crash)).checkpoints.select(
            steps=[1, 3]).predict(on=data.from_items([{'x': 9}]))
        assert sorted(row['step'] for row in scores.take_all()) == [1, 3]
    launches = {p.parent: json.loads(p.read_bytes()) for p in (tmp_path / 'run' / 'training').glob('*/launch.json')}
    assert len(launches) == 3
    inspector, = [launch for launch in launches.values() if 'inspect_source' in launch]
    original = tmp_path / 'run' / 'training' / inspector['inspect_source']['key']
    assert int((original / 'inspection-pid').read_text()) != os.getpid()
    from demiflow.execution.contracts.training import FitRequest
    assert len({FitRequest.from_dict(launch['request']).instance_id for launch in launches.values()}) == 1
