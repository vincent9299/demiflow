"""Kill the original driver at a durable cut between two real prediction frontiers."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pyarrow as pa

from demiflow import data
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.training.deployment import TrainingDeployment
from test_training_dataset_action import recipe
from test_training_domain import setup_request
from training_action_programs import managed_trainer


class SeedScores:
    def __init__(self, *, model, inputs, parameters):
        self.step = model['step']
        self.input = inputs['x']
        self.seeds = list(parameters['seeds'])
        self.manifest = model['manifest_digest']
        self.audit = Path(parameters['audit'])
        self.attempt = 2 if (self.audit / 'resuming').exists() else 1

    def snapshot_state(self):
        return {'manifest': self.manifest, 'step': self.step, 'seeds': self.seeds}

    def restore_state(self, state):
        if state != self.snapshot_state():
            raise ValueError("Model step changed during restore")

    def __call__(self, batch):
        import signal
        import time
        import pyarrow as pa
        values = batch[self.input].to_pylist()
        if self.attempt == 1 and self.step == 800 and values == list(range(20,30)):
            # Hold the real third inference call while the driver sends the native cut request.
            # Its completed rows therefore precede the barrier on every host, independent of speed.
            (self.audit / 'request-cut').touch()
            limit = time.monotonic() + 20
            while not (self.audit / 'sent-cut').exists():
                if time.monotonic() > limit:
                    raise TimeoutError('Driver did not send the original checkpoint request')
                time.sleep(.01)
        with (self.audit / f'calls-{self.attempt}.jsonl').open('a') as trace:
            trace.write(json.dumps([self.step,values]) + '\n')
        outputs = [[] for _ in values]
        for i, seed in enumerate(self.seeds):
            for x, scores in zip(values,outputs):
                scores.append(self.step + seed*x)
            if self.attempt == 1 and self.step == 800 and i == 0 and (self.audit / 'fault.json').exists():
                (self.audit / 'interrupted-batch.json').write_text(json.dumps({'ids':values,'scores':outputs}))
                os.kill(int((self.audit / 'driver-pid').read_text()),signal.SIGKILL)
        return batch.append_column('step', pa.array([self.step] * len(values))).append_column(
            'scores', pa.array(outputs))


def _install_driver_crash(run, location):
    from demiflow.execution import commit
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    # Drive the real public checkpoint control on the driver's own pump thread.
    # Preparation, barriers, evidence, publication and recovery execute unchanged.
    pump = DataflowExecution.pump
    def pump_with_cut(execution, *, deadline_ms):
        ready, sent = location.parent / 'request-cut', location.parent / 'sent-cut'
        if ready.exists() and not sent.exists():
            execution.request_checkpoint()
            sent.touch()
        return pump(execution, deadline_ms=deadline_ms)
    DataflowExecution.pump = pump_with_cut
    models = {}
    def fault(name, commit_id):
        if name != 'after_checkpoint_committed':
            return
        record = run.journal.execution_commit(commit_id)
        prepared = record['prepared']
        if 'dataflow-cut-v3' not in prepared['state']:
            return
        cut = StateArtifacts(run).read(prepared['state']['dataflow-cut-v3'])
        if cut['cut']['final'] or len(cut.get('model_bindings', {})) != 2:
            return
        if not models:
            graph = load_sealed_plan(run.directory, record['action_id'])['graph']
            models.update({e.consumer:e.step for e in graph.value_edges if e.input_port == 'model'})
        rows = {step: 0 for step in models.values()}
        for worker in cut['contributions']:
            for ref in worker['operator_states'].values():
                if ref['media_type'] != 'application/json':
                    continue
                obj = run.directory / '.dataflow-store' / ref['store_id'] / 'objects' / ref['key']
                state = json.loads(obj.read_bytes())
                if isinstance(state, dict) and state.get('schema') == 'demiflow.worker-frontier.v3':
                    for occurrence, metrics in state['metrics'].items():
                        if occurrence in models:
                            rows[models[occurrence]] += metrics['rows_output']
        if rows == {400:100, 800:30} and not location.exists():
            location.write_text(json.dumps({'cut':commit_id,'steps':rows,'attempt':run.attempt,
                                            'action_id':record['action_id'], 'bindings':cut['model_bindings']}))
    commit.fault_point = fault


def _execute(root, mode):
    _, binding = setup_request(root, managed=True)
    binding = replace(binding, provider_options={'cpu_millis':100, 'memory_bytes':16 << 20})
    deployment = TrainingDeployment('project', binding)
    directory = root / 'run'
    ctx = RunContext.start(directory, training=deployment,
        limits={'checkpoint_rows':256,'checkpoint_interval_ms':60000,'timeout_s':300}) if mode == 'crash' else (
        RunContext.resume(RunRef(str(directory), RunJournal(directory).recovery_identity()['run_id']),
                          training=deployment))
    with ctx:
        if mode == 'crash':
            (root / 'driver-pid').write_text(str(os.getpid()))
            _install_driver_crash(ctx, root / 'fault.json')
        predictor = replace(recipe().predictor, factory=SeedScores, batch_size=10, parameters={'seeds':[1,3],'audit':str(root)},
            parameter_schema={'type':'object','properties':{'seeds':{'type':'array','items':{'type':'integer'}},'audit':{'type':'string'}}},
            output_schema={'x':'int64','step':'int64','scores':'list<item: int64>'})
        train = data.read_lance(str(root / 'train.lance'), version=1)
        holdout = data.range(100).rename_columns({'id':'x'})
        models = train.fit(recipe=replace(recipe(), trainer=managed_trainer, predictor=predictor,
                            budget={'optimizer_updates':800}, save_steps=(400,800))).checkpoints.select(steps=[400,800])
        models.predict(on=holdout).write_lance(str(root / 'scores.lance'), mode='create')


def test_two_checkpoint_predictions_recover_original_partial_model_frontier(tmp_path):
    lance.write_dataset(pa.table({'x':[0,1,2]}), str(tmp_path / 'train.lance'))
    cmd = [sys.executable,'-B','-c','import sys; from pathlib import Path; '
        'from test_training_prediction_recovery import _execute; _execute(Path(sys.argv[1]),sys.argv[2])',str(tmp_path)]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(s for s in sys.path if s))
    crash = subprocess.run([*cmd, 'crash'], env=env, text=True, capture_output=True, timeout=320)
    import signal
    assert crash.returncode == -signal.SIGKILL, crash.stderr
    fault = json.loads((tmp_path / 'fault.json').read_bytes())
    run = RunJournal(tmp_path / 'run')
    before = run.execution_commit(fault['cut'])
    assert before['status'] in {'checkpoint_committed', 'acked'}
    partial = json.loads((tmp_path / 'interrupted-batch.json').read_bytes())
    assert partial['ids'] == list(range(30,40)) and all(len(v)==1 for v in partial['scores'])
    assert not (tmp_path / 'scores.lance').exists()
    launch, = (tmp_path / 'run' / 'training').glob('*/launch.json')
    receipt = (launch.parent / 'result.json').read_bytes()
    (tmp_path / 'resuming').touch()
    resumed = subprocess.run([*cmd, 'resume'], env=env, text=True, capture_output=True, timeout=320)
    assert resumed.returncode == 0, resumed.stderr
    calls = [json.loads(line) for line in (tmp_path / 'calls-2.jsonl').read_text().splitlines()]
    assert {step for step, _ in calls} == {800}
    resumed_ids = [x for _, batch in calls for x in batch]
    assert resumed_ids == list(range(30,100))
    assert (launch.parent / 'result.json').read_bytes() == receipt
    assert len(list(launch.parent.parent.glob('*/launch.json'))) == 1
    assert run.execution_status()['attempt'] == 2 and run.execution_commit(fault['cut'])['status'] == 'acked'
    after = run.latest_execution_commit(fault['action_id'])
    cut_path = after['prepared']['state']['dataflow-cut-v3']['path']
    final = json.loads((tmp_path / 'run' / cut_path).read_bytes())
    assert final['cut']['final'] and final['model_bindings'] == fault['bindings']
    table = lance.dataset(str(tmp_path / 'scores.lance'))
    assert table.version == 1
    rows = table.to_table().to_pylist()
    outcomes = sorted((row['step'], row['x'], seed, score)
                      for row in rows for seed, score in enumerate(row['scores']))
    expected = [(step,x,seed,step+scale*x) for step in [400,800] for x in range(100)
                for seed,scale in enumerate((1,3))]
    assert outcomes == expected
    for step in [400,800]:
        assert sum(s for m,_,_,s in outcomes if m == step)/200 == step+99
