"""Kill the submitting driver before the first row cut and resume the original action."""
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import time

import lance
import pyarrow as pa
from demiflow import data
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.training.deployment import TrainingDeployment
from test_training_dataset_action import recipe
from test_training_domain import setup_request
from training_action_programs import waiting_trainer


def execute(root, resume):
    _, binding = setup_request(root, managed=True)
    deployment = TrainingDeployment('project', replace(binding, provider_options={
        'cpu_millis': 100, 'memory_bytes': 16 << 20}))
    location = root / 'run'
    if resume:
        run_id = RunJournal(location).recovery_identity()['run_id']
        ctx = RunContext.resume(RunRef(str(location), run_id), training=deployment)
    else:
        ctx = RunContext.start(location, training=deployment)
    with ctx:
        scores = data.read_lance(str(root / 'train.lance'), version=1).fit(
            recipe=replace(recipe(), trainer=waiting_trainer)).checkpoints.select(steps=[3]).predict(
            on=data.from_items([{'x': 19}]))
        scores.write_lance(str(root / 'scores.lance'), mode='create')


def test_original_lance_create_adopts_running_fit_without_row_cut(tmp_path):
    lance.write_dataset(pa.table({'x': [1]}), str(tmp_path / 'train.lance'))
    command = [sys.executable, '-B', '-c',
               'import sys; from pathlib import Path; from test_training_driver_crash import execute; '
               'execute(Path(sys.argv[1]), sys.argv[2]=="resume")', str(tmp_path)]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
    with subprocess.Popen(command + ['start'], env=env) as process:
        try:
            deadline = time.monotonic() + 50
            while not (tmp_path / 'project' / 'started').exists():
                assert process.poll() is None and time.monotonic() < deadline
                time.sleep(.05)
            process.kill()
            process.wait(timeout=5)
            journal = RunJournal(tmp_path / 'run')
            import json
            launch, = (tmp_path / 'run' / 'training').glob('*/launch.json')
            source_id = json.loads(launch.read_bytes())['action']
            assert journal.latest_execution_commit(source_id) is None
        finally:
            if process.poll() is None:
                process.kill()
            (tmp_path / 'project' / 'proceed').touch()
    subprocess.run(command + ['resume'], env=env, check=True, timeout=75)
    score_table = lance.dataset(str(tmp_path / 'scores.lance'))
    assert score_table.version == 1
    assert score_table.to_table().to_pylist() == [{'x': 19, 'step': 3}]
    launch, = (tmp_path / 'run' / 'training').iterdir()
    assert launch.name == (tmp_path / 'project' / 'started').read_text()
    assert RunJournal(tmp_path / 'run').execution_status()['attempt'] == 2
