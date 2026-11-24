"""Kill the submitting driver before the first row cut and resume the original action."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.contracts.recovery_contract import RunRef
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.pipeline.training_deployment import TrainingDeployment
from test_training_dataset_action import recipe
from test_training_domain import setup_request
from training_action_programs import waiting_trainer


def execute(root, resume, stopped_at):
    if stopped_at == 'ready' and not resume:
        from demiflow.execution.hosts.training_host import ManagedFitHost
        activate = ManagedFitHost._activate
        def wait_before_activation(self, key, location):
            if (location / 'ready.json').exists():
                (root / 'ready_to_authorize').touch()
                while not (root / 'release_driver').exists():
                    time.sleep(.05)
            return activate(self, key, location)
        ManagedFitHost._activate = wait_before_activation
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


@pytest.mark.parametrize('stopped_at', ['training', 'ready'])
def test_original_lance_create_adopts_running_fit_without_row_cut(tmp_path, stopped_at):
    lance.write_dataset(pa.table({'x': [1]}), str(tmp_path / 'train.lance'))
    command = [sys.executable, '-B', '-c',
               'import sys; from pathlib import Path; from test_training_driver_crash import execute; '
               'execute(Path(sys.argv[1]), sys.argv[3]=="resume", sys.argv[2])', str(tmp_path), stopped_at]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
    started = tmp_path / 'project' / 'started'
    stop_point = started if stopped_at == 'training' else tmp_path / 'ready_to_authorize'
    proceed = tmp_path / 'project' / 'proceed'
    try:
        with subprocess.Popen(command + ['start'], env=env) as process:
            try:
                deadline = time.monotonic() + 60
                while not stop_point.exists():
                    assert process.poll() is None and time.monotonic() < deadline
                    time.sleep(.05)
                if stopped_at == 'ready':
                    assert not started.exists()
            finally:
                process.kill()
                process.wait(timeout=5)
        journal = RunJournal(tmp_path / 'run')
        launch, = (tmp_path / 'run' / 'training').glob('*/launch.json')
        source_id = json.loads(launch.read_bytes())['action']
        assert journal.latest_execution_commit(source_id) is None
        with subprocess.Popen(command + ['resume'], env=env) as successor:
            try:
                deadline = time.monotonic() + 60
                while journal.execution_status()['attempt'] < 2 or not started.exists():
                    assert successor.poll() is None and time.monotonic() < deadline
                    time.sleep(.05)
                assert journal.latest_execution_commit(source_id) is None
                proceed.touch()
                assert successor.wait(timeout=60) == 0
            finally:
                proceed.touch()
                if successor.poll() is None:
                    successor.kill()
    finally:
        if proceed.parent.exists():
            proceed.touch()
    score_table = lance.dataset(str(tmp_path / 'scores.lance'))
    assert score_table.version == 1
    assert score_table.to_table().to_pylist() == [{'x': 19, 'step': 3}]
    launch, = (tmp_path / 'run' / 'training').iterdir()
    assert launch.name == started.read_text()
    assert journal.execution_status()['attempt'] == 2


def test_job_guardian_enforces_time_after_driver_exit(tmp_path):
    """An uncooperative numerical worker cannot outlive the original job limit."""
    import fcntl
    from demiflow.resources.process_observation import UNIT_ENV, unit_stopped
    from demiflow.resources.process_unit import birth_start
    fd = os.open(tmp_path / 'unit.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        until = time.time() + 4
        command = [sys.executable, '-B', '-c', '''
from demiflow.resources.process_group import supervise_unit
from pathlib import Path
import sys
raise SystemExit(supervise_unit([sys.executable, '-c',
    "from pathlib import Path; import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
    "Path(__import__('sys').argv[1]).write_text(str(os.getpid())); time.sleep(300)", sys.argv[1]],
    deadline_unix=float(sys.argv[2]), max_rss_bytes=1024**3, shutdown_timeout_s=.2))
''', str(tmp_path / 'child'), str(until)]
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
        with subprocess.Popen(command, env={**env, UNIT_ENV: str(fd)}, pass_fds=(fd,)) as process:
            os.close(fd)
            fd = None
            while not (tmp_path / 'child').exists():
                assert process.poll() is None and time.time() < until
                time.sleep(.03)
            child = int((tmp_path / 'child').read_text())
            born = birth_start(child)
            assert born is not None and not unit_stopped(tmp_path / 'unit.lock')
            assert process.wait(timeout=7) != 0
            assert birth_start(child) != born and unit_stopped(tmp_path / 'unit.lock')
    finally:
        if fd is not None:
            os.close(fd)
