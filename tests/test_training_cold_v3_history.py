"""An independently imported published runtime creates each original v3 seal and cut."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest

from demiflow.execution.run_journal import RunJournal
from demiflow.execution.sealed_plans import load_sealed_plan


REPO = Path(__file__).resolve().parents[1]
PUBLISHED = ('f12f2e405f6d88b94c7f74dfa2e9db9b657a9719',
             'b2613ba8901d1482763385c2b3a7c81c00a8710d')


@pytest.fixture(scope='module', params=PUBLISHED, ids=lambda rev:rev[:8])
def old_sdk(request, tmp_path_factory):
    checkout = tmp_path_factory.mktemp('published-sdk')
    archive = checkout / 'sdk.tar'
    with archive.open('wb') as out:
        subprocess.run(['git','archive',request.param,'demiflow'], cwd=REPO, stdout=out, check=True)
    with tarfile.open(archive) as bundle:
        bundle.extractall(checkout, filter='data')
    archive.unlink()
    return checkout


DRIVER = """
import json, os, sys
from pathlib import Path
import demiflow
from demiflow import data
from demiflow.execution import commit
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.state_artifacts import StateArtifacts

checkout, root, fault, operation = sys.argv[1:]
assert Path(demiflow.__file__).resolve().parent.parent == Path(checkout)
root = Path(root)
directory = root / 'run'
if operation == 'generate':
    scope = RunContext.start(directory, limits={'checkpoint_rows':8, 'checkpoint_interval_ms':60000})
else:
    scope = RunContext.resume(RunRef(str(directory), RunJournal(directory).recovery_identity()['run_id']))
with scope as run:
    if operation == 'generate':
        def boundary(name, commit_id):
            if name != 'after_checkpoint_committed':
                return
            fact = run.journal.execution_commit(commit_id)
            cut = StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
            if cut['cut']['final'] != (fault == 'final'):
                return
            (root / 'interruption.json').write_text(json.dumps({'commit':commit_id,
                'action':fact['action_id'],'run':run.ref.run_id,'attempt':run.attempt}))
            os._exit(75)
        commit.fault_point = boundary
    rows = data.range(24).take_all()
    (root/'result.json').write_text(json.dumps(rows))
"""


@pytest.mark.parametrize('cut', ['nonfinal','final'])
def test_published_v3_engine_cold_resume_reads_original_cut(old_sdk, tmp_path, cut):
    def invoke(checkout, operation):
        return subprocess.run([sys.executable, '-B','-c',DRIVER,str(checkout),str(tmp_path),cut,operation],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(checkout)),
            capture_output=True, text=True, timeout=120)

    crashed = invoke(old_sdk, 'generate')
    assert crashed.returncode == 75, crashed.stderr
    interrupted = json.loads((tmp_path / 'interruption.json').read_bytes())
    directory = tmp_path / 'run'
    journal = RunJournal(directory)
    action = interrupted['action']
    before = journal.source_binding(action+'-dataflow-sealed')
    assert before['schema'] == 'demiflow.dataflow_seal.v3'
    assert journal.execution_commit(interrupted['commit'])['status'] == 'checkpoint_committed'
    loaded = load_sealed_plan(directory, action, version='v3')
    assert loaded['graph'].dag_id == before['dag_id'] and loaded['physical'].plan_id == before['plan_id']
    resumed = invoke(REPO, 'resume')
    assert resumed.returncode == 0, resumed.stderr
    rows = json.loads((tmp_path / 'result.json').read_bytes())
    assert sorted(r['id'] for r in rows) == list(range(24))
    assert journal.recovery_identity()['run_id'] == interrupted['run']
    assert journal.execution_status()['attempt'] == 2 and interrupted['attempt'] == 1
    assert journal.execution_commit(interrupted['commit'])['status'] == 'acked'
    assert journal.source_binding(action+'-dataflow-sealed') == before
