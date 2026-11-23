"""The retired private method only invalidates resumes which captured the entire Dataset class."""
from collections import Counter
import os
import subprocess
import sys

import pytest

from runtime_v3_history_scenarios import _extract_original
from test_training_cold_v3_history import DRIVER, REPO


PUBLISHED = "59f3b5027ff8b386040ca55525ce2cae8b0fc41b"
BUSINESS = """from demiflow.data.dataset import Dataset
from pathlib import Path
import os

from test_execution_recovery_entrypoints import FormalSink


def effect(name):
    with Path(os.environ['P8_TEST_EFFECTS']).open('a') as stream:
        print(name, file=stream)


class ObservedSink(FormalSink):
    def on_write_start(self, schema=None):
        effect('start')

    def on_write_failed(self, error):
        effect('failed')

    def on_write_complete(self, result):
        effect('complete')

    def prepare(self, blocks, context):
        effect('prepare')
        return super().prepare(blocks, context)

    def commit(self, prepared, context):
        effect('commit')
        return super().commit(prepared, context)


class CountingMap:
    def __init__(self, owner):
        self.owner = owner
        self.count = 0

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, state):
        self.count = state['count']

    def __call__(self, row):
        assert callable(self.owner.join if isinstance(self.owner, type) else self.owner)
        self.count += 1
        effect('row')
        return row
"""


@pytest.fixture(scope='module')
def published(tmp_path_factory):
    checkout, _ = _extract_original(tmp_path_factory.mktemp('p8-dataset-capture'), PUBLISHED)
    return checkout


def _invoke(checkout, root, script, args=(), log=None):
    return subprocess.run(
        [sys.executable, '-B', '-c', script, *args], cwd=root,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(map(str, (checkout, REPO/'tests', root))),
                 P8_TEST_EFFECTS=str(log or root/'effects'), DEMIFLOW_RESOURCE_ROOT=str(root/'resources'),
                 DEMIFLOW_RESOURCE_HOME=str(root/'home')),
        text=True, capture_output=True, timeout=180,
    )


@pytest.mark.parametrize(('capture', 'operation', 'rejection'), [
    ('Dataset', 'resume', 'Action semantic/source identity differs on resume'),
    ('Dataset.join', 'resume', None),
    ('Dataset.join', 'reject-at-seal', 'test rejection inside sealed program verification'),
], ids=['captured-class', 'captured-public-method', 'seal-validation'])
def test_old_cut_and_sink_effects(published, tmp_path, capture, operation, rejection):
    (tmp_path / 'historical_business.py').write_text(
        BUSINESS + f'\ndef observed():\n    return CountingMap({capture})\n'
    )

    def invoke(checkout, step):
        log = tmp_path / (step + '.effects')
        run = _invoke(checkout, tmp_path, DRIVER,
                      [str(checkout), str(tmp_path), 'nonfinal', step, 'capture'], log)
        return run, Counter(log.read_text().splitlines() if log.exists() else ())

    interrupted, before = invoke(published, 'generate')
    assert interrupted.returncode == 75, interrupted.stderr
    assert before['start'] == 1 and 0 < before['row'] < 48
    resumed, after = invoke(REPO, operation)
    if rejection:
        assert resumed.returncode != 0 and rejection in resumed.stderr, resumed.stderr
        assert not after
    else:
        assert resumed.returncode == 0, resumed.stderr
        assert before['row'] + after['row'] == 48
        assert after == {'row': after['row'], 'start': 1, 'prepare': 1, 'commit': 1, 'complete': 1}


def test_whole_class_capture_published_results_still_readable(published, tmp_path):
    (tmp_path / 'historical_business.py').write_text(BUSINESS)
    write = """from historical_business import CountingMap, Dataset
from demiflow import data
from demiflow.execution import RunContext
with RunContext.start('run', limits={'checkpoint_rows': 8, 'checkpoint_interval_ms': 60000}):
    data.range(48).map(CountingMap(Dataset)).materialize()
"""
    read = """import json
from pathlib import Path
from types import SimpleNamespace
from demiflow.execution.run_reader import _ReadonlyJournal
from _native_receipt_fixtures import committed_rows
root = Path('run').resolve()
facts = _ReadonlyJournal(root).execution_commits('action-1')
rows = committed_rows(SimpleNamespace(directory=root), 'action-1', facts[-1])
assert rows == [{'id': n} for n in range(48)]
print(json.dumps([rows, [fact['receipts'] for fact in facts]], sort_keys=True))
"""
    writer = _invoke(published, tmp_path, write)
    assert writer.returncode == 0, writer.stderr
    before = _invoke(published, tmp_path, read)
    after = _invoke(REPO, tmp_path, read)
    assert before.returncode == 0, before.stderr
    assert after.returncode == 0, after.stderr
    assert after.stdout == before.stdout
