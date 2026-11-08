"""Real Lance publication without a returned receipt, using ordinary Dataset."""
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext, RunRef, UnresolvedCommit
from demiflow.execution.run_journal import RunJournal


def write_graph(root, mode, window=None):
    root = Path(root)
    directory = root / 'run'
    prior = RunJournal(directory).recovery_identity()
    scope = RunContext.resume(RunRef(str(directory), prior['run_id'])) if prior else RunContext.start(directory)
    if window:
        import demiflow.execution.commit as protocol
        def crash(name, commit_id):
            if name == window:
                os._exit(37)
        protocol.fault_point = crash
    with scope:
        return data.from_items([{'value': 2}, {'value': None}, {'value': 2}]).write_lance(
            str(root / 'out.lance'), mode=mode, schema=pa.schema([('value', pa.int64())]), return_receipt=True)


@pytest.mark.parametrize('mode', ['create', 'append', 'overwrite'])
@pytest.mark.parametrize('window', ['after_prepared', 'after_sink_commit_before_receipt'])
def test_native_write_has_one_version_after_process_loss(tmp_path, mode, window):
    target = str(tmp_path / 'out.lance')
    if mode != 'create':
        lance.write_dataset(pa.table({'value': [1]}), target)
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(repo), str(repo / 'tests')]),
           'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1'}
    child = subprocess.run([sys.executable, '-B', '-c',
        'from test_execution_recovery_lance import write_graph; import sys; write_graph(*sys.argv[1:])',
        str(tmp_path), mode, window], env=env, capture_output=True, text=True, timeout=40)
    assert child.returncode == 37, child.stdout + child.stderr
    receipt = write_graph(tmp_path, mode)
    assert receipt.status == 'committed'
    table = lance.dataset(target)
    assert table.version == (1 if mode == 'create' else 2)
    values = table.to_table()['value'].to_pylist()
    assert values == ([1] if mode == 'append' else []) + [2, None, 2]
    fact = RunJournal(tmp_path / 'run').execution_commits('action-1')[0]
    assert fact['status'] == 'acked'
    transaction = table.read_transaction(table.version)
    assert transaction.transaction_properties['__lance_commit_message'] == 'demiflow-lance:' + fact['commit_id']


def test_foreign_successor_is_not_adopted_or_overwritten(tmp_path, monkeypatch):
    import demiflow.execution.commit as protocol
    target = str(tmp_path / 'out.lance')
    def stop(name, commit_id):
        if name == 'after_prepared':
            raise RuntimeError('prepared')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='prepared'):
        write_graph(tmp_path, 'create')
    lance.write_dataset(pa.table({'value': [999]}), target)
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with pytest.raises(UnresolvedCommit, match='does not prove'):
        write_graph(tmp_path, 'create')
    assert lance.dataset(target).version == 1
    assert lance.dataset(target).to_table()['value'].to_pylist() == [999]


def test_observer_projects_the_same_native_receipt_without_advancing_progress(tmp_path, monkeypatch):
    import demiflow.execution.commit as protocol
    from demiflow.workbench.runs.manager import _native_execution_receipt
    def stop(name, commit_id):
        if name == 'after_sink_commit_before_receipt':
            raise RuntimeError('lost response')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='lost response'):
        write_graph(tmp_path, 'create')
    journal = RunJournal(tmp_path / 'run')
    before = journal.execution_commits('action-1')
    receipt = _native_execution_receipt(journal, 'action-1')
    assert receipt['committed_version'] == 1
    assert journal.execution_commits('action-1') == before
    assert before[0]['status'] == 'prepared' and before[0]['receipts'] == {}
