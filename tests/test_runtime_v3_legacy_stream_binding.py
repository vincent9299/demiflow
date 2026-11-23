"""Authenticate the original published native terminal without its old dispatcher."""
import json
import os
import subprocess
import sys

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.executors.dataflow_api import make_action_request, run_dataset_action
from demiflow.execution.pipeline.checked import action
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError, RunRef
from demiflow.execution.pipeline.run_context import RunContext
from runtime_v3_history_scenarios import _extract_original
from test_runtime_v3_native_execution_binding import source


# Generate the real placement and seal with the version that wrote this layout.
# Intercept only after the original request was built and after the original
# checkpoint was committed, never synthesizing the history or callback payload.
OLD = """
import json, os, sys
from pathlib import Path
from types import SimpleNamespace
import pyarrow as pa
from demiflow import data
from demiflow.execution import commit
from demiflow.execution.checked import action
from demiflow.execution import dataflow_api
from demiflow.execution.dataset_native import execute
from demiflow.execution.run_context import RunContext
from demiflow.execution.state_artifacts import StateArtifacts

root, uri, when = sys.argv[1:]
root = Path(root)
make = dataflow_api.make_action_request
def record(*args, **kwargs):
    request = make(*args, **kwargs)
    (root/'args.json').write_text(json.dumps(kwargs['arguments']))
    return request
dataflow_api.make_action_request = record
schema = pa.schema([('id', pa.int32())], metadata={b'sealed': b'original'})
profile = SimpleNamespace(profile={'limits': {'memory_bytes': 96<<20, 'cpu_millis': 4000}},
                          stage_ids=('original',), stage_limits={'original': {'inflight_rows': 2,
                                                                              'inflight_bytes': 1<<20}})
with RunContext.start(root/'run', limits={'checkpoint_rows':4, 'checkpoint_interval_ms':60000}) as run, data.local_execution(workers=4, worker_mode='thread') as session:
    (root/'run-id').write_text(run.ref.run_id)
    def stop(name, ident):
        if name == 'after_checkpoint_committed':
            fact = run.journal.execution_commit(ident)
            cut = StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
            if cut['cut']['final'] == (when == 'final'):
                (root/'original-commit').write_text(ident)
                os._exit(75)
    commit.fault_point = stop
    ds = data.read_lance(uri, version=1).select_columns(['id'])
    with action('iter_batches', ds._source, ds._plan, arguments={'batch_format':'pyarrow'}):
        list(execute(session._executor, ds._source, ds._plan, arrow=True, sink_schema=schema, execution=profile))
"""


@pytest.mark.parametrize('cut', ['nonfinal', 'final'])
def test_original_terminal_keeps_its_bounds_schema_and_borrowed_action(tmp_path, monkeypatch, cut):
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path/'resources'))
    old, _ = _extract_original(tmp_path, '59f3b5027ff8b386040ca55525ce2cae8b0fc41b')
    uri = source(tmp_path, range(24))
    result = subprocess.run([sys.executable, '-B', '-c', OLD, str(tmp_path), uri, cut], cwd=old,
                            env={**os.environ, 'PYTHONPATH': str(old),
                                 'DEMIFLOW_RESOURCE_ROOT': str(tmp_path/'resources')},
                            timeout=90, capture_output=True, text=True)
    assert result.returncode == 75, result.stderr
    original = json.loads((tmp_path/'args.json').read_text())
    ref = RunRef(str(tmp_path/'run'), (tmp_path/'run-id').read_text())
    commit_id = (tmp_path/'original-commit').read_text()
    launches = []
    launch = DataflowExecution._prepare_launch
    def observed(execution, *args, **kw):
        launches.append(execution._physical.policy.provider)
        return launch(execution, *args, **kw)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', observed)

    def read(arguments, *, projection='id'):
        with RunContext.resume(ref) as run, data.local_execution(workers=4, worker_mode='thread') as session:
            ds = data.read_lance(uri, version=1).select_columns([projection])
            with action('iter_batches', ds._source, ds._plan, arguments={'batch_format':'pyarrow'}) as owner:
                original_seal = run.journal.source_binding('action-1-dataflow-sealed')
                original_fact = run.journal.execution_commit(commit_id)
                req = make_action_request(session._executor, owner.source, owner.plan,
                                          kind='iter_batches', arguments=arguments)
                assert req.action is owner.execution
                tables = list(run_dataset_action(req))
                assert run.journal.execution_commit(commit_id)['prepared'] == original_fact['prepared']
                assert run.journal.source_binding('action-1-dataflow-sealed') == original_seal
            assert run.position == 1 and run.journal.execution_action('action-2') is None
            assert all(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
            return tables

    with pytest.raises(RecoveryContractError, match='identity differs|declaration'):
        read(original, projection='extra')
    assert not launches

    with pytest.raises(DataflowError, match='limits or schema differ'):
        read({**original, 'native_execution_limits':{**original['native_execution_limits'], 'cpu_millis':1000}})
    assert not launches
    with pytest.raises(DataflowError, match='limits or schema differ'):
        read({**original, 'declared_arrow_schema': None})
    assert not launches
    tables = read(original)
    assert all(0 < len(table) <= 2 for table in tables)
    expected_schema = pa.schema([('id', pa.int32())], metadata={b'sealed': b'original'})
    assert all(table.schema.equals(expected_schema, check_metadata=True) for table in tables)
    assert pa.concat_tables(tables).to_pylist() == [{'id':i} for i in range(24)]
    if cut == 'final':
        assert not launches
