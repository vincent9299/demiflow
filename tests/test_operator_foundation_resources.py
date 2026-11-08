"""Reject growth before owned spill creation or native effect admission."""
import pickle
from pathlib import Path

import pytest

from demiflow.data.sources import MaterializedSource
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects.workspace import resolve_flow_resources


def test_materialization_spill_budget_preserves_other_live_handles(tmp_path, monkeypatch):
    monkeypatch.setattr('tempfile.tempdir', str(tmp_path))
    executor = LocalDatasetExecutor(workers=1, block_size=1, materialize_memory_limit=1)
    row = {'payload': 'x' * 128}
    size = len(pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL))
    existing = executor._materialize_blocks(None, lambda append: append(row))
    paths = set(executor._spill_paths)
    context = CheckedExecution(nodes=[], edges=[], actions=[], operators=[], source_root=tmp_path,
        limits={**DEFAULT_LIMITS, 'max_scratch_bytes': size * 2}, record=lambda *a: None, check_stop=lambda: None)
    try:
        with pytest.raises(RuntimeContractError, match='spill reservation'):
            with context.bind_context():
                executor._materialize_blocks(None, lambda append: [append(row) for _ in range(3)])
        assert set(executor._spill_paths) == paths
        assert executor._spill_bytes == size
        assert set(str(p) for p in tmp_path.glob('demiflow-block-*')) == paths
        assert pickle.loads(Path(next(iter(paths))).read_bytes()) == row
        executor.release(MaterializedSource(existing))
        assert executor._spill_bytes == 0 and not executor._spill_paths
    finally:
        executor.close()


def test_resource_closure_applies_to_ledgers_and_parameter_collisions(tmp_path):
    reference = dict(kind='ledger', parameter='journal', path='evidence/calls.sqlite',
                     format='demiflow_prompt_sqlite_v1', max_bytes=4 * 1024**2)
    with pytest.raises(ForgeError, match='duplicate'):
        resolve_flow_resources(tmp_path, None, [reference, reference], {})
    with pytest.raises(ForgeError, match='count budget'):
        resolve_flow_resources(tmp_path, None, [reference], {str(i): {} for i in range(256)})
    with pytest.raises(ForgeError, match='format'):
        resolve_flow_resources(tmp_path, None, [{**reference, 'format': {}}], {})


def test_prompt_zero_queue_is_not_default(server, tmp_path):
    import inspect
    from demiflow.data.dataset import Dataset
    from demiflow.data.callable_contract import bind_native_parameters, signature_parameters, default_value
    from demiflow.operator_llm.flow_contract import normalize_prompt
    signature = inspect.signature(Dataset.map_prompt_async)
    parameters = signature_parameters(signature)
    for parameter in parameters:
        parameter['default_value'] = default_value(signature.parameters[parameter['name']].default)
    args = bind_native_parameters(parameters, [None, 'test'], dict(config='flows/prompt.yaml',
        inputs=['x'], output='answer', max_requests=1, queue_depth=0))
    with pytest.raises(ValueError, match='positive'):
        normalize_prompt(args)
    assert not server['requests']


from test_map_prompt_async import server
