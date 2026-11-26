"""Reject growth before owned spill creation or native effect admission."""
import pickle
from pathlib import Path

import pytest

from demiflow.data.sources import MaterializedSource
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.forge.runs.manager import DEFAULT_LIMITS
from demiflow.forge.schema import ForgeError
from demiflow.forge.project_bindings import resolve_flow_resources


def test_releasing_one_result_keeps_the_other_readable(tmp_path):
    from demiflow import data
    from demiflow.execution import RunContext

    with RunContext.start(tmp_path / 'run'):
        first = data.from_items([{'value': 1}]).materialize()
        second = data.from_items([{'value': 2}]).materialize()
        first.release()
        assert second.take_all() == [{'value': 2}]
        second.release()


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
