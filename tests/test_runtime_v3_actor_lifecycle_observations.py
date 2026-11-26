"""Actual actor hooks cross worker teardown; declarations never imply completion."""
from pathlib import Path
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


@pytest.mark.parametrize('phase', ['success', 'start', 'call_and_close', 'close', 'cancel', 'empty', 'stale_boot'])
def test_real_process_hooks_survive_cleanup_and_preserve_failures(tmp_path, monkeypatch, phase):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    observed = []
    original = DataflowExecution.close
    def close(execution):
        try:
            return original(execution)
        finally:
            observed.extend(dict(value) for _, value in
                getattr(execution, '_operator_lifecycles', {}).values())
    monkeypatch.setattr(DataflowExecution, 'close', close)
    if phase == 'stale_boot':
        from demiflow.execution.hosts.endpoints import _EndpointBase
        accept = _EndpointBase._accept_final_observations
        def stale(endpoint, response):
            if response.get('final_progress'):
                response = {**response, 'final_progress': [dict(item, boot_id='stale-boot')
                    for item in response['final_progress']]}
            return accept(endpoint, response)
        monkeypatch.setattr(_EndpointBase, '_accept_final_observations', stale)
    marker = str(tmp_path/'hooks')
    class Actor:
        def __init__(self):
            with open(marker, 'a') as f: f.write('created\n')
        async def astart(self):
            with open(marker, 'a') as f: f.write('started\n')
            if phase == 'start': raise ValueError('actual start failure')
        async def __call__(self, row):
            if phase == 'call_and_close': raise ValueError('actual call failure')
            if phase == 'cancel':
                import asyncio
                raise asyncio.CancelledError('actual callback cancellation')
            return row
        async def aclose(self):
            with open(marker, 'a') as f: f.write('closed\n')
            if phase in {'close', 'call_and_close'}: raise LookupError('actual close failure')
    def execute():
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=4, worker_mode='process'):
            return data.from_items([] if phase == 'empty' else [{'x':1}]).map_async(Actor, concurrency=1).take_all()
    if phase in {'success', 'empty'}:
        assert execute() == ([] if phase == 'empty' else [{'x':1}])
    elif phase == 'call_and_close':
        with pytest.raises(ValueError, match='actual call failure'):
            execute()
    elif phase == 'stale_boot':
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        with pytest.raises(DataflowError, match='FENCED'):
            execute()
    elif phase == 'start':
        with pytest.raises(ValueError, match='actual start failure'):
            execute()
    elif phase == 'cancel':
        import asyncio
        with pytest.raises(asyncio.CancelledError, match='actual callback cancellation'):
            execute()
    else:
        with pytest.raises(ExceptionGroup, match='worker cleanup failed') as failed:
            execute()
        assert any(isinstance(error, LookupError) and str(error) == 'actual close failure'
                   for error in failed.value.exceptions)
    if phase == 'empty':
        assert not observed and not Path(marker).exists()
        return
    assert Path(marker).read_text().splitlines() == ['created','started','closed']
    assert observed
    facts = observed[-1]
    assert facts['created'] == 1
    assert facts['started'] == (0 if phase == 'start' else 1)
    assert facts['start_failed'] == (1 if phase == 'start' else 0)
    assert facts['closed'] == (0 if phase in {'close','call_and_close','stale_boot'} else 1)
    assert facts['close_failed'] == (1 if phase in {'close','call_and_close'} else 0)


def test_declared_payload_limits_are_not_silently_shrunk():
    from demiflow.execution.compiler_support.payload_limits import stream_limits
    from demiflow.execution.seal.plan_facts import operation_facts
    from demiflow.forge.pipeline_policy import limits_with
    async def batch(rows): return rows
    flow = data.from_items([{'x':1}]).flat_map(lambda row:[row,row]).batch_map(
        batch, max_batch=2, concurrency=1, queue_depth=1)
    facts = [operation_facts(op) for op in flow._plan.operations]
    limits = limits_with()
    with pytest.raises(ValueError, match='fixed residency'):
        stream_limits(facts, limits, adjustable_fields=())
    admitted = stream_limits(facts, limits)
    assert admitted['memory_bytes'] == limits['memory_bytes']
    assert admitted['max_row_bytes'] < limits['max_row_bytes']
    assert admitted['max_batch_bytes'] < limits['max_batch_bytes']


def test_invalid_run_limits_reach_original_admission_validation(root):
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.runs import manager as runs

    with pytest.raises(ForgeError, match='Invalid run argument: limits'):
        runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json', limits=[{}])


from test_forge_runs import base, root


@pytest.mark.parametrize('overrides,fixed,adjusted,supported', [
    ({'max_row_bytes':12*1024**2}, 'max_row_bytes', 'max_batch_bytes', True),
    ({'max_row_bytes':16*1024**2}, 'max_row_bytes', 'max_batch_bytes', False),
    ({'max_batch_bytes':16*1024**2}, 'max_batch_bytes', 'max_row_bytes', True),
    ({'max_batch_bytes':16*1024**2,'memory_bytes':128*1024**2}, 'max_batch_bytes', 'max_row_bytes', False),
])
def test_partial_default_payload_budget_is_sealed_and_executed(root, overrides, fixed, adjusted, supported):
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.runs import manager as runs

    from demiflow.forge.pipeline_policy import limits_with
    from test_forge_runs import custom, execute, rows
    operators = '''from typing import TypedDict
class Input(TypedDict):
    case_id: str | None
class Output(TypedDict):
    case_id: str
def transform(row: Input) -> list[Output]:
    return [{'case_id': str(row['case_id'])+'-a'}, {'case_id': str(row['case_id'])+'-b'}]
async def batch(rows: list[Output]) -> list[Output]:
    return rows
'''
    flow = '''from operators.custom import transform,batch
def run(*,source,target):
    result=data.read_lance(source['uri'], version=source['version'], columns=['case_id']).flat_map(transform).batch_map(batch,max_batch=2,concurrency=1,queue_depth=1).materialize()
    return result.write_lance(target['uri'],mode='create',return_receipt=True)
'''
    if not supported:
        with pytest.raises(ForgeError, match='fixed residency'):
            custom(root, flow, operators=operators, limits=overrides)
        assert not list((root/'data').glob('runs/*/*.lance'))
        return
    prepared = custom(root, flow, operators=operators, limits=overrides)
    _, sealed = runs._load(root, prepared['run_id'])
    caps = sealed['manifest']['limits']
    assert caps[fixed] == overrides[fixed]
    assert caps[adjusted] < limits_with(overrides)[adjusted]
    assert caps['max_row_bytes'] <= caps['max_batch_bytes']
    assert caps['memory_bytes'] == limits_with(overrides)['memory_bytes']
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    import lance
    source = lance.dataset(str(root/'data/candidates.lance'), version=3).to_table(columns=['case_id']).to_pylist()
    expected = sorted(str(row['case_id'])+suffix for row in source for suffix in ('-a','-b'))
    assert sorted(row['case_id'] for row in rows(root,result)) == expected
