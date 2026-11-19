"""Admitted prompt usage survives failure/cancellation and worker teardown."""
import pytest

from demiflow.data.api import DataAPI
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
@pytest.mark.parametrize('exhausted', [False, True])
@pytest.mark.parametrize('action', ['materialize', 'run_stream'])
def test_final_prompt_usage_does_not_depend_on_progress_delivery(server, monkeypatch, method, exhausted, action):
    from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
    # No periodic progress can rescue the counters. Use the real worker Drain
    # response after calls finish, including budget failure before HTTP starts.
    monkeypatch.setattr(WorkerGraphRunner, '_flush_metrics', lambda self: None)
    api = DataAPI(workers=4, block_size=1)
    kwargs = {'concurrency': 4} if method == 'map_prompt_async' else {}
    ds = getattr(api.from_items([{'item': n} for n in range(8 if exhausted else 2)]), method)(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        max_requests=2, **kwargs)
    if action == 'run_stream' and method == 'map_prompt':
        # run_stream needs an explicit streaming operation; retain the sync
        # prompt upstream while making the public action admissible.
        ds = ds.map_async(lambda row: row, concurrency=1)
    if exhausted:
        with pytest.raises(PromptBudgetExceededError):
            getattr(ds, action)()
    else:
        result = getattr(ds, action)()
        assert (result.emitted if action == 'run_stream' else len(result.take_all())) == 2
    usage = api.prompt_usage()
    assert usage['provider_requests_reserved'] == 2
    assert usage['provider_requests_started'] <= 2
    assert usage['provider_requests_completed'] + usage['provider_requests_failed'] == 2
    assert len(server['requests']) <= 2
    # Reading an observational projection never applies it twice.
    assert api.prompt_usage() == usage
