"""Real cache workers, delegated business state and native model authority."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server


@pytest.mark.parametrize('synchronous', [False, True])
def test_cache_real_workers_keep_current_row_and_reuse_result(tmp_path, synchronous):
    calls = tmp_path / 'calls'
    def compute(value):
        with calls.open('a') as file:
            file.write(value + '\n')
        return value.upper()
    async def async_compute(value):
        return compute(value)
    for n in range(2):
        with RunContext.start(tmp_path / f'run-{n}'), data.local_execution():
            rows = data.from_items([{'value': 'one', 'current': n}]).map_cached(
                compute if synchronous else async_compute, cache_dir=tmp_path / 'cache', version='v1',
                synchronous=synchronous, inputs=['value'], output='answer').take_all()
            assert rows == [{'value': 'one', 'current': n, 'answer': 'ONE'}]
    assert calls.read_text().splitlines() == ['one']


def test_cached_prompt_binds_native_journal_before_dispatch(tmp_path, native_server):
    for n in range(2):
        with RunContext.start(tmp_path / f'run-{n}') as run, data.local_execution():
            actor = data.from_items([]).map_prompt_async('enrich', config=parse_prompt_pack(PACK),
                inputs={'payload': 'item'}, output='answer', max_requests=1)._stages[-1]
            rows = data.from_items([{'item': 'one'}]).map_cached(actor, cache_dir=tmp_path / 'cache',
                version='v1').take_all()
            assert rows == [{'item': 'one', 'answer': 'ok'}]
            assert run.journal.budget_consumed('requests') == (1 if n == 0 else 0)
    assert len(native_server) == 1


def test_public_worker_error_preserves_builtin_and_closes_owner(tmp_path):
    closed = tmp_path / 'closed'
    class Actor:
        def __call__(self, row):
            raise ValueError('business input rejected')
        async def aclose(self):
            closed.write_text('closed')
    with pytest.raises(ValueError, match='business input rejected'):
        with RunContext.start(tmp_path / 'run'), data.local_execution():
            data.from_items([{'i': 1}]).map(Actor).take_all()
    assert closed.read_text() == 'closed'


@pytest.mark.parametrize('synchronous', [False, True])
def test_cached_partial_actor_state_and_lifecycle_survive_checkpoint(tmp_path, monkeypatch, synchronous):
    from functools import partial
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    closed = tmp_path / 'closed'
    class Counter:
        def __init__(self):
            self.count = 0
            self.started = False
        async def astart(self):
            self.started = True
        def __call__(self, value, *, delta):
            assert self.started
            self.count += delta
            return self.count
        async def aclose(self):
            with closed.open('a') as stream:
                stream.write('closed\n')
    def pipeline():
        return data.from_items([{'value': n} for n in range(6)]).map_cached(
            partial(Counter(), delta=1), cache_dir=tmp_path / 'cache', version='v1',
            synchronous=synchronous, inputs=['value'], output='count')
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        result = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run, data.local_execution():
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().take_all()
    assert closed.read_text().splitlines() == ['closed']
    with RunContext.resume(reference), data.local_execution():
        assert pipeline().take_all() == [{'value': n, 'count': n + 1} for n in range(6)]
    assert closed.read_text().splitlines() == ['closed', 'closed']
