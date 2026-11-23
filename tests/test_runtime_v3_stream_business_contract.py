"""Public stream behavior across the real sealed process worker boundary."""

from demiflow.execution.action_driver import services as action_services
import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.contracts.dataflow_errors import DataflowError


@pytest.mark.parametrize('batch', [False, True])
def test_local_exception_whitelist_preserves_qualified_identity(batch):
    class Soft(Exception):
        pass

    async def callback(value):
        rows = value if batch else [value]
        if rows[0]['id'] == 0:
            raise Soft('declared business miss')
        return rows if batch else rows[0]

    ds = DataAPI().from_items([{'id': 0}, {'id': 1}])
    ds = (ds.batch_map(callback, max_batch=1, catch=(Soft,)) if batch
          else ds.map_async(callback, catch=(Soft,)))
    assert ds.take_all() == [{'id': 1}]


def test_identically_named_exception_outside_whitelist_still_fails():
    def declared():
        class Same(Exception):
            pass
        return Same
    def other():
        class Same(Exception):
            pass
        return Same
    allowed, rejected = declared(), other()

    async def callback(row):
        raise rejected('outside whitelist')
    with pytest.raises(DataflowError, match='outside whitelist') as error:
        DataAPI().from_items([{'id': 1}]).map_async(callback, catch=(allowed,)).take_all()
    assert error.value.code == 'INTERNAL_PROTOCOL'


def test_stream_progress_comes_from_intermediate_business_frontiers():
    seen = []
    stats = (DataAPI().from_items([{'id': i} for i in range(25)])
             .map_async(lambda row: row, label='actual', concurrency=2)
             .run_stream(log_every=5, on_progress=lambda value: seen.append(
                 (value.stage('actual')['in'], value.emitted))))
    assert [count for count, _ in seen] == [5, 10, 15, 20, 25]
    assert all(0 <= output <= count for count, output in seen)
    assert stats.emitted == 25
    assert list(stats.stages) == ['actual']
    assert stats.metrics['committed_output']['rows'] == 25


@pytest.mark.parametrize('drain_failure', ['same', 'unprintable'])
def test_stream_cleanup_preserves_primary_failure_and_runs_each_owner(monkeypatch, drain_failure):
    from types import SimpleNamespace
    from demiflow.execution.journal import run_telemetry as dataflow_api
    from demiflow.execution import dataflow_model_observations
    from demiflow.execution.journal import dataflow_operator_observations
    from demiflow.execution.action_driver import stream as dataflow_stream

    events = []
    failure = RuntimeError('worker failed')

    def pump(*, deadline_ms):
        raise failure

    def observe(execution, stats):
        events.append('observe')
        raise ValueError('observation failed')

    class UnprintableError(Exception):
        def __str__(self):
            raise ValueError('failed to format error')

    def drain(stats):
        events.append('drain')
        if drain_failure == 'same':
            raise failure
        raise UnprintableError()

    def metadata(request, execution):
        events.append('metadata')
        raise OSError('metadata failed')

    action = SimpleNamespace(context=SimpleNamespace(check_stop=lambda: None))
    execution = SimpleNamespace(pump=pump)
    handle = SimpleNamespace(_execution=execution, close=lambda: events.append('close'))
    callbacks = SimpleNamespace(resolve=lambda *args, **kwargs: None,
                                close=lambda: events.append('callbacks.close'))
    monkeypatch.setattr(dataflow_stream, 'AsyncInstance', lambda *args: callbacks)
    monkeypatch.setattr(dataflow_stream, 'execute_action', lambda *args, **kwargs: handle)
    monkeypatch.setattr(action_services, 'resolve_action_runtime', lambda request: (None, None))
    monkeypatch.setattr(dataflow_api, 'record_execution_metadata', metadata)
    monkeypatch.setattr(dataflow_operator_observations, 'stream_observations', observe)
    monkeypatch.setattr(dataflow_model_observations, 'finish_journal_audit',
                        lambda execution, stats: events.append('audit'))

    with pytest.raises(RuntimeError) as raised:
        dataflow_stream.run_stream_action(SimpleNamespace(action=action),
                                          on_drain=drain)

    assert raised.value is failure
    assert events == ['observe', 'close', 'observe', 'metadata', 'audit', 'drain', 'callbacks.close']
    if hasattr(BaseException, 'add_note'):
        assert any('observation failed' in note for note in failure.__notes__)
        assert any('metadata failed' in note for note in failure.__notes__)
    assert action._observation_rows == 0
    assert execution._observe_progress is None
