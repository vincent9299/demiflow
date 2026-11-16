"""Public stream behavior across the real sealed process worker boundary."""
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
