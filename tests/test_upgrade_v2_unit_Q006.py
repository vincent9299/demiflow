"""Q006 verify-preserving unit tests: StreamResources.make_queue (BOUNDS/STREAM).

make_queue is unchanged production code. It must build a native byte-bounded
ObservedQueue (depth + per-item + total triple limit, no pre-allocation), reject a
custom transport, require a run context, bound depth to the admission budget, derive
defaults from the admitted run limits, and enforce the item byte budget on put while
reserving a complete item before admission (full()).
"""
import pytest

from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.request_limits import ObservedQueue
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext, current_run
from demiflow.execution.stream_resources import StreamResources


def test_returns_observed_queue_with_triple_limit(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        resources = StreamResources([])
        queue = resources.make_queue(4, max_item_bytes=100, max_bytes=400)
        assert isinstance(queue, ObservedQueue)
        assert queue.maxsize == 4
        assert queue.max_item_bytes == 100 and queue.max_bytes == 400
        assert resources.queues == [queue]


def test_defaults_derive_from_admitted_run_limits(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        queue = StreamResources([]).make_queue(3)
        maximum = run.limits['max_row_bytes']
        assert queue.max_item_bytes == maximum
        assert queue.max_bytes == 3 * maximum


def test_custom_queue_factory_rejected(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        resources = StreamResources([], queue_factory=lambda *a, **k: None)
        with pytest.raises(RecoveryContractError, match='native byte-bounded'):
            resources.make_queue(4)


def test_no_run_context_rejected():
    assert current_run() is None
    with pytest.raises(RecoveryContractError, match='run context'):
        StreamResources([]).make_queue(4)


@pytest.mark.parametrize('depth', [0, 257, -1, '4', 1.5])
def test_depth_bounds_enforced(tmp_path, depth):
    with RunContext.start(tmp_path / 'run'):
        with pytest.raises(RecoveryContractError, match='depth'):
            StreamResources([]).make_queue(depth)


def test_byte_capacity_must_cover_one_item(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        with pytest.raises(ValueError, match='cover one maximum item'):
            StreamResources([]).make_queue(4, max_item_bytes=100, max_bytes=50)


def test_item_byte_budget_enforced_on_put(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        queue = StreamResources([]).make_queue(4, max_item_bytes=64, max_bytes=256)
        with pytest.raises(RuntimeContractError, match='byte budget'):
            queue.put_nowait(b'x' * 1000)


def test_full_reserves_complete_item_before_admission(tmp_path):
    with RunContext.start(tmp_path / 'run'):
        queue = StreamResources([]).make_queue(4, max_item_bytes=100, max_bytes=100)
        queue.put_nowait(b'x' * 60)  # one item fits, a second full item would not
        assert queue.full() is True
        assert queue.get_nowait() == b'x' * 60
        assert queue.bytes == 0 and queue.full() is False