"""The published bounded row queue preserves its limits and wait metrics."""
import pytest
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.request_limits import ObservedQueue


def test_byte_capacity_must_cover_one_item():
    with pytest.raises(ValueError, match='cover one maximum item'):
        ObservedQueue(4, max_item_bytes=100, max_bytes=50)


def test_item_byte_budget_enforced_on_put():
    queue = ObservedQueue(4, max_item_bytes=64, max_bytes=256)
    with pytest.raises(RuntimeContractError, match='byte budget'):
        queue.put_nowait(b'x' * 1000)


def test_full_reserves_complete_item_before_admission():
    queue = ObservedQueue(4, max_item_bytes=100, max_bytes=100)
    queue.put_nowait(b'x' * 60)  # one item fits, a second full item would not
    assert queue.full() is True
    assert queue.get_nowait() == b'x' * 60
    assert queue.bytes == 0 and queue.full() is False
