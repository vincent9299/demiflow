"""Cache result sharing cannot impersonate this owner's acknowledged state."""
import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from demiflow.data.result_cache import CachedSyncMap
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.kv import SQLiteKV


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, value):
        self.count += 1
        return self.count


def cache(root, owner='a', counter=None, **kwargs):
    actor = CachedSyncMap(counter or Counter(), root, 'v1', inputs=['value'], output='count', **kwargs)
    actor.bind_checkpoint_cache(hashlib.sha256(owner.encode()).hexdigest())
    return actor


def test_native_frontier_replays_only_own_unacknowledged_state(tmp_path):
    first = cache(tmp_path)
    assert first({'value': 0})['count'] == 1
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 2
    assert first({'value': 2})['count'] == 3
    # Other owners share the exact completed result without advancing state.
    other = cache(tmp_path, 'other-run-or-partition')
    assert other({'value': 2})['count'] == 3
    assert other.fn.count == 0
    resumed = cache(tmp_path)
    asyncio.run(resumed.restore_state(image))
    assert resumed({'value': 0})['count'] == 1
    assert resumed.fn.count == 1
    assert resumed({'value': 1})['count'] == 2
    assert resumed({'value': 1})['count'] == 2  # once per actual miss, including replay
    assert resumed.fn.count == 2
    assert resumed({'value': 2})['count'] == 3
    assert resumed({'value': 3})['count'] == 4
    assert first.prepare({'value': 2}) == resumed.prepare({'value': 2})


@pytest.mark.parametrize('failure', ['different', 'exception'])
def test_replay_never_replaces_a_shared_result_or_error_diagnostic(tmp_path, failure):
    first = cache(tmp_path)
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 1
    _, key = first.prepare({'value': 1})
    before = first.store.get(key, _with_origin=True)
    error = tmp_path / (key[:3] + '.error.json')
    error.write_text('prior diagnostic')
    resumed = cache(tmp_path)
    asyncio.run(resumed.restore_state(image))
    if failure == 'different':
        resumed.fn.count = 10
        expected = DataflowError
    else:
        def rejected(value):
            raise ValueError('state replay rejected')
        resumed.fn = rejected
        expected = ValueError
    with pytest.raises(expected):
        resumed({'value': 1})
    assert first.store.get(key, _with_origin=True) == before
    assert error.read_text() == 'prior diagnostic'


def test_old_stateful_image_rejected_and_old_stateless_result_readable(tmp_path):
    actor = cache(tmp_path)
    image = asyncio.run(actor.snapshot_state())
    del image['cache_frontier']
    with pytest.raises(DataflowError) as failure:
        asyncio.run(cache(tmp_path).restore_state(image))
    assert failure.value.code == 'INCOMPATIBLE_HISTORY'
    plain = CachedSyncMap(lambda value: value + 10, tmp_path / 'plain', 'v1', inputs=['value'], output='count')
    plain.bind_checkpoint_cache('b' * 64)
    plain_image = asyncio.run(plain.snapshot_state())
    assert 'cache_frontier' not in plain_image
    assert plain({'value': 0})['count'] == 10
    asyncio.run(plain.restore_state(plain_image))
    assert plain({'value': 0})['count'] == 10
    assert 'origin' not in plain.store.get(plain.prepare({'value': 0})[1])


def test_origin_and_value_share_capacity_transaction_and_plain_writes_clear_origin(tmp_path):
    store = SQLiteKV(tmp_path / 'cache.sqlite', 'n', max_value_bytes=128, max_entries=1, max_bytes=128)
    store.put_many([('a', 1)], _origin='first')
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('b', 2)], _origin='rejected')
    assert store.get('b', _with_origin=True) == {'hit': False, 'value': None}
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': 'first'}
    with pytest.raises(ValueError, match='changed'):
        store.put_many([('a', 2)], _origin='wrong', _origin_only=True)
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': 'first'}
    store.put_many([('a', 1)], _origin='replayed', _origin_only=True)
    assert store.get('a', _with_origin=True)['origin'] == 'replayed'
    store.put_many([('a', 3)])
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 3, 'origin': None}
