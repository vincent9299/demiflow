"""R001-R005 · StageBufferAdapter protocol closure (demiflow.stage_buffer.v1).

Pure per-slot buffer contract: classification, JSON-safe group keys, legacy
fixed-batch compatibility, identity/shape/budget fail-closed validation, slot
lineage and idempotent close. These exercise the adapter directly; the stream
crash-recovery counterexamples live in the R006/R012 unit files.
"""
import math
from types import SimpleNamespace

import pytest

from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.stage_buffers import (
    SCHEMA, StageBufferAdapter, classify_stage_buffer, make_stage_buffer_adapter,
    _encode_group_key, _decode_group_key)


def _stage(**kw):
    base = dict(concurrency=1, max_batch=None, flush_interval=None,
                grouping=None, prefix=None, batch_ordinal=0)
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.fixture
def action(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'buffer-unit'}}) as act:
            yield act
            act.complete({'kind': 'materialized', 'rows': 0})


def test_classify_covers_every_native_kind_and_row_local():
    assert classify_stage_buffer(_stage(prefix=object())) == 'prepend'
    assert classify_stage_buffer(_stage(grouping=object())) == 'grouped'
    assert classify_stage_buffer(_stage(max_batch=3, flush_interval=0.5)) == 'timed_batch'
    assert classify_stage_buffer(_stage(max_batch=3, concurrency=2)) == 'concurrent_batch'
    assert classify_stage_buffer(_stage(max_batch=3)) == 'fixed_batch'
    assert classify_stage_buffer(_stage()) is None


def test_group_key_encoding_keeps_scalar_types_and_signed_zero():
    key = ((str, 'a'), (int, 1), (float, -0.0), (bool, True), (type(None), None))
    decoded = _decode_group_key(_encode_group_key(key))
    assert decoded == key
    assert [type(v) for _, v in decoded] == [str, int, float, bool, type(None)]
    assert math.copysign(1.0, decoded[2][1]) == -1.0
    # bool must not fold into int despite True == 1.
    assert decoded[3][0] is bool and decoded[1][0] is int


def test_decode_rejects_unknown_type_name():
    with pytest.raises(RecoveryContractError, match='unsupported key encoding'):
        _decode_group_key([['complex', 1]])


def test_factory_returns_none_for_row_local_and_adapter_for_buffer(action):
    assert make_stage_buffer_adapter(_stage(), action=action, stage_key='node-0') is None
    adapter = make_stage_buffer_adapter(_stage(max_batch=3), action=action, stage_key='node-0')
    assert isinstance(adapter, StageBufferAdapter) and adapter.kind == 'fixed_batch'


def test_slot_ordinal_lineage_and_legacy_scope_identity(action):
    adapter = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
    from demiflow.execution.recovery_contract import identity
    assert adapter.next_ordinal(0) == 0
    assert adapter.next_ordinal(0) == 1
    assert adapter.stage.batch_ordinal == 2
    assert adapter.scope_identity(0, 1) == identity(['fixed-batch', 'node-0', 1])


def test_concurrent_scope_identity_includes_slot(action):
    adapter = StageBufferAdapter(_stage(max_batch=3, concurrency=2), action=action, stage_key='node-0')
    from demiflow.execution.recovery_contract import identity
    assert adapter.scope_identity(0, 0) == identity(['fixed-batch', 'node-0', 0, 0])
    assert adapter.scope_identity(1, 0) == identity(['fixed-batch', 'node-0', 1, 0])
    assert adapter.scope_identity(0, 0) != adapter.scope_identity(1, 0)


def test_invalid_slot_fails_closed(action):
    adapter = StageBufferAdapter(_stage(max_batch=3, concurrency=2), action=action, stage_key='node-0')
    for bad in (-1, 2, '0', None, 1.0):
        with pytest.raises(RecoveryContractError, match='worker slot'):
            adapter.next_ordinal(bad)


def test_legacy_fixed_batch_snapshot_restores_as_slot_zero(action):
    stage = _stage(max_batch=3)
    writer = StageBufferAdapter(stage, action=action, stage_key='node-0')
    import asyncio
    barrier = SimpleNamespace(states={}, snapshot_bytes=0)
    asyncio.run(writer.align(barrier, {0: [{'id': 0}, {'id': 1}]}))
    snapshot = barrier.states['node-0-buffer']
    # Reduce to the historical {batch_ordinal, data} shape and restore verbatim.
    legacy = {'batch_ordinal': snapshot['slots']['0']['batch_ordinal'],
              'data': snapshot['slots']['0']['data']}
    reader = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
    reader.restore(legacy)
    payload, ordinal = reader.take_restored(0)
    assert payload == [{'id': 0}, {'id': 1}] and ordinal == 0


def test_legacy_shape_rejected_for_non_fixed_kind(action):
    reader = StageBufferAdapter(_stage(max_batch=3, concurrency=2), action=action, stage_key='node-0')
    with pytest.raises(RecoveryContractError, match='Legacy fixed-batch'):
        reader.restore({'batch_ordinal': 0, 'data': {'rows': 0}})


def _snapshot(action, stage, pending):
    import asyncio
    adapter = StageBufferAdapter(stage, action=action, stage_key='node-0')
    barrier = SimpleNamespace(states={}, snapshot_bytes=0)
    asyncio.run(adapter.align(barrier, pending))
    return barrier.states['node-0-buffer']


def test_restore_rejects_identity_and_shape_mismatch(action):
    good = _snapshot(action, _stage(max_batch=3), {0: [{'id': 0}]})
    cases = [
        ({**good, 'schema': 'demiflow.stage_buffer.v0'}, 'schema'),
        ({**good, 'kind': 'grouped'}, 'kind'),
        ({**good, 'stage_key': 'node-9'}, 'identity'),
        ({**good, 'participants': 2}, 'participants'),
        ({**good, 'slots': {'0': good['slots']['0'], '1': good['slots']['0']}}, 'slots'),
    ]
    for bad, match in cases:
        reader = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
        with pytest.raises(RecoveryContractError, match=match):
            reader.restore(bad)


def test_restore_rejects_negative_or_noninteger_ordinal(action):
    good = _snapshot(action, _stage(max_batch=3), {0: [{'id': 0}]})
    for bad in (-1, '0', 1.5, None):
        slot = dict(good['slots']['0'], batch_ordinal=bad)
        reader = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
        with pytest.raises(RecoveryContractError, match='batch_ordinal'):
            reader.restore({**good, 'slots': {'0': slot}})


def test_fixed_batch_restore_rejects_full_group(action):
    full = _snapshot(action, _stage(max_batch=2), {0: [{'id': 0}, {'id': 1}]})
    reader = StageBufferAdapter(_stage(max_batch=2), action=action, stage_key='node-0')
    with pytest.raises(RecoveryContractError, match='exceeds its original group'):
        reader.restore(full)


def test_align_rejects_slot_set_mismatch(action):
    import asyncio
    adapter = StageBufferAdapter(_stage(max_batch=3, concurrency=2), action=action, stage_key='node-0')
    barrier = SimpleNamespace(states={}, snapshot_bytes=0)
    with pytest.raises(RecoveryContractError, match='active participants'):
        asyncio.run(adapter.align(barrier, {0: [{'id': 0}]}))


def test_align_charges_shared_budget_and_fails_closed(action):
    import asyncio
    adapter = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
    barrier = SimpleNamespace(states={}, snapshot_bytes=action.context.limits['max_state_bytes'])
    with pytest.raises(RecoveryContractError, match='state budget'):
        asyncio.run(adapter.align(barrier, {0: [{'id': 0}]}))


def test_grouped_round_trip_preserves_keys_rows_and_deadline(action):
    import asyncio
    stage = _stage(grouping=object(), max_batch=4)
    key = ((str, 'a'), (int, 2))
    pending = {0: [(key, [{'id': 0}, {'id': 1}], 1500)]}
    snapshot = _snapshot(action, stage, pending)
    reader = StageBufferAdapter(_stage(grouping=object(), max_batch=4), action=action, stage_key='node-0')
    reader.restore(snapshot)
    payload, _ordinal = reader.take_restored(0)
    (rkey, rows, deadline_ms), = payload
    assert rkey == key and rows == [{'id': 0}, {'id': 1}] and deadline_ms == 1500


def test_prepend_round_trip_preserves_position_and_exhausted(action):
    import asyncio
    stage = _stage(prefix=object())
    snapshot = _snapshot(action, stage, {0: {'position': 3, 'exhausted': True}})
    reader = StageBufferAdapter(_stage(prefix=object()), action=action, stage_key='node-0')
    reader.restore(snapshot)
    assert reader.take_prefix() == (3, True)


def test_close_is_idempotent_and_blocks_reuse(action):
    import asyncio
    adapter = StageBufferAdapter(_stage(max_batch=3), action=action, stage_key='node-0')
    adapter.close()
    adapter.close()
    with pytest.raises(RecoveryContractError, match='closed'):
        asyncio.run(adapter.align(SimpleNamespace(states={}, snapshot_bytes=0), {0: []}))
    with pytest.raises(RecoveryContractError, match='closed'):
        adapter.restore({'schema': SCHEMA})