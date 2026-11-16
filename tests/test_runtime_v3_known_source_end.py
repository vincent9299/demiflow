"""Known immutable EOF must not hide the final unacknowledged record."""
import pytest

from test_runtime_v3_s import _items_setup, _range_setup, open_reader


@pytest.mark.parametrize('kind', ['items', 'range'])
def test_known_end_keeps_last_pending_row_until_original_ack(tmp_path, kind):
    setup = (_items_setup(tmp_path, [1]) if kind == 'items' else _range_setup(1))
    store, plan, snapshot, split, services = setup
    reader = open_reader(snapshot, split, services)
    try:
        last = reader.next_record()
        assert last.status == 'record'
        cursor = reader.snapshot()
        assert cursor.end_of_input and cursor.next_sequence == 1
        assert len(cursor.pending_refs) == 1
        restored = open_reader(snapshot, split, services, cursor=cursor)
        try:
            # Known EOF is never permission to omit the read-but-unconfirmed row.
            assert restored.next_record().record == last.record
            restored.acknowledge_output()
            confirmed = restored.snapshot()
            assert confirmed.end_of_input and not confirmed.pending_refs
            assert restored.next_record().status == 'eoi'
        finally:
            restored.close()
    finally:
        reader.close()


@pytest.mark.parametrize('kind', ['items', 'range'])
def test_partial_split_never_claims_known_end(tmp_path, kind):
    setup = (_items_setup(tmp_path, [1, 2]) if kind == 'items' else _range_setup(2))
    store, plan, snapshot, split, services = setup
    reader = open_reader(snapshot, split, services)
    try:
        assert reader.next_record().status == 'record'
        assert not reader.snapshot().end_of_input
    finally:
        reader.close()
