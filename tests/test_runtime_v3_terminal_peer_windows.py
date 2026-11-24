"""Actual peer sockets through the original terminal decoder and ACK order."""
from functools import partial
from demiflow.execution.hosts.endpoints import batch_business_commands
from types import SimpleNamespace
import socket

import pytest

from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.outputs.terminals import TerminalSubscription
from demiflow.execution.contracts.record_codec import _encode_record_document
from demiflow.execution.contracts.record_codec import row_bytes as _row_bytes
from demiflow.execution.contracts.recovery_contract import identity
from test_runtime_v3_ready_peer_reads import peer_store


@pytest.mark.parametrize('hint,expected_connections', [(None, 3), (True, 3), (33, 3), (1, 3), (2, 2), (32, 1)])
def test_terminal_honours_original_or_low_fd_peer_window(tmp_path, monkeypatch, hint, expected_connections):
    with peer_store(tmp_path) as (peer, store, bounds):
        payloads = []
        for index in range(3):
            row = {'n': index}
            record = Record(identity(['row', index]), {}, None, row,
                            _row_bytes(row, max_bytes=bounds.max_row_bytes), index)
            locator = peer.put(_encode_record_document(record, max_bytes=bounds.max_row_bytes))
            if hint is None:
                locator.pop('max_batch_records')
            else:
                locator['max_batch_records'] = hint
            payloads.append(dict(peer=locator, record_id=record.record_id, sequence=index,
                                 delivery_id=str(index), producer_partition='p0'))
        connects, original = [], socket.create_connection
        def connect(*args, **kwargs):
            connection = original(*args, **kwargs)
            connects.append(connection)
            return connection
        monkeypatch.setattr(socket, 'create_connection', connect)
        acked = []
        def ack(delivery_id):
            acked.append(delivery_id)
            peer.release(payloads[int(delivery_id)]['peer']['key'])
        exchange = SimpleNamespace(terminal=lambda key, producer: None,
                                   acknowledge=lambda key, receiver: ack(key))
        subscription = TerminalSubscription(bounds, exchange, batch_commands=partial(batch_business_commands, ()))
        for payload in payloads:
            subscription.receive(payload, None)
        records = subscription.read_admitted(max_records=3, deadline_ms=1000)
        assert [thaw_record_value(record.inline) for record in records] == [{'n': i} for i in range(3)]
        assert acked == ['0', '1', '2'] and not subscription._queue
        assert len(connects) == expected_connections and all(c.fileno() == -1 for c in connects)
        assert not peer._buffers and peer._bytes == 0


def test_result_batch_uses_one_ack_and_replays_original_record_positions(tmp_path):
    from dataclasses import asdict
    import pyarrow as pa
    from demiflow.execution.contracts.record_codec import decode_document
    from demiflow.execution.outputs.replay import PublishedRecordReader
    from demiflow.execution.outputs.arrow_rows import pack_result
    from demiflow.execution.storage.record_schema import RecordSchemas
    from demiflow.execution.outputs.terminal_blocks import TerminalBlocks

    from test_runtime_v3_artifact_accounting import store_fixture

    with store_fixture(tmp_path) as (_, store, _):
        bounds = store._bounds
        services = SimpleNamespace(store=store, bounds=bounds)
        blocks = TerminalBlocks(services, 'test')
        value = pa.record_batch({'row_id': [b'first', b'second'], 'position': [9, 0]})
        positions = pa.record_batch({'fragment_id': [1, 5], 'row_offset': [8, 2], 'row_id': [38, 53]})
        source, split, op = map(identity, ('source', 'split', 'op'))
        schema_ref = RecordSchemas(services).put(value.schema)
        leaf = pack_result(value, positions, source_id=source, split_id=split, version=7,
                           occurrence=op, sequence=0, bounds=bounds)
        ref = blocks.append_arrow_batch(leaf)
        acked = []
        exchange = SimpleNamespace(terminal=lambda key, producer: None,
                                   acknowledge=lambda key, receiver: acked.append(key))
        sub = TerminalSubscription(bounds, exchange, batch_commands=partial(batch_business_commands, ()), store=store)
        event = dict(result_batch=asdict(ref), schema_ref=asdict(schema_ref), rows=2, sequence=0, delivery_id='one')
        sub.receive(event, None)
        batch, = sub.read_admitted(max_records=1, deadline_ms=0, batch_values=True)
        assert list(batch.values()) == value.to_pylist()
        assert acked == ['one']
        sub.receive(dict(event, delivery_id='two'), None)
        first, = sub.read_admitted(max_records=1, deadline_ms=0)
        assert acked == ['one']
        sub.close()
        assert acked == ['one', 'two']
        publication = {'manifest': {'partitions': blocks.manifest_partitions(), 'schema_ref': asdict(schema_ref)}}
        replay = PublishedRecordReader(publication, store=store, bounds=bounds)
        for i in range(2):
            raw, record_id, seq, _ = replay.pop()
            record = decode_document(raw, record_id=record_id, sequence=seq, bounds=bounds)
            pos = {**positions.to_pylist()[i], 'version': 7}
            assert (record.source_position, seq, record_id, dict(record.inline), record.schema_ref) == (
                pos, i, identity([op, identity([source, split, pos])]), value.to_pylist()[i], schema_ref)
            if i == 0:
                assert first == record
        with pytest.raises(StopIteration):
            replay.pop()
        replay.close()
        blocks.close()
