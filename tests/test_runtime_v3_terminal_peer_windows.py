"""Actual peer sockets through the original terminal decoder and ACK order."""
from types import SimpleNamespace
import socket

import pytest

from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.dataflow_terminals import TerminalSubscription
from demiflow.execution.exchange.dataflow_bridge import _encode_record_document
from demiflow.execution.kernels.dataflow import _row_bytes
from demiflow.execution.recovery_contract import identity
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
        subscription = TerminalSubscription(bounds, exchange, ())
        for payload in payloads:
            subscription.receive(payload, None)
        records = subscription.read_admitted(max_records=3, deadline_ms=1000)
        assert [thaw_record_value(record.inline) for record in records] == [{'n': i} for i in range(3)]
        assert acked == ['0', '1', '2'] and not subscription._queue
        assert len(connects) == expected_connections and all(c.fileno() == -1 for c in connects)
        assert not peer._buffers and peer._bytes == 0
