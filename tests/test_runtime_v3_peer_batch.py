"""Real bounded peer transport: every frame still owns its hash and credit."""
from dataclasses import replace
import sys
import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_transport import PeerBuffers, read_peer, read_peer_many
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def test_short_lived_batch_preserves_order_bytes_and_per_record_release(monkeypatch):
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    bounds = replace(bounds, max_pending_events=32, max_row_bytes=65536,
        max_resident_bytes=2**20)
    peer = PeerBuffers(bounds=bounds)
    try:
        expected = [bytes([i]) * (i + 1) * 100 for i in range(32)]
        refs = tuple(peer.put(value) for value in expected)
        import socket
        connect, connections = socket.create_connection, []
        def counted(*args, **kwargs):
            result = connect(*args, **kwargs)
            connections.append(result)
            return result
        monkeypatch.setattr(socket, 'create_connection', counted)
        frames = read_peer_many(refs, bounds=bounds)
        for index in range(len(expected)):
            raw = next(frames)
            assert raw == expected[index]
            # Only the consumer owns this received copy, even while the read
            # generator is suspended for a slow callback or native poll.
            assert sys.getrefcount(raw) == 2
            assert peer._bytes == sum(map(len, expected[index:]))
            peer.release(refs[index]['key'])
        with pytest.raises(StopIteration):
            next(frames)
        assert len(connections) == 1 and connections[0].fileno() == -1
        assert peer._bytes == 0 and not peer._buffers
        # Historical single-key request remains supported on the same server.
        ref = peer.put(b'old single-key request')
        assert read_peer(ref, bounds=bounds) == b'old single-key request'
    finally:
        peer.close()


@pytest.mark.parametrize('corruption', ['size', 'digest', 'authority', 'count'])
def test_batch_does_not_hide_invalid_later_frame_or_release_its_credit(corruption):
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    peer = PeerBuffers(bounds=bounds)
    try:
        first, second = peer.put(b'first'), peer.put(b'second')
        refs = [first, dict(second)]
        if corruption == 'size': refs[1]['bytes'] += 1
        if corruption == 'digest': refs[1]['sha256'] = '0' * 64
        if corruption == 'authority': refs[1]['token'] = 'wrong-owner'
        if corruption == 'count': refs *= 17
        with pytest.raises(DataflowError) as caught:
            list(read_peer_many(refs, bounds=bounds))
        assert caught.value.code == {'count': 'BUDGET_EXCEEDED', 'authority': 'INVALID_CONTRACT'}.get(
            corruption, 'CORRUPT_ARTIFACT')
        assert peer._bytes == 11 and len(peer._buffers) == 2
    finally:
        peer.close()
