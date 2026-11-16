"""Real sockets and original native worker authorities; no replacement kernels."""
from contextlib import contextmanager
from dataclasses import replace
import json
import socket
import struct
import threading
import time
import pytest

from demiflow.execution.contracts.dataflow import thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_store import RunArtifactStore
from demiflow.execution.exchange.dataflow_transport import PeerBuffers, read_peer
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_runtime_v3_b import worker_env, _events, _cut


@contextmanager
def peer_store(tmp_path, *, max_files=16):
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    bounds = replace(bounds, max_row_bytes=4 << 20, max_resident_bytes=16 << 20,
                     max_open_files=max_files, timeout_ms=1000)
    store = RunArtifactStore(root=tmp_path / 'store', store_id='peer-test', journal=None, bounds=bounds)
    peer = PeerBuffers(bounds=bounds, store=store)
    try:
        yield peer, store, bounds
    finally:
        peer.close()
        assert not peer._active and not peer._sending
        assert not any(t.is_alive() for t in (peer._thread, *peer._threads))
        assert store.open_file_slots == 0
        store.close()


@pytest.mark.parametrize('max_files,capacity', [(7, 1), (8, 4)])
def test_peer_concurrency_uses_original_fd_grant_and_low_budget_falls_back(tmp_path, max_files, capacity):
    with peer_store(tmp_path, max_files=max_files) as (peer, store, bounds):
        assert peer._connections == capacity
        assert store.open_file_slots == capacity - 1
        with store.file_slots(store.PUT_FILES):
            assert store.open_file_slots == capacity - 1 + store.PUT_FILES
        locator = peer.put(b'same bytes')
        assert locator['max_batch_records'] == (32 if capacity > 1 else 1)
        assert read_peer(locator, bounds=bounds) == b'same bytes'
        peer.release(locator['key'])


def test_slow_peer_does_not_block_another_reader_and_ack_cannot_free_sender(tmp_path):
    with peer_store(tmp_path) as (peer, store, bounds):
        slow, fast = peer.put(b'x' * (4 << 20)), peer.put(b'fast')
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.connect((slow['host'], slow['port']))
        request = json.dumps({'key': slow['key'], 'token': slow['token']}).encode()
        client.sendall(struct.pack('!I', len(request)) + request)
        deadline = time.monotonic() + 1
        while not peer._sending and time.monotonic() < deadline:
            time.sleep(.001)
        assert peer._sending.get(slow['key']) == 1
        assert read_peer(fast, bounds=bounds) == b'fast'
        peer.release(fast['key'])
        released, errors = threading.Event(), []
        def release():
            try:
                peer.release(slow['key'])
                released.set()
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=release)
        thread.start()
        try:
            assert not released.wait(.04)
            assert peer._bytes == 4 << 20 and slow['key'] in peer._buffers
        finally:
            client.close()
            thread.join(2)
        assert not errors and released.is_set() and peer._bytes == 0


@contextmanager
def native_batch(tmp_path, monkeypatch):
    with worker_env(tmp_path, rows=[{'x': n} for n in range(4)]) as env:
        producer, consumer = env.make(), env.make('transform')
        producer.worker.start()
        consumer.worker.start()
        # This fixture is the original driver boundary, with one source owner.
        # Grant its original run allowance: a one-max-row helper window pauses
        # after one small row because the next unknown row needs a full reserve.
        limits = env.execution._action.context.limits
        assert limits['checkpoint_unconfirmed_rows'] >= 4
        producer.worker.receive({'kind': 'resume', 'source_credit': {
            'rows': 4, 'bytes': limits['checkpoint_unconfirmed_bytes']}})
        for _ in range(4):
            producer.worker.step(max_records=1, deadline_ms=50)
        commands = [dict(thaw_record_value(e.payload), kind='data')
                    for e in _events(producer) if e.kind == 'data']
        assert len(commands) == 4
        assert all(c['peer']['max_batch_records'] == 32 for c in commands)
        sockets, connect = [], socket.create_connection
        def counted(*args, **kwargs):
            result = connect(*args, **kwargs)
            sockets.append(result)
            return result
        monkeypatch.setattr(socket, 'create_connection', counted)
        yield producer, consumer, commands, sockets


def test_ready_batch_runs_original_step_order_and_per_record_ack(tmp_path, monkeypatch):
    with native_batch(tmp_path, monkeypatch) as (producer, consumer, commands, sockets):
        for command in commands:
            consumer.worker.receive(command)
        runner = consumer.worker._runner
        polls, original = [], runner.step
        def step(**kwargs):
            polls.append(kwargs)
            return original(**kwargs)
        monkeypatch.setattr(runner, 'step', step)
        assert runner._step_ready() >= 4
        assert polls == [{'max_records': 1, 'deadline_ms': 50}] * 4
        assert len(sockets) == 1 and sockets[0].fileno() == -1
        acks = [thaw_record_value(e.payload) for e in _events(consumer) if e.kind == 'ack']
        assert [a['delivery_id'] for a in acks] == [c['delivery_id'] for c in commands]
        for ack in acks:
            producer.worker.receive(dict(ack, kind='ack'))
        assert not producer.worker._runner._credits
        assert producer.worker._runner._peer._bytes == 0


@pytest.mark.parametrize('failure', ['digest', 'sequence', 'business'])
def test_second_frame_failure_keeps_original_success_prefix_only(tmp_path, monkeypatch, failure):
    with native_batch(tmp_path, monkeypatch) as (producer, consumer, commands, sockets):
        runner = consumer.worker._runner
        if failure == 'digest':
            commands[1]['peer']['sha256'] = '0' * 64
        elif failure == 'sequence':
            commands[1]['route_sequence'] = commands[0]['route_sequence']
        else:
            original, called = runner._push, []
            def push(*args):
                called.append(True)
                if len(called) == 2:
                    raise ValueError('original business failure')
                return original(*args)
            monkeypatch.setattr(runner, '_push', push)
        for command in commands:
            consumer.worker.receive(command)
        with pytest.raises((DataflowError, ValueError)) as caught:
            runner._step_ready()
        assert ('checksum' if failure == 'digest' else 'duplicate' if failure == 'sequence'
                else 'original business failure') in str(caught.value)
        acks = [thaw_record_value(e.payload) for e in _events(consumer) if e.kind == 'ack']
        assert [a['delivery_id'] for a in acks] == [commands[0]['delivery_id']]
        assert len(runner._queue) == 2
        assert all(s.fileno() == -1 for s in sockets) and runner._read_batch is None
        assert len(producer.worker._runner._credits) == 4


def test_ready_read_stops_before_cut_and_closes_slow_step_at_original_deadline(tmp_path, monkeypatch):
    with native_batch(tmp_path, monkeypatch) as (producer, consumer, commands, sockets):
        runner = consumer.worker._runner
        consumer.worker.receive(commands[0])
        consumer.worker.receive(commands[1])
        consumer.worker.request_cut(_cut(consumer))
        consumer.worker.receive(commands[2])
        assert runner._ready_peer_commands() == commands[:2]
        original = runner._push
        def slow(*args):
            time.sleep(.06)
            return original(*args)
        monkeypatch.setattr(runner, '_push', slow)
        runner._step_ready()
        assert runner._queue[0] is commands[1]
        assert all(s.fileno() == -1 for s in sockets)
        monkeypatch.setattr(runner, '_push', original)
        runner._step_ready()
        assert runner._queue[0]['kind'] == 'cut' and runner._queue[1] is commands[2]
        acks = [thaw_record_value(e.payload) for e in _events(consumer) if e.kind == 'ack']
        assert [a['delivery_id'] for a in acks] == [c['delivery_id'] for c in commands[:2]]


@pytest.mark.parametrize('hint', [None, True, 0, 33, '32'])
def test_absent_or_invalid_hint_cannot_expand_the_consumer_window(tmp_path, monkeypatch, hint):
    with native_batch(tmp_path, monkeypatch) as (_, consumer, commands, sockets):
        for command in commands:
            if hint is None:
                command['peer'].pop('max_batch_records')
            else:
                command['peer']['max_batch_records'] = hint
            consumer.worker.receive(command)
        runner = consumer.worker._runner
        assert len(runner._ready_peer_commands()) <= 1
        runner._step_ready()
        assert len(runner._queue) == 3 and len(sockets) == 1
        assert sockets[0].fileno() == -1


def test_close_unblocks_full_connection_window_and_releases_original_grant(tmp_path):
    with peer_store(tmp_path) as (peer, store, bounds):
        clients = [socket.create_connection(peer._address) for _ in range(4)]
        try:
            deadline = time.monotonic() + 1
            while len(peer._active) < 4 and time.monotonic() < deadline:
                time.sleep(.001)
            assert len(peer._active) == 4
            assert len(peer._threads) == 4 and store.open_file_slots == 3
            started = time.monotonic()
            peer.close()
            assert time.monotonic() - started < .5
            assert not peer._active and not peer._requests.qsize()
            assert store.open_file_slots == 0
        finally:
            for client in clients:
                client.close()
