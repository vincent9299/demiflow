"""Original frames remain ordered and bounded through coalesced transport."""
from collections import deque
import json
import socket
import threading
import time
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.dataflow_hosts import _EndpointBase, LocalHostEndpoint
from demiflow.execution.recovery_contract import document, RecoveryContractError
from demiflow.execution.worker.bootstrap import (
    ControlFrameReader, send_messages, _MAX_CONTROL_BYTES,
)
from demiflow.execution.worker.dataflow_host import HostedDataflowWorker
from test_runtime_v3_ray_terminal_seams import private_ray_provider


class Capture:
    def __init__(self):
        self.packets = []

    def sendall(self, raw):
        self.packets.append(bytes(raw))


def _frames(packets):
    result = []
    for packet in packets:
        offset = 0
        while offset < len(packet):
            size = int.from_bytes(packet[offset:offset + 4], 'big')
            result.append(json.loads(packet[offset + 4:offset + 4 + size]))
            offset += 4 + size
        assert offset == len(packet)
    return result


def _endpoint(sock):
    endpoint = LocalHostEndpoint.__new__(LocalHostEndpoint)
    endpoint._sock = sock
    endpoint._business_batch = None
    endpoint._business_bytes = 0
    endpoint._services = NS(bounds=NS(max_pending_events=4096), policy=NS(control_timeout_ms=1000))
    return endpoint


def test_coalescing_preserves_every_original_frame_byte_and_order():
    messages = [{'kind': 'ack', 'id': i, 'unicode': '汉\n\x00'} for i in range(130)]
    capture = Capture()
    send_messages(capture, messages)
    expected = b''.join(len(raw).to_bytes(4, 'big') + raw for raw in map(document, messages))
    assert b''.join(capture.packets) == expected
    assert len(capture.packets) == 3 and _frames(capture.packets) == messages
    assert all(len(p) <= _MAX_CONTROL_BYTES for p in capture.packets)


def test_aggregate_byte_budget_and_legacy_exact_maximum_single_frame():
    capture = Capture()
    messages = [{'value': 'x' * 400000} for _ in range(5)]
    send_messages(capture, messages)
    assert len(capture.packets) == 3
    assert all(len(p) <= _MAX_CONTROL_BYTES for p in capture.packets)
    assert _frames(capture.packets) == messages
    exact = {'v': 'x' * (_MAX_CONTROL_BYTES - len(document({'v': ''})))}
    capture = Capture()
    send_messages(capture, [exact])
    assert len(capture.packets[0]) == _MAX_CONTROL_BYTES + 4
    with pytest.raises(RecoveryContractError):
        send_messages(capture, [{'v': exact['v'] + 'x'}])


@pytest.mark.parametrize('value', [float('nan'), float('inf'), object()])
def test_batch_sender_keeps_original_document_validation(value):
    with pytest.raises(RecoveryContractError):
        send_messages(Capture(), [{'value': value}])


def test_reader_retains_partial_header_payload_and_coalesced_following_frames():
    sender, receiver = socket.socketpair()
    try:
        reader = ControlFrameReader(receiver, timeout_s=1)
        capture = Capture()
        messages = [{'kind': 'data', 'id': 1}, {'type': 'control_reply', 'wire': 'reply'}, {'kind': 'Cancel'}]
        send_messages(capture, messages)
        raw = b''.join(capture.packets)
        for chunk in (raw[:2], raw[2:7]):
            sender.sendall(chunk)
            assert reader.read_ready(max_messages=64) == ()
        sender.sendall(raw[7:])
        assert reader.read_ready(max_messages=1) == (messages[0],)
        # The remaining frames were already read; socket readiness is no longer
        # required and no alternate recv_message owner may steal them.
        assert reader.read_ready(max_messages=64) == tuple(messages[1:])
        assert reader.read_ready(max_messages=64) == ()
        sender.close()
        assert reader.read_ready(max_messages=64) is None
    finally:
        sender.close(); receiver.close()


@pytest.mark.parametrize('raw', [b'\x00', (12).to_bytes(4, 'big') + b'{'])
def test_reader_rejects_early_eof_without_losing_partial_frame(raw):
    sender, receiver = socket.socketpair()
    try:
        sender.sendall(raw); sender.close()
        with pytest.raises(RecoveryContractError, match='Truncated'):
            ControlFrameReader(receiver, timeout_s=1).read_ready(max_messages=64)
    finally:
        sender.close(); receiver.close()


def test_partial_deadline_and_oversized_header_fail_before_body_allocation(monkeypatch):
    import demiflow.execution.worker.bootstrap as bootstrap
    sender, receiver = socket.socketpair()
    clock = [100.0]
    monkeypatch.setattr(bootstrap.time, 'monotonic', lambda: clock[0])
    try:
        reader = ControlFrameReader(receiver, timeout_s=1)
        sender.sendall(b'\x00')
        assert reader.read_ready(max_messages=64) == ()
        clock[0] += 2
        with pytest.raises(TimeoutError, match='Partial'):
            reader.read_ready(max_messages=64)
        sender.sendall((_MAX_CONTROL_BYTES + 1).to_bytes(4, 'big'))
        other = ControlFrameReader(receiver, timeout_s=1)
        with pytest.raises(RecoveryContractError, match='byte budget'):
            other.read_ready(max_messages=64)
        assert len(other._buffer) == 4
    finally:
        sender.close(); receiver.close()


def test_large_frame_uses_one_bounded_pending_buffer_and_delivers_without_waiting_for_more():
    sender, receiver = socket.socketpair()
    message = {'v': 'x' * (_MAX_CONTROL_BYTES - len(document({'v': ''})))}
    errors = []
    def send():
        try:
            send_messages(sender, [message])
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=send)
    thread.start()
    try:
        reader = ControlFrameReader(receiver, timeout_s=2)
        deadline = time.monotonic() + 5
        values = ()
        while not values and time.monotonic() < deadline:
            values = reader.read_ready(max_messages=64)
            assert len(reader._buffer) <= _MAX_CONTROL_BYTES + 4
        assert values == (message,)
        thread.join(timeout=1)
        assert not thread.is_alive() and not errors
    finally:
        sender.close(); receiver.close(); thread.join(timeout=1)


def test_business_scope_flushes_on_end_exception_and_before_control():
    capture = Capture(); endpoint = _endpoint(capture)
    commands = [{'kind': 'ack', 'delivery_id': str(i)} for i in range(12)]
    with endpoint.batch_business():
        for command in commands:
            endpoint.send_business(command)
        assert capture.packets == []
    assert len(capture.packets) == 1
    assert [f['command'] for f in _frames(capture.packets)] == commands
    with pytest.raises(RuntimeError, match='after original ack'):
        with endpoint.batch_business():
            endpoint.send_business(commands[0])
            raise RuntimeError('after original ack')
    assert len(capture.packets) == 2
    with endpoint.batch_business():
        endpoint.send_business(commands[1])
        endpoint.send_business({'kind': 'pause'})
        assert len(capture.packets) == 3
    assert [f['command']['kind'] for f in _frames(capture.packets)][-2:] == ['ack', 'pause']
    # Outside the ready-event scope, the first/only command is sent immediately.
    endpoint.send_business(commands[2])
    assert len(capture.packets) == 4


def test_unknown_send_is_propagated_and_never_automatically_retried():
    class Lost:
        calls = 0
        def sendall(self, raw):
            self.calls += 1
            raise OSError('reply outcome unknown')
    socket = Lost(); endpoint = _endpoint(socket)
    with pytest.raises(OSError, match='unknown'):
        with endpoint.batch_business():
            endpoint.send_business({'kind': 'ack', 'delivery_id': 'one'})
    assert socket.calls == 1 and endpoint._business_batch is None


def test_ray_method_preserves_per_command_receive_and_does_not_hide_partial_failure():
    observed = []
    host = NS(_mailbox=NS(_bounds=NS(max_pending_events=64)), receive=observed.append)
    commands = [{'kind': 'ack', 'delivery_id': str(i)} for i in range(3)]
    HostedDataflowWorker.receive_many(host, commands)
    assert observed == commands
    def fail(command):
        observed.append(command)
        if command == commands[1]:
            raise RuntimeError('original receive rejected ack')
    host.receive = fail; observed.clear()
    with pytest.raises(RuntimeError, match='rejected ack'):
        HostedDataflowWorker.receive_many(host, commands)
    assert observed == commands[:2]
    with pytest.raises(Exception, match='event budget'):
        HostedDataflowWorker.receive_many(host, commands * 22)
    with pytest.raises(Exception, match='byte budget'):
        HostedDataflowWorker.receive_many(host, [{'value': 'x' * 600000}] * 2)


def test_lifecycle_response_and_events_share_one_reader_and_keep_fencing():
    from test_runtime_v3_c import make, bounds
    from demiflow.execution.contracts.dataflow_codec import encode_wire
    from demiflow.execution.worker.bootstrap import recv_message
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    event = make('WorkerEvent', kind='progress')
    budget = bounds()
    payload = json.loads(encode_wire(event, type_name='WorkerEvent', bounds=budget))['payload']
    driver, worker = socket.socketpair()
    endpoint = _endpoint(driver)
    endpoint._services.bounds = budget
    endpoint._events = deque()
    endpoint._assignment = event
    endpoint._allocation_id = event.allocation_id
    endpoint._boot_id = event.boot_id
    endpoint._control_reader = ControlFrameReader(driver, timeout_s=1)
    endpoint._k3_envelope = lambda kind, payload: {'operation_id': 'command-op', 'kind': kind, 'payload': payload}
    endpoint._model = NS(handle=lambda *a, **k: None)
    failures = []
    def respond():
        try:
            request = recv_message(worker)
            send_messages(worker, [{'type': 'worker_event', 'event': payload},
                {'response_to': request['operation_id'], 'receipt': {'status': 'ok'}},
                {'type': 'worker_event', 'event': payload}])
        except BaseException as exc:
            failures.append(exc)
    thread = threading.Thread(target=respond); thread.start()
    try:
        assert endpoint._send_k3('Restore', {}) == {'status': 'ok'}
        assert endpoint.poll(max_events=1, deadline_ms=100) == (event,)
        assert endpoint.poll(max_events=1, deadline_ms=100) == (event,)
        thread.join(timeout=1)
        assert not thread.is_alive() and not failures
        # An invalid bool-as-int still passes through the original strict codec.
        send_messages(worker, [{'type': 'worker_event', 'event': dict(payload, attempt=True)}])
        with pytest.raises(DataflowError):
            endpoint.poll(max_events=1, deadline_ms=100)
        with pytest.raises(DataflowError, match='FENCED'):
            endpoint._early_event(make('WorkerEvent', kind='progress', boot_id='another-boot'))
    finally:
        driver.close(); worker.close(); thread.join(timeout=1)


def test_real_ray_terminal_acks_use_bounded_batch_rpcs(tmp_path, monkeypatch, private_ray_provider):
    import sys
    from demiflow import data
    from demiflow.execution.dataflow_hosts import RayHostEndpoint
    from demiflow.execution.run_context import RunContext
    original = _EndpointBase.flush_business
    terminal = []
    def flush(endpoint):
        pending = tuple(endpoint._business_batch or ())
        frame, at_terminal = sys._getframe(1), False
        while frame is not None:
            if frame.f_code.co_name == 'read_admitted':
                at_terminal = True
                break
            frame = frame.f_back
        result = original(endpoint)  # Count only successful actual actor replies.
        if isinstance(endpoint, RayHostEndpoint) and pending and at_terminal:
            assert all(command['kind'] == 'ack' for command, _ in pending)
            terminal.append(tuple(command['delivery_id'] for command, _ in pending))
        return result
    monkeypatch.setattr(_EndpointBase, 'flush_business', flush)
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path/'capacity'))
    with RunContext.start(tmp_path/'run', limits={'timeout_s':120,
            'memory_bytes':256*1024**2, 'checkpoint_rows':512}) as run:
        assert data.range(512).map(lambda row: {'id': row['id'] * 2}).take_all() == [
            {'id':i*2} for i in range(512)]
        facts = run.journal.execution_commits('action-1')
        assert facts and facts[-1]['status'] == 'acked'
    deliveries = [item for batch in terminal for item in batch]
    assert len(deliveries) == 512 and len(set(deliveries)) == 512
    assert all(1 <= len(batch) <= 64 for batch in terminal)
    assert len(terminal) < len(deliveries), 'actual terminal ACKs still make one RPC per row'
