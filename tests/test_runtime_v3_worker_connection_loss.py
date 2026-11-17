"""Actual local transport loss keeps unknown effects distinct from bad frames."""
from collections import deque
from contextlib import contextmanager
import json
import socket
import struct
import threading
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_hosts import LocalHostEndpoint
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.worker.bootstrap import ControlFrameReader, recv_message, _MAX_CONTROL_BYTES


def _endpoint(sock):
    value = LocalHostEndpoint.__new__(LocalHostEndpoint)
    value._sock = sock
    value._events = deque()
    value._business_batch = None
    value._business_bytes = 0
    value._services = NS(bounds=NS(max_pending_events=4), policy=NS(control_timeout_ms=1000))
    value._control_reader = ControlFrameReader(sock, timeout_s=1)
    value._deliver_pending_journal_replies = lambda: None
    value._k3_envelope = lambda kind, payload: {'operation_id':'original-op', 'kind':kind, 'payload':payload}
    return value


@contextmanager
def _connection():
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    driver = socket.create_connection(listener.getsockname(), timeout=2)
    worker, _ = listener.accept()
    listener.close()
    worker.settimeout(2)
    try:
        yield driver, worker
    finally:
        driver.close()
        worker.close()


@pytest.mark.parametrize('operation', ['poll', 'lifecycle'])
@pytest.mark.parametrize('reset', [False, True])
def test_real_eof_and_reset_leave_the_original_operation_unknown(operation, reset):
    with _connection() as (driver, worker):
        endpoint = _endpoint(driver)
        received, failures = [], []
        def disconnect():
            try:
                if operation == 'lifecycle':
                    received.append(recv_message(worker))
                if reset:
                    worker.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                worker.close()
            except BaseException as exc:
                failures.append(exc)
        thread = threading.Thread(target=disconnect)
        thread.start()
        try:
            with pytest.raises(DataflowError) as error:
                if operation == 'lifecycle':
                    endpoint._send_k3('Restore', {'original':'state'})
                else:
                    thread.join(timeout=2)
                    endpoint.poll(max_events=4, deadline_ms=1000)
            assert error.value.code == 'UNKNOWN_EFFECT'
            if reset:
                assert isinstance(error.value.__cause__, ConnectionResetError)
            assert not endpoint._events
            assert not getattr(endpoint, '_cleanup_completed', False)
        finally:
            thread.join(timeout=2)
        assert not thread.is_alive() and not failures
        assert received == ([{'operation_id':'original-op','kind':'Restore','payload':{'original':'state'}}]
                            if operation == 'lifecycle' else [])


@pytest.mark.parametrize('raw,expected,match', [
    (b'\x00', RecoveryContractError, 'Truncated'),
    ((_MAX_CONTROL_BYTES + 1).to_bytes(4, 'big'), RecoveryContractError, 'byte budget'),
    ((1).to_bytes(4, 'big') + b'{', json.JSONDecodeError, None),
])
def test_actual_broken_protocol_is_not_relabelled_as_worker_loss(raw, expected, match):
    driver, worker = socket.socketpair()
    try:
        endpoint = _endpoint(driver)
        worker.sendall(raw)
        worker.close()
        with pytest.raises(expected, match=match):
            endpoint.poll(max_events=4, deadline_ms=1000)
    finally:
        driver.close()
        worker.close()
