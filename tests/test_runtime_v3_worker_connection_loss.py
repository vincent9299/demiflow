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
from demiflow.execution.hosts.endpoints import LocalHostEndpoint
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
@pytest.mark.parametrize('operation', ['poll', 'registration'])
def test_actual_broken_protocol_is_not_relabelled_as_worker_loss(raw, expected, match, operation):
    driver, worker = socket.socketpair()
    try:
        endpoint = _endpoint(driver)
        worker.sendall(raw)
        worker.close()
        with pytest.raises(expected, match=match):
            if operation == 'registration':
                endpoint._allocation_id = 'allocation-before-register'
                endpoint._boot_id = 'original-boot'
                endpoint._receive_registration()
            else:
                endpoint.poll(max_events=4, deadline_ms=1000)
    finally:
        driver.close()
        worker.close()


@pytest.mark.parametrize('reset', [False, True])
def test_registration_transport_loss_is_unknown_and_does_not_activate(reset):
    with _connection() as (driver, worker):
        endpoint = _endpoint(driver)
        endpoint._allocation_id = 'allocation-before-register'
        endpoint._boot_id = 'original-boot'
        endpoint._activated = False
        endpoint._provider = NS(_spawned={})
        if reset:
            worker.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        worker.close()
        with pytest.raises(DataflowError) as error:
            endpoint._receive_registration()
        assert error.value.code == 'UNKNOWN_EFFECT'
        assert dict(error.value.context) == {
            'allocation_id': 'allocation-before-register', 'boot_id': 'original-boot'}
        assert not endpoint._activated and not endpoint._events


def test_pre_registration_process_death_retains_its_actual_exit_code():
    import subprocess
    import sys
    driver, worker = socket.socketpair()
    process = subprocess.Popen([sys.executable, '-I', '-c', 'import os; os._exit(79)'],
                               pass_fds=(worker.fileno(),))
    worker.close()
    try:
        assert process.wait(timeout=5) == 79
        endpoint = _endpoint(driver)
        endpoint._allocation_id = 'allocation-before-register'
        endpoint._boot_id = 'original-boot'
        endpoint._activated = False
        endpoint._provider = NS(_spawned={endpoint._allocation_id: process})
        with pytest.raises(DataflowError) as error:
            endpoint._receive_registration()
        assert error.value.code == 'UNKNOWN_EFFECT'
        assert error.value.context['worker_exit_code'] == 79
        assert not endpoint._activated and not endpoint._events
    finally:
        driver.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.parametrize('message', [{'kind': 'Heartbeat'}, []])
def test_registration_rejects_wrong_frames_without_hiding_protocol_failure(message):
    from demiflow.execution.worker.bootstrap import send_message
    driver, worker = socket.socketpair()
    try:
        endpoint = _endpoint(driver)
        endpoint._allocation_id = 'allocation-before-register'
        endpoint._boot_id = 'original-boot'
        send_message(worker, message)
        with pytest.raises(DataflowError) as error:
            endpoint._receive_registration()
        assert error.value.code == 'INTERNAL_PROTOCOL'
    finally:
        driver.close()
        worker.close()


@pytest.mark.parametrize('after_ack', [False, True])
def test_owned_worker_loss_before_registration_preserves_recovery_admission(tmp_path, monkeypatch, after_ack):
    from dataclasses import replace
    from demiflow import data
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.scheduler import native_limits as action_limits
    from demiflow.execution.pipeline.run_context import RunContext
    from runtime_v3_owned_crash import assert_released, process_records
    from runtime_v3_graph_history import capture_acks, assert_acks_retained

    def double(row):
        return {'n': row['n'] * 2}

    def execute():
        return data.from_items([{'n': i} for i in range(4)]).map(double).take_all()

    # Keep real separate stages so a later bootstrap can fail after a real
    # acknowledged phase, without manufacturing a recoverable journal record.
    resolve = action_limits.resolve_runtime_policy

    def unfused(*args, **kwargs):
        policy, bounds = resolve(*args, **kwargs)
        return replace(policy, fusion=False), bounds

    monkeypatch.setattr(action_limits, 'resolve_runtime_policy', unfused)
    initialize, activate = LocalHostEndpoint.__init__, LocalHostEndpoint.activate
    paused = 'import time; time.sleep(30)'
    deaths, actions = [], []

    def paused_bootstrap(endpoint, *args, **kwargs):
        import sys
        initialize(endpoint, *args, **kwargs)
        action = endpoint._services.action
        acked = capture_acks(action.context, action.action_id)
        if bool(acked) == after_ack and not deaths:
            # A real managed process retains its unit lock/control FD but
            # cannot register before the kill, independent of host scheduling.
            endpoint._argv = (sys.executable, '-I', '-c', paused)

    def die_before_register(endpoint, assignment):
        if endpoint._argv[-1] == paused:
            process = endpoint._provider._spawned[endpoint._allocation_id]
            process.kill()
            deaths.append(process.wait(timeout=5))
            actions.append(endpoint._services.action.action_id)
        activate(endpoint, assignment)

    with pytest.raises(DataflowError) as error:
        with RunContext.start(tmp_path / 'run') as run, data.local_execution(workers=1):
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(LocalHostEndpoint, '__init__', paused_bootstrap)
                patch.setattr(LocalHostEndpoint, 'activate', die_before_register)
                execute()
    assert error.value.code == 'UNKNOWN_EFFECT'
    assert deaths == [-9] and error.value.context['worker_exit_code'] == -9
    assert_released(tmp_path / 'run')
    originals = process_records(tmp_path / 'run')
    acks = capture_acks(run, actions[0])
    assert bool(acks) == after_ack
    if after_ack:
        with RunContext.resume(reference) as resumed, data.local_execution(workers=1):
            assert resumed.attempt == 2
            assert execute() == [{'n': i * 2} for i in range(4)]
            assert_acks_retained(resumed, actions[0], acks)
        assert process_records(tmp_path / 'run').keys() > originals.keys()
    else:
        # Merely having sealed files is never authority to replay an action.
        with pytest.raises(DataflowError) as replay_error:
            with RunContext.resume(reference), data.local_execution(workers=1):
                execute()
        assert replay_error.value.code == 'INCOMPATIBLE_HISTORY'
        assert process_records(tmp_path / 'run') == originals
    assert_released(tmp_path / 'run')


def test_registration_timeout_is_distinct_from_eof(monkeypatch):
    from demiflow.execution.worker import bootstrap
    endpoint = LocalHostEndpoint.__new__(LocalHostEndpoint)
    endpoint._sock = object()
    endpoint._allocation_id = 'allocation-before-register'
    endpoint._boot_id = 'original-boot'

    def expired(sock):
        raise TimeoutError('socket deadline')

    monkeypatch.setattr(bootstrap, 'recv_message', expired)
    with pytest.raises(DataflowError) as error:
        endpoint._receive_registration()
    assert error.value.code == 'DEADLINE_EXCEEDED'
    assert isinstance(error.value.__cause__, TimeoutError)
    assert 'worker_exit_code' not in error.value.context
