"""A control reply remains authoritative; wakeups only announce queued work."""
import hashlib
import select
import socket
import threading
from dataclasses import replace

from demiflow.execution.contracts.dataflow import ControlReply, WorkerEvent
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.worker.mailbox import ControlMailbox
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def event(index):
    return WorkerEvent(schema='demiflow.dataflow_event.v3', kind='artifact',
        operation_id=hashlib.sha256(str(index).encode()).hexdigest(),
        run_id='run', attempt=1, fence='fence', allocation_id='allocation',
        boot_id='boot', plan_revision=0, scope_id='scope', controller_epoch=1,
        payload_ref=None, payload={'index': index})


def test_notifications_coalesce_only_until_the_outbox_is_drained():
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    wakes = []
    box = ControlMailbox(bounds=bounds, check_stop=lambda: None,
        on_outbox=lambda: wakes.append(True))
    try:
        first, second, third = (event(i) for i in range(3))
        box.send(first)
        box.send(second)
        assert len(wakes) == 1
        assert box.drain(max_events=1) == (first,)
        box.send(third)
        assert len(wakes) == 1
        assert box.drain(max_events=3) == (second, third)
        box.send(first)
        assert len(wakes) == 2
    finally:
        box.close()


def test_sequential_rpc_wakes_socket_reader_without_manufacturing_receipts():
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    results, failures = [], []
    reader, writer = socket.socketpair()
    box = ControlMailbox(bounds=replace(bounds, max_pending_events=2),
        check_stop=lambda: None, on_outbox=lambda: writer.send(b'\0'))
    def worker():
        try:
            for index in range(20):
                results.append(box.request(event(index), deadline_ms=2000))
        except DataflowError as exc:
            failures.append(exc.code)
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        for index in range(20):
            assert select.select([reader], [], [], 2)[0] == [reader]
            assert reader.recv(1) == b'\0'
            assert box.drain(max_events=2) == (event(index),)
            # A wakeup itself has not released this invocation.
            assert len(results) == index
            if index == 19:
                box.cancel_pending()
            else:
                box.deliver(ControlReply(operation_id=event(index).operation_id,
                    status='ok', payload={'index': index}, payload_ref=None, error_code=None))
    finally:
        box.close()
        thread.join(timeout=3)
        reader.close()
        writer.close()
    assert not thread.is_alive()
    assert [r.payload['index'] for r in results] == list(range(19))
    assert failures == ['UNKNOWN_EFFECT']
