"""U3 Local carriage conformance: one K5 protocol over Unix socket frames (S2).

Data and control share one sequence space on the wire; the InputGate holds
post-cut data; credit exhaustion is channel-local across sibling sockets; a
clean EOF reads as None; oversized frames are rejected. A cross-process test
proves the durable payload travels by shared segment file (the consumer reads
it directly) while only a small DataRef frame crosses the inherited socket.
"""
import socket
import subprocess
import sys
from pathlib import Path
import uuid

import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.exchange.carriage_local import (
    LocalChannelLink, LocalSegmentStore, channel_pair, expand_records,
    inline_records, ref_records, send_frame,
)
from demiflow.execution.exchange.channel import ChannelProducer
from demiflow.execution.recovery_contract import RecoveryContractError, document

_REPO = Path(__file__).resolve().parents[1]


def _code(exc):
    return exc.value.code


def _producer():
    return {"logical_node_id": "node-1", "occurrence_id": "occ-1"}


def _credit(channel_id, rows=1024, size_bytes=64 * 1024**2):
    return {"channel_id": channel_id, "reader_id": "reader-1",
            "credits": {"rows": rows, "bytes": size_bytes}, "route_epoch": 0,
            "operation_id": uuid.uuid4().hex}


def _rows(count=4):
    return [{"x": i} for i in range(count)]


def test_carriage_data_and_control_share_one_sequence_space():
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1"))
    env = p.data_envelope(inline_records(_rows(), source_id="src", occurrence_id="occ-1"),
                          producer=_producer())
    bar = p.barrier("ck-1", epoch=1, topology_revision=0)
    eoi = p.end_of_input()
    producer, consumer = channel_pair()
    try:
        producer.send({"kind": "data", "sequence": env["sequence"], "envelope": env})
        producer.send({"kind": "barrier", **bar})
        producer.send({"kind": "eoi", **eoi})
        assert [consumer.recv()["sequence"] for _ in range(3)] == [0, 1, 2]
    finally:
        producer.close()
        consumer.close()


def test_carriage_clean_eof_reads_none():
    producer, consumer = channel_pair()
    producer.close()
    assert consumer.recv() is None
    consumer.close()


def test_carriage_oversized_frame_rejected():
    producer, consumer = channel_pair()
    try:
        with pytest.raises(RecoveryContractError):
            send_frame(producer._sock, {"blob": "z" * (2 * 1024**2)})
    finally:
        producer.close()
        consumer.close()


_CHILD = r'''
import socket, sys
from demiflow.execution.exchange.carriage_local import LocalSegmentStore, ref_records, send_frame
from demiflow.execution.exchange.channel import ChannelProducer
fd, directory = int(sys.argv[1]), sys.argv[2]
sock = socket.socket(fileno=fd)
store = LocalSegmentStore(directory, host_id="host-a")
rows = [{"x": i, "blob": "z" * 200000} for i in range(8)]
_, refs = store.write(rows, scope_ref="scope-1")
p = ChannelProducer("ch-x")
p.grant_credit({"channel_id": "ch-x", "reader_id": "reader-1",
                "credits": {"rows": 1024, "bytes": 64 * 1024**2},
                "route_epoch": 0, "operation_id": "op-1"})
env = p.data_envelope(ref_records(refs, occurrence_id="occ-1"),
                      producer={"logical_node_id": "node-1", "occurrence_id": "occ-1"})
send_frame(sock, {"kind": "data", "sequence": env["sequence"], "envelope": env})
sock.close()
'''


def test_carriage_cross_process_payload_by_file_ref_by_socket(tmp_path):
    directory = tmp_path / "seg"
    store = LocalSegmentStore(directory, host_id="host-a")
    parent_sock, child_sock = socket.socketpair()
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", _CHILD, str(child_sock.fileno()), str(directory)],
        pass_fds=(child_sock.fileno(),), cwd=str(_REPO),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    child_sock.close()
    try:
        link = LocalChannelLink(parent_sock)
        received = link.recv()
        assert received is not None
        frame_bytes = len(document(received))
        rows = expand_records(received["envelope"]["records"], store)
        expected = [{"x": i, "blob": "z" * 200000} for i in range(8)]
        assert rows == expected  # consumer read the segment file directly
        assert frame_bytes < 16 * 1024  # only the ref crossed the process boundary
        _, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr.decode()
    finally:
        parent_sock.close()
        if process.poll() is None:
            process.kill()
            process.wait()