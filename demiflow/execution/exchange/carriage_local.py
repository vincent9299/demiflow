"""Same-host Local data-plane carriage: Unix socket frames + segment files (U3/S2).

One K5 protocol over three exchange modes. The wire frame is the bootstrap
length-prefixed strict-JSON format (4-byte big-endian length + ``document()``),
so control and data share one bounded framing. Payload carriage differs by mode,
never the protocol: memory_incremental carries records inline; durable_incremental
and finite_sealed write business rows to segment files and put only a K4 DataRef
on the wire — the consumer reads the segment directly from the shared run
directory, so large payloads never transit the socket/driver (D3). Segment blocks
reuse the BlockSink block format (pickle protocol 4, run-relative path/bytes/sha256)
and the same path-escape/symlink/digest verification, without its commit coupling.
Cross-host read domains stay rejected (U6); Ray ray_object carriage is S5.
"""
import hashlib
import io
import json
from pathlib import Path
import pickle
import socket
import uuid

from ..block_sink import BLOCK_BYTES
from ..contracts.errors import ProtocolError
from ..contracts.exchange import (
    build_data_ref, check_read_domain, record_identity, validate_data_ref,
    DATA_REF_SCHEMA,
)
from ..recovery_contract import RecoveryContractError, document, identifier
from ..state_artifacts import atomic_bytes
from .segments import read_domain

FRAME_MAX_BYTES = 1 << 20


def send_frame(sock, value):
    raw = document(value)
    if len(raw) > FRAME_MAX_BYTES:
        raise RecoveryContractError("Channel frame exceeds byte budget")
    sock.sendall(len(raw).to_bytes(4, "big") + raw)


def _recv_exactly(sock, count):
    chunks = []
    remaining = count
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            if remaining == count:
                return None
            raise RecoveryContractError("Truncated channel frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock):
    header = _recv_exactly(sock, 4)
    if header is None:
        return None
    length = int.from_bytes(header, "big")
    if length > FRAME_MAX_BYTES:
        raise RecoveryContractError("Channel frame exceeds byte budget")
    raw = _recv_exactly(sock, length)
    if raw is None:
        raise RecoveryContractError("Truncated channel frame")
    return json.loads(raw)


class LocalChannelLink:
    """One physical channel endpoint over a connected same-host socket."""

    def __init__(self, sock):
        self._sock = sock

    def send(self, event):
        send_frame(self._sock, event)

    def recv(self):
        return recv_frame(self._sock)

    def close(self):
        self._sock.close()

    def shutdown(self):
        # close() alone does not interrupt a peer thread blocked in recv() on
        # Linux; shutdown forces the blocked read to return EOF/error so the
        # reader thread can join promptly instead of burning its timeout.
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    @property
    def fileno(self):
        return self._sock.fileno()


def channel_pair():
    producer, consumer = socket.socketpair()
    return LocalChannelLink(producer), LocalChannelLink(consumer)


class LocalSegmentStore:
    """Staging segment blocks under one run directory; direct same-host reads."""

    def __init__(self, directory, *, host_id, block_bytes=BLOCK_BYTES):
        self._directory = Path(directory).resolve()
        self._directory.mkdir(parents=True, exist_ok=True)
        self._domain = read_domain(self._directory, host_id)
        if type(block_bytes) is not int or block_bytes < 1:
            raise ValueError("block_bytes must be positive")
        self._block_bytes = block_bytes

    @property
    def domain(self):
        return self._domain

    def write(self, rows, *, scope_ref, durability="durable"):
        """Write business rows to bounded blocks; return (descriptor, DataRefs)."""
        identifier(scope_ref)
        if durability not in ("ephemeral", "durable"):
            raise ValueError("durability must be declared")
        generation = uuid.uuid4().hex
        blocks, buffer, buffer_bytes, row_count = [], [], 0, 0
        for row in rows:
            raw = pickle.dumps(row, protocol=4)
            if buffer and buffer_bytes + len(raw) > self._block_bytes:
                blocks.append(self._flush(generation, len(blocks), buffer))
                buffer, buffer_bytes = [], 0
            buffer.append(raw)
            buffer_bytes += len(raw)
            row_count += 1
        if buffer:
            blocks.append(self._flush(generation, len(blocks), buffer))
        descriptor = {"blocks": blocks, "rows": row_count}
        refs = [build_data_ref(kind="file", read_domain=self._domain, location=block["path"],
                               payload_bytes=block["bytes"], sha256=block["sha256"],
                               durability=durability, scope_ref=scope_ref)
                for block in blocks]
        return descriptor, refs

    def _flush(self, generation, index, buffer):
        payload = b"".join(buffer)
        relative = Path("segments") / generation / (str(index) + ".pkl")
        atomic_bytes(self._directory / relative, payload)
        return {"path": str(relative), "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest()}

    def _resolve(self, ref):
        validate_data_ref(ref)
        check_read_domain(ref, self._domain)
        if ref["kind"] != "file":
            raise RecoveryContractError("Local segment carriage reads file blocks only")
        relative = Path(ref["location"])
        path = self._directory / relative
        if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(self._directory):
            raise RecoveryContractError("Segment reference escapes its store")
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise RecoveryContractError("Segment reference contains a symlink")
        if not path.is_file() or path.stat().st_size != ref["bytes"]:
            raise RecoveryContractError("Segment block size differs")
        return path

    def read(self, refs):
        """Read business rows back from DataRefs (direct file read, no socket).

        The block format is the platform's BlockSink pickle protocol 4. Blocks are
        platform-internal staging under a private fenced run directory; the exact
        bytes deserialized are verified (size + sha256) before any unpickling.
        """
        rows = []
        for ref in refs:
            path = self._resolve(ref)
            raw = path.read_bytes()
            if len(raw) != ref["bytes"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
                raise ProtocolError("ARTIFACT_MISMATCH", "segment block digest differs")
            stream = io.BytesIO(raw)
            while stream.tell() < len(raw):
                rows.append(pickle.load(stream))
        return rows


    def discard(self, refs):
        """Reclaim consumed ephemeral blocks so per-row carriage stays bounded.

        Resolves each ref with the same path-escape/symlink/size checks as read,
        then removes the block file and its now-empty generation directory.
        """
        for ref in refs:
            path = self._resolve(ref)
            path.unlink(missing_ok=True)
            try:
                path.parent.rmdir()
            except OSError:
                pass


def inline_records(rows, *, source_id, occurrence_id, start_ordinal=0):
    """memory_incremental: business rows inline, one protocol record per row."""
    identifier(occurrence_id)
    records = []
    for offset, row in enumerate(rows):
        raw = document(row)
        records.append({"record_id": record_identity(source_id, start_ordinal + offset),
                        "parent_record_ids": [], "occurrence_id": occurrence_id,
                        "size_bytes": len(raw), "payload": row})
    return records


def ref_records(refs, *, occurrence_id):
    """durable/sealed: one protocol record per block DataRef; rows live in the block."""
    identifier(occurrence_id)
    records = []
    for ref in refs:
        validate_data_ref(ref)
        records.append({"record_id": ref["sha256"], "parent_record_ids": [],
                        "occurrence_id": occurrence_id, "size_bytes": ref["bytes"],
                        "payload": ref})
    return records


def expand_records(records, store):
    """Consumer-side: inline payloads pass through; DataRef payloads read directly."""
    rows = []
    for record in records:
        payload = record["payload"]
        if type(payload) is dict and payload.get("schema_version") == DATA_REF_SCHEMA:
            rows.extend(store.read([payload]))
        else:
            rows.append(payload)
    return rows