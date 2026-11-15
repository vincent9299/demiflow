"""Bounded hash join over one journal-owned random-access spool.

Rows are encoded once. Hash partitioning only relinks frame offsets; neither
partitioning nor the hash index carries business payloads. A bounded index is
built for each partition. Oversized distinct-key partitions split recursively;
an irreducible hot key uses a bounded scan (its cartesian output already costs
that work). No global sort, temporary run ledger, or native unbudgeted spill.
"""
import hashlib
import struct

from ..exchange.dataflow_bridge import _encode_record_document, _decode_record_document
from .dataflow import _check_stop, _group_key, _materialize_row, _err

# next offset, key digest prefix, key bytes, document bytes, sequence.
_HEADER = struct.Struct('!qQIIQ')
_NEXT = struct.Struct('!q')
_ID_BYTES = 64
_FANOUT = 16
_NONE = -1


def _resident_limit(services):
    stages = getattr(services.assignment, 'stages', ())
    grant = sum(max(stage.resident_bytes, stage.profile.requests['memory_bytes'])
                for stage in stages) if stages else services.bounds.max_resident_bytes
    return min(grant, services.bounds.max_resident_bytes)


class _Chain:
    __slots__ = ('head', 'tail', 'count')

    def __init__(self):
        self.head = self.tail = _NONE
        self.count = 0

    def link(self, spool, offset):
        spool.file.write_at(offset, _NEXT.pack(_NONE))
        if self.tail != _NONE:
            spool.file.write_at(self.tail, _NEXT.pack(offset))
        else:
            self.head = offset
        self.tail = offset
        self.count += 1


class _Spool:
    def __init__(self, file, services):
        self.file, self.services = file, services
        bounds = services.bounds
        self.document_limit = bounds.max_row_bytes + min(bounds.max_control_bytes, 65536)
        # Leave space for encoded/decoded inputs, two output rows, the codec,
        # hash dictionaries and the caller's transport batch at the same time.
        self.index_limit = max(1, _resident_limit(services) // 16)
        self.row_window = max(1, _resident_limit(services) // 4)

    def append(self, record, fields):
        _check_stop(self.services)
        row = _materialize_row(record, self.services)
        # Validate the whole declared key even when one component is null.
        token = _group_key(row, fields).encode('utf-8')
        key = b'' if any(row[name] is None for name in fields) else token
        if len(key) > self.row_window:
            raise _err('BUDGET_EXCEEDED', 'Join key exceeds its admitted resident window')
        document = _encode_record_document(record, max_bytes=self.document_limit)
        if len(document) + len(key) + record.encoded_bytes > self.row_window:
            raise _err('BUDGET_EXCEEDED', 'Join input and encoding exceed the admitted resident window')
        hashed = int.from_bytes(hashlib.sha256(key).digest()[:8], 'big')
        header = _HEADER.pack(_NONE, hashed, len(key), len(document), record.sequence)
        record_id = record.record_id.encode('ascii')
        frame_size = len(header) + len(record_id) + len(key) + len(document)
        # Coalesce only within the existing simultaneous row/encoding window.
        # Large indivisible rows retain the original piecewise write path.
        if frame_size + len(document) + len(key) + record.encoded_bytes <= self.row_window:
            offset = self.file.append(b''.join((header, record_id, key, document)))
            return offset, hashed
        offset = self.file.append(header)
        self.file.append(record_id)
        self.file.append(key)
        self.file.append(document)
        return offset, hashed

    def header(self, offset):
        return _HEADER.unpack(self.file.read_at(offset, _HEADER.size))

    def entries(self, chain):
        offset, count = chain.head, 0
        while offset != _NONE:
            _check_stop(self.services)
            if count >= chain.count:
                raise _err('CORRUPT_ARTIFACT', 'Join spool chain contains a cycle')
            header = self.header(offset)
            yield offset, header
            offset, count = header[0], count + 1
        if count != chain.count:
            raise _err('CORRUPT_ARTIFACT', 'Join spool chain was truncated')

    def key(self, offset, header):
        return self.file.read_at(offset + _HEADER.size + _ID_BYTES, header[2])

    def record(self, offset, header):
        start = offset + _HEADER.size
        span = _ID_BYTES + header[2] + header[3]
        if span + header[3] <= self.row_window:
            frame = self.file.read_at(start, span)
            record_id = frame[:_ID_BYTES].decode('ascii')
            raw = frame[_ID_BYTES + header[2]:]
            del frame  # The coalesced buffer must not coexist with decoding.
        else:
            record_id = self.file.read_at(start, _ID_BYTES).decode('ascii')
            raw = self.file.read_at(start + _ID_BYTES + header[2], header[3])
        return _decode_record_document(raw, record_id=record_id, sequence=header[4],
            max_bytes=self.document_limit)

    def partition(self, chain, shift):
        children = [_Chain() for _ in range(_FANOUT)]
        # Save the next offset before relinking; payload bytes are never copied.
        for offset, header in self.entries(chain):
            children[(header[1] >> shift) & (_FANOUT - 1)].link(self, offset)
        return children

    def index(self, chain):
        table, used = {}, 0
        for offset, header in self.entries(chain):
            if not header[2]:
                continue  # SQL null never matches, including another null.
            # Reserve conservative dictionary/list/int storage before insertion.
            cost = 512 + header[2]
            if used + cost > self.index_limit:
                return None
            key = self.key(offset, header)
            table.setdefault(key, []).append((offset, header))
            used += cost
        return table


def _pairs(spool, left, right, *, how, shift):
    if not left.count:
        return
    if not right.count:
        if how in ('left', 'anti'):
            for offset, header in spool.entries(left):
                yield spool.record(offset, header), None
        return
    index = spool.index(right)
    if index is None and shift < 64:
        first_hash = None
        for _, header in spool.entries(right):
            if first_hash is None:
                first_hash = header[1]
            elif first_hash != header[1]:
                break
        else:
            shift = 64  # A single hot digest cannot benefit from another pass.
        if shift < 64:
            left_children, right_children = spool.partition(left, shift), spool.partition(right, shift)
            for lpart, rpart in zip(left_children, right_children):
                yield from _pairs(spool, lpart, rpart, how=how, shift=shift + 4)
            return
    for loffset, lheader in spool.entries(left):
        key = spool.key(loffset, lheader)
        if not key:
            matches = ()
        elif index is not None:
            matches = index.get(key, ())
        else:
            # Hash prefix collisions or a single hot key cannot be subdivided.
            # Only headers and the current key/row are resident during this scan.
            matches = ((offset, header) for offset, header in spool.entries(right)
                if header[1] == lheader[1] and spool.key(offset, header) == key)
        seen = False
        lrecord = None
        for roffset, rheader in matches:
            seen = True
            if how == 'anti':
                break
            if lrecord is None:
                lrecord = spool.record(loffset, lheader)
            if how == 'semi':
                yield lrecord, None
                break
            yield lrecord, spool.record(roffset, rheader)
        if not seen and how in ('left', 'anti'):
            yield spool.record(loffset, lheader), None


def join_pairs(left, right, *, left_keys, right_keys, how, services):
    """Yield matching real Records; spill/FD/RAM limits precede growth."""
    bounds = services.bounds
    if bounds.max_open_files < 2:
        raise _err('RESOURCE_INFEASIBLE', 'Join needs one scratch file and its owner lease')
    from ..dataflow_store import RunArtifactStore
    if isinstance(services.store,RunArtifactStore):
        # One scratch extent, a streaming input record and a possible external
        # payload read coexist. Preserve the original same-store file authority.
        services.store.require_file_slots(services.store.SCRATCH_FILES + 2*services.store.READ_FILES,
                                          maximum=bounds.max_open_files)
    with services.store.scratch(max_bytes=bounds.max_spill_bytes,
            max_io_bytes=max(1, _resident_limit(services) // 4)) as file:
        spool = _Spool(file, services)
        lparts, rparts = [_Chain() for _ in range(_FANOUT)], [_Chain() for _ in range(_FANOUT)]
        for records, keys, chains in ((left, left_keys, lparts), (right, right_keys, rparts)):
            for record in records:
                offset, hashed = spool.append(record, keys)
                chains[hashed & (_FANOUT - 1)].link(spool, offset)
        for lpart, rpart in zip(lparts, rparts):
            yield from _pairs(spool, lpart, rpart, how=how, shift=4)
