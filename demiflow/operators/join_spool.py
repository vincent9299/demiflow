"""Join spool implementation."""
from __future__ import annotations

import hashlib
import struct
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.operators.relational import JOIN_PARTITION_BITS, JoinKeyFilter, hash_join_pairs, right_index, probe_right_index, group_key

SPOOL_HEADER = struct.Struct('!qQIIQ')
_NEXT = struct.Struct('!q')
RECORD_ID_BYTES = 64
PARTITIONS = 1 << JOIN_PARTITION_BITS
NO_OFFSET = -1


class JoinChain:
    __slots__ = ('head', 'tail', 'count')

    def __init__(self):
        self.head = self.tail = NO_OFFSET
        self.count = 0

    def link(self, spool, offset):
        spool.file.write_at(offset, _NEXT.pack(NO_OFFSET))
        if self.tail != NO_OFFSET:
            spool.file.write_at(self.tail, _NEXT.pack(offset))
        else:
            self.head = offset
        self.tail = offset
        self.count += 1

class JoinSpool:
    def __init__(self, file, *, max_row_bytes, max_control_bytes, resident_bytes,
                 check_stop, read_key_row, encode_record, decode_record, restore_record,
                 record_memory, bounded=False, retained_key=None, reuse_groups=False):
        self.file = file
        self.check_stop, self.read_key_row = check_stop, read_key_row
        self.encode_record, self.decode_record = encode_record, decode_record
        self.restore_record, self.record_memory = restore_record, record_memory
        self.document_limit = max_row_bytes + min(max_control_bytes, 65536)
        self.index_limit = max(1, resident_bytes // 16)
        self.group_budget = self.index_limit // 4 if reuse_groups else 0
        self.group_key = None
        self.group_size = 0
        self.group_documents = {}
        self.bounded = bounded
        self.live_bytes = 0
        self.retained_key = retained_key
        self.row_window = max(1, resident_bytes - self.index_limit - 16384
                              if bounded else resident_bytes // 4)

    def negative_filter(self, fields, how):
        if how in ('inner', 'semi') and self.index_limit >= 4096:
            return JoinKeyFilter(self.index_limit - self.group_budget, single_key=len(fields) == 1)
        return None

    def encode_index_record(self, record):
        """Keep the original encoded small-right index and coexistence limit."""
        raw, resident, record_id, sequence = self.encode_record(record, bounded=self.bounded,
            retained=self.index_limit, max_bytes=self.document_limit)
        if len(raw) + resident + RECORD_ID_BYTES + 8 > self.row_window:
            raise DataflowError('BUDGET_EXCEEDED', 'Join input and encoding exceed the admitted resident window')
        return struct.pack('!Q', sequence) + record_id.encode('ascii') + raw

    def decode_index_record(self, payload):
        sequence, = struct.unpack('!Q', payload[:8])
        return self._decode((None, None, None, None, sequence),
                            (payload[8:8 + RECORD_ID_BYTES].decode('ascii'), payload[8 + RECORD_ID_BYTES:]))

    def partition_input(self, records, fields, *, build_filter=None, probe_filter=None):
        parts = [JoinChain() for _ in range(PARTITIONS)]
        count = 0
        for record in records:
            entry = self.append(record, fields, build_filter=build_filter, probe_filter=probe_filter)
            count += 1
            if entry is not None:
                offset, hashed = entry
                parts[hashed & (PARTITIONS - 1)].link(self, offset)
        return parts, count

    def row_key(self, record, fields, *, build_filter=None, probe_filter=None):
        self.check_stop()
        selected = self.read_key_row(record, fields, build_filter=build_filter, probe_filter=probe_filter)
        if selected is None:
            return None
        row, = selected
        del selected
        # Validate every key component even when another component is null.
        token = group_key(row, fields).encode('utf-8')
        key = b'' if any(row[name] is None for name in fields) else token
        if len(key) > self.row_window:
            raise DataflowError('BUDGET_EXCEEDED', 'Join key exceeds its admitted resident window')
        hashed = int.from_bytes(hashlib.sha256(key).digest()[:8], 'big')
        if build_filter is not None and key:
            build_filter.add(hashed, row[fields[0]], len(key))
        del row
        if probe_filter is not None and (not key or not probe_filter.may_match(hashed)):
            return None
        return key, hashed

    def append(self, record, fields, *, build_filter=None, probe_filter=None):
        entry = self.row_key(record, fields, build_filter=build_filter, probe_filter=probe_filter)
        if entry is None:
            return None
        key, hashed = entry
        document, resident, identifier, sequence = self.encode_record(record, bounded=self.bounded,
            retained=self.index_limit + len(key), max_bytes=self.document_limit)
        if len(document) + len(key) + resident > self.row_window:
            raise DataflowError('BUDGET_EXCEEDED', 'Join input and encoding exceed the admitted resident window')
        header = SPOOL_HEADER.pack(NO_OFFSET, hashed, len(key), len(document), sequence)
        record_id = identifier.encode('ascii')
        frame_size = len(header) + len(record_id) + len(key) + len(document)
        # Coalesce only within the existing simultaneous row/encoding window.
        # Large indivisible rows retain the original piecewise write path.
        if frame_size + len(document) + len(key) + resident <= self.row_window:
            offset = self.file.append(b''.join((header, record_id, key, document)))
            return offset, hashed
        offset = self.file.append(header)
        self.file.append(record_id)
        self.file.append(key)
        self.file.append(document)
        return offset, hashed

    def header(self, offset):
        return SPOOL_HEADER.unpack(self.file.read_at(offset, SPOOL_HEADER.size))

    def entries(self, chain):
        offset, count = chain.head, 0
        while offset != NO_OFFSET:
            self.check_stop()
            if count >= chain.count:
                raise DataflowError('CORRUPT_ARTIFACT', 'Join spool chain contains a cycle')
            header = self.header(offset)
            yield offset, header
            offset, count = header[0], count + 1
        if count != chain.count:
            raise DataflowError('CORRUPT_ARTIFACT', 'Join spool chain was truncated')

    def key(self, offset, header):
        return self.file.read_at(offset + SPOOL_HEADER.size + RECORD_ID_BYTES, header[2])

    def probe_key(self, offset, header):
        key = self.key(offset, header)
        if self.bounded:
            self.live_bytes = len(key)
            if self.retained_key is not None:
                self.retained_key(self.live_bytes)
        return key

    def hold_probe_record(self, offset, header):
        record = self.record(offset, header)
        if self.bounded:
            self.live_bytes += self.record_memory(record)
        return record

    def hold_stream_record(self, record, key):
        record = self.restore_record(record, retained=self.index_limit + len(key))
        if self.bounded:
            self.live_bytes = len(key) + self.record_memory(record)
            if self.retained_key is not None:
                self.retained_key(len(key))
        return record

    def release_probe(self):
        self.live_bytes = 0

    def _document(self, offset, header):
        self.check_stop()
        start = offset + SPOOL_HEADER.size
        span = RECORD_ID_BYTES + header[2] + header[3]
        if span + header[3] <= self.row_window:
            frame = self.file.read_at(start, span)
            record_id = frame[:RECORD_ID_BYTES].decode('ascii')
            raw = frame[RECORD_ID_BYTES + header[2]:]
            del frame  # The coalesced buffer must not coexist with decoding.
        else:
            record_id = self.file.read_at(start, RECORD_ID_BYTES).decode('ascii')
            raw = self.file.read_at(start + RECORD_ID_BYTES + header[2], header[3])
        return record_id, raw

    def record(self, offset, header):
        return self._decode(header, self._document(offset, header))

    def matched_record(self, key, offset, header):
        if not self.group_budget:
            return self.record(offset, header)
        self.check_stop()
        if self.group_key != key:
            self.group_documents.clear()
            self.group_key = None
            self.group_size = len(key) + 256
            if self.group_size > self.group_budget:
                return self.record(offset, header)
            self.group_key = key
        cached = self.group_documents.get(offset)
        if cached is not None:
            return self._decode(header, cached)
        size = header[3] + RECORD_ID_BYTES + 512
        if self.group_size + size > self.group_budget:
            # Once too large it stays disk-backed until the next key. No
            # unbounded cache for a skewed duplicate group or its output.
            self.group_documents.clear()
            self.group_size = self.group_budget
            return self.record(offset, header)
        data = self._document(offset, header)
        self.group_documents[offset] = data
        self.group_size += size
        return self._decode(header, data)

    def _decode(self, header, data):
        record_id, raw = data
        return self.decode_record(record_id, header[4], raw, bounded=self.bounded,
            retained=self.index_limit + self.live_bytes, max_bytes=self.document_limit)

    def partition(self, chain, shift):
        children = [JoinChain() for _ in range(PARTITIONS)]
        # Save the next offset before relinking; payload bytes are never copied.
        for offset, header in self.entries(chain):
            children[(header[1] >> shift) & (PARTITIONS - 1)].link(self, offset)
        return children

    def index(self, chain, limit=None):
        table, used = {}, 0
        limit = self.index_limit - self.group_budget if limit is None else limit
        for offset, header in self.entries(chain):
            if not header[2]:
                continue  # SQL null never matches, including another null.
            # Reserve conservative dictionary/list/int storage before insertion.
            cost = 512 + header[2]
            if used + cost > limit:
                return None
            key = self.key(offset, header)
            table.setdefault(key, []).append((offset, header))
            used += cost
        return table

def _probe_stream(left, fields, index, *, spool, how, membership):
    """The right index is complete before any left input or output is consumed."""
    def key_of(record):
        entry = spool.row_key(record, fields, probe_filter=membership)
        return entry[0] if entry is not None else b''
    pairs = probe_right_index(left, index, how, key_of=key_of,
                              load_left=spool.hold_stream_record, decode=spool.decode_index_record)
    try:
        for left_record, right_record in pairs:
            # Pair identity/row validation belongs to the partition binding;
            # matching and duplicate/null semantics belong to the operator.
            yield left_record, right_record
            spool.release_probe()
    finally:
        pairs.close()
        spool.release_probe()

def join_inputs(left, right, *, left_keys, right_keys, how, spool, try_small_right=False):
    membership = spool.negative_filter(right_keys, how)
    if try_small_right:
        available = spool.index_limit - spool.group_budget
        index, remaining = right_index(right, right_keys,
            available // 2 if membership else available,
            key_of=lambda record, fields: spool.row_key(record, fields, build_filter=membership)[0],
            encode=spool.encode_index_record, decode=spool.decode_index_record)
        if membership is not None:
            membership.finish()
        if index is not None:
            yield from _probe_stream(left, left_keys, index, spool=spool, how=how, membership=membership)
            return
        # The overflow prefix is replayed from its retained encoded rows,
        # never by reopening the source or invoking an upstream callback.
        # A partial membership bitmap must not reject the unseen suffix.
        membership = spool.negative_filter(right_keys, how)
        rparts, _ = spool.partition_input(remaining, right_keys, build_filter=membership)
        if membership is not None:
            membership.finish()
        lparts, _ = spool.partition_input(left, left_keys, probe_filter=membership)
        del membership
    elif membership is None:
        lparts, _ = spool.partition_input(left, left_keys)
        rparts, _ = spool.partition_input(right, right_keys)
    else:
        # Both finite ports have closed after all upstream callbacks. Build
        # on the right, preserving traversal order within each input chain.
        rparts, _ = spool.partition_input(right, right_keys, build_filter=membership)
        membership.finish()
        lparts, _ = spool.partition_input(left, left_keys, probe_filter=membership)
        del membership  # exact hash matching now owns the same memory grant
    for lpart, rpart in zip(lparts, rparts):
        yield from hash_join_pairs(spool, lpart, rpart, how=how)
