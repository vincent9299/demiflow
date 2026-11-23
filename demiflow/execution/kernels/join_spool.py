"""Bounded hash join over one journal-owned random-access spool.

Rows are encoded once. Hash partitioning only relinks frame offsets; neither
partitioning nor the hash index carries business payloads. A bounded index is
built for each partition. Oversized distinct-key partitions split recursively;
an irreducible hot key uses a bounded scan (its cartesian output already costs
that work). No global sort, temporary run ledger, or native unbudgeted spill.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
import hashlib
import struct

from demiflow.operators.relational import JOIN_PARTITION_BITS, JoinKeyFilter, hash_join_pairs, right_index, probe_right_index

from demiflow.execution.contracts.record_codec import _decode_record_document as decode_record_document, _encode_record_document as encode_record_document
from demiflow.execution.kernels.dataflow import _check_stop, _err
from demiflow.execution.kernels.dataflow import _group_key as group_key
from demiflow.execution.kernels.dataflow import _materialize_row as materialize_row

# next offset, key digest prefix, key bytes, document bytes, sequence.
_HEADER = struct.Struct('!qQIIQ')
_NEXT = struct.Struct('!q')
_ID_BYTES = 64
_FANOUT = 1 << JOIN_PARTITION_BITS
_NONE = -1
_PROJECTED_RESIDENT_MULTIPLIER = 32


class _EncodedJoinInput:
    """Kernel-private original document plus a typed key projection.

    Never crosses a public Record/transport boundary or a recovery image.
    Only a fully validated borrowed finite member can produce this carrier.
    """
    __slots__ = ('record_id', 'sequence', 'encoded_bytes', 'document', 'inline',
                 'payload_ref', 'resident_bytes')

    def __init__(self, raw, record_id, sequence, encoded_bytes, projection):
        self.record_id, self.sequence = record_id, sequence
        self.encoded_bytes, self.document = encoded_bytes, raw
        self.inline, self.payload_ref = projection, None
        # Outer line/string, canonical document, selected decoded key and the
        # transient JSON tree all coexist during projection. Reserve that peak
        # as well as the later scratch copy under the existing row window.
        self.resident_bytes = _PROJECTED_RESIDENT_MULTIPLIER * len(raw)


def project_join_document(raw, *, record_id, sequence, max_bytes, fields, row_window,
                           payload_max_bytes=None):
    from demiflow.execution.contracts.record_codec import _decode_value as decode_value, _codec_loads as load_codec_json
    # Large/unsupported records keep the same original Record path. Do not
    # enlarge a grant to make the optimized representation fit.
    # Include JSON container overhead and typed-key expansion before parsing,
    # plus the original scratch frame/copy. Unfitting inputs use the old path.
    peak = (_PROJECTED_RESIDENT_MULTIPLIER + 4 + 8 * len(fields)) * len(raw)
    if len(raw) > max_bytes or peak > row_window:
        return decode_record_document(raw, record_id=record_id, sequence=sequence, max_bytes=max_bytes,
                                       payload_max_bytes=payload_max_bytes)
    doc = load_codec_json(raw)
    payload = doc.get('p') if type(doc) is dict else None
    if (type(payload) is not dict or payload.get('t') != 'map' or doc.get('r') is not None):
        return decode_record_document(raw, record_id=record_id, sequence=sequence, max_bytes=max_bytes,
                                       payload_max_bytes=payload_max_bytes)
    if type(doc.get('eb')) is not int or not 0 <= doc['eb'] <= (max_bytes if payload_max_bytes is None else payload_max_bytes):
        raise _err('CORRUPT_ARTIFACT', 'projected finite record size is invalid')
    # Decode only declared keys, using the original codec and original join
    # key function below. Opaque payload bytes stay in their canonical form.
    projection = {}
    for pair in payload['v']:
        if pair[0] in fields:
            projection[pair[0]] = decode_value(pair[1])
    return _EncodedJoinInput(raw, record_id, sequence, doc['eb'], projection)


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
    def __init__(self, file, services, *, bounded=False, retained_key=None, reuse_groups=False):
        self.file, self.services = file, services
        bounds = services.bounds
        self.document_limit = bounds.max_row_bytes + min(bounds.max_control_bytes, 65536)
        # Leave space for encoded/decoded inputs, two output rows, the codec,
        # hash dictionaries and the caller's transport batch at the same time.
        self.index_limit = max(1, _resident_limit(services) // 16)
        self.group_budget = self.index_limit // 4 if reuse_groups else 0
        self.group_key = None
        self.group_size = 0
        self.group_documents = {}
        self.bounded = bounded
        self.live_bytes = 0
        self.retained_key = retained_key
        self.row_window = max(1, _resident_limit(services) - self.index_limit - 16384
                              if bounded else _resident_limit(services) // 4)

    def negative_filter(self, fields, how):
        if how in ('inner', 'semi') and self.index_limit >= 4096:
            return JoinKeyFilter(self.index_limit - self.group_budget, single_key=len(fields) == 1)
        return None

    def encode_index_record(self, record):
        """Use the original record codec in the in-memory small-right index."""
        if type(record) is _EncodedJoinInput:
            raw = record.document
        elif self.bounded:
            from demiflow.execution.contracts.record_codec import encode_document
            raw = encode_document(record, self.services.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                                  retained=self.index_limit)
        else:
            raw = encode_record_document(record, max_bytes=self.document_limit)
        resident = record.resident_bytes if type(record) is _EncodedJoinInput else record.encoded_bytes
        if len(raw) + resident + _ID_BYTES + 8 > self.row_window:
            raise _err('BUDGET_EXCEEDED', 'Join input and encoding exceed the admitted resident window')
        return struct.pack('!Q', record.sequence) + record.record_id.encode('ascii') + raw

    def decode_index_record(self, payload):
        sequence, = struct.unpack('!Q', payload[:8])
        return self._decode((None, None, None, None, sequence),
                            (payload[8:8 + _ID_BYTES].decode('ascii'), payload[8 + _ID_BYTES:]))

    def partition_input(self, records, fields, *, build_filter=None, probe_filter=None):
        parts = [_Chain() for _ in range(_FANOUT)]
        count = 0
        for record in records:
            entry = self.append(record, fields, build_filter=build_filter, probe_filter=probe_filter)
            count += 1
            if entry is not None:
                offset, hashed = entry
                parts[hashed & (_FANOUT - 1)].link(self, offset)
        return parts, count

    def row_key(self, record, fields, *, build_filter=None, probe_filter=None):
        _check_stop(self.services)
        encoded = type(record) is _EncodedJoinInput
        if encoded:
            row = record.inline
        elif (build_filter is not None or probe_filter is not None) and record.inline is not None:
            from demiflow.execution.contracts.dataflow import thaw_record_value
            row = {field: thaw_record_value(record.inline[field]) for field in fields}
        elif (build_filter is not None or probe_filter is not None) and record.payload_ref is not None:
            from demiflow.execution.contracts.record_codec import is_arrow_record
            from demiflow.execution.kernels.arrow_payload import batch_cache
            if is_arrow_record(record):
                cache = batch_cache(self.services.store, self.services.bounds, services=self.services)
                batch, index = cache.locate(record)
                if (probe_filter is not None and len(fields) == 1
                        and probe_filter.arrow_rejects(batch.column(fields[0]), index, record.payload_ref)):
                    return None
                row = {field: batch.column(field)[index].as_py() for field in fields}
            else:
                row = materialize_row(record, self.services)
        else:
            row = materialize_row(record, self.services)
        # Validate every key component even when another component is null.
        token = group_key(row, fields).encode('utf-8')
        key = b'' if any(row[name] is None for name in fields) else token
        if len(key) > self.row_window:
            raise _err('BUDGET_EXCEEDED', 'Join key exceeds its admitted resident window')
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
        encoded = type(record) is _EncodedJoinInput
        if self.bounded and not encoded:
            from demiflow.execution.contracts.record_codec import encode_document
            document = encode_document(record, self.services.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                                       retained=self.index_limit + len(key))
        else:
            document = record.document if encoded else encode_record_document(record, max_bytes=self.document_limit)
        resident = record.resident_bytes if encoded else record.encoded_bytes
        if len(document) + len(key) + resident > self.row_window:
            raise _err('BUDGET_EXCEEDED', 'Join input and encoding exceed the admitted resident window')
        header = _HEADER.pack(_NONE, hashed, len(key), len(document), record.sequence)
        record_id = record.record_id.encode('ascii')
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
            from demiflow.execution.contracts.record_codec import _typed_shape as typed_shape
            self.live_bytes += typed_shape(record.inline, self.services.bounds.max_row_bytes)[1]
        return record

    def hold_stream_record(self, record, key):
        if type(record) is _EncodedJoinInput:
            from demiflow.execution.contracts.record_codec import decode_document
            record = decode_document(record.document, record_id=record.record_id, sequence=record.sequence,
                                     bounds=self.services.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                                     retained=self.index_limit + len(key))
        if self.bounded:
            from demiflow.execution.contracts.record_codec import _typed_shape
            self.live_bytes = len(key) + _typed_shape(record.inline, self.services.bounds.max_row_bytes)[1]
            if self.retained_key is not None:
                self.retained_key(len(key))
        return record

    def release_probe(self):
        self.live_bytes = 0

    def _document(self, offset, header):
        _check_stop(self.services)
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
        return record_id, raw

    def record(self, offset, header):
        return self._decode(header, self._document(offset, header))

    def matched_record(self, key, offset, header):
        if not self.group_budget:
            return self.record(offset, header)
        _check_stop(self.services)
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
        size = header[3] + _ID_BYTES + 512
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
        if self.bounded:
            from demiflow.execution.contracts.record_codec import decode_document
            return decode_document(raw, record_id=record_id, sequence=header[4],
                bounds=self.services.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                retained=self.index_limit + self.live_bytes)
        return decode_record_document(raw, record_id=record_id, sequence=header[4], max_bytes=self.document_limit)

    def partition(self, chain, shift):
        children = [_Chain() for _ in range(_FANOUT)]
        # Save the next offset before relinking; payload bytes are never copied.
        for offset, header in self.entries(chain):
            children[(header[1] >> shift) & (_FANOUT - 1)].link(self, offset)
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


def join_pairs(left, right, *, left_keys, right_keys, how, services, bounded=False, retained_key=None, try_small_right=False):
    """Yield matching real Records; spill/FD/RAM limits precede growth."""
    bounds = services.bounds
    if bounds.max_open_files < 2:
        raise _err('RESOURCE_INFEASIBLE', 'Join needs one scratch file and its owner lease')
    from demiflow.execution.storage.objects import RunArtifactStore
    if isinstance(services.store,RunArtifactStore):
        # One scratch extent, a streaming input record and a possible external
        # payload read coexist. Preserve the original same-store file authority.
        services.store.require_file_slots(services.store.SCRATCH_FILES + 2*services.store.READ_FILES,
                                          maximum=bounds.max_open_files)
    with services.store.scratch(max_bytes=bounds.max_spill_bytes,
            max_io_bytes=max(1, _resident_limit(services) // 4)) as file:
        spool = _Spool(file, services, bounded=bounded, retained_key=retained_key, reuse_groups=try_small_right)
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
