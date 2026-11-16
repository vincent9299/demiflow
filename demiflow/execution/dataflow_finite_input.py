"""Read original finite seals without allocating a second payload spool.

The first scan authenticates every member and record before any reference is
adopted. Borrowers use only their existing assignment and revalidate the same
immutable bytes on reads. No routing, storage or commit authority is created.
"""
import json

from .contracts.dataflow import ArtifactRef, Record
from .contracts.dataflow_errors import DataflowError
from .dataflow_handles import _ChunkStream
from .dataflow_segments import block_chunks, block_totals
from .exchange.dataflow_bridge import _validate_record, _decode_record_document
from .dataflow_record_frames import (decode_frame, decode_document, admit_document, document_limit,
    line_limit, frame_stream, read_chunk_bytes, read_document, decode_metadata,
    admit_workspace, metadata_bytes)

_FIELDS = {'schema', 'seal_ref', 'manifest_ref', 'edge_id', 'producer', 'target',
           'rows', 'bytes', 'total_rows', 'total_bytes'}
_SCHEMA = 'demiflow.borrowed-finite-input.v1'
_FIELD_ORDER = tuple(sorted(_FIELDS))


class _ValidatedFiniteEntry(dict):
    """Process-local evidence of a complete original finite member scan.

    The serialized mapping is unchanged. The capability is deliberately lost
    by snapshot/restore, and is bound to this store, assignment, bounds and the
    complete immutable entry (whose digest binds every schema/payload frame).
    """
    __slots__ = ('_proof',)

    def __init__(self, entry, services, spec):
        super().__init__(entry)
        self._proof = (services.store, services.assignment, services.bounds, spec,
                       tuple(entry[key] for key in _FIELD_ORDER))

    def permits(self, services, entry, spec):
        store, assignment, bounds, original_spec, original_entry = self._proof
        return (set(self) == _FIELDS and services.store is store
                and services.assignment is assignment and services.bounds == bounds
                and spec == original_spec and tuple(self[key] for key in _FIELD_ORDER) == original_entry
                and tuple(entry[key] for key in _FIELD_ORDER) == original_entry)


def _validated_entry(services, entry, proof=None):
    spec = validate_borrowed_entry(services, entry)
    proof = entry if proof is None else proof
    if type(proof) is not _ValidatedFiniteEntry or not proof.permits(services, entry, spec):
        raise DataflowError('FENCED', 'finite projection lacks this attempt full-scan proof')
    return spec


def _fail(message):
    raise DataflowError('CORRUPT_ARTIFACT', message)


def _json(raw):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid finite input JSON') from exc


def _scope(services, ref, spec, target, *, frame_call=None):
    if spec.mode != 'finite_sealed' or target not in spec.consumer_partitions:
        raise DataflowError('FENCED', 'finite input differs from assigned consumer')
    if frame_call is None:
        raw = b''.join(services.store.read(ref, max_bytes=services.bounds.max_control_bytes))
        seal = _json(raw)
    else:
        raw = frame_call(read_document, ref, services.store, services.bounds,
                         _maximum=services.bounds.max_control_bytes)
        try:
            seal = frame_call(decode_metadata, raw, services.bounds)
        except (ValueError, UnicodeError) as exc:
            raise DataflowError('CORRUPT_ARTIFACT', 'invalid finite input JSON') from exc
    del raw
    if (type(seal) is not dict or set(seal) != {
            'schema', 'scope_id', 'generation', 'membership', 'producers', 'status'}
            or seal['schema'] != 'demiflow.seal.v3' or seal['scope_id'] != spec.scope_id
            or type(seal['generation']) is not int or seal['generation'] != spec.route_epoch
            or seal['status'] != 'success' or type(seal['membership']) is not list
            or seal['membership'] != sorted(spec.producer_partitions)
            or type(seal['producers']) is not list
            or len(seal['producers']) != len(spec.producer_partitions)):
        raise DataflowError('FENCED', 'finite scope seal differs from physical membership')
    members = []
    for expected, member in zip(seal['membership'], seal['producers']):
        if (type(member) is not dict or set(member) != {'partition_id','generation','status',
                'last_sequence','manifest_ref'} or member['partition_id'] != expected
                or type(member['generation']) is not int or member['generation'] != spec.route_epoch
                or member['status'] != 'succeeded' or type(member['last_sequence']) is not int
                or member['last_sequence'] < -1):
            _fail('finite producer membership or completion differs from seal')
        members.append(member)
    return members


def _records(services, ref, spec, *, target, total_rows, total_bytes, project=None, frame_call=None):
    """Yield selected rows only; still validate every producer frame and route."""
    declared, size = block_totals(services.store, ref, services.bounds, frame_call=frame_call)
    if size > spec.storage_bytes or size > services.bounds.max_spill_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'finite input exceeds original storage bound')
    if size != total_bytes or declared is not None and declared != total_rows:
        _fail('finite producer index counts differ from seal')
    index_bytes = [0]
    broadcast = None
    stream = None
    def call(function, *args, _frame_retained=0, **kwargs):
        held = len(broadcast[2]) if broadcast is not None else 0
        retained = _frame_retained + index_bytes[0] + held
        if frame_call is not None:
            return frame_call(function, *args, _frame_retained=retained, **kwargs)
        return function(*args, services=services, retained=retained, **kwargs)
    def index_call(function, *args, _frame_retained=0, **kwargs):
        return call(function, *args, _frame_retained=(
            _frame_retained + (stream.buffered_bytes if stream is not None else 0)), **kwargs)
    stream = frame_stream(block_chunks(services.store, ref, services.bounds,
        chunk_bytes=read_chunk_bytes(services.bounds, services),
        frame_call=index_call if frame_call is not None else None,
        on_retained=(lambda size: index_bytes.__setitem__(0, size)) if frame_call is not None else None),
        services.bounds, services=services,
        check_workspace=(lambda size: call(admit_workspace, size, services.bounds))
                        if frame_call is not None else None)
    consumers = spec.consumer_partitions
    count = byte_count = 0
    try:
        while True:
            services.check_stop()
            line = stream.readline(line_limit(services.bounds))
            if not line:
                break
            byte_count += len(line)
            if spec.routing == 'broadcast' and count % len(consumers) == 0:
                broadcast = None
            retained = stream.buffered_bytes
            raw, item = call(decode_frame, line, services.bounds, finite=True, _frame_retained=retained)
            del line
            if type(item['target']) is not str or item['target'] not in consumers:
                _fail('finite record frame or target differs from channel')
            call(admit_document, raw, services.bounds, _frame_retained=retained)
            if project is None:
                record = _decode_record_document(raw, record_id=item['record_id'], sequence=item['sequence'],
                    max_bytes=document_limit(services.bounds), payload_max_bytes=services.bounds.max_row_bytes,
                    metadata_max_bytes=services.bounds.max_control_bytes)
            else:
                record = project(raw, record_id=item['record_id'], sequence=item['sequence'],
                    max_bytes=document_limit(services.bounds), payload_max_bytes=services.bounds.max_row_bytes)
            if isinstance(record, Record):
                _validate_record(record)
            if record.encoded_bytes > services.bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'finite payload exceeds max_row_bytes')
            if record.payload_ref is not None:
                # Authenticate an existing external row too, without retaining
                # another payload copy or taking over its original ownership.
                chunk_bytes = read_chunk_bytes(services.bounds, services)
                # Authenticate external bytes in small pieces; grant their
                # read buffers before advancing the original store iterator.
                from .dataflow_record_frames import raw_workspace
                if frame_call is not None:
                    call(admit_workspace, raw_workspace(raw) + 2 * chunk_bytes,
                         services.bounds, _frame_retained=retained)
                options = {'chunk_bytes': chunk_bytes} if frame_call is not None else {}
                for _ in services.store.read(record.payload_ref, max_bytes=services.bounds.max_row_bytes,
                                              **options):
                    services.check_stop()
            if spec.routing == 'forward':
                if len(consumers) != 1:
                    _fail('finite forward route has multiple readers')
                expected = consumers[0]
            elif spec.routing == 'rebalance':
                expected = consumers[count % len(consumers)]
            elif spec.routing == 'broadcast':
                index = count % len(consumers)
                current = (item['record_id'], item['sequence'], raw)
                if index == 0:
                    broadcast = current
                elif current != broadcast:
                    _fail('finite broadcast does not contain identical records for each reader')
                expected = consumers[index]
            else:
                # Reuse the versioned typed hash/target rule. This is only
                # validation of immutable evidence, not execution of a route.
                from .exchange.dataflow_channels import _route_targets
                routes = _route_targets(record, spec=spec, ordinal=count, window=spec.storage_bytes)
                if len(routes) != 1:
                    _fail('finite keyed route has multiple unexpected readers')
                expected = routes[0]
            if item['target'] != expected:
                _fail('finite record target does not match original routing')
            count += 1
            if count > total_rows or byte_count > total_bytes:
                _fail('finite producer exceeds sealed row or byte counts')
            # Routing has consumed this frame. A suspended downstream join
            # need not retain its wide encoded copy as well as the Record.
            # Broadcast's independent comparison tuple keeps its own raw ref.
            del raw
            if item['target'] == target:
                yield record
            del record, item
        if (count != total_rows or byte_count != total_bytes
                or spec.routing == 'broadcast' and count % len(consumers)):
            _fail('finite producer row/byte count differs from seal')
    finally:
        stream.close()


def validate_finite_members(services, seal_ref, spec, *, target, frame_call=None):
    """Return bounded descriptors after every declared member was fully read."""
    entries = []
    members = _scope(services, seal_ref, spec, target, frame_call=frame_call)
    held_metadata = metadata_bytes((members, entries)) if frame_call is not None else 0
    def call(function, *args, _frame_retained=0, **kwargs):
        return frame_call(function, *args, _frame_retained=(
            _frame_retained + held_metadata), **kwargs)
    scoped_call = call if frame_call is not None else None
    if len(members) > services.bounds.max_partitions:
        raise DataflowError('BUDGET_EXCEEDED', 'finite producer membership exceeds max_partitions')
    for member in members:
        ref = ArtifactRef(**member['manifest_ref'])
        total_rows = member['last_sequence'] + 1
        _, total_bytes = block_totals(services.store, ref, services.bounds, frame_call=scoped_call)
        rows = size = 0
        for record in _records(services, ref, spec, target=target,
                               total_rows=total_rows, total_bytes=total_bytes, frame_call=scoped_call):
            rows += 1
            size += record.encoded_bytes
            del record  # Do not retain the previous decoded row into next().
        entry = {'schema':_SCHEMA, 'seal_ref':seal_ref, 'manifest_ref':ref,
            'edge_id':spec.edge_id,'producer':member['partition_id'],'target':target,
            'rows':rows,'bytes':size,'total_rows':total_rows,'total_bytes':total_bytes}
        entries.append(entry)
        if frame_call is not None:
            held_metadata = metadata_bytes((members, entries))
    # The original kernel image budget also bounds the proposed metadata.
    from .kernels import state_codec
    if scoped_call is not None:
        # This closed metadata tree adds list/dict/value tags, one pair list
        # per field, and JSON text+bytes. Six times retained metadata plus 4KiB
        # per entry bounds those wrappers before state_codec creates them.
        scoped_call(admit_workspace, 6 * metadata_bytes(entries) + 4096 * len(entries), services.bounds)
    raw = state_codec.dumps(entries, max_bytes=services.bounds.max_state_bytes)
    # Proof/binding metadata is resident too. If it cannot coexist inside the
    # original state window, retain the original fully decoded Record path.
    if 3 * len(raw) <= services.bounds.max_state_bytes:
        return tuple(_ValidatedFiniteEntry(entry, services, spec) for entry in entries)
    return tuple(entries)


def validate_borrowed_entry(services, entry):
    if (type(entry) not in (dict, _ValidatedFiniteEntry) or set(entry) != _FIELDS or entry['schema'] != _SCHEMA
            or not isinstance(entry['seal_ref'], ArtifactRef)
            or not isinstance(entry['manifest_ref'], ArtifactRef)
            or any(type(entry[k]) is not str for k in ('edge_id','producer','target'))
            or any(type(entry[k]) is not int or entry[k] < 0
                   for k in ('rows','bytes','total_rows','total_bytes'))
            or entry['rows'] > entry['total_rows']):
        _fail('invalid borrowed finite input descriptor')
    spec = next((x for x in services.assignment.inputs if x.edge_id == entry['edge_id']), None)
    if (spec is None or spec.mode != 'finite_sealed'
            or entry['producer'] not in spec.producer_partitions
            or entry['target'] not in services.assignment.lease.partition_ids
            or entry['target'] not in spec.consumer_partitions):
        raise DataflowError('FENCED', 'borrowed finite input differs from assignment')
    if entry['total_bytes'] > min(spec.storage_bytes, services.bounds.max_spill_bytes):
        _fail('borrowed finite byte counts exceed original input bound')
    return spec


def borrowed_records(services, entry):
    yield from _borrowed_records(services, entry)


def borrowed_join_records(services, entry, proof, *, fields, row_window):
    """Project keys from original bytes after this attempt fully checked scope.

    All original digests, routes and row/byte totals are rechecked on this read.
    A join drains both streams before producing any output; a damaged late
    member can therefore never publish a partially authenticated result.
    """
    spec = _validated_entry(services, entry, proof)
    if len(set(fields)) != len(fields):
        yield from _borrowed_records(services, entry)
        return
    from .kernels.join_spool import _project_join_document
    selected = tuple(dict.fromkeys((*fields, *spec.keys)))
    def project(raw, **kwargs):
        return _project_join_document(raw, fields=selected, row_window=row_window, **kwargs)
    yield from _borrowed_records(services, entry, project=project)


def _borrowed_records(services, entry, *, project=None):
    spec = validate_borrowed_entry(services, entry)
    members = _scope(services, entry['seal_ref'], spec, entry['target'])
    member = next(m for m in members if m['partition_id'] == entry['producer'])
    if (ArtifactRef(**member['manifest_ref']) != entry['manifest_ref']
            or member['last_sequence'] + 1 != entry['total_rows']):
        _fail('borrowed input no longer matches its original producer seal')
    count = size = 0
    for record in _records(services, entry['manifest_ref'], spec, target=entry['target'],
                           total_rows=entry['total_rows'], total_bytes=entry['total_bytes'], project=project):
        count += 1
        size += record.encoded_bytes
        yield record
    if (count, size) != (entry['rows'], entry['bytes']):
        _fail('borrowed selected rows differ from validated input')
