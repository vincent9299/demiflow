"""Read original finite seals without allocating a second payload spool.

The first scan authenticates every member and record before any reference is
adopted. Borrowers use only their existing assignment and revalidate the same
immutable bytes on reads. No routing, storage or commit authority is created.
"""
import json

from .contracts.dataflow import ArtifactRef
from .contracts.dataflow_errors import DataflowError
from .dataflow_handles import _ChunkStream
from .dataflow_segments import block_chunks, block_totals
from .exchange.dataflow_bridge import _decode_record_document

_FIELDS = {'schema', 'seal_ref', 'manifest_ref', 'edge_id', 'producer', 'target',
           'rows', 'bytes', 'total_rows', 'total_bytes'}
_SCHEMA = 'demiflow.borrowed-finite-input.v1'


def _fail(message):
    raise DataflowError('CORRUPT_ARTIFACT', message)


def _json(raw):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid finite input JSON') from exc


def _scope(services, ref, spec, target):
    if spec.mode != 'finite_sealed' or target not in spec.consumer_partitions:
        raise DataflowError('FENCED', 'finite input differs from assigned consumer')
    raw = b''.join(services.store.read(ref, max_bytes=services.bounds.max_control_bytes))
    seal = _json(raw)
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


def _records(services, ref, spec, *, target, total_rows, total_bytes):
    """Yield selected rows only; still validate every producer frame and route."""
    declared, size = block_totals(services.store, ref, services.bounds)
    if size > spec.storage_bytes or size > services.bounds.max_spill_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'finite input exceeds original storage bound')
    if size != total_bytes or declared is not None and declared != total_rows:
        _fail('finite producer index counts differ from seal')
    stream = _ChunkStream(block_chunks(services.store, ref, services.bounds))
    consumers = spec.consumer_partitions
    count = byte_count = 0
    broadcast = None
    try:
        while True:
            services.check_stop()
            line = stream.readline(services.bounds.max_row_bytes + 1)
            if not line:
                break
            byte_count += len(line)
            if not line.endswith(b'\n'):
                _fail('finite record frame is truncated')
            item = _json(line)
            if (type(item) is not dict or set(item) != {'record_id','sequence','target','document'}
                    or type(item['document']) is not str or type(item['target']) is not str
                    or item['target'] not in consumers):
                _fail('finite record frame or target differs from channel')
            record = _decode_record_document(item['document'].encode(), record_id=item['record_id'],
                sequence=item['sequence'], max_bytes=services.bounds.max_row_bytes)
            if record.encoded_bytes > services.bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'finite payload exceeds max_row_bytes')
            if record.payload_ref is not None:
                # Authenticate an existing external row too, without retaining
                # another payload copy or taking over its original ownership.
                for _ in services.store.read(record.payload_ref, max_bytes=services.bounds.max_row_bytes):
                    services.check_stop()
            if spec.routing == 'forward':
                if len(consumers) != 1:
                    _fail('finite forward route has multiple readers')
                expected = consumers[0]
            elif spec.routing == 'rebalance':
                expected = consumers[count % len(consumers)]
            elif spec.routing == 'broadcast':
                index = count % len(consumers)
                current = (item['record_id'], item['sequence'], item['document'])
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
            if item['target'] == target:
                yield record
        if (count != total_rows or byte_count != total_bytes
                or spec.routing == 'broadcast' and count % len(consumers)):
            _fail('finite producer row/byte count differs from seal')
    finally:
        stream.close()


def validate_finite_members(services, seal_ref, spec, *, target):
    """Return bounded descriptors after every declared member was fully read."""
    entries = []
    members = _scope(services, seal_ref, spec, target)
    if len(members) > services.bounds.max_partitions:
        raise DataflowError('BUDGET_EXCEEDED', 'finite producer membership exceeds max_partitions')
    for member in members:
        ref = ArtifactRef(**member['manifest_ref'])
        total_rows = member['last_sequence'] + 1
        _, total_bytes = block_totals(services.store, ref, services.bounds)
        rows = size = 0
        for record in _records(services, ref, spec, target=target,
                               total_rows=total_rows, total_bytes=total_bytes):
            rows += 1
            size += record.encoded_bytes
        entry = {'schema':_SCHEMA, 'seal_ref':seal_ref, 'manifest_ref':ref,
            'edge_id':spec.edge_id,'producer':member['partition_id'],'target':target,
            'rows':rows,'bytes':size,'total_rows':total_rows,'total_bytes':total_bytes}
        entries.append(entry)
    # The original kernel image budget also bounds the proposed metadata.
    from .kernels import state_codec
    state_codec.dumps(entries, max_bytes=services.bounds.max_state_bytes)
    return tuple(entries)


def validate_borrowed_entry(services, entry):
    if (type(entry) is not dict or set(entry) != _FIELDS or entry['schema'] != _SCHEMA
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
    spec = validate_borrowed_entry(services, entry)
    members = _scope(services, entry['seal_ref'], spec, entry['target'])
    member = next(m for m in members if m['partition_id'] == entry['producer'])
    if (ArtifactRef(**member['manifest_ref']) != entry['manifest_ref']
            or member['last_sequence'] + 1 != entry['total_rows']):
        _fail('borrowed input no longer matches its original producer seal')
    count = size = 0
    for record in _records(services, entry['manifest_ref'], spec, target=entry['target'],
                           total_rows=entry['total_rows'], total_bytes=entry['total_bytes']):
        count += 1
        size += record.encoded_bytes
        yield record
    if (count, size) != (entry['rows'], entry['bytes']):
        _fail('borrowed selected rows differ from validated input')
