"""Bind authenticated partition records and admitted scratch to join computation."""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from demiflow.execution.contracts.record_codec import _decode_record_document as decode_record_document, _encode_record_document as encode_record_document
from demiflow.execution.kernels.dataflow import _err
from demiflow.execution.storage.streams import materialize_row as materialize_row

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


def bind_spool(file, services, *, spool_type=None, **options):
    """Bind one admitted scratch to original Record access and codec operations."""
    from demiflow.operators.join_spool import JoinSpool
    if spool_type is None:
        spool_type = JoinSpool

    def read_key_row(record, fields, *, build_filter=None, probe_filter=None):
        encoded = type(record) is _EncodedJoinInput
        if encoded:
            row = record.inline
        elif (build_filter is not None or probe_filter is not None) and record.inline is not None:
            from demiflow.execution.contracts.dataflow import thaw_record_value
            row = {field: thaw_record_value(record.inline[field]) for field in fields}
        elif (build_filter is not None or probe_filter is not None) and record.payload_ref is not None:
            from demiflow.execution.contracts.record_codec import is_arrow_record
            from demiflow.execution.storage.arrow_payload import batch_cache
            if is_arrow_record(record):
                cache = batch_cache(services.store, services.bounds, assignment=getattr(services, 'assignment', None))
                batch, index = cache.locate(record)
                if (probe_filter is not None and len(fields) == 1
                        and probe_filter.arrow_rejects(batch.column(fields[0]), index, record.payload_ref)):
                    return None
                row = {field: batch.column(field)[index].as_py() for field in fields}
            else:
                row = materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
        else:
            row = materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
        return (row,)

    def encode_record(record, *, bounded, retained, max_bytes):
        if type(record) is _EncodedJoinInput:
            raw = record.document
        elif bounded:
            from demiflow.execution.contracts.record_codec import encode_document
            raw = encode_document(record, services.bounds,
                resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
        else:
            raw = encode_record_document(record, max_bytes=max_bytes)
        resident = record.resident_bytes if type(record) is _EncodedJoinInput else record.encoded_bytes
        return raw, resident, record.record_id, record.sequence

    def decode_record(record_id, sequence, raw, *, bounded, retained, max_bytes):
        if bounded:
            from demiflow.execution.contracts.record_codec import decode_document
            return decode_document(raw, record_id=record_id, sequence=sequence, bounds=services.bounds,
                resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
        return decode_record_document(raw, record_id=record_id, sequence=sequence, max_bytes=max_bytes)

    def restore_record(record, *, retained):
        if type(record) is _EncodedJoinInput:
            from demiflow.execution.contracts.record_codec import decode_document
            record = decode_document(record.document, record_id=record.record_id, sequence=record.sequence,
                bounds=services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),
                retained=retained)
        return record

    def record_memory(record):
        from demiflow.execution.contracts.record_codec import _typed_shape
        return _typed_shape(record.inline, services.bounds.max_row_bytes)[1]

    return spool_type(file, max_row_bytes=services.bounds.max_row_bytes,
        max_control_bytes=services.bounds.max_control_bytes, resident_bytes=_resident_limit(services),
        check_stop=services.check_stop, read_key_row=read_key_row,
        encode_record=encode_record, decode_record=decode_record, restore_record=restore_record,
        record_memory=record_memory, **options)


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
        spool = bind_spool(file, services, bounded=bounded, retained_key=retained_key, reuse_groups=try_small_right)
        from demiflow.operators.join_spool import join_inputs
        yield from join_inputs(left, right, left_keys=left_keys, right_keys=right_keys,
            how=how, spool=spool, try_small_right=try_small_right)
