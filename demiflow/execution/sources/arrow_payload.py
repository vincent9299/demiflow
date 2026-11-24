"""Bounded shared Arrow batches behind the original, still per-row Record.

Only this versioned MIME changes ref payload interpretation. Old row bytes,
identity, source positions and commit/cursor authorities remain unchanged.
"""
from demiflow.execution.storage.streams import store_mapped_arrow
from demiflow.execution.storage.streams import put_arrow_batch
from demiflow.execution.contracts.record_codec import ArrowPayload, _MAX_ROWS, _resident_budget, _window, row_upper

from demiflow.execution.contracts.record_codec import ARROW_BATCH_MEDIA


import hashlib
import json


from demiflow.execution.contracts.dataflow_errors import DataflowError


def publish_batch(batch, *, reader, fragment_id, start):
    """Return one shared ref or None for the original row fallback.

    All preallocation checks precede IPC serialization. Arrow native scanner
    allocation is additionally constrained by its existing row/batch admission.
    """
    import pyarrow as pa
    from demiflow.execution.contracts.record_schema import encode_schema
    bounds, services = reader._services.bounds, reader._services
    if not 0 < batch.num_rows <= _MAX_ROWS or batch.get_total_buffer_size() > _window(bounds, getattr(services, 'assignment', None)):
        return None
    sizes = [row_upper(batch, i) for i in range(batch.num_rows)]
    # A conservative estimate must not reject an otherwise legal row.
    if any(n is None or n > bounds.max_row_bytes for n in sizes):
        return None
    schema = encode_schema(batch.schema, bounds=bounds)
    header = dict(v=1, source_id=reader._snapshot.source_id, split_id=reader._split.split_id,
        version=reader._version, fragment_id=fragment_id, start=start, count=batch.num_rows,
        schema=hashlib.sha256(schema).hexdigest(), sizes=sizes)
    raw_header = json.dumps(header, sort_keys=True, separators=(',', ':')).encode()
    if len(raw_header) > min(8192, bounds.max_control_bytes):
        return None
    # Arrow's size helper includes the record message metadata and padding;
    # serialized schema includes its encapsulation. Add frame/EOS/alignment
    # headroom, not an unrelated 64 KiB floor that disables ordinary batches.
    estimate = pa.ipc.get_record_batch_size(batch) + len(schema) + len(raw_header) + 256
    prefetched = min(bounds.max_batch_bytes,
                     reader._arrow_scan_rows * bounds.max_row_bytes)
    peak = prefetched + batch.get_total_buffer_size() + _window(bounds, getattr(services, 'assignment', None)) + 8*estimate
    if estimate > _window(bounds, getattr(services, 'assignment', None)) or peak > _resident_budget(bounds, getattr(services, 'assignment', None)):
        return None
    # Ref/provenance metadata can be wider than a tiny legal inline row. Check
    # both the original transfer and terminal/pending frame before publishing
    # any batch artifact. The dummy digest/key have the original store's fixed
    # 64-hex shape; maximum offset/sequence/bytes conservatively bound this batch.
    from demiflow.execution.contracts.dataflow import ArtifactRef, Record
    from demiflow.execution.contracts.record_codec import _encode_record_document
    from demiflow.execution.contracts.recovery_contract import document, RecoveryContractError
    position = dict(version=reader._version, fragment_id=fragment_id,
                    row_offset=start + batch.num_rows - 1)
    probe = Record('0'*64, position,
        ArtifactRef('0'*64, estimate, reader._schema_ref.store_id, '0'*64, ARROW_BATCH_MEDIA),
        None, max(sizes), reader._next_sequence + batch.num_rows - 1, reader._schema_ref)
    try:
        transfer = _encode_record_document(probe, max_bytes=bounds.max_row_bytes)
        document(dict(record_id=probe.record_id, sequence=probe.sequence,
                      document=transfer.decode()), min(bounds.max_row_bytes, bounds.max_control_bytes))
    except DataflowError as exc:
        if exc.code != 'BUDGET_EXCEEDED':
            raise
        return None
    except RecoveryContractError:
        return None
    ref = put_arrow_batch(batch, raw_header, estimate, ARROW_BATCH_MEDIA, services.store)
    return ref, sizes


def publish_source_rows(batch, *, reader, position_at, retained_bytes):
    """Share fixed-source columns under the original per-record identities and cursors."""
    from types import SimpleNamespace
    from demiflow.execution.contracts.recovery_contract import identity
    offset = 0
    while offset < batch.num_rows:
        reader._services.check_stop()
        count = min(_MAX_ROWS, batch.num_rows - offset)
        while True:
            part = batch.slice(offset, count)
            bindings = []
            for index in range(count):
                position = position_at(offset + index)
                key = identity([reader._snapshot.source_id, reader._split.split_id, position])
                bindings.append((key, SimpleNamespace(source_position=position, sequence=0)))
            records = store_mapped_arrow(part, bindings, reader._schema_ref, store=reader._services.store, bounds=reader._services.bounds, assignment=getattr(reader._services, "assignment", None),
                                        retained_bytes=batch.get_total_buffer_size() + retained_bytes)
            if records is not None:
                for record in records:
                    yield ArrowPayload(record.payload_ref, record.encoded_bytes)
                break
            if count == 1:
                if part.nbytes > reader._services.bounds.max_row_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'source row exceeds max_row_bytes')
                yield fallback_row(part, 0)
                break
            count //= 2
        offset += count


def compact_scan_batches(batches, *, services, target_rows):
    """Coalesce adjacent native pieces of one ordered fragment under its existing grant."""
    import pyarrow as pa
    target_rows = min(_MAX_ROWS, target_rows)
    maximum = _window(services.bounds, getattr(services, 'assignment', None))
    pieces = []
    rows = retained = 0
    def flush():
        if len(pieces) == 1:
            return pieces.pop()
        table = pa.Table.from_batches(pieces).combine_chunks()
        pieces.clear()
        return table.to_batches()[0]
    try:
        for batch in batches:
            services.check_stop()
            if not batch.num_rows:
                continue
            size = batch.get_total_buffer_size()
            if pieces and (rows + batch.num_rows > target_rows or retained + size > maximum):
                yield flush()
                rows = retained = 0
            pieces.append(batch)
            rows += batch.num_rows
            retained += size
            if rows >= target_rows or retained >= maximum:
                yield flush()
                rows = retained = 0
        if pieces:
            yield flush()
    finally:
        batches.close()


def fallback_row(batch, index):
    """The existing typed-row path, also the pure-copy negative-test seam."""
    return batch.slice(index, 1).to_pylist()[0]
