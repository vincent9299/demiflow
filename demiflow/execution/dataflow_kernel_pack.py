"""Bounded physical packing of unchanged, quiescent pure kernel images.

This is an original ArtifactStore object, not a commit/state authority. Each
member retains its original bytes and digest; all readers authenticate the whole
carrier before using a selected member. Stateful/native/buffered kernels keep
their existing individual image protocol.
"""
import hashlib
import json

from .contracts.dataflow_errors import DataflowError


PACK_MEDIA = 'application/x-demiflow-kernel-pack-v1'
_MAGIC = b'demiflow.kernel-pack.v1\n'
_MAX_MEMBERS = 8
_HEADER_BYTES = 512
# Pure images contain fixed platform counters and at most 33 bounded miss names.
# Account their encoding workspace and all bounded member headers before packing.
_IMAGE_WORKSPACE = 256 * 1024


def pack_limit(bounds):
    return min(bounds.max_state_bytes // 4, bounds.max_control_bytes // 4,
               bounds.max_resident_bytes // 64, bounds.max_batch_bytes // 4, 65536)


def _header(occurrence, partition, raw):
    if (type(occurrence) is not str or type(partition) is not str
            or not 1 <= len(occurrence) <= 64 or not 1 <= len(partition) <= 128):
        return None
    value = [occurrence, partition, len(raw), hashlib.sha256(raw).hexdigest()]
    encoded = json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'
    return (value, encoded) if len(encoded) <= _HEADER_BYTES else None


def _pack(kernels, *, store, bounds):
    """Return one ref or decline before side effects; no user snapshot is repeated."""
    limit = min(pack_limit(bounds), bounds.max_spill_bytes)
    if (len(kernels) < 2 or limit < _HEADER_BYTES * len(kernels)
            or bounds.max_control_bytes < _MAX_MEMBERS * _HEADER_BYTES * 4
            or bounds.max_state_bytes < 4 * _IMAGE_WORKSPACE
            or bounds.max_batch_bytes < _IMAGE_WORKSPACE
            or bounds.max_resident_bytes < 4 * _IMAGE_WORKSPACE):
        return None
    # Placement already reserves one input window per fused pure occurrence.
    # At this drained cut those windows contain no batch/ports. At least two
    # original windows cover either one metadata encoding plus bounded headers,
    # or the read/selected-member copies (at most 4 * 64 KiB). The stream is
    # closed before decoding the selected image. These peaks do not overlap;
    # no new grant, row/batch/state ceiling, or process-RSS promise is introduced.
    descriptors = []
    total = len(_MAGIC) + 2  # Member count is one decimal digit and a newline.
    for (occurrence, partition), kernel in kernels:
        raw = kernel._snapshot_bytes()
        header = _header(occurrence, partition, raw)
        size = len(raw)
        del raw  # Preflight retains only bounded descriptors, never prior images.
        if header is None or total + len(header[1]) + size + 1 > limit:
            return None
        descriptors.append(header)
        total += len(header[1]) + size + 1

    began_encoding = False

    def chunks():
        nonlocal began_encoding
        began_encoding = True
        yield _MAGIC
        yield str(len(kernels)).encode() + b'\n'
        for (_, kernel), (value, header) in zip(kernels, descriptors):
            raw = kernel._snapshot_bytes()
            if len(raw) != value[2] or hashlib.sha256(raw).hexdigest() != value[3]:
                raise DataflowError('INVALID_CONTRACT', 'pure kernel changed during checkpoint packing')
            yield header
            yield raw
            del raw  # Store must release its view/chunk before advancing us.
            yield b'\n'

    from .run_journal import ExecutionBudgetExceeded
    try:
        return store.put(chunks(), max_bytes=total, media_type=PACK_MEDIA)
    except ExecutionBudgetExceeded as error:
        # Only a definite initial non-admission may use the old path. An unknown
        # publication/settlement keeps its charge and propagates unchanged.
        if not began_encoding and error.budget == 'artifact_staging_bytes':
            return None
        raise


def snapshot_kernel_images(kernels, *, store, bounds):
    """Snapshot in original order; combine up to eight adjacent eligible images."""
    from .kernels.operator import OperatorKernel
    states, pending = {}, []

    def flush():
        if not pending:
            return
        ref = _pack(pending, store=store, bounds=bounds)
        for (occurrence, _), kernel in pending:
            own = ref if ref is not None else kernel.snapshot()
            if own is not None:
                states[occurrence] = own
        pending.clear()

    for key, kernel in kernels.items():
        if type(kernel) is OperatorKernel and kernel._pack_snapshot_ready():
            pending.append((key, kernel))
            if len(pending) == _MAX_MEMBERS:
                flush()
        else:
            flush()
            ref = kernel.snapshot()
            if ref is not None:
                states[key[0]] = ref
    flush()
    return states


def iter_kernel_images(store, ref, bounds):
    """Yield verified member bytes, then authenticate the original outer object.

    A caller must exhaust this iterator before acting on any returned member.
    GC only mutates after its entire root trace; read_kernel_image retains just
    the requested member and exhausts all other members before returning.
    """
    from .dataflow_handles import _ChunkStream
    limit = pack_limit(bounds)
    if ref.media_type != PACK_MEDIA or ref.size_bytes > limit:
        raise DataflowError('BUDGET_EXCEEDED', 'kernel pack exceeds original recovery workspace')
    stream = _ChunkStream(store.read(ref, max_bytes=max(1, limit)))
    seen = set()
    try:
        if stream.readline(len(_MAGIC)) != _MAGIC:
            raise ValueError('kernel pack version')
        count = stream.readline(2)
        if count not in tuple(str(n).encode() + b'\n' for n in range(2, _MAX_MEMBERS + 1)):
            raise ValueError('kernel pack member count')
        for _ in range(int(count)):
            value = json.loads(stream.readline(_HEADER_BYTES))
            if (type(value) is not list or len(value) != 4
                    or type(value[0]) is not str or not 1 <= len(value[0]) <= 64
                    or type(value[1]) is not str or not 1 <= len(value[1]) <= 128
                    or type(value[2]) is not int or not 1 <= value[2] <= limit
                    or type(value[3]) is not str or len(value[3]) != 64
                    or any(c not in '0123456789abcdef' for c in value[3])):
                raise ValueError('kernel pack member header')
            key = (value[0], value[1])
            if key in seen:
                raise ValueError('duplicate kernel pack member')
            seen.add(key)
            raw = stream.read(value[2])
            if (len(raw) != value[2] or hashlib.sha256(raw).hexdigest() != value[3]
                    or stream.read(1) != b'\n'):
                raise ValueError('kernel pack member digest/length')
            yield key, raw
            del raw
        if stream.read(1):
            raise ValueError('kernel pack trailing bytes')
    except (ValueError, TypeError, UnicodeError) as error:
        raise DataflowError('CORRUPT_ARTIFACT', str(error)) from error
    finally:
        stream.close()


def read_kernel_image(store, ref, bounds, *, occurrence, partition):
    """Single bounded compatibility reader shared by runtime and observations."""
    if ref.media_type != PACK_MEDIA:
        if ref.size_bytes > bounds.max_state_bytes:
            from .kernels.state_codec import size_error
            raise size_error('kernel image exceeds state byte budget', bounds.max_state_bytes,
                             actual=ref.size_bytes, occurrence=occurrence)
        return b''.join(store.read(ref, max_bytes=bounds.max_state_bytes))
    selected = None
    for key, raw in iter_kernel_images(store, ref, bounds):
        if key == (occurrence, partition):
            selected = raw
        del raw
    if selected is None:
        raise DataflowError('INCOMPATIBLE_HISTORY', 'kernel pack has no matching occurrence/partition')
    from .kernels import state_codec
    image = state_codec.loads(selected, max_bytes=bounds.max_state_bytes, occurrence=occurrence)
    if (type(image) is not dict or image.get('schema_version') != 'demiflow-kernel-image-v3'
            or image.get('occurrence_id') != occurrence or image.get('partition_id') != partition):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'kernel pack header differs from original image identity')
    return selected
