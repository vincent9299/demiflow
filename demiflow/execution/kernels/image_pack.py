"""Bounded physical packing of unchanged, quiescent pure kernel images.

This is an original ArtifactStore object, not a commit/state authority. Each
member retains its original bytes and digest; all readers authenticate the whole
carrier before using a selected member. Stateful/native/buffered kernels keep
their existing individual image protocol.
"""
from demiflow.execution.contracts.state_codec import PACK_MEDIA, _HEADER_BYTES, _MAGIC, _MAX_MEMBERS, _header, pack_limit
import hashlib

from demiflow.execution.contracts.dataflow_errors import DataflowError


# Pure images contain fixed platform counters and at most 33 bounded miss names.
# Account their encoding workspace and all bounded member headers before packing.
_IMAGE_WORKSPACE = 256 * 1024


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

    from demiflow.execution.run_journal import ExecutionBudgetExceeded
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
    from demiflow.execution.kernels.operator import OperatorKernel
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
