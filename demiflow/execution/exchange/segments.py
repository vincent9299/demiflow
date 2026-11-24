"""Staging block descriptor <-> K4 DataRef conversion (U2/S4, D3 data plane).

The first-batch result-out carrier is the existing ``BlockSink`` staging block
descriptor (run-relative path + bytes + sha256). This module converts those
blocks to K4 DataRefs for the ordered channel, validates the same-machine
read_domain, and converts back for the driver commit terminal. It builds no new
physical transport; artifact registration/release stays with BlockSink under the
granted fence (worker) and the CommitCoordinator (driver).
"""

from dataclasses import asdict
from demiflow.execution.storage.segments import RecordBlocks
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.artifact_manifest import entries
from ..contracts.exchange import (
    build_data_ref, check_read_domain, validate_data_ref,
)
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identifier


def read_domain(run_directory, host_id):
    """U2 declares only a same-machine read domain (D3); cross-host is U6."""
    identifier(host_id)
    directory = str(run_directory)
    if not directory:
        raise RecoveryContractError("read_domain needs a run directory")
    return "file://%s:%s" % (host_id, directory)


def descriptor_refs(run, descriptor, *, domain, scope_ref):
    """Expand a staging descriptor (plain list or paged manifest) to DataRefs."""
    if type(descriptor) is not dict or set(descriptor) != {"blocks", "rows"}:
        raise RecoveryContractError("Invalid staging block descriptor")
    identifier(scope_ref)
    refs = []
    for block in entries(run, descriptor["blocks"]):
        refs.append(build_data_ref(
            kind="file", read_domain=domain, location=block["path"],
            payload_bytes=block["bytes"], sha256=block["sha256"],
            durability="durable", scope_ref=scope_ref))
    return refs


def validate_reader_domain(ref, reader_domain):
    """Same-named paths in different read domains are not the same object."""
    return check_read_domain(ref, reader_domain)


def refs_descriptor(refs, *, rows, reader_domain):
    """Validate DataRefs and rebuild a plain-list staging descriptor.

    The driver commit terminal re-reads these blocks via BlockSink over the same
    child-action root. Paged-manifest reconstruction across hosts is U3; the
    first batch is bounded below the page ceiling, so a plain list is faithful.
    """
    if type(refs) is not list or type(rows) is not int or rows < 0:
        raise RecoveryContractError("Invalid channel data refs")
    blocks = []
    for ref in refs:
        validate_data_ref(ref)
        check_read_domain(ref, reader_domain)
        if ref["kind"] != "file":
            raise RecoveryContractError("First-batch result carrier must be file blocks")
        blocks.append(dict(path=ref["location"], bytes=ref["bytes"], sha256=ref["sha256"]))
    return dict(blocks=blocks, rows=rows)


def finite_buffer(buffers, spec, *, store, bounds, assignment, check_stop, partition,
                  frame_call, coalesce_allowed):
    if spec.channel_id not in buffers:
        if (len(buffers) + 2) * 2 >= bounds.max_open_files:
            raise DataflowError('BUDGET_EXCEEDED', 'finite spool exceeds open file budget')
        buffers[spec.channel_id] = RecordBlocks(store, bounds, partition,
            assignment=assignment, check_stop=check_stop, partitioned=False,
            frame_call=frame_call, coalesce_allowed=coalesce_allowed)
    return buffers[spec.channel_id]


def seal_finite_blocks(stream):
    keys = stream.prepare_blocks()
    out = []
    with stream.hold_metadata(keys), stream.hold_metadata(out):
        for key in keys:
            tag, ref, rows, size = stream.seal_block(key)
            # Preserve the original bounded descriptor and its identity bytes.
            out.append({'partition_id': stream.partition_id, 'rows': rows,
                        'bytes': size, 'data_ref': asdict(ref)})
    part = out[0]
    return ArtifactRef(**part['data_ref']), part['rows']
