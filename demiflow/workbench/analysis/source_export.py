"""Trusted Web-side fixed table to opaque Arrow spool in workspace_worker.

This function is not an HTTP entrypoint or a SQL interpreter. The Web parent
passes its own exclusive regular-file FD; callers supply only an existing fixed
locator/projection and exact source snapshot. Native reads inherit the existing
workspace worker's allocation/process/time admission, never the Web thread.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import resource
import stat
import struct

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.sources import locator
from demiflow.workbench.analysis.web_sources import _snapshot


def fixed_input(root, value):
    """Worker-only resolution; never recursively launch another worker."""
    from demiflow.lance import preview
    from demiflow.workbench.projects import discovery as project_discovery
    value = locator(value)
    root, workspace, _ = ws.load_workspace(root)
    if value["kind"] == "workspace_table":
        control, ref, uri = ws.table_binding(root, workspace, value["alias"])
        if ws.preview_binding(root, control, uri)["digest"] != value["binding"]:
            c.reject("Source binding changed before Arrow export", code="conflict")
        metadata = preview.describe_table(uri, ref.lance_version)
        version = ref.lance_version
    else:
        metadata = project_discovery.table(root, value["id"], expected=value["discovery"], version=value["version"])
        if metadata["binding"] != value["binding"]:
            c.reject("Recognized source binding changed before Arrow export", code="conflict")
        uri, version = ws.relative_path(root, metadata["path"]), metadata["version"]
    return Path(uri), version, _snapshot(metadata, value)


def export(root, *, source_locator, source_snapshot, output_fd):
    """Called only with the parent-owned FD explicitly inherited by the worker."""
    c.sha(source_snapshot, "fixed export source")
    c.integer(output_fd, "inherited exclusive export FD", 127, 3)
    before = os.fstat(output_fd)
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.geteuid()
            or before.st_size != 0 or os.lseek(output_fd, 0, os.SEEK_CUR) != 0):
        c.reject("Arrow export FD is not the parent's exclusive new regular file")
    _, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    maximum = min(64*1024*1024, hard) if hard != resource.RLIM_INFINITY else 64*1024*1024
    if maximum < 64*1024*1024:
        c.reject("Export worker cannot admit its fixed file reservation", code="resource_limit")
    resource.setrlimit(resource.RLIMIT_FSIZE, (maximum, maximum))
    uri, version, source = fixed_input(root, source_locator)
    if source["digest"] != source_snapshot or source["rows"] > 100000:
        c.reject("Source changed or exceeds explicit Notebook import row quota", code="resource_limit" if source["rows"] > 100000 else "conflict")
    from demiflow.lance import preview
    from demiflow.workbench.analysis.arrow_bridge import encode_batches
    from demiflow.workbench.analysis.arrow_bridge import schema_description
    from demiflow.workbench.analysis.arrow_bridge import schema_digest
    import pyarrow as pa
    snapshot = preview._open(uri, version)
    names = source_locator["columns"]
    schema = pa.schema([snapshot.schema.field(name) for name in names], metadata=snapshot.schema.metadata)
    if schema_description(schema) != source["schema"]:
        c.reject("Source projection type is outside the explicit Arrow bridge contract", code="operation_rejected")
    batches = snapshot.scanner(columns={name:preview._column(snapshot.schema,name) for name in names},
        batch_size=1024,batch_size_bytes=1024*1024,batch_readahead=1,fragment_readahead=1,
        io_buffer_size=1024*1024,scan_in_order=True).to_batches()
    observed_rows=0
    def checked_batches():
        nonlocal observed_rows
        for batch in batches:
            if (batch.get_total_buffer_size()>8*1024*1024 or observed_rows+batch.num_rows>source["rows"]):
                c.reject("Source batch/row receipt exceeds export admission",code="resource_limit")
            observed_rows+=batch.num_rows
            yield batch
    reader = pa.RecordBatchReader.from_batches(schema,checked_batches())
    digest = hashlib.sha256(); encoded_bytes = chunks = 0
    try:
        for chunk in encode_batches(reader):
            prefix = struct.pack("!I",len(chunk))
            if encoded_bytes+4+len(chunk)>64*1024*1024:
                c.reject("Export exceeds its admitted spool byte budget",code="resource_limit")
            for part in (prefix,chunk):
                remaining=memoryview(part)
                while remaining:
                    count=os.write(output_fd,remaining)
                    if count<=0:c.reject("Export write made no progress",code="operation_failed")
                    remaining=remaining[count:]
            digest.update(prefix);digest.update(chunk);encoded_bytes+=4+len(chunk);chunks+=1
        os.fsync(output_fd)
    finally:
        reader.close();batches.close()
    if observed_rows!=source["rows"]:
        c.reject("Export scan differs from its fixed row receipt",code="conflict")
    after=os.fstat(output_fd)
    if after.st_ino!=before.st_ino or after.st_dev!=before.st_dev or after.st_size!=encoded_bytes:
        c.reject("Exclusive Arrow export FD identity/size differs",code="conflict")
    # Re-resolve the same fixed source after reading. Alias/version changes do
    # not silently rebind this exported input to a newer table.
    _,_,current=fixed_input(root,source_locator)
    if current!=source:c.reject("Source authority changed during export",code="conflict")
    return dict(source_snapshot=source_snapshot,schema_sha256=schema_digest(schema),stream_sha256=digest.hexdigest(),
                encoded_bytes=encoded_bytes,chunks=chunks,rows=source["rows"])
