"""Trusted Arrow-to-Lance-v1 materializer inside the native analysis job."""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

import hashlib
from itertools import chain
import os
from pathlib import Path
import re
import stat
import struct

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import CHUNK_BYTES
from demiflow.workbench.analysis.arrow_bridge import MAX_CHUNKS
from demiflow.workbench.analysis.arrow_bridge import MAX_ENCODED_BYTES
from demiflow.workbench.analysis.arrow_bridge import ArrowBudget
from demiflow.workbench.analysis.arrow_bridge import decoded_batches
from demiflow.workbench.analysis.arrow_bridge import schema_description
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.workbench.analysis.query import NATIVE_OPTIONS
from demiflow.workbench.analysis.query import NATIVE_CAPACITY
from demiflow.workbench.analysis.result_transfer import send_result

_INPUT = re.compile(r"i[0-9a-f]{32}\Z")


def materialize_config(value):
    c.fields(value, {"operation", "input_id", "stream_sha256", "schema_sha256", "encoded_bytes", "chunks", "publish_id"}, label="materializer configuration")
    if value["operation"] != "materialize" or type(value["input_id"]) is not str or not _INPUT.fullmatch(value["input_id"]):
        c.reject("Invalid fixed Arrow input ID")
    c.sha(value["stream_sha256"], "Arrow stream digest"); c.sha(value["schema_sha256"], "Arrow schema digest")
    c.integer(value["encoded_bytes"], "Arrow input bytes", MAX_ENCODED_BYTES, 1)
    c.integer(value["chunks"], "Arrow input chunks", MAX_CHUNKS, 1)
    c.identity(value["publish_id"], "owned publish ID")
    return value


def input_chunks(path, config):
    """Verify the complete content commitment, then yield finite IPC chunks.

    Production only passes /input/data.bin, a helper-sealed read-only mount.
    Unit tests pass owned temporary files. No user-facing API accepts a path.
    """
    materialize_config(config)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != config["encoded_bytes"]:
            c.reject("Arrow ingress size differs from its content commitment", code="conflict")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
            digest.update(chunk)
        after = os.fstat(source.fileno())
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or digest.hexdigest() != config["stream_sha256"]):
            c.reject("Arrow ingress bytes differ from its committed stream", code="conflict")
        source.seek(0)
        for _ in range(config["chunks"]):
            prefix = source.read(4)
            if len(prefix) != 4:
                c.reject("Arrow ingress has a partial frame", code="conflict")
            size = struct.unpack("!I", prefix)[0]
            if not 1 <= size <= CHUNK_BYTES:
                c.reject("Arrow ingress frame exceeds byte limit", code="resource_limit")
            raw = source.read(size)
            if len(raw) != size:
                c.reject("Arrow ingress frame is incomplete", code="conflict")
            yield raw
        if source.read(1):
            c.reject("Arrow ingress has more frames than its commitment", code="conflict")


def run_materialize(config):
    materialize_config(config)
    # Only native_worker calls this, after require_job('native'). The input
    # mount is dedicated to this job and never writable by the Python kernel.
    import pyarrow as pa
    import lance
    from demiflow.execution.datafusion import DataFusionOptions
    from demiflow.execution.datafusion import DataFusionSession
    stream = decoded_batches(input_chunks(job_path("/input/data.bin"), config))
    first = next(stream, None)
    if first is None or schema_digest(first.schema) != config["schema_sha256"]:
        c.reject("Arrow materialization schema is missing or changed", code="conflict")
    schema = first.schema
    budget = ArrowBudget()

    def batches():
        for batch in chain((first,), stream):
            budget.add(batch, 0)  # exact ingress bytes counted by input_chunks
            yield batch

    target = Path(job_path("/scratch/materialized/data.lance"))
    target.parent.mkdir(mode=0o700)
    with DataFusionSession(resource_directory=job_path("/var/lib/demiflow-analysis/native-slots"),
            options=DataFusionOptions(**NATIVE_OPTIONS), max_concurrent=1,
            slot_capacity=NATIVE_CAPACITY, temp_directory=job_path("/scratch/native"),
            diagnostics_directory=job_path("/scratch/diagnostics"), managed_diagnostics=True) as session:
        # Reuse the same native capacity/admission lock for this trusted storage
        # operation. The writer runs here, not as a DataFusion UDF/callback.
        # Its final RSS guard is the 1GiB native cgroup; do not claim the query
        # subprocess's 512MiB sampled RSS guard measures this parent writer.
        with session._admit():
            snapshot = lance.write_dataset(pa.RecordBatchReader.from_batches(schema, batches()), str(target),
                schema=schema, mode="create", max_rows_per_file=100000, max_rows_per_group=1024,
                max_bytes_per_file=64*1024*1024)
            if snapshot.version != 1 or snapshot.count_rows() != budget.rows:
                c.reject("Lance materialization differs from its complete Arrow stream", code="operation_failed")
            description = dict(format="lance", version=1, rows=budget.rows, schema=schema_description(schema),
                schema_sha256=schema_digest(schema), logical_bytes=budget.logical_bytes, complete=True)
            from demiflow.workbench.analysis.case_index import build_index
            build_index(snapshot, target)
            return send_result(target, publish_id=config["publish_id"], description=description)
