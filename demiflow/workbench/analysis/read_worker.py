"""Trusted fixed-version native Lance-to-Arrow reader inside the native job."""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

from contextlib import closing
import socket

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import encode_batches
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.oci import object_id


def read_config(value):
    c.fields(value, {"operation", "object_id", "transfer_id"}, label="native Arrow read configuration")
    if value["operation"] != "read":
        c.reject("Unsupported native Arrow operation")
    object_id(value["object_id"]); c.identity(value["transfer_id"], "owned Arrow transfer")
    return value


def run_read(config):
    read_config(config)
    from demiflow.execution.datafusion import DataFusionOptions
    from demiflow.execution.datafusion import DataFusionSession
    from demiflow.workbench.analysis.query import NATIVE_OPTIONS
    from demiflow.workbench.analysis.query import NATIVE_CAPACITY
    import lance
    import pyarrow as pa
    path = job_path("/sources/"+config["object_id"]+"/data.lance")
    with DataFusionSession(resource_directory=job_path("/var/lib/demiflow-analysis/native-slots"),
            options=DataFusionOptions(**NATIVE_OPTIONS), max_concurrent=1, slot_capacity=NATIVE_CAPACITY,
            temp_directory=job_path("/scratch/native"), diagnostics_directory=job_path("/scratch/diagnostics"), managed_diagnostics=True) as session:
        # Reuse the same native slot for its trusted reader, not a Python UDF.
        with session._admit():
            snapshot = lance.dataset(path, version=1)
            if snapshot.count_rows() > 100000:
                c.reject("Native Arrow read exceeds row quota", code="resource_limit")
            scanner = snapshot.scanner(batch_size=1024, batch_size_bytes=1024*1024, batch_readahead=1,
                fragment_readahead=1, io_buffer_size=1024*1024, scan_in_order=True)
            batches = scanner.to_batches()
            reader = pa.RecordBatchReader.from_batches(snapshot.schema, batches)
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); connection.settimeout(40)
            try:
                with closing(connection):
                    connection.connect(job_path("/broker/arrow.sock"))
                    frames = Frames(connection)
                    frames.send("json", dict(operation="arrow-read", transfer_id=config["transfer_id"]))
                    if frames.json() != {"accepted": True}:
                        c.reject("Bounded Arrow stream was not admitted", code="operation_rejected")
                    for chunk in encode_batches(reader):
                        frames.send("arrow", chunk)
                    frames.send("end")
                    if frames.json() != {"read": True}:
                        c.reject("Bounded Arrow stream completion is unknown", code="operation_failed")
            finally:
                reader.close(); batches.close()
