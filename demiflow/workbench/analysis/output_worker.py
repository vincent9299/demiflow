"""Fixed output/media entry behind the common admitted worker-role guard.

Explicit trusted contexts map fixed registered object/broker paths; absent a
trusted context the original OCI guard is required. There is no host fallback
or launcher, and browser payloads cannot supply these internal paths.
"""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

from contextlib import closing
import socket
from pathlib import Path

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.output_reader import open_document
from demiflow.workbench.analysis.output_reader import project_document
from demiflow.workbench.analysis.output_reader import embedded_media
from demiflow.workbench.analysis.output_reader import raw_chunk
from demiflow.workbench.analysis.media import byte_range
from demiflow.forge.schema import ForgeError


def request(value):
    c.fields(value, {"version", "operation", "witness"},
             {"offset", "limit", "output_index", "mime", "sha256", "range"}, label="output job request")
    if value["version"] != 1 or type(value["version"]) is not int:
        c.reject("Unsupported output job version")
    kind = c.one_of(value["operation"], {"output-page", "output-original", "output-image", "output-media"}, "output job operation")
    expected = {"output-page": {"offset", "limit"}, "output-original": {"range"},
                "output-image": {"output_index", "mime", "sha256", "range"},
                "output-media": {"output_index", "mime", "sha256", "range"}}[kind] | {"version", "operation", "witness"}
    if set(value) != expected:
        c.reject("Output job fields do not match operation")
    c.encoded_size(value, maximum=56*1024)
    return value


def execute(value, *, state_root, probe_tool=None):
    """Pure worker operation; tests can supply ONLY their private owned root.

    Production calls run(), which fixes the root and channel. No access is
    inferred from this request: its witness came from registered_witness and
    the controller must recheck live authority before publishing this reply.
    """
    request(value)
    witness = value["witness"]
    raw = open_document(state_root, witness)
    if value["operation"] == "output-page":
        return dict(kind="json", value=project_document(raw, witness, offset=value["offset"], limit=value["limit"], probe_tool=probe_tool))
    if value["operation"] == "output-original":
        result = raw_chunk(raw, witness, range_header=value["range"])
        return dict(kind="blob", data=result["data"], metadata=dict(status=result["status"], mime="application/json",
            start=result["start"], end=result["end"], total=result["total"], attachment=True, sha256=witness["artifact"]["sha256"]))
    descriptor, data = embedded_media(raw, witness, output_index=value["output_index"], mime=value["mime"], expected_sha256=value["sha256"], probe_tool=probe_tool)
    start, end, status = byte_range(value["range"], len(data))
    return dict(kind="blob", data=data[start:end+1], metadata=dict(status=status, mime=descriptor["mime"],
        start=start, end=end, total=len(data), descriptor=descriptor, attachment=False))


def run():
    from demiflow.workbench.analysis.sandbox import _read
    from demiflow.workbench.analysis.job_context import require_worker as require_job
    job = require_job("media")
    value = request(job["config"])
    # Optional immutable image config; never accepted in a public job request.
    tool = None
    path = Path("/opt/demiflow/media-tools.json")
    if current() is not None and current().value.get("media_tool") is not None:
        from demiflow.workbench.analysis.media import ProbeTool
        tool=ProbeTool(**current().value["media_tool"])
    elif current() is None and path.exists():
        from demiflow.workbench.analysis.media import ProbeTool
        config = c.decode(_read(path, 4096), maximum=4096)
        c.fields(config, {"ffprobe"}, label="private media tool config")
        c.fields(config["ffprobe"], {"path", "sha256", "version"}, label="pinned ffprobe")
        tool = ProbeTool(**config["ffprobe"])
    try:
        result = execute(value, state_root=Path(job_path("/objects")).parent, probe_tool=tool)
    except (ForgeError, OSError) as error:
        failure = dict(code=getattr(error, "code", "operation_failed"),
            message="Fixed output validation or read failed; no data was returned")
        reason = getattr(error, "reason", None)
        if reason in {"media_unavailable", "range_not_satisfiable"}:
            failure["reason"] = reason
        result = dict(kind="error", error=failure)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(5)
    with closing(connection):
        connection.connect(job_path("/broker/output-read.sock"))
        frames = Frames(connection)
        if result["kind"] == "error":
            frames.send("json", dict(operation="output-read", artifact=value["witness"]["artifact"], error=result["error"]))
        elif result["kind"] == "json":
            frames.send("json", dict(operation="output-read", artifact=value["witness"]["artifact"], value=result["value"]))
        else:
            frames.send("json", dict(operation="output-read", artifact=value["witness"]["artifact"], metadata=result["metadata"]))
            frames.send("blob", result["data"])
        frames.end_read()


if __name__ == "__main__":
    run()
