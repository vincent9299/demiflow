"""Immutable Notebook output witnesses and bounded MIME projection.

The controller may obtain the small registered witness. Reading/parsing the
16MiB document and decoding embedded media MUST run in the owned media/output
job, never in HTTP or the 128MiB controller. This module does not start an
executor, grant permissions, register origins or write records. Integration
must pass its existing admitted helper job; there is no host-process fallback.
"""
from __future__ import annotations

from contextlib import contextmanager
import base64
import binascii
import hashlib
import os
from pathlib import Path
import stat
import tempfile

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.media import probe_image
from demiflow.workbench.analysis.media import probe_wav
from demiflow.workbench.analysis.media import probe_av_fd
from demiflow.workbench.analysis.media import unavailable
from demiflow.workbench.analysis.media import LIMITS
from demiflow.workbench.analysis.media import byte_range
from demiflow.workbench.analysis.object_files import candidate_manifest
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.outputs import OUTPUT_MIME
from demiflow.workbench.analysis.outputs import mime_representations
from demiflow.workbench.analysis.outputs import _bounded_value
from demiflow.workbench.analysis.records import _document
from demiflow.workbench.analysis.records import _source
from demiflow.workbench.analysis.sessions import _execution
from demiflow.workbench.analysis.sessions import _session

MAX_DOCUMENT = 16*1024*1024
MAX_PAGE = 56*1024


def registered_witness(records, access, execution_id, *, expected_output_id=None, expected_binding=None, dependency_resolver=None):
    """Small controller-side, read-only witness from SAME project's real rows.

    The witness is a trusted job input, not an accepted public output envelope.
    The controller must reauthorize before returning a worker's page/bytes.
    A historical generation is legitimate: it is not rebound to the live one.
    """
    access.check()
    with records.database.locked() as db:
        execution = _execution(db, access.project, execution_id)
        if execution["state"] != "finished" or not execution["output_id"]:
            c.reject("Notebook execution has no committed output", code="not_found")
        if expected_output_id is not None and execution["output_id"] != expected_output_id:
            c.reject("Execution output differs from requested identity", code="conflict")
        row = db.execute("SELECT * FROM analysis_objects WHERE project=? AND id=? AND state='ready'",
                         (access.project, execution["output_id"])).fetchone()
        if row is None or row["execution_id"] != execution_id:
            c.reject("Notebook output is not ready for this execution/project", code="not_found")
        manifest = c.decode(bytes(row["manifest"]))
        seed = c.decode(bytes(row["seed"]))
        canonical = candidate_manifest(seed, manifest.get("payload"), manifest.get("files"))
        if (canonical != manifest or c.digest(manifest) != row["manifest_digest"]
                or manifest["payload"].get("mime") != OUTPUT_MIME
                or manifest["execution_id"] != execution_id or manifest["project"] != access.project
                or manifest["object_id"] != row["id"] or manifest["generation"] != row["generation"]):
            c.reject("Registered output manifest differs", code="conflict")
        if expected_binding is not None and row["manifest_digest"] != expected_binding:
            c.reject("Output manifest binding expired or differs", code="conflict")
        session = _session(db, access.project, execution["session_id"])
        document = _document(db, access.project, "notebook", session["notebook_id"], execution["notebook_revision"])
        cell = next((item for item in document["document"]["cells"] if item["id"] == execution["cell_id"]), None)
        if cell is None or hashlib.sha256(cell["source"].encode()).hexdigest() != execution["source_sha256"]:
            c.reject("Output cell source witness differs", code="conflict")
        # Session source bindings survive restart and historical output reading.
        # A newer live kernel generation never relabels this execution.
        source_aliases = c.decode(bytes(session["sources"]))
        sources = {digest: _source(db, access.project, digest) for digest in set(source_aliases.values())}
        # Current-cell committed artifacts are real output facts. Prior-cell
        # handles/sources require the integrator's exact broker dependency
        # ledger; never expose every object/source in a project/generation.
        from demiflow.workbench.analysis.operations import reference
        from demiflow.workbench.analysis.case_index import NAME as CASE_INDEX
        handles = {}
        dependencies = dict(handles=[], sources=[])
        if dependency_resolver is not None:
            if not callable(dependency_resolver):
                c.reject("Output dependencies require a trusted ledger resolver")
            dependencies = dependency_resolver(db, access, execution)
            c.fields(dependencies, {"handles", "sources"}, label="server-owned execution dependencies")
            for digest in c.sequence(dependencies["sources"], "execution source dependencies", 16):
                sources[c.sha(digest)] = _source(db, access.project, digest)
        dependency_handles = {}
        for handle in c.sequence(dependencies["handles"], "execution handle dependencies", 16):
            actual = reference(db, access.project, handle)
            dependency_handles[actual["id"]] = actual
        cursor = db.execute("SELECT id,manifest FROM analysis_objects WHERE project=? AND state='ready' AND execution_id=?", (access.project, execution_id))
        def candidates():
            seen = set()
            for item in cursor:
                seen.add(item["id"])
                yield item
            for identifier in dependency_handles:
                if identifier not in seen:
                    yield db.execute("SELECT id,manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'", (access.project, identifier)).fetchone()
        for count, item in enumerate(candidates()):
            if count >= 1024:
                c.reject("Output artifact witness count exceeds reservation bound", code="resource_limit")
            description = c.decode(bytes(item["manifest"]))
            if description["payload"]["format"] != "lance":
                continue
            actual = reference(db, access.project, {"id": item["id"], "generation": description["generation"], "digest": c.digest(description)})
            handles[actual["id"]] = actual
            if actual["retention"] == "pinned" and CASE_INDEX in description["files"]:
                candidate = c.source_snapshot(dict(kind="pinned_object", binding=actual["digest"], schema=actual["schema"],
                    rows=actual["rows"], case_key=dict(kind="row_id", field=None), retention="pinned"))
                registered = db.execute("SELECT body FROM source_snapshots WHERE project=? AND digest=?", (access.project, candidate["digest"])).fetchone()
                if registered is not None and c.decode(bytes(registered[0])) == candidate:
                    sources[candidate["digest"]] = candidate
            c.encoded_size(dict(handles=handles, sources=sources), maximum=40*1024)
        provenance = dict(project=access.project, notebook_id=session["notebook_id"],
            document_revision=execution["notebook_revision"], document_sha256=document["digest"],
            cell_id=execution["cell_id"], source_sha256=execution["source_sha256"], language=execution["language"],
            execution_id=execution_id, session_id=execution["session_id"], kernel_generation=execution["generation"],
            output_generation=row["generation"], source_snapshots=source_aliases,
            derived_source_snapshots=sorted(set(sources)-set(source_aliases.values())), status=execution["status"])
        access.check()
        result = dict(artifact=dict(id=row["id"], generation=row["generation"], digest=row["manifest_digest"],
            sha256=manifest["payload"]["sha256"], bytes=manifest["payload"]["bytes"]),
            provenance=provenance, sources=sources, table_handles=handles)
        c.encoded_size(result, maximum=48*1024)
        return result


def open_document(state_root, witness):
    """Fixed receiver-owned path and digest; no caller filename is resolved."""
    artifact = witness["artifact"]
    object_id(artifact["id"]); c.sha(artifact["sha256"]); c.sha(artifact["digest"])
    c.integer(artifact["bytes"], "Notebook document bytes", MAX_DOCUMENT, 1)
    # openat all path components: a concurrent link swap cannot redirect an
    # already checked intermediate path. The root itself is deployment-owned.
    root = os.open(Path(state_root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptors = [root]
    try:
        current = root
        for name in ("objects", artifact["id"], "payload"):
            current = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            descriptors.append(current)
        fd = os.open("document.json", os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=current)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_size != artifact["bytes"] or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o400):
                c.reject("Committed Notebook file metadata differs", code="conflict")
            raw = bytearray()
            while len(raw) < info.st_size:
                chunk = os.read(fd, min(1024*1024, info.st_size-len(raw)))
                if not chunk:
                    c.reject("Notebook output file is truncated", code="conflict")
                raw.extend(chunk)
            after = os.fstat(fd)
            if ((info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    or hashlib.sha256(raw).hexdigest() != artifact["sha256"]):
                c.reject("Notebook output file digest changed", code="conflict")
            return bytes(raw)
        finally:
            os.close(fd)
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def decoded_document(raw, witness):
    """Validate a worker's output against controller-owned immutable facts."""
    if type(raw) is not bytes or len(raw) != witness["artifact"]["bytes"] or hashlib.sha256(raw).hexdigest() != witness["artifact"]["sha256"]:
        c.reject("Output bytes differ from the committed witness", code="conflict")
    value = c.decode(raw, maximum=MAX_DOCUMENT, nodes=32768, depth=16)
    required = {"generation", "parent_msg_id", "status", "complete", "outputs", "output_bytes", "messages", "ignored_messages",
                "stdin_supported", "active_widgets_supported", "execution_id", "cell_id", "source_sha256", "language", "session_id",
                "final_status", "raw_fd_output_collected"}
    c.fields(value, required, label="Notebook output artifact")
    provenance = witness["provenance"]
    for name in ("execution_id", "cell_id", "source_sha256", "language", "session_id"):
        if value[name] != provenance[name]:
            c.reject("Notebook output provenance differs: " + name, code="conflict")
    if value["generation"] != provenance["kernel_generation"] or value["final_status"] != provenance["status"]:
        c.reject("Notebook output generation/status differs", code="conflict")
    if value["stdin_supported"] is not False or value["active_widgets_supported"] is not False or value["raw_fd_output_collected"] is not False:
        c.reject("Output asserts unsupported raw/widget/stdin collection", code="conflict")
    if type(value["complete"]) is not bool:
        c.reject("Output completeness must be an actual boolean")
    c.identity(value["parent_msg_id"], "Jupyter parent message")
    c.sequence(value["outputs"], "Notebook outputs", 256)
    return value


def _media_bytes(mime, value):
    from demiflow.workbench.analysis.outputs import MIME_FORMATS
    kind = MIME_FORMATS.get(mime)
    if kind not in {"image", "audio", "video"}:
        c.reject("Embedded MIME is not allowed typed media")
    c.text(value, "embedded media base64", (LIMITS[kind]+2)//3*4)
    try:
        data = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        c.reject("Embedded media base64 is invalid")
    return data


def project_document(raw, witness, *, offset=0, limit=8, probe_tool=None):
    """At most 56KiB/page. Oversized originals remain separately addressable."""
    c.integer(offset, "output offset", 256); c.integer(limit, "output page limit", 16, 1)
    value = decoded_document(raw, witness)
    outputs = value["outputs"]
    result = dict(version="output_v1", artifact=witness["artifact"], provenance=witness["provenance"],
        coverage=dict(complete=value["complete"], status=value["final_status"], protocol_status=value["status"],
                      raw_fd_output_collected=False), total=len(outputs), offset=offset, items=[], next_offset=None)
    def source_resolver(digest):
        if digest not in witness["sources"]:
            c.reject("Output references a source outside its admitted execution", code="operation_rejected")
        return witness["sources"][digest]
    for index in range(offset, min(offset+limit, len(outputs))):
        output = outputs[index]
        if type(output) is not dict:
            c.reject("Notebook output must be an object")
        kind = c.one_of(output.get("output_type"), {"stream", "error", "display_data", "execute_result"}, "Notebook output type")
        item = dict(index=index, output_type=kind)
        if kind == "stream":
            c.fields(output, {"output_type", "name", "text"}, label="stream output")
            c.one_of(output["name"], {"stdout", "stderr"}, "stream name")
            item.update(name=output["name"], representations=[dict(mime="text/plain", format="text", available=True, **_bounded_value(output["text"]))])
        elif kind == "error":
            c.fields(output, {"output_type", "ename", "evalue", "traceback"}, label="error output")
            c.text(output["ename"], "error name", 256); c.text(output["evalue"], "error value", 4096, empty=True)
            lines = c.sequence(output["traceback"], "error traceback", 128)
            for line in lines:
                c.text(line, "traceback line", 4096, empty=True)
            item.update(ename=output["ename"], representations=[dict(mime="text/plain", format="text", available=True,
                **_bounded_value(output["ename"]+": "+output["evalue"]+"\n"+"\n".join(lines)))])
        else:
            c.fields(output, {"output_type", "data", "metadata"}, {"execution_count"}, label="MIME output")
            if "execution_count" in output:
                item["execution_count"] = c.integer(output["execution_count"], "Jupyter execution count", 2**31-1)
            def media_reference(mime, encoded):
                return _probe_embedded(_media_bytes(mime, encoded), mime=mime, artifact_id=witness["artifact"]["id"], output_index=index, probe_tool=probe_tool)
            item["representations"] = mime_representations(output["data"], media_reference=media_reference, source_resolver=source_resolver,
                table_resolver=lambda candidate: witness.get("table_handles", {}).get(candidate["id"]))
        for rep in item["representations"]:
            if rep["format"] == "case" and rep.get("available"):
                rep["definition_witness"] = case_definition_witness(witness, index, rep["value"])
        # One alternative cannot make the whole page unbounded. Keep the MIME
        # identity/reason and original index; don't mislabel omitted JSON null.
        if c.encoded_size(item, maximum=MAX_DOCUMENT, nodes=32768) > MAX_PAGE//2:
            item["representations"] = [{key: rep[key] for key in ("mime", "format", "reason") if key in rep} |
                dict(available=False, reason="representation_page_budget_use_original_artifact") for rep in item["representations"]]
        if c.encoded_size(result, maximum=MAX_PAGE) + c.encoded_size(item, maximum=MAX_PAGE) > MAX_PAGE-1024:
            break
        result["items"].append(item)
    following = offset + len(result["items"])
    result["next_offset"] = following if following < len(outputs) else None
    c.encoded_size(result, maximum=MAX_PAGE)
    return result


def embedded_media(raw, witness, *, output_index, mime, expected_sha256, probe_tool=None):
    """Decode ONLY a fixed MIME/index in an authorized output artifact."""
    c.integer(output_index, "embedded output index", 255); c.sha(expected_sha256)
    value = decoded_document(raw, witness)
    if output_index >= len(value["outputs"]):
        c.reject("Embedded media output does not exist", code="not_found")
    item = value["outputs"][output_index]
    if item.get("output_type") not in {"display_data", "execute_result"} or mime not in item.get("data", {}):
        c.reject("Embedded media representation does not exist", code="not_found")
    data = _media_bytes(mime, item["data"][mime])
    descriptor = _probe_embedded(data, mime=mime, artifact_id=witness["artifact"]["id"], output_index=output_index, probe_tool=probe_tool)
    if descriptor["sha256"] != expected_sha256:
        c.reject("Embedded media digest differs", code="conflict")
    return descriptor, data


def raw_chunk(raw, witness, *, range_header):
    # This is a chunk of the opaque ORIGINAL artifact, not a permissive file
    # or MIME proxy. Public integration sets attachment + application/json.
    if len(raw) != witness["artifact"]["bytes"] or hashlib.sha256(raw).hexdigest() != witness["artifact"]["sha256"]:
        c.reject("Original output bytes differ", code="conflict")
    start, end, status = byte_range(range_header, len(raw))
    return dict(data=raw[start:end+1], status=status, start=start, end=end, total=len(raw))


def case_definition_witness(witness, output_index, descriptor):
    """Immutable origin registration input; this function grants NO authority."""
    c.integer(output_index, "Case output index", 255)
    source = witness["sources"].get(descriptor.get("source_snapshot"))
    if source is None:
        c.reject("Case output source is not registered", code="operation_rejected")
    c.case_view(descriptor, source)
    descriptor_sha256 = c.digest(descriptor)
    binding = dict(artifact_id=witness["artifact"]["id"], manifest_sha256=witness["artifact"]["digest"],
        output_index=output_index, descriptor_sha256=descriptor_sha256, source_snapshot=source["digest"],
        execution_id=witness["provenance"]["execution_id"], kernel_generation=witness["provenance"]["kernel_generation"],
        question_sha256=[c.digest(question) for question in descriptor["questions"]])
    return dict(origin=dict(kind="notebook_output", reference="no"+c.digest(binding)), binding=binding, registered=False)


@contextmanager
def _embedded_fd():
    """One bounded media buffer, using only the admitted job's scratch map.

    Some supported Python builds omit memfd_create. The closed worker caller
    already bounds encoded bytes; the temporary file never selects a caller
    path or falls back to host TMPDIR. Both implementations close on errors.
    """
    create = getattr(os, "memfd_create", None)
    if callable(create):
        descriptor = create("demiflow-owned-media", flags=os.MFD_CLOEXEC)
        try:
            yield descriptor
        finally:
            os.close(descriptor)
    else:
        with tempfile.TemporaryFile(mode="w+b", dir=job_path("/scratch")) as stream:
            yield stream.fileno()


def _probe_embedded(data, *, mime, artifact_id, output_index, probe_tool):
    from demiflow.workbench.analysis.outputs import MIME_FORMATS
    kind = MIME_FORMATS[mime]
    if kind == "image":
        return probe_image(data, artifact_id=artifact_id, mime=mime, output_index=output_index)
    if mime == "audio/wav":
        result = probe_wav(data, artifact_id=artifact_id)
    else:
        if probe_tool is None:
            unavailable("Pinned ffprobe is unavailable for embedded compressed audio/video")
        with _embedded_fd() as fd:
            for offset in range(0, len(data), 1024*1024):
                part = memoryview(data)[offset:offset+1024*1024]
                while part:
                    written = os.write(fd, part)
                    if written <= 0:
                        c.reject("Private media scratch write failed", code="operation_failed")
                    part = part[written:]
            result = probe_av_fd(fd, artifact_id=artifact_id, mime=mime, kind=kind, tool=probe_tool)
    result["output_index"] = output_index
    return result
