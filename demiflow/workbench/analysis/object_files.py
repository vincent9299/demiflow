"""Quota-bounded opaque byte receiver, separate from native decoders.

This module parses no Arrow, Jupyter, HTML or codecs. A trusted controller
supplies its reservation/seed; a native producer supplies bounded file bytes.
Preparation and rename are not DB-ready publication. The controller alone
commits ready after rechecking authorization, receipt and manifest identity.
The dedicated single-thread controller may host this receiver: large chunk
writes then require its explicit FileSizeGuard.object_write scope, with SQL
forbidden until the normal WAL file limit has been restored.
"""
from __future__ import annotations

import ctypes
import errno
import hashlib
import os
from pathlib import Path
import stat

from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import CHUNK_BYTES
from demiflow.workbench.analysis.arrow_bridge import MAX_ENCODED_BYTES
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.result_transfer import relative_file


def verify_object(state_root, identifier, expected_digest):
    """Read-only exact immutable manifest and content witness after rename."""
    object_id(identifier); c.sha(expected_digest)
    root = no_symlinks(Path(state_root)/"objects"/identifier)
    descriptor = os.open(root/"manifest.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        raw = source.read(65537)
    manifest = c.decode(raw)
    if c.digest(manifest) != expected_digest or manifest.get("object_id") != identifier:
        c.reject("Renamed object manifest identity differs", code="conflict")
    seed = {key: manifest[key] for key in ("project", "object_id", "generation", "retention", "execution_id", "parents", "source_binding")}
    if candidate_manifest(seed, manifest["payload"], manifest["files"]) != manifest:
        c.reject("Renamed object manifest is not canonical", code="conflict")
    payload_root = "data.lance" if manifest["payload"]["format"] == "lance" else "payload"
    allowed_directories = {".", payload_root}
    for name in manifest["files"]:
        allowed_directories.update(payload_root + "/" + str(parent) for parent in Path(name).parents if str(parent) != ".")
    observed, entries = set(), 0
    pending = [(root, 0)]
    while pending:
        directory, depth = pending.pop()
        info = directory.stat()
        if (depth > 10 or directory.relative_to(root).as_posix() not in allowed_directories
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o500):
            c.reject("Renamed object directory ownership/mode differs", code="conflict")
        with os.scandir(directory) as scan:
            for item in scan:
                entries += 1
                if entries > 128 or item.is_symlink():
                    c.reject("Renamed object contains extra entries/links", code="conflict")
                if item.is_dir(follow_symlinks=False):
                    pending.append((Path(item.path), depth+1)); continue
                relative = Path(item.path).relative_to(root).as_posix()
                if relative == "manifest.json":
                    expected = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                elif relative.startswith(payload_root + "/") and relative[len(payload_root)+1:] in manifest["files"]:
                    name = relative[len(payload_root)+1:]
                    expected = manifest["files"][name]; observed.add(name)
                else:
                    c.reject("Renamed object has an undeclared file", code="conflict")
                descriptor = os.open(item.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as source:
                    before = os.fstat(source.fileno())
                    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.geteuid()
                            or stat.S_IMODE(before.st_mode) != 0o400 or before.st_size != expected["bytes"]):
                        c.reject("Renamed object file metadata differs", code="conflict")
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
                        digest.update(chunk)
                    after = os.fstat(source.fileno())
                if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) or digest.hexdigest() != expected["sha256"]:
                    c.reject("Renamed object bytes changed or differ", code="conflict")
    if observed != set(manifest["files"]):
        c.reject("Renamed object is incomplete", code="conflict")
    return True


def _fsync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _noreplace(source_parent, source_name, target_parent, target_name):
    """Linux atomic no-replace rename; never replace an unknown existing tree."""
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "renameat2", None)
    if function is None:
        c.reject("Atomic no-replace publication is unsupported", code="operation_rejected")
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    old_fd = os.open(source_parent, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    new_fd = os.open(target_parent, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        if function(old_fd, source_name.encode("ascii"), new_fd, target_name.encode("ascii"), 1) != 0:
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                c.reject("Immutable object target already exists; recovery must inspect its original manifest", code="conflict")
            raise OSError(error, os.strerror(error))
    finally:
        os.close(old_fd); os.close(new_fd)


def candidate_manifest(seed, description, files):
    c.encoded(dict(seed=seed, description=description, files=files))
    c.fields(seed, {"project", "object_id", "generation", "retention", "execution_id", "parents", "source_binding"}, label="controller publication seed")
    project_identity(seed["project"]); object_id(seed["object_id"])
    c.integer(seed["generation"], "object generation", 2**31-1, 1)
    c.one_of(seed["retention"], {"ephemeral", "pinned"}, "private object retention")
    c.identity(seed["execution_id"], "owning analysis execution")
    if seed["source_binding"] is not None:
        c.sha(seed["source_binding"], "original source binding")
    for parent in c.sequence(seed["parents"], "object parents", 16):
        c.fields(parent, {"id", "generation", "digest"}, label="parent object reference")
        object_id(parent["id"]); c.integer(parent["generation"], "parent generation", 2**31-1, 1); c.sha(parent["digest"])
    if type(description) is not dict:
        c.reject("Invalid object payload description")
    kind = c.one_of(description.get("format"), {"lance", "document", "media"}, "object payload format")
    if kind == "lance":
        c.fields(description, {"format", "version", "rows", "schema", "schema_sha256", "logical_bytes", "complete"}, label="native output description")
        if type(description["version"]) is not int or description["version"] != 1 or description["complete"] is not True:
            c.reject("Only a complete native Lance v1 candidate can be published")
        c.integer(description["rows"], "result rows", 100000)
        c.integer(description["logical_bytes"], "result logical bytes", 32*1024*1024)
        c.sha(description["schema_sha256"], "Arrow schema digest")
        seen = set()
        for field in c.sequence(description["schema"], "result schema", 256):
            c.fields(field, {"name", "type", "nullable"}, label="result field")
            c.text(field["name"], "result field name", 256)
            c.one_of(field["type"], {"bool", "int64", "float64", "utf8", "binary"}, "result field type")
            if field["name"] in seen or type(field["nullable"]) is not bool:
                c.reject("Invalid native result schema")
            seen.add(field["name"])
        # Lance's v2 naming reverses the uint64 version in the basename.
        # Both spellings below are precisely dataset version1. A newer Lance
        # writer defaults to v2 paths; do not confuse that layout with a new
        # dataset version or silently fall back to a latest manifest.
        version_one={"_versions/1.manifest","_versions/18446744073709551614.manifest"}
        if type(files) is not dict or not 1 <= len(files) <= 64 or len(version_one.intersection(files))!=1:
            c.reject("Native candidate requires a bounded fixed-version file manifest")
    elif kind=="document":
        c.fields(description, {"format", "mime", "bytes", "sha256", "complete"}, label="document output")
        c.one_of(description["mime"], {"application/vnd.demiflow.output+json", "application/vnd.demiflow.profile+json", "application/json"}, "document MIME")
        c.integer(description["bytes"], "document bytes", 16*1024*1024, 1)
        c.sha(description["sha256"], "document bytes digest")
        if type(description["complete"]) is not bool or type(files) is not dict or set(files) != {"document.json"}:
            c.reject("Document output requires one declared JSON payload")
    else:
        from demiflow.workbench.analysis.registered_media import payload
        payload(description,artifact_id=seed["object_id"])
        if seed["retention"]!="pinned" or type(files) is not dict or set(files)!={"media.bin"}:
            c.reject("Media artifact requires a pinned reservation and one immutable media file")
    payload_root = "data.lance" if kind == "lance" else "payload"
    total = 0; directories = {payload_root}
    for name, item in files.items():
        relative_file(name)
        c.fields(item, {"bytes", "sha256"}, label="native result file")
        c.integer(item["bytes"], "result file bytes", MAX_ENCODED_BYTES)
        c.sha(item["sha256"], "result file digest")
        total += item["bytes"]
        directories.update(payload_root + "/" + str(parent) for parent in Path(name).parents if str(parent) != ".")
        if total > MAX_ENCODED_BYTES or len(files) + len(directories) + 2 > 128:
            c.reject("Native object bytes/inodes exceed reservation", code="resource_limit")
    # No collision where one declared file is another file's parent.
    if any(str(parent) in files for name in files for parent in Path(name).parents if str(parent) != "."):
        c.reject("Native candidate has conflicting file/directory paths")
    if kind == "document" and files["document.json"] != {"bytes": description["bytes"], "sha256": description["sha256"]}:
        c.reject("Document description differs from its content manifest", code="conflict")
    if kind=="media" and files["media.bin"]!={"bytes":description["bytes"],"sha256":description["sha256"]}:
        c.reject("Media description differs from its content manifest",code="conflict")
    result = {"format": "demiflow_analysis_object_v1", **seed, "payload": description,
              "files": dict(sorted(files.items())), "encoded_bytes": total,
              "filesystem_entries": len(files) + len(directories) + 2}
    c.encoded(result)
    return result


class Ingress:
    def __init__(self, state_root, *, publish_id, seed, description, files):
        c.identity(publish_id, "owned publish ID")
        self.manifest = candidate_manifest(seed, description, files)
        self.digest = c.digest(self.manifest)
        self.root = private_directory(state_root)
        self.staging_root = private_directory(self.root/".staging")
        self.objects_root = private_directory(self.root/"objects")
        self.stage = no_symlinks(self.staging_root/publish_id)
        self.stage.mkdir(mode=0o700, exist_ok=False)
        self.data = self.stage/("data.lance" if description["format"] == "lance" else "payload"); self.data.mkdir(mode=0o700)
        self.publish_id = publish_id
        self.finished = set()
        self.current = None
        self.sealed = False

    def start_file(self, name):
        relative_file(name)
        if self.current is not None or name in self.finished or name not in self.manifest["files"] or self.sealed:
            c.reject("Unexpected/repeated native result file", code="conflict")
        path = self.data/name
        no_symlinks(path)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # mkdir(parents=True) may use the default mode for intermediates.
        for parent in path.parents:
            if parent == self.stage:
                break
            parent.chmod(0o700)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.current = dict(name=name, descriptor=descriptor, bytes=0, hash=hashlib.sha256())

    def write(self, raw):
        if self.current is None or type(raw) is not bytes or not 1 <= len(raw) <= CHUNK_BYTES:
            c.reject("Invalid native file data frame")
        item = self.current
        expected = self.manifest["files"][item["name"]]
        if item["bytes"] + len(raw) > expected["bytes"]:
            c.reject("Native file grew beyond its declared reservation", code="resource_limit")
        view = memoryview(raw)
        while view:
            written = os.write(item["descriptor"], view)
            if written == 0:
                raise OSError("Native object write made no progress")
            view = view[written:]
        item["bytes"] += len(raw); item["hash"].update(raw)

    def finish_file(self):
        if self.current is None:
            c.reject("Native file stream has no active file")
        item = self.current
        try:
            expected = self.manifest["files"][item["name"]]
            if item["bytes"] != expected["bytes"] or item["hash"].hexdigest() != expected["sha256"]:
                c.reject("Native file content differs from its fixed manifest", code="conflict")
            os.fsync(item["descriptor"])
            self.finished.add(item["name"])
        finally:
            os.close(item["descriptor"]); self.current = None

    def seal(self):
        if self.current is not None or self.finished != set(self.manifest["files"]) or self.sealed:
            c.reject("Native object is incomplete or already sealed", code="conflict")
        raw = c.encoded(self.manifest)
        fd = os.open(self.stage/"manifest.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as destination:
            destination.write(raw); destination.flush(); os.fsync(destination.fileno())
        # At most128 entries, already admitted before writing. Files are sealed
        # before the no-replace rename; no job gets a writable ready mount.
        block_size = os.statvfs(self.root).f_frsize
        if not 512 <= block_size <= 65536:
            c.reject("Analysis volume allocation unit is outside the candidate profile", code="operation_rejected")
        charged_bytes = 65536
        for directory, folders, files in os.walk(self.stage, topdown=False, followlinks=False):
            for name in files:
                path = no_symlinks(Path(directory)/name)
                info = path.stat()
                charged_bytes += max(info.st_size, info.st_blocks*512) + block_size
                path.chmod(0o400)
            charged_bytes += Path(directory).stat().st_blocks*512 + block_size
            _fsync(Path(directory))
            Path(directory).chmod(0o500)
        _fsync(self.staging_root)
        if charged_bytes > 128*1024*1024:
            c.reject("Object physical charge exceeds its reservation", code="resource_limit")
        self.sealed = True
        return dict(publish_id=self.publish_id, manifest_digest=self.digest,
                    encoded_bytes=self.manifest["encoded_bytes"], filesystem_entries=self.manifest["filesystem_entries"],
                    charged_bytes=charged_bytes, prepared=True, database_ready=False)

    def rename(self, *, authorize):
        if not self.sealed or not callable(authorize) or authorize(self.publish_id, self.digest) is not True:
            c.reject("Publication requires the controller's current publishing authorization", code="conflict")
        target = self.manifest["object_id"]
        _noreplace(self.staging_root, self.publish_id, self.objects_root, target)
        _fsync(self.objects_root); _fsync(self.staging_root)
        return dict(object_id=target, manifest_digest=self.digest, renamed=True, database_ready=False)

    def close(self):
        if self.current is not None:
            os.close(self.current["descriptor"]); self.current = None
        # Partial staging and its reservation remain for owned recovery; this
        # parser cannot declare cleanup or discard an unknown successful write.
