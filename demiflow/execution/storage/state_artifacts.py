"""Owned, bounded state artifacts referenced by the single RunJournal."""
from pathlib import Path
import hashlib
import json
import os
import uuid

from demiflow.execution.contracts.recovery_contract import document, RecoveryContractError, identifier


def atomic_bytes(path, raw):
    _atomic_write(path, (raw,), Path(path).name + ".staging")


def atomic_chunks(path, chunks):
    """Publish a verified stream only after consuming it in full."""
    _atomic_write(path, chunks, Path(path).name + "." + uuid.uuid4().hex + ".staging")


def _atomic_write(path, chunks, staging_name):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise RecoveryContractError("Artifact path contains a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(staging_name)
    try:
        with temporary.open("xb") as stream:
            stream.writelines(chunks)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)



class StateArtifacts:
    def __init__(self, context):
        self.context = context
        self.root = context.directory / "state"

    def put(self, state_id, value):
        identifier(state_id)
        raw = document(value, self.context.limits["max_state_bytes"])
        sha = hashlib.sha256(raw).hexdigest()
        name = sha + ".json"
        self.context.allocate_artifact(self.root / name, size=len(raw))
        with self.context.journal.publication_guard(self.context.fence):

            total = self.context.journal.storage_usage()
            target = self.root / name
            if not target.exists():
                atomic_bytes(target, raw)
            elif target.read_bytes() != raw:
                raise RecoveryContractError("Existing state artifact is corrupt")
        self.context.release_temporaries([target.with_name(target.name + ".staging")])
        return dict(state_id=state_id, path="state/" + name, bytes=len(raw), sha256=sha)

    def read(self, reference):
        if type(reference) is not dict or set(reference) != {"state_id", "path", "bytes", "sha256"}:
            raise RecoveryContractError("Invalid state reference")
        identifier(reference["state_id"])
        digest = reference["sha256"]
        if type(digest) is not str or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise RecoveryContractError("Invalid state checksum")
        if reference["path"] != "state/" + reference["sha256"] + ".json":
            raise RecoveryContractError("State reference escapes its owner")
        size = reference["bytes"]
        if type(size) is not int or not 0 <= size <= self.context.limits["max_state_bytes"]:
            raise RecoveryContractError("State reference exceeds byte budget")
        path = self.context.directory / reference["path"]
        if any(p.is_symlink() for p in (path, *path.parents)) or path.stat().st_size != size:
            raise RecoveryContractError("State artifact size differs")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
            raise RecoveryContractError("State artifact checksum differs")
        value = json.loads(raw)
        document(value, self.context.limits["max_state_bytes"])
        return value
