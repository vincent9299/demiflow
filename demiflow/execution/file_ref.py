"""Content-verified immutable JSON artifacts, for control-plane diagnostics."""

from demiflow.execution.contracts.artifact import validate_manifest
from dataclasses import asdict, dataclass
from pathlib import Path
from demiflow.execution.contracts.artifact import digest
from demiflow.execution.artifacts import immutable, read


@dataclass(frozen=True)
class JsonArtifactRef:
    artifact_path: str
    sha256: str

    def to_dict(self):
        return asdict(self)

    def read(self, root=None):
        path = Path(self.artifact_path)
        if not path.is_absolute():
            path = Path(root or '.') / path
        value = read(path)
        if digest(value) != self.sha256:
            raise ValueError('Immutable JSON artifact digest mismatch')
        from ..operator_llm.sqlite_offline import restore
        return restore(value)


def save_json_artifact(directory, value):
    from ..operator_llm.sqlite_offline import externalize
    directory = Path(directory)
    value = externalize(value, directory / 'inputs')
    sha = digest(value)
    path = directory / (sha + '.json')
    immutable(path, value)
    return JsonArtifactRef(str(path), sha)


def save_manifest(directory, value):
    """Persist via the existing immutable content-addressed JSON mechanism."""

    validate_manifest(value)
    ref = save_json_artifact(Path(directory), value)
    path = Path(ref.artifact_path)
    return {"manifest_id": value["manifest_id"], "path": str(path),
            "bytes": path.stat().st_size, "sha256": ref.sha256}
