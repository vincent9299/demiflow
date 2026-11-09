"""K1 RunArtifactManifest: immutable run-input artifact identity (§6).

Sections aggregate existing authorities (PlatformRuntimeIdentity,
PipelineBundleRef, environment resolution) by reference and digest; their
internal fields and verification algorithms are not redefined here. Distinct
from the E1 sink-output ``artifact_manifest`` (paged-artifacts-v1), which stays
owned by the commit path.
"""
import re
from pathlib import Path

from ..recovery_contract import document, identifier, identity
from .envelope import protocol_version
from .errors import ProtocolError

MANIFEST_SCHEMA = "demiflow.run_artifact_manifest.v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_TOP_KEYS = frozenset({
    "schema_version", "manifest_id", "run_id", "platform", "business", "config",
    "environment", "protocol_versions", "secret_refs", "sealed_at",
})
_MUTABLE_SECTIONS = frozenset({
    "platform", "business", "config", "environment", "protocol_versions", "secret_refs",
})


def _sha(value, label):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ProtocolError("INVALID_SPEC", label + " must be a sha256 hex digest")
    return value


def _text(value, label):
    if type(value) is not str or not value:
        raise ProtocolError("INVALID_SPEC", label + " must be a nonempty string")
    if value.startswith("data:") and ";base64," in value:
        raise ProtocolError("INVALID_SPEC", label + " must not inline payload material")
    return value


def _closed(value, keys, label):
    if type(value) is not dict or set(value) != set(keys):
        raise ProtocolError("INVALID_SPEC", label + " key set is unsupported")
    return value


def _bundle_hex(bundle_digest):
    _text(bundle_digest, "bundle_digest")
    if not bundle_digest.startswith("sha256:") or _SHA256.fullmatch(bundle_digest[7:]) is None:
        raise ProtocolError("INVALID_SPEC", "bundle_digest must be sha256:<64 hex>")
    return bundle_digest[7:]


def _validate_platform(value):
    _closed(value, {"source_digest", "distribution", "version", "runtime_identity_ref"}, "platform")
    _sha(value["source_digest"], "platform.source_digest")
    _text(value["distribution"], "platform.distribution")
    _text(value["version"], "platform.version")
    _text(value["runtime_identity_ref"], "platform.runtime_identity_ref")


def _validate_business(value):
    _closed(value, {"bundle_digest", "entrypoints"}, "business")
    bundle_hex = _bundle_hex(value["bundle_digest"])
    entrypoints = value["entrypoints"]
    if type(entrypoints) is not list or not entrypoints:
        raise ProtocolError("INVALID_SPEC", "business.entrypoints must be a nonempty list")
    names = set()
    for entry in entrypoints:
        _closed(entry, {"name", "module_key", "definition_ref"}, "entrypoint")
        identifier(entry["name"])
        if entry["name"] in names:
            raise ProtocolError("INVALID_SPEC", "duplicate entrypoint name")
        names.add(entry["name"])
        if entry["module_key"] != "p_" + bundle_hex[:24]:
            raise ProtocolError("ARTIFACT_MISMATCH",
                                "entrypoint module_key is not bound to the bundle digest")
        ref = entry["definition_ref"]
        if type(ref) is not dict or "sha256" not in ref or set(ref) - {"sha256", "location"}:
            raise ProtocolError("ARTIFACT_MISMATCH",
                                "entrypoint definition requires a digest binding, not a mutable repo path")
        _sha(ref["sha256"], "definition_ref.sha256")
        if "location" in ref:
            _text(ref["location"], "definition_ref.location")


def _validate_config(value):
    _closed(value, {"resolved_config_digest", "model_config_refs"}, "config")
    _sha(value["resolved_config_digest"], "config.resolved_config_digest")
    refs = value["model_config_refs"]
    if type(refs) is not list:
        raise ProtocolError("INVALID_SPEC", "model_config_refs must be a list")
    for ref in refs:
        _closed(ref, {"name", "sha256", "location"}, "model_config_ref")
        identifier(ref["name"])
        _sha(ref["sha256"], "model_config_ref.sha256")
        _text(ref["location"], "model_config_ref.location")


def _validate_environment(value):
    if type(value) is not dict or not {"requirements_sha256", "lock_digest", "resolution_digest"} <= set(value) \
            or set(value) - {"requirements_sha256", "lock_digest", "resolution_digest", "image_digest"}:
        raise ProtocolError("INVALID_SPEC", "environment key set is unsupported")
    for name in ("requirements_sha256", "lock_digest", "resolution_digest"):
        _sha(value[name], "environment." + name)
    if "image_digest" in value:
        _text(value["image_digest"], "environment.image_digest")


def _validate_protocol_versions(value):
    _closed(value, {"worker", "exchange", "checkpoint_schema", "state_schema", "resource_protocol"},
            "protocol_versions")
    protocol_version(value["worker"])
    protocol_version(value["exchange"])
    for name in ("checkpoint_schema", "state_schema"):
        if type(value[name]) is not int or value[name] < 1:
            raise ProtocolError("INVALID_SPEC", name + " must be a positive int")
    _text(value["resource_protocol"], "resource_protocol")


def _validate_secrets(value):
    if type(value) is not list:
        raise ProtocolError("INVALID_SPEC", "secret_refs must be a list")
    names = set()
    for ref in value:
        # References only: any inline secret material is structurally rejected.
        _closed(ref, {"name", "external_ref"}, "secret_ref")
        identifier(ref["name"])
        if ref["name"] in names:
            raise ProtocolError("INVALID_SPEC", "duplicate secret name")
        names.add(ref["name"])
        _text(ref["external_ref"], "secret_ref.external_ref")


_VALIDATORS = {
    "platform": _validate_platform, "business": _validate_business,
    "config": _validate_config, "environment": _validate_environment,
    "protocol_versions": _validate_protocol_versions, "secret_refs": _validate_secrets,
}


def validate_manifest(value):
    """Closed schema validation; manifest_id must bind the unsigned content."""
    if type(value) is not dict or set(value) != _TOP_KEYS:
        raise ProtocolError("INVALID_SPEC", "manifest key set is unsupported")
    if value["schema_version"] != MANIFEST_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "unsupported manifest schema: " + str(value["schema_version"]))
    identifier(value["run_id"])
    for name, validator in _VALIDATORS.items():
        validator(value[name])
    sealed_at = value["sealed_at"]
    if sealed_at is not None and (type(sealed_at) is not int or sealed_at < 0):
        raise ProtocolError("INVALID_SPEC", "sealed_at must be epoch ms or null")
    if manifest_id(value) != value["manifest_id"]:
        raise ProtocolError("ARTIFACT_MISMATCH", "manifest_id does not bind the content")
    document(value)
    return value


def manifest_id(value):
    """Content identity excludes manifest_id itself and the seal timestamp."""
    content = {key: item for key, item in value.items() if key not in ("manifest_id", "sealed_at")}
    return identity(content)


def build_manifest(*, run_id, platform, business, config, environment,
                   protocol_versions, secret_refs=()):
    value = {
        "schema_version": MANIFEST_SCHEMA, "run_id": run_id,
        "platform": platform, "business": business, "config": config,
        "environment": environment, "protocol_versions": protocol_versions,
        "secret_refs": list(secret_refs), "sealed_at": None,
    }
    value["manifest_id"] = manifest_id(value)
    return validate_manifest(value)


def seal_manifest(value, sealed_at):
    validate_manifest(value)
    if value["sealed_at"] is not None:
        raise ProtocolError("ARTIFACT_SEALED", "manifest is already sealed")
    if type(sealed_at) is not int or sealed_at < 0:
        raise ProtocolError("INVALID_SPEC", "sealed_at must be epoch ms")
    sealed = dict(value, sealed_at=sealed_at)
    return validate_manifest(sealed)


def amend_manifest(value, changes):
    """Pre-seal edits only; sealed manifests are immutable."""
    validate_manifest(value)
    if value["sealed_at"] is not None:
        raise ProtocolError("ARTIFACT_SEALED", "sealed manifests cannot be amended")
    if type(changes) is not dict or not changes or set(changes) - _MUTABLE_SECTIONS:
        raise ProtocolError("INVALID_SPEC", "amendments may only replace registered sections")
    amended = dict(value, **changes, sealed_at=None)
    amended["manifest_id"] = manifest_id(amended)
    return validate_manifest(amended)


def manifest_digest(value):
    """Storage digest in the existing immutable-artifact encoding convention."""
    from ..artifacts import digest
    return digest(value)


def verify_manifest(value, ref):
    """Worker Verify-phase recheck: identity and storage digest must both bind."""
    validate_manifest(value)
    if type(ref) is not dict or not {"manifest_id", "sha256"} <= set(ref) \
            or set(ref) - {"manifest_id", "sha256", "path", "bytes"}:
        raise ProtocolError("INVALID_SPEC", "manifest ref key set is unsupported")
    if ref["manifest_id"] != value["manifest_id"] or ref["sha256"] != manifest_digest(value):
        raise ProtocolError("ARTIFACT_MISMATCH", "manifest reference does not match content")
    return True


def save_manifest(directory, value):
    """Persist via the existing immutable content-addressed JSON mechanism."""
    from ..file_ref import save_json_artifact
    validate_manifest(value)
    ref = save_json_artifact(Path(directory), value)
    path = Path(ref.artifact_path)
    return {"manifest_id": value["manifest_id"], "path": str(path),
            "bytes": path.stat().st_size, "sha256": ref.sha256}