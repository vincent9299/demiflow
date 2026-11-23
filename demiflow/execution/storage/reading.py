"""Bounded, authenticated JSON carriers for sealed plans and recovery history."""
from __future__ import annotations

from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import validate_sink_reference as _validate_ref

import hashlib
import json
from pathlib import Path
from demiflow.execution.contracts.errors import PlanContractError, require_plan_contract as _require
from demiflow.execution.contracts.artifact import validate_plan_artifact_ref as _validate_artifact_ref
from demiflow.execution.contracts.dataflow_errors import DataflowError as _err
_MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2


def read_plan_artifact(directory, ref, *, max_bytes) -> dict:
    """Bounded, run-relative, symlink-refusing, digest-gated carrier read."""
    _validate_artifact_ref(ref, "artifact")
    _require(type(max_bytes) is int and max_bytes >= 0,
             "invalid_contract", "max_bytes must be a nonnegative int")
    if ref["bytes"] > max_bytes:
        raise PlanContractError("artifact_too_large", "declared artifact bytes exceed the read budget")
    root = Path(directory)
    path = root / Path(ref["path"])
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise PlanContractError("unsafe_artifact", "artifact path contains a symlink")
    resolved, root_resolved = path.resolve(), root.resolve()
    if resolved != root_resolved and root_resolved not in resolved.parents:
        raise PlanContractError("unsafe_artifact", "artifact path escapes the run directory")
    if not path.exists():
        raise PlanContractError("corrupt_artifact", "artifact carrier is missing")
    if not path.is_file():
        raise PlanContractError("unsafe_artifact", "artifact carrier is not a regular file")
    actual = path.stat().st_size
    if actual > max_bytes:
        raise PlanContractError("artifact_too_large", "artifact carrier exceeds the read budget")
    if actual != ref["bytes"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier length differs")
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise PlanContractError("artifact_too_large", "artifact carrier exceeds the read budget")
    if len(raw) != ref["bytes"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier changed while reading")
    if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier digest differs")
    try:
        value = json.loads(raw)
    except ValueError:
        raise PlanContractError("corrupt_artifact", "artifact carrier is not valid JSON")
    _require(type(value) is dict, "invalid_contract", "artifact carrier is not a JSON object")
    return value

def read_history_carrier(directory, ref, *, max_bytes):
    """Bounded, run-relative, digest-gated read of one v1/v2 carrier."""
    ceiling = min(max_bytes, _MAX_SEALED_PLAN_BYTES)
    try:
        return read_plan_artifact(directory, ref, max_bytes=ceiling)
    except PlanContractError as exc:
        if exc.reason in ("artifact_too_large",):
            raise _err("BUDGET_EXCEEDED", str(exc.detail))
        if exc.reason in ("unsafe_artifact",):
            raise _err("INVALID_CONTRACT", str(exc.detail))
        raise _err("CORRUPT_ARTIFACT", str(exc.detail))

def read_json_artifact(store, ref, *, max_bytes):
    """Bounded read of one store artifact into a JSON object."""
    chunks = []
    total = 0
    for chunk in store.read(ref, max_bytes=max_bytes):
        total += len(chunk)
        if total > max_bytes:
            raise _err("BUDGET_EXCEEDED", "artifact exceeds the read budget")
        chunks.append(chunk)
    raw = b"".join(chunks)
    if hashlib.sha256(raw).hexdigest() != ref.digest:
        raise _err("CORRUPT_ARTIFACT", "artifact digest differs from its reference")
    try:
        value = json.loads(raw)
    except ValueError:
        raise _err("CORRUPT_ARTIFACT", "artifact is not valid JSON")
    if type(value) is not dict:
        raise _err("INVALID_CONTRACT", "artifact is not a JSON object")
    return value


def read_sink_json(store, ref: ArtifactRef, *, max_bytes: int, media_type: str):
    _validate_ref(ref, what="sink artifact")
    if ref.media_type != media_type:
        raise DataflowError("INVALID_CONTRACT", "Sink artifact media type differs")
    raw = b"".join(store.read(ref, max_bytes=max_bytes))
    if len(raw) > max_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "Sink artifact exceeds read budget")
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "Sink artifact is not valid JSON") from exc
