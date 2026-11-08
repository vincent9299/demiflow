"""Review, Freeze, and current lifecycle status."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path

from .checks import check_project
from .contract import load_contract
from .io import read_json
from .schema import ForgeError, exact_fields, positive_int, required_text
from .snapshot import snapshot
from .store import locked, read_record, write_record


def status(project: Path) -> dict:
    project = project.absolute()
    check = check_project(project)
    current = check["revision"]
    verification = read_record(project, "verification")
    review = read_record(project, "review")
    frozen = read_record(project, "freeze")
    gates = {
        "check": "passed" if check["passed"] else "failed",
        "verification": _verification_gate(verification, current),
        "review": _gate(review, current),
        "freeze": _gate(frozen, current, passed_value="frozen"),
    }
    if gates["freeze"] == "passed":
        state = "frozen"
    elif gates["review"] == "passed":
        state = "reviewed"
    elif gates["verification"] == "passed":
        state = "tested"
    elif check["passed"]:
        state = "implemented"
    else:
        state = "draft"
    if not check["passed"]:
        next_action = "fix check issues"
    elif gates["verification"] != "passed":
        next_action = "run verify"
    elif gates["review"] != "passed":
        next_action = "submit current review"
    elif gates["freeze"] != "passed":
        next_action = "freeze current revision"
    else:
        next_action = "run frozen project"
    return {
        "schema_version": "demiflow_forge_status_v1",
        "project": str(project),
        "state": state,
        "revision": current,
        "gates": gates,
        "issues": check["issues"],
        "next": next_action,
    }


def review_project(project: Path, review_path: Path) -> dict:
    project = project.absolute()
    with locked(project):
        current_status = status(project)
        current = current_status["revision"]
        if current_status["gates"]["verification"] != "passed":
            raise ForgeError("Review requires current passing verification")
        value = read_json(review_path)
        exact_fields(value, {
            "schema_version", "revision", "reviewer", "review_type", "status",
            "summary", "findings",
        }, "Review")
        if value["schema_version"] != "demiflow_forge_review_v1" or value["revision"] != current:
            raise ForgeError("Review schema/revision does not match current source")
        required_text(value["reviewer"], "reviewer")
        if value["review_type"] not in {"self", "independent"}:
            raise ForgeError("review_type must be self or independent")
        if value["status"] not in {"passed", "failed"}:
            raise ForgeError("Review status must be passed or failed")
        required_text(value["summary"], "summary")
        findings = value["findings"]
        if not isinstance(findings, list) or len(findings) > 64:
            raise ForgeError("Review findings must be a bounded list")
        files = snapshot(project, load_contract())
        for finding in findings:
            exact_fields(finding, {"path", "line", "severity", "message", "evidence"}, "Review finding")
            if finding["path"] not in files:
                raise ForgeError("Review finding path is outside the project snapshot")
            positive_int(finding["line"], "finding.line", maximum=2**31 - 1)
            if finding["severity"] not in {"blocker", "warning"}:
                raise ForgeError("Review finding severity is invalid")
            required_text(finding["message"], "finding.message")
            required_text(finding["evidence"], "finding.evidence")
        if value["status"] == "passed" and any(item["severity"] == "blocker" for item in findings):
            raise ForgeError("Passing Review cannot contain blockers")
        write_record(project, "review", value)
        return value


def freeze_project(project: Path) -> dict:
    project = project.absolute()
    with locked(project):
        current = status(project)
        if current["gates"]["verification"] != "passed" or current["gates"]["review"] != "passed":
            raise ForgeError("Freeze requires current passing verification and Review")
        manifest = read_json(project / "manifest.json")
        receipt = {
            "schema_version": "demiflow_forge_freeze_v1",
            "status": "frozen",
            "revision": current["revision"],
            "manifest_digest": manifest["manifest_digest"],
            "context_digest": manifest["context_digest"],
            "time": datetime.now(timezone.utc).isoformat(),
        }
        write_record(project, "freeze", receipt)
        return receipt


def require_frozen(project: Path) -> dict:
    current = status(project)
    if any(current["gates"][name] != "passed" for name in ("check", "verification", "review", "freeze")):
        raise ForgeError("Run requires a current frozen revision with passing verification and Review")
    return read_record(project, "freeze")


def _verification_gate(record, current: str | None) -> str:
    result = _gate(record, current)
    if result != "passed":
        return result
    test = record.get("test") or {}
    for log in (test.get("logs") or {}).values():
        path = Path(log["path"])
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != log["sha256"]:
            return "corrupt"
    return "passed"


def _gate(record, current: str | None, *, passed_value: str = "passed") -> str:
    if record is None:
        return "missing"
    if current is None or record.get("revision") != current:
        return "stale"
    if passed_value == "frozen":
        return "passed" if record.get("status") == "frozen" else "failed"
    return "passed" if record.get("passed") is True or record.get("status") == "passed" else "failed"


__all__ = ["freeze_project", "require_frozen", "review_project", "status"]
