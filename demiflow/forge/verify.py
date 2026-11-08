"""Contract checks, bounded tests, and revision-bound verification evidence."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path

from .checks import check_project
from .contract import load_contract
from .definition import load_definition
from .io import atomic_write_text
from .process import run_pytest
from .runner import dry_run
from .schema import ForgeError
from .snapshot import changes, snapshot
from .store import invocation, locked, write_record


def verify_project(project: Path) -> dict:
    project = project.absolute()
    with locked(project):
        directory = invocation(project)
        contract = load_contract()
        before = snapshot(project, contract)
        check = check_project(project)
        current = check["revision"]
        report = {
            "schema_version": "demiflow_forge_verification_v1",
            "revision": current,
            "time": _now(),
            "invocation": directory.name,
            "passed": False,
            "check": check,
            "dry_run": None,
            "test": None,
            "source_changed_during_tests": False,
            "changes": None,
            "sandbox": False,
            "unverified": ["business_semantics", "production_execution", "operating_system_resource_isolation"],
        }
        if current is not None and check["passed"]:
            try:
                report["dry_run"] = dry_run(project, project / "run.example.json")
            except ForgeError as exc:
                report["dry_run"] = {"passed": False, "error": str(exc)}
            if report["dry_run"] is not None and report["dry_run"].get("schema_version"):
                definition = load_definition(project).to_dict()
                outcome = run_pytest(
                    project, directory / "scratch",
                    timeout=definition["budgets"]["test_timeout_seconds"],
                    tail_limit=contract["limits"]["output_tail_bytes"],
                    hard_limit=contract["limits"]["output_hard_limit_bytes"],
                )
                stdout = outcome.pop("stdout")
                stderr = outcome.pop("stderr")
                logs = {}
                for name, text in (("stdout", stdout), ("stderr", stderr)):
                    path = directory / f"{name}.log"
                    atomic_write_text(path, text)
                    logs[name] = {"path": str(path), "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "bytes": len(text.encode("utf-8"))}
                outcome["logs"] = logs
                report["test"] = outcome
        if current is not None:
            after = snapshot(project, contract)
            report["changes"] = changes(before, after)
            report["source_changed_during_tests"] = before != after
        test_passed = report["test"] is not None and report["test"]["passed"]
        report["passed"] = bool(
            current and check["passed"] and report["dry_run"] and test_passed
            and not report["source_changed_during_tests"]
        )
        write_record(project, "verification", report)
        return report


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = ["verify_project"]
