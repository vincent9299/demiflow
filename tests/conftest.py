"""Evidence export for the lane G unit gate (G-G).

Captures per-phase pytest results, in-process module paths and source digests,
then evaluates them with the frozen evidence_guard schema. Test-domain only;
writes artifacts under /tmp/demiflow-* per the work order.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

_TREE = Path(__file__).resolve().parents[1]
_SOURCE_FILES = (
    "demiflow/planning/dataflow_compile.py",
    "demiflow/planning/dataflow_registry.py",
    "demiflow/execution/plan_facts.py",
    "demiflow/data/dataflow_operation.py",
)
_REQUIRED = [f"tests/test_runtime_v3_g.py::test_g{n:03d}" for n in range(1, 14)]
_REPORT_PATH = Path("/tmp/demiflow-g-evidence.json")

_state = {"collected": [], "results": [], "source_before": {}, "deselected": [],
          "collection_errors": [], "exit_code": None}


def _digests():
    return {rel: hashlib.sha256((_TREE / rel).read_bytes()).hexdigest() for rel in _SOURCE_FILES}


def pytest_configure(config):
    _state["source_before"] = _digests()


def pytest_collection_modifyitems(session, config, items):
    _state["collected"] = [item.nodeid for item in items]


def pytest_collectreport(report):
    if report.failed:
        _state["collection_errors"].append(str(report.longrepr))


def pytest_runtest_logreport(report):
    if report.when not in {"setup", "call", "teardown"}:
        return
    _state["results"].append({
        "nodeid": report.nodeid,
        "when": report.when,
        "outcome": report.outcome,
        "wasxfail": hasattr(report, "wasxfail"),
        "rerun": 0,
    })


def pytest_sessionfinish(session, exitstatus):
    if not any(node.startswith("tests/test_runtime_v3_g.py") for node in _state["collected"]):
        return
    _state["exit_code"] = int(exitstatus)
    sys.path.insert(0, str(_TREE / "tests"))
    from _runtime_v3_g_fakes import assert_tree_binding
    module_paths = assert_tree_binding()
    source_after = _digests()
    report = {
        "exit_code": _state["exit_code"],
        "collection_errors": _state["collection_errors"],
        "deselected": _state["deselected"],
        "collected": _state["collected"],
        "source_before": _state["source_before"],
        "source_after": source_after,
        "module_paths": module_paths,
        "results": _state["results"],
    }
    _REPORT_PATH.write_text(json.dumps(report, indent=2, sort_keys=True))
    sys.path.insert(0, str(_TREE / "docs/unified-runtime-v3-20261114/tools"))
    from evidence_guard import evaluate
    verdict = evaluate(report, required_nodeids=_REQUIRED,
                       expected_tree=str(_TREE), expected_digests=_state["source_before"])
    print("\n[G-G EVIDENCE]", json.dumps(verdict, sort_keys=True))
    print("[G-G REPORT]", _REPORT_PATH)