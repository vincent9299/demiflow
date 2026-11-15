"""G-F gate evidence plugin (lane F test infrastructure).

Records per-nodeid setup/call/teardown phases and writes an evidence_guard-shaped
report to /tmp/demiflow-f-gate/report.json. Real in-tree demiflow modules are
reported under ``module_paths``; synthetic spec fakes are listed separately under
``test_doubles`` and never impersonate a real file binding.
"""
import hashlib
import json
import sys
from pathlib import Path

TREE = Path(__file__).resolve().parents[1]
REPORT_DIR = Path("/tmp/demiflow-f-gate")
SOURCE_FILES = [
    "demiflow/execution/dataflow_inputs.py",
    "demiflow/execution/local_kernel.py",
    "demiflow/data/api.py",
    "demiflow/execution/driver.py",
    "demiflow/execution/chain.py",
    "demiflow/execution/graph.py",
    "demiflow/execution/queue_chain.py",
    "demiflow/execution/dataset_native.py",
    "demiflow/execution/stream.py",
    "demiflow/execution/source_snapshot.py",
]


def _digests():
    out = {}
    for rel in SOURCE_FILES:
        out[rel] = hashlib.sha256((TREE / rel).read_bytes()).hexdigest()
    return out


def _module_paths():
    paths = {}
    for name, mod in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            f = getattr(mod, "__file__", None)
            if not f:
                continue
            try:
                resolved = Path(f).resolve()
            except (OSError, ValueError):
                continue
            if resolved.is_relative_to(TREE):
                paths[name] = str(resolved)
    return paths


class _GateRecorder:
    def __init__(self):
        self.collected = []
        self.results = []
        self.collection_errors = []
        self.deselected = []
        self.source_before = None
        self.test_doubles = []

    def pytest_sessionstart(self, session):
        self.source_before = _digests()

    def pytest_collection_modifyitems(self, session, config, items):
        self.collected = [item.nodeid for item in items]

    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append(str(report.longrepr))

    def pytest_deselected(self, items):
        self.deselected.extend(item.nodeid for item in items)

    def pytest_runtest_logreport(self, report):
        self.results.append({
            "nodeid": report.nodeid,
            "when": report.when,
            "outcome": report.outcome,
            "wasxfail": hasattr(report, "wasxfail"),
            "rerun": getattr(report, "rerun", 0) or 0,
        })

    def pytest_sessionfinish(self, session, exitstatus):
        doubles = getattr(session.config, "_f_test_doubles", [])
        report = {
            "exit_code": int(exitstatus),
            "collection_errors": self.collection_errors,
            "deselected": self.deselected,
            "collected": self.collected,
            "source_before": self.source_before,
            "source_after": _digests(),
            "module_paths": _module_paths(),
            "test_doubles": doubles,
            "results": self.results,
        }
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "report.json").write_text(json.dumps(report, indent=1))


def pytest_configure(config):
    config._f_recorder = _GateRecorder()
    config.pluginmanager.register(config._f_recorder)