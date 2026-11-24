"""Workbench process monitoring stays independent of the pipeline API."""
import json
import subprocess
import sys


def test_independent_watchdog_import_does_not_load_pipeline_or_native_runtime():
    code = """
import json, os, sys
from demiflow.workbench.process import process_identity
assert "demiflow.execution" not in sys.modules
assert not any(name == "demiflow.workbench.runs" or name.startswith("demiflow.workbench.runs.") for name in sys.modules)
import demiflow.workbench.analysis.watchdog_service
from demiflow.workbench.analysis.watchdog import ParentWatch
watch = ParentWatch(process_identity(os.getpid()))
assert not watch.stopped()
watch.close()
print(json.dumps(sorted(sys.modules)))
"""
    result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, check=True, timeout=5)
    assert len(result.stdout) < 128*1024
    names = set(json.loads(result.stdout))
    assert "demiflow.workbench.process" in names
    assert "demiflow.execution" not in names
    assert not any(name == "demiflow.workbench.runs" or name.startswith("demiflow.workbench.runs.") for name in names)
    assert not names & {'demiflow.execution.pipeline.readiness', 'demiflow.execution.pipeline.run_context', "pyarrow", "numpy", "lance", "jupyter_client"}
