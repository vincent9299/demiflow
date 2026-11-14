"""W027 · deterministic logical_selection state-protocol checks (node harness).

Runs the pure workbench-state.js selection protocol in a node vm. Skips when node
is unavailable. Real native execution / browser acceptance is covered separately
by the runtime-browser suites; this is a bounded client-protocol unit check.
"""
import json
import shutil
import subprocess
from pathlib import Path

import pytest


def test_logical_selection_state_protocol():
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node is required for deterministic browser-module checks')
    script = Path(__file__).parent / 'forge_workbench_protocol' / 'check-state-selection.cjs'
    result = subprocess.run([node, str(script)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report['status'] == 'passed' and report['unit_groups'] == 10