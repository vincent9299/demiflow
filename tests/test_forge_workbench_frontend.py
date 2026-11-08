"""Deterministic client identity/storage/subscription protocol checks.

These use controlled API replies. Real native execution is covered separately
by test_forge_workbench_runtime_browser and test_forge_runtime_core_flow.
"""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize('script,groups',[('check-state-commands.cjs',18),('check-observer.cjs',8)])
def test_bounded_client_protocol(script,groups):
    node=shutil.which('node')
    if node is None:pytest.skip('Node is required for deterministic browser-module checks')
    result=subprocess.run([node,str(Path(__file__).parent/'forge_workbench_protocol'/script)],capture_output=True,text=True,timeout=20)
    assert result.returncode==0,result.stderr
    report=json.loads(result.stdout)
    assert report['status']=='passed' and report['unit_groups']==groups
