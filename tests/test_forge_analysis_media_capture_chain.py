"""Actual private fixed Lance→export→pending→probe→ready→range→basis chain.

Dedicated real SQLite guard/Web process and bounded codec child; Runtime,
watchdog and deferred selector scheduling are explicitly doubled. No OCI/B1.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from test_forge_analysis_media_source_export import declared,fixed


@pytest.mark.parametrize('scenario',['complete','revoked'])
def test_actual_media_capture_and_persistent_basis_chain(scenario,tmp_path):
    root,raw=declared(tmp_path);locator,source=fixed(root)
    outbox=tmp_path/'outbox';outbox.mkdir();outbox.chmod(0o2770)
    context=dict(workspace=str(root),outbox=str(outbox),state=str(tmp_path/'state'),project='w'+'7'*32,locator=locator,source=source)
    config=tmp_path/'context.json';config.write_text(json.dumps(context))
    repository=Path(__file__).resolve().parents[1]
    run=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('_forge_analysis_media_capture_driver.py')),scenario,str(config)],
        cwd=repository,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(repository)),capture_output=True,text=True,timeout=170)
    assert run.returncode==0,run.stderr+'\n'+run.stdout
    value=json.loads(run.stdout);assert value['ok'] and value['scenario']==scenario
    if scenario=='complete':assert value['history_reopened'] and value['no_notebook_created']
    else:assert value['receipt_status']=='unavailable' and not value['log']
