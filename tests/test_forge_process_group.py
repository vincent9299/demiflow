"""A dead worker leader must not leave a child writing after final accounting."""
import json
import os
from pathlib import Path
import subprocess
import sys


def test_supervisor_reaps_an_orphan_even_after_the_group_leader_exits(tmp_path):
    source = '''
import json, os, subprocess, sys, time
from demiflow.forge.runs.worker import _adopt_descendants, _drain_descendants
_adopt_descendants()
process = subprocess.Popen([sys.executable, '-c', "import os,time; pid=os.fork(); print(os.getpid(), flush=True); time.sleep(60) if pid==0 else None"], stdout=subprocess.PIPE, start_new_session=True)
first = int(process.stdout.readline())
second = int(process.stdout.readline())
process.wait(timeout=5)
drained = _drain_descendants(process)
print(json.dumps({'drained': drained, 'children': [first, second], 'alive': [os.path.exists('/proc/'+str(p)) for p in [first, second]]}))
'''
    result = subprocess.run([sys.executable, '-c', source], capture_output=True, text=True,
                            timeout=15, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'})
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value['drained'] and not any(value['alive']), value
