"""Private Case returns enter the real deferred task and its SQLite lock fence.

The complete native worker/kernel journey is V05. This regression specifically
keeps its return adapter from being hidden by the older Task test double.
"""
import os
from pathlib import Path
import subprocess
import sys


def test_private_case_return_uses_original_deferred_database(tmp_path):
    tree = Path(__file__).resolve().parents[1]
    script = r'''
from pathlib import Path
from types import SimpleNamespace
import sys
import demiflow
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis.database import Database, FileSizeGuard
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.output_reads import ReturnCase
from demiflow.workbench.analysis.deferred_metadata import Task
assert Path(demiflow.__file__).resolve().is_relative_to(Path(sys.argv[1]))
root = Path(sys.argv[2]); root.mkdir(mode=0o700)
guard = FileSizeGuard(database_bytes=16*1024**2, wal_bytes=8*1024**2)
guard.install()
database = Database(root, guard=guard)
try:
    controller = SimpleNamespace(records=Records(database))
    task = Task(None, None, 'w' + '0'*32, ReturnCase(controller, {}), 'output-return', {})
    task._unlocked()
    with database.locked():
        try:
            task._unlocked()
        except ForgeError as error:
            assert error.code == 'conflict'
        else:
            raise AssertionError('Case return bypassed the original database lock fence')
    task._unlocked()
    task.close()
finally:
    database.close()
'''
    result = subprocess.run([sys.executable, '-B', '-c', script, str(tree), str(tmp_path / 'db')],
                            cwd=tree, env={**os.environ, 'PYTHONPATH': str(tree),
                                          'PYTHONDONTWRITEBYTECODE': '1'},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
