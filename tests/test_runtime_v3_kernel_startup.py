"""Pure Dataset workers must not initialize an unrelated native writer stack."""
import os
from pathlib import Path

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


def _inspect_modules(row):
    import os
    import sys
    return {**row, 'pid': os.getpid(),
            'native_writer_loaded': 'demiflow.execution.dataflow_stream_lance' in sys.modules,
            'lance_loaded': 'lance' in sys.modules}


def test_pure_callable_worker_does_not_initialize_lance(tmp_path, monkeypatch):
    import demiflow
    root = Path(__file__).resolve().parents[1]
    assert Path(demiflow.__file__).resolve().is_relative_to(root)
    for key, directory in (('DEMIFLOW_RESOURCE_ROOT', 'capacity'),
                           ('DEMIFLOW_RESOURCE_HOME', 'resource-home'),
                           ('DEMIFLOW_FORGE_DOMAIN', 'forge-domain')):
        monkeypatch.setenv(key, str(tmp_path / directory))
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(workers=2):
        output = data.from_items([{'id': 1}, {'id': 2}]).map(_inspect_modules).take_all()
        assert [row['id'] for row in output] == [1, 2]
        assert all(row['pid'] != os.getpid() for row in output)
        assert all(not row['native_writer_loaded'] and not row['lance_loaded'] for row in output)
        facts = run.journal.execution_commits('action-1')
        assert facts and all(fact['status'] == 'acked' for fact in facts)
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM runtime_actions').fetchone()[0] == 1
