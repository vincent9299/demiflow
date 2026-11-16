"""Two actual public Dataset routes must reject before native allocation."""
from runtime_v3_resource_scenarios import arrow_worker_preallocation, lance_cast_preallocation
import pytest


@pytest.mark.parametrize('rejected', [True, False])
def test_worker_typed_conversion_is_admitted_before_arrow(tmp_path, rejected):
    arrow_worker_preallocation(tmp_path, rejected=rejected)


@pytest.mark.parametrize('case', ['conversion_negative', 'conversion_positive', 'cast_negative', 'cast_positive'])
def test_native_lance_cast_is_admitted_before_arrow(tmp_path, case):
    lance_cast_preallocation(tmp_path, case=case)


@pytest.mark.parametrize('rejected', [True, False])
def test_equal_width_cast_owns_input_and_target_at_the_same_time(tmp_path, rejected):
    from runtime_v3_resource_scenarios import equal_width_cast_residency
    equal_width_cast_residency(tmp_path, rejected=rejected)


def test_resource_evidence_backup_closes_every_sqlite_handle(tmp_path, monkeypatch):
    import gc
    import os
    import sqlite3
    from contextlib import closing
    from runtime_v3_resource_scenarios import _export_evidence
    source = tmp_path/'source'; source.mkdir()
    (source/'result.json').write_text('{"status":"test-only-export"}')
    (source/'process.log').write_text('owned fixture\n')
    for i in range(12):
        path = source/('case-'+str(i))/'run'/'run.sqlite'
        path.parent.mkdir(parents=True)
        with closing(sqlite3.connect(path)) as db:
            db.execute('CREATE TABLE original(value INTEGER)')
            db.execute('INSERT INTO original VALUES (?)',(i,)); db.commit()
    import tempfile
    destination = tempfile.mkdtemp(prefix='demiflow-resource-export-fd-')
    monkeypatch.setenv('DEMIFLOW_RESOURCE_EVIDENCE',destination)
    enabled = gc.isenabled(); gc.disable()
    try:
        before = len(os.listdir('/proc/self/fd'))
        exported = _export_evidence(source,provider='local',scenario='test')
        assert len(os.listdir('/proc/self/fd')) <= before
        from pathlib import Path
        for i in range(12):
            with closing(sqlite3.connect(Path(exported)/('case-'+str(i))/'run'/'run.sqlite')) as db:
                assert db.execute('SELECT value FROM original').fetchone() == (i,)
    finally:
        if enabled: gc.enable()
        import shutil
        shutil.rmtree(destination)
