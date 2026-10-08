import time
import pytest
from demiflow.execution import datafusion as engine

GiB = 1024**3


def memory_files(root, *, inactive=6*GiB, dirty=0, writeback=0):
    (root/'memory.max').write_text(str(10*GiB))
    (root/'memory.current').write_text(str(9*GiB))
    (root/'memory.stat').write_text(f'inactive_file {inactive}\nfile_dirty {dirty}\nfile_writeback {writeback}\n')


@pytest.mark.parametrize('dirty,writeback,expected', [(0,0,3), (2,1,6), (6,1,9)])
def test_only_clean_inactive_file_is_discounted(tmp_path, dirty, writeback, expected):
    memory_files(tmp_path, dirty=dirty*GiB, writeback=writeback*GiB)
    assert engine._cgroup_memory(tmp_path) == (expected*GiB, 10*GiB)


@pytest.mark.parametrize('content', [None, 'inactive_file invalid\n', 'inactive_file 999\n'])
def test_unavailable_or_partial_statistics_keep_raw_charge(tmp_path, content):
    memory_files(tmp_path)
    (tmp_path/'memory.stat').unlink()
    if content is not None:
        (tmp_path/'memory.stat').write_text(content)
    assert engine._cgroup_memory(tmp_path) == (9*GiB, 10*GiB)


def test_cache_saturated_cgroup_can_run_query_and_real_pressure_still_blocks(tmp_path, monkeypatch):
    cg = tmp_path/'cgroup'; cg.mkdir()
    memory_files(cg)
    original = engine._cgroup_memory
    monkeypatch.setattr(engine, '_cgroup_memory', lambda: original(cg))
    options = engine.DataFusionOptions(memory_bytes=128*1024**2, max_rss_bytes=2*GiB,
        threads=1, partitions=2, timeout_s=15, admission_timeout_s=.1)
    with engine.DataFusionSession(resource_directory=tmp_path/'resources', options=options) as session:
        result = session.query('SELECT 1 AS k', sources={})
        assert result.row_count == 1
        assert result.report['cgroup_memory_policy'] == 'current-minus-clean-inactive-file-v1'
        memory_files(cg, dirty=6*GiB)
        with pytest.raises(TimeoutError, match='admission timed out'):
            session.query('SELECT 2 AS k', sources={})


def test_nonreclaimable_pressure_stops_active_worker(tmp_path, monkeypatch):
    cg = tmp_path/'cgroup'; cg.mkdir()
    memory_files(cg)
    original = engine._cgroup_memory
    monkeypatch.setattr(engine, '_cgroup_memory', lambda: original(cg))
    def pressure(batch):
        (cg/'memory.stat').write_text(f'inactive_file {6*GiB}\nfile_dirty {6*GiB}\nfile_writeback 0\n')
        time.sleep(5)
        return batch
    options = engine.DataFusionOptions(memory_bytes=128*1024**2, max_rss_bytes=2*GiB,
        threads=1, partitions=2, timeout_s=15, admission_timeout_s=.1)
    with engine.DataFusionSession(resource_directory=tmp_path/'resources', options=options) as session:
        with pytest.raises(engine.DataFusionQueryError) as caught:
            session.query('SELECT 1 AS k', sources={}, batch_transform=pressure)
        assert caught.value.report['guard'] == 'cgroup_memory'
        assert caught.value.report['complete'] and not caught.value.report['success']
