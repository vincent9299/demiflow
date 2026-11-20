"""The same native CSV declaration runs on actual Ray Core and replays its ACK."""
import gzip

import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan
from runtime_v3_recovery_scenarios import _provider
from runtime_v3_support import _RayDataBlocker


def test_native_csv_ray_core_join_and_completed_recovery(tmp_path, monkeypatch):
    ray = pytest.importorskip('ray')
    assert not ray.is_initialized(), 'test owns its private Ray cluster'
    path = tmp_path / 'rows.tsv.gz'
    with gzip.open(path, 'wt', newline='', encoding='utf-8-sig') as stream:
        stream.write('id\tvalue\n01\t"quoted"\n01\t\n02\ttail\n')
    def pipeline():
        return data.from_items([{'id': '01', 'key': 1}, {'id': '02', 'key': 2}]).join(
            data.read_csv(str(path),
                read_options=pacsv.ReadOptions(block_size=64),
                parse_options=pacsv.ParseOptions(delimiter='\t', quote_char=False),
                convert_options=pacsv.ConvertOptions(column_types={'id': pa.string(), 'value': pa.string()})),
            on='id')
    expected = [{'id': '01', 'key': 1, 'value': '"quoted"'},
                {'id': '01', 'key': 1, 'value': ''}, {'id': '02', 'key': 2, 'value': 'tail'}]
    key = lambda row: (row['id'], row['value'])
    class InterruptedAfterCommit(Exception):
        pass
    with _RayDataBlocker() as blocker, _provider('ray_core'):
        with pytest.raises(InterruptedAfterCommit):
            with RunContext.start(tmp_path / 'run', limits={'timeout_s': 60}) as run:
                ref = run.ref
                ds = pipeline()
                assert sorted(ds.take_all(), key=key) == sorted(expected, key=key)
                metadata = ds.execution_metadata().diagnostics
                assert metadata['observed_worker_count'] > 0
                sealed = load_sealed_plan(run.directory, 'action-1')
                assert sealed['physical'].policy.provider == 'ray_core'
                commits = run.journal.execution_commits('action-1')
                assert commits and commits[-1]['status'] == 'acked'
                raise InterruptedAfterCommit()
        from demiflow.execution.action_driver.dataflow import DataflowExecution
        def no_launch(*args, **kwargs):
            raise AssertionError('completed CSV replay must not launch workers')
        monkeypatch.setattr(DataflowExecution, '_launch_phase', no_launch)
        with RunContext.resume(ref) as run:
            assert sorted(pipeline().take_all(), key=key) == sorted(expected, key=key)
            assert run.journal.execution_commits('action-1') == commits
        assert not blocker.attempts
    assert not ray.is_initialized()
