"""Actual Ray Core workers send native publications to their original driver."""
import lance
import pyarrow as pa
from demiflow import data
from demiflow.execution.run_context import RunContext
from runtime_v3_recovery_scenarios import _provider
from runtime_v3_support import _RayDataBlocker


def test_real_ray_stream_save_uses_confirmed_driver_receipts(tmp_path):
    target=tmp_path/'ray.lance'
    with _RayDataBlocker() as blocker,_provider('ray_core'):
        with RunContext.start(tmp_path/'run',limits={'timeout_s':60}) as run:
            output=data.from_items([{'id':'a','value':1},{'id':'b','value':2}]).save_lance(
                target,schema=pa.schema([('id',pa.string()),('value',pa.int64())]),key='id',
                stage='saved',max_batch=1,output_ref='ref').take_all()
            assert [row['ref']['version'] for row in output]==[1,2]
            assert lance.dataset(**output[0]['ref']).to_table().to_pylist()==[{'id':'a','value':1}]
            assert lance.dataset(**output[1]['ref']).to_table().to_pylist()==[{'id':'a','value':1},{'id':'b','value':2}]
            with run.journal._connection() as db:
                scopes=list(db.execute('SELECT scope_id FROM runtime_commit_scopes'))
            assert len(scopes)==3
            assert all(run.journal.execution_commits(key)[0]['status']=='acked' for key, in scopes)
        assert not blocker.attempts
