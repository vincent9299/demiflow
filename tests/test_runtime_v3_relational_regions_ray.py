"""The same sealed relational region and original ACK on private Ray Core."""
import tempfile
from types import SimpleNamespace

import pytest


def test_real_ray_region_values_seal_and_final_ack_replay(tmp_path,monkeypatch):
    ray=pytest.importorskip('ray')
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.dataflow import DataflowExecution
    from runtime_v3_support import _RayDataBlocker
    from test_runtime_v3_relational_regions import build_chain
    from _native_relational_fixtures import native_region_evidence
    assert not ray.is_initialized()
    prepares=[]
    prepare=DataflowExecution.prepare
    def observed(execution):
        result=prepare(execution)
        prepares.append(execution._physical)
        return result
    monkeypatch.setattr(DataflowExecution,'prepare',observed)
    class Interrupted(Exception):pass
    executor=RayDatasetExecutor(workers=2)
    with tempfile.TemporaryDirectory(prefix='demiflow-region-ray-',dir='/tmp') as directory:
        try:
            ray.init(address='local',num_cpus=4,num_gpus=0,include_dashboard=False,
                     object_store_memory=100*1024**2,_temp_dir=directory)
            with _RayDataBlocker() as blocker,_use_executor(executor):
                with pytest.raises(Interrupted):
                    with RunContext.start(tmp_path/'run') as run:
                        reference=run.ref
                        actual=build_chain().take_all()
                        assert actual==[{'k':1,'values':[('a',10,7),('a',11,7),('c',10,7),('c',11,7)]},
                                        {'k':2,'values':[('b',20,8)]}]
                        _,physical,counts=native_region_evidence(SimpleNamespace(
                            run_ref=reference,stats={'action_id':'action-1'}))
                        assert physical.policy.provider=='ray_core' and len(counts)==1
                        assert counts[0]['partition_builds']==3
                        assert counts[0]['intermediate_payload_bytes']==0
                        original=run.journal.execution_commits('action-1')
                        assert original and all(item['status']=='acked' for item in original)
                        raise Interrupted('after actual action ACK')
                launches=[]
                monkeypatch.setattr(DataflowExecution,'_prepare_launch',lambda *args:launches.append(args))
                with RunContext.resume(reference) as run:
                    assert build_chain().take_all()==actual
                    assert run.journal.execution_commits('action-1')==original
                assert not launches and not blocker.attempts
                assert prepares and all(p.policy.provider=='ray_core' for p in prepares)
        finally:
            executor.close()
            ray.shutdown()
        assert not ray.is_initialized()
