"""Bounded native affinity counts; unavailable to this worker is not global liveness."""
import json
import pytest
from demiflow.execution.contracts.queue_contract import ArtifactDomainUnavailable
from test_queue_v2_protocol import channel
from test_queue_v2_boundaries import seed,worker
from test_raw_fetch import origin_server


def test_prepared_domain_is_reported_without_scanning_tasks_or_claiming_elsewhere(channel,tmp_path,monkeypatch):
    import demiflow.execution.checkpoint.commit as commits
    with origin_server() as (url,calls):
        producer=seed(channel,tmp_path,url);fired=[]
        def fault(name,commit_id):
            if name!='after_prepared' or fired:return
            with channel.database.connection() as db:
                row=db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
            if row and json.loads(row[0])['sinks']==['raw']:
                fired.append(True);raise RuntimeError('prepared-domain')
        monkeypatch.setattr(commits,'fault_point',fault)
        with pytest.raises(Exception,match='prepared-domain'):worker(channel,tmp_path/'original')
        state=channel.state('p')
        assert state['artifact_bound_tasks']==sum(state['artifact_domains'].values())==1
        assert state['blocked_artifact_domain'] is None and state['artifact_availability']=='not_probed'
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?',(producer.generation,))
        monkeypatch.setattr(commits,'fault_point',lambda *_:None)
        with pytest.raises(ArtifactDomainUnavailable):worker(channel,tmp_path/'another-domain')
        assert calls['/file']==1 and channel.state('p')['artifact_bound_tasks']==1
        worker(channel,tmp_path/'original')
        state=channel.state('p')
        assert calls['/file']==1 and state['artifact_bound_tasks']==0 and state['queue_drained']
