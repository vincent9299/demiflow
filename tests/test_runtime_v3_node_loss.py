"""Actual V027 components; no fake worker, allocator, store or commit."""
import pytest
from copy import deepcopy
from types import SimpleNamespace


def test_actual_producer_owner_loss_recovers_original_ack(runtime_v3_fixture):
    if runtime_v3_fixture.provider != 'ray_core':
        from runtime_v3_support import NotRunError
        raise NotRunError('node_loss', {'missing_environment':'real Ray Core'})
    from runtime_v3_node_loss_scenarios import producer_loss
    evidence = producer_loss(runtime_v3_fixture)
    runtime_v3_fixture._save_evidence('producer-owner-loss',evidence)
    assert evidence['loss']['object_ref_failure']['type'] in {'OwnerDiedError','ObjectLostError'}


@pytest.mark.parametrize('kind',['prompt','embedding','agent'])
def test_actual_unknown_paid_actor_loss_never_reissues(runtime_v3_fixture,kind):
    if runtime_v3_fixture.provider != 'ray_core':
        from runtime_v3_support import NotRunError
        raise NotRunError('node_loss', {'missing_environment':'real Ray Core'})
    from runtime_v3_node_loss_scenarios import unknown_model_owner_loss
    evidence = unknown_model_owner_loss(runtime_v3_fixture,kind)
    runtime_v3_fixture._save_evidence('unknown-model-owner-'+kind,evidence)
    assert len(evidence['http'])==1


def _guard_sample():
    """Synthetic DTOs exercise only refusal checks, never count as actor proof."""
    dead={'state':'DEAD','num_restarts':0}
    native={'keys':['original-key'],'requests_consumed':1,'unresolved':1,'confirmed':0}
    producer={'loss':{'borrowed_before_kill':True,'borrowed_ref':'ref',
        'ack_before_kill':{'status':'acked','final':False},'object_ref_failure':{'type':'OwnerDiedError'},
        'death':dict(dead),'after_resume_death':dict(dead),'dead_worker_cache_removed':True,
        'dead_worker_cache_entries':2,'plan_id':'plan','first_attempt':1},
        'initial_error':{'code':'UNKNOWN_EFFECT'},'original_durable_store_present':True,
        'original_commits_preserved':1,'output':[{'id':i} for i in range(12)],
        'resumed':[{'attempt':2,'final_status':'acked','plan_id':'plan'}]}
    models=[{'kind':kind,'initial_error':{'code':'UNKNOWN_EFFECT'},
        'resume_error':{'type':'UncertainPromptCall','message':'automatic regeneration is forbidden'},
        'http':[{}],'commits':[],'attempt_before':1,'attempt_after':2,
        'fence_before':'old','fence_after':'new','before_resume':deepcopy(native),'after_resume':deepcopy(native),
        'death':{'confirmed_dead':dict(dead),'after_resume_dead':dict(dead)}} for kind in ('prompt','embedding','agent')]
    return {'producer':producer,'models':models,
        'fault_scope':'actual actor owner loss; same-host Ray; no physical outage or network partition'}


@pytest.mark.parametrize('defect',['empty','not_borrowed','no_owner_loss','no_original_ack','cache_unchanged',
    'wrong_output','plan_drift','restarted_actor','missing_model','http_repeated','new_request_key',
    'budget_refund','fake_confirmed','same_fence','claimed_host_outage'])
def test_node_loss_guard_rejects_incomplete_proof(defect):
    from runtime_v3_node_loss_scenarios import validate_evidence
    v=_guard_sample();p=v['producer'];loss=p['loss'];m=v['models'][0]
    if defect=='empty':v={}
    elif defect=='not_borrowed':loss['borrowed_before_kill']=False
    elif defect=='no_owner_loss':loss['object_ref_failure']['type']='InjectedTestError'
    elif defect=='no_original_ack':loss['ack_before_kill']['status']='prepared'
    elif defect=='cache_unchanged':loss['dead_worker_cache_removed']=False
    elif defect=='wrong_output':p['output'].pop()
    elif defect=='plan_drift':p['resumed'][0]['plan_id']='changed'
    elif defect=='restarted_actor':loss['after_resume_death']['num_restarts']=1
    elif defect=='missing_model':v['models'].pop()
    elif defect=='http_repeated':m['http'].append({})
    elif defect=='new_request_key':m['after_resume']['keys']=['new-key']
    elif defect=='budget_refund':m['after_resume']['requests_consumed']=0
    elif defect=='fake_confirmed':m['after_resume']['confirmed']=1
    elif defect=='same_fence':m['fence_after']=m['fence_before']
    elif defect=='claimed_host_outage':v['fault_scope']='physical host outage'
    with pytest.raises(AssertionError):validate_evidence(v)


def test_local_process_cannot_substitute_for_real_actor_loss():
    from runtime_v3_node_loss_scenarios import node_loss
    from runtime_v3_support import NotRunError
    records=[]
    fx=SimpleNamespace(provider='local',_record_not_run=lambda *value:records.append(value))
    with pytest.raises(NotRunError):node_loss(fx)
    assert records[0][0]=='node_loss'


def test_node_loss_delegate_rejects_names_without_real_proof(monkeypatch):
    import runtime_v3_node_loss_scenarios as scenarios
    import runtime_v3_support as support
    monkeypatch.setattr(scenarios,'node_loss',lambda *_:{'killed':True})
    with pytest.raises(AssertionError):support._s_node_loss(SimpleNamespace(),None)
