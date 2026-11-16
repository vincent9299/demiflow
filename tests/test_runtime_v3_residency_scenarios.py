from types import SimpleNamespace
import json
import os
import pytest
from runtime_v3_residency_scenarios import multiworker_barrier, actor_state_lifecycle, route_recovery


@pytest.fixture
def residency_fx(tmp_path):
    root=tmp_path
    fx=SimpleNamespace(root=root,journal_dir=root/'journal',evidence_dir=root/'evidence',
        provider=os.environ.get('DF_RESIDENCY_TEST_PROVIDER','local'),bounds={'timeout_ms':120000},_children=[])
    fx.journal_dir.mkdir();fx.evidence_dir.mkdir()
    fx._save_evidence=lambda name,value:(fx.evidence_dir/(name+'.json')).write_text(json.dumps(value,default=str))
    fx._record_not_run=lambda name,detail:fx._save_evidence('not-run-'+name,detail)
    return fx


def test_real_multiworker_partial_batch_original_ack_resume(residency_fx):
    result=multiworker_barrier(residency_fx)
    assert result['joined']==9
    assert len(result['boundary']['participants'])>=3


def test_real_actor_state_and_resource_lifecycle(residency_fx):
    result=actor_state_lifecycle(residency_fx)
    assert result['final_total']==10
    assert len(result['lifecycle_cases'])==4


def test_original_channel_contracts_restore_four_route_suffixes(residency_fx):
    from runtime_v3_residency_scenarios import _channel_recovery
    result=_channel_recovery(residency_fx)
    assert {case['routing'] for case in result}=={'hash','rebalance','target','broadcast'}
    assert all(case['prefix']+case['suffix']==case['expected'] for case in result)
