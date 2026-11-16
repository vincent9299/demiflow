"""Real public routing plus original native exchange recovery proofs."""
from pathlib import Path
from types import SimpleNamespace
import os
import pytest
import runtime_v3_route_recovery_scenarios as scenarios

@pytest.fixture
def fx(tmp_path,monkeypatch):
    import demiflow
    tree=Path(__file__).resolve().parents[1]
    assert Path(demiflow.__file__).resolve().is_relative_to(tree)
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))
    return SimpleNamespace(root=tmp_path,journal_dir=tmp_path/'journal',provider=os.getenv('DEMIFLOW_V3_PROVIDER','local'))

def test_public_shared_models_original_ack_worker_loss(fx):
    result=scenarios.public_shared_models(fx)
    assert result['verified_suffix'] and len(result['http_requests'])==12

def test_public_hash_rebalance_original_ack_worker_loss(fx):
    result=scenarios.public_hash_rebalance(fx)
    assert result['verified_suffix']

@pytest.mark.parametrize('routing',['hash','rebalance','target','broadcast'])
def test_native_route_contract_real_workers_original_ack_recovery(fx,routing):
    result=scenarios.native_exchange(fx,routing)
    assert result['preparation']['plan_id']==result['physical']['plan_id']

@pytest.mark.parametrize('name',['native_unknown_target','public_unknown_model'])
def test_unknown_target_cannot_silently_select_a_different_reader_or_model(fx,name):
    result=getattr(scenarios,name)(fx)
    assert result['error'] and result['commits']==[]
