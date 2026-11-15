"""Evidence rules, not claims that the corresponding business scenarios passed."""
from types import SimpleNamespace

import pytest
import runtime_v3_support as support


@pytest.mark.parametrize("scenario", [
    "cpu_gpu_isolation", "fairness_deadlock", "multiworker_barrier", "route_recovery",
    "actor_state_lifecycle",
    "queue_full_business", "memory_peak", "disk_gc", "two_hosts",
    "node_loss", "driver_restart", "history_identity", "workbench_real_plan",
    "product_regression", "artifact_security", "performance", "all_regressions",
])
def test_placeholder_scenarios_cannot_produce_passing_evidence(scenario):
    recorded = []
    fixture = SimpleNamespace(_record_not_run=lambda name, detail: recorded.append((name, detail)))
    with pytest.raises(support.NotRunError):
        support._SCENARIO_FUNCS[scenario](fixture, None)
    assert recorded[0][0] == scenario
    assert recorded[0][1]["missing_semantic_harness"]


def _api_report():
    apis={str(i):{'status':'tested'} for i in range(98)}
    return {'api_count':98,'api':apis,'tested':dict(apis),'executed':list(apis),
            'failed':{},'not_run':{},'complete':True,'cases':[{'status':'passed'}]}


@pytest.mark.parametrize('defect',['failure','missing_execution','missing_test','missing_behavior'])
def test_api_matrix_guard_rejects_claimed_success_without_complete_actual_evidence(monkeypatch,defect):
    import runtime_v3_api_scenarios
    report=_api_report()
    if defect=='failure':report['failed']={'0':{'failed':['real-positive']}}
    elif defect=='missing_execution':report['executed'].pop()
    elif defect=='missing_test':report['tested'].pop('0')
    else:report['cases']=[]
    monkeypatch.setattr(runtime_v3_api_scenarios,'api_matrix',lambda fx,fault:report)
    with pytest.raises(AssertionError):support._s_api_matrix(SimpleNamespace(),None)


def test_api_matrix_guard_preserves_explicit_unverified_entries(monkeypatch):
    import runtime_v3_api_scenarios
    report=_api_report();report['not_run']={'0':{'not_run':['external source unavailable']}}
    report['complete']=False;recorded=[]
    monkeypatch.setattr(runtime_v3_api_scenarios,'api_matrix',lambda fx,fault:report)
    fixture=SimpleNamespace(_record_not_run=lambda name,detail:recorded.append((name,detail)))
    with pytest.raises(support.NotRunError):support._s_api_matrix(fixture,None)
    assert recorded[0][0]=='api_matrix'
    assert recorded[0][1]['unverified_api']==report['not_run']


def test_stream_facade_alias_is_observed_even_when_imported_before_recorder(tmp_path):
    from demiflow import data
    from demiflow.execution import dataflow_stream
    from demiflow.execution.run_context import RunContext
    for attempt in range(2):
        with RunContext.start(tmp_path/str(attempt)),data.local_execution():
            with support._ExecuteActionRecorder() as recorder:
                data.from_items([{'i':attempt}]).batch_map(lambda rows:rows,max_batch=1).run_stream()
            assert len(recorder.calls)==1
            assert recorder.calls[0]['terminal_kind']=='run_stream'
