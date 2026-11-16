"""Evidence rules, not claims that the corresponding business scenarios passed."""
from types import SimpleNamespace

import pytest
import runtime_v3_support as support


@pytest.mark.parametrize("scenario", [
    "fairness_deadlock",
    "queue_full_business", "memory_peak", "two_hosts",
    "history_identity", "workbench_real_plan",
    "product_regression", "all_regressions",
])
def test_placeholder_scenarios_cannot_produce_passing_evidence(scenario):
    recorded = []
    fixture = SimpleNamespace(_record_not_run=lambda name, detail: recorded.append((name, detail)))
    with pytest.raises(support.NotRunError):
        support._SCENARIO_FUNCS[scenario](fixture, None)
    assert recorded[0][0] == scenario
    assert recorded[0][1]["missing_semantic_harness"]


@pytest.mark.parametrize('scenario,module', [
    ('multiworker_barrier', 'runtime_v3_residency_scenarios'),
    ('route_recovery', 'runtime_v3_residency_scenarios'),
    ('actor_state_lifecycle', 'runtime_v3_residency_scenarios'),
    ('driver_restart', 'runtime_v3_recovery_scenarios'),
    ('disk_gc', 'runtime_v3_gc_scenarios'),
    ('artifact_security', 'runtime_v3_artifact_security_scenarios'),
    ('cpu_gpu_isolation', 'runtime_v3_gpu_scenarios'),
    ('node_loss', 'runtime_v3_node_loss_scenarios'),
])
def test_implemented_gate_cannot_replace_missing_native_evidence_with_success(monkeypatch, scenario, module):
    import importlib
    error = support.NotRunError(scenario, {'missing': 'native evidence unavailable'})
    def missing(*args, **kwargs):
        raise error
    monkeypatch.setattr(importlib.import_module(module), scenario, missing)
    with pytest.raises(support.NotRunError) as observed:
        support._SCENARIO_FUNCS[scenario](SimpleNamespace(), None)
    assert observed.value is error


def test_performance_requires_an_actual_complete_report(monkeypatch):
    monkeypatch.delenv('DEMIFLOW_PERFORMANCE_REPORT', raising=False)
    with pytest.raises(support.NotRunError) as observed:
        support._s_performance(SimpleNamespace(), None)
    assert observed.value.missing['missing'] == 'fixed candidate nine-class comparative report'


def _api_report():
    apis={str(i):{'status':'tested'} for i in range(98)}
    return {'api_count':98,'api':apis,'tested':dict(apis),'executed':list(apis),
            'failed':{},'not_run':{},'complete':True,'cases':[{'status':'passed'}]}


@pytest.mark.parametrize('defect',['failure','missing_execution','missing_test','missing_behavior'])
@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_api_matrix_guard_rejects_claimed_success_without_complete_actual_evidence(monkeypatch,defect,provider):
    import runtime_v3_api_scenarios
    import runtime_v3_ray_matrix_process
    report=_api_report()
    if defect=='failure':report['failed']={'0':{'failed':['real-positive']}}
    elif defect=='missing_execution':report['executed'].pop()
    elif defect=='missing_test':report['tested'].pop('0')
    else:report['cases']=[]
    monkeypatch.setattr(runtime_v3_api_scenarios,'api_matrix',lambda fx,fault:report)
    monkeypatch.setattr(runtime_v3_ray_matrix_process,'run_ray_api_matrix',lambda fx,fault:report)
    with pytest.raises(AssertionError):support._s_api_matrix(SimpleNamespace(provider=provider),None)


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_api_matrix_guard_preserves_explicit_unverified_entries(monkeypatch,provider):
    import runtime_v3_api_scenarios
    import runtime_v3_ray_matrix_process
    report=_api_report();report['not_run']={'0':{'not_run':['external source unavailable']}}
    report['complete']=False;recorded=[]
    monkeypatch.setattr(runtime_v3_api_scenarios,'api_matrix',lambda fx,fault:report)
    monkeypatch.setattr(runtime_v3_ray_matrix_process,'run_ray_api_matrix',lambda fx,fault:report)
    fixture=SimpleNamespace(provider=provider,_record_not_run=lambda name,detail:recorded.append((name,detail)))
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
