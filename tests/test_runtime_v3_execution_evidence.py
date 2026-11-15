"""Invocation evidence accounting; business equality never proves one call."""
from types import SimpleNamespace

import pytest
import runtime_v3_support as support


def entry(observation_id=None, **changes):
    value={'provider':'local','terminal_kind':'take','action_id':'action-1','plan_id':'same-plan'}
    if observation_id is not None:value['observation_id']=observation_id
    return {**value,**changes}


def test_duplicate_observer_records_merge_without_losing_original_lists():
    outer=[entry('one'),entry('two')]
    per_case=[dict(x) for x in outer]
    assert support._merge_execution_observations(outer,per_case)==outer
    assert len(outer)==len(per_case)==2


def test_equal_business_fields_do_not_collapse_distinct_or_legacy_invocations():
    a,b=entry('one'),entry('two')
    assert support._merge_execution_observations([a],[b])==[a,b]
    legacy=entry()
    assert support._merge_execution_observations([legacy],[dict(legacy)])==[legacy,legacy]


def test_conflicting_or_invalid_observation_id_fails_closed():
    with pytest.raises(support.RuntimeV3Error,match='Conflicting'):
        support._merge_execution_observations([entry('one')],[entry('one',plan_id='different-plan')])
    for invalid in ('',False,7):
        with pytest.raises(support.RuntimeV3Error,match='Invalid execution observation'):
            support._merge_execution_observations([entry(invalid)])


def test_real_nested_recorders_observe_one_action_once_each(tmp_path):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        with support._ExecuteActionRecorder() as outer:
            with support._ExecuteActionRecorder() as inner:
                assert data.from_items([{'n':1},{'n':2}]).take_all()==[{'n':1},{'n':2}]
        assert len(outer.calls)==len(inner.calls)==1
        assert outer.calls==inner.calls
        assert len(support._merge_execution_observations(outer.calls,inner.calls))==1
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM runtime_actions').fetchone()[0]==1
        commits=run.journal.execution_commits('action-1')
        assert commits and all(c['status']=='acked' for c in commits)


def test_nested_facade_invocations_get_separate_observation_identities(monkeypatch):
    # Counting-only component boundary, not a business acceptance substitute:
    # simulate a facade that itself calls the public facade again. This proves
    # identity sharing is limited to observer wrappers, not dynamic call scope.
    from demiflow.execution import dataflow
    from demiflow.execution import dataflow_api
    calls=[]
    request=SimpleNamespace(provider='local',kind='take',action=SimpleNamespace(action_id='action-1'))
    def facade(request,depth=0):
        calls.append(depth)
        if depth==0:dataflow.execute_action(request,depth=1)
        return SimpleNamespace(plan_id='same-plan')
    monkeypatch.setattr(dataflow,'execute_action',facade)
    monkeypatch.setattr(dataflow_api,'execute_action',facade)
    with support._ExecuteActionRecorder() as outer,support._ExecuteActionRecorder() as inner:
        dataflow_api.execute_action(request)
    assert calls==[0,1]
    assert len(outer.calls)==len(inner.calls)==2
    assert len({c['observation_id'] for c in outer.calls})==2
    assert len(support._merge_execution_observations(outer.calls,inner.calls))==2


def test_fixture_preserves_both_observers_but_counts_one_real_action(runtime_v3_fixture,monkeypatch):
    from demiflow import data
    def one_real_case(fx,fault):
        with support._ExecuteActionRecorder() as recorder:
            rows=data.from_items([{'n':3}]).take_all()
        return {'rows':rows,'child_execute_action_calls':list(recorder.calls)}
    # One real public call under the original fixture's RunContext, blockers,
    # tree binding and resource cleanup; only the scenario length is narrowed.
    monkeypatch.setitem(support._SCENARIO_FUNCS,'no_ray_data',one_real_case)
    result=runtime_v3_fixture.run('no_ray_data')
    assert result['rows']==[{'n':3}]
    assert len(result['execute_action_calls'])==1
    observations=result['execute_action_observations']
    assert observations['outer']==observations['case_or_child']==result['execute_action_calls']
    assert result['ray_data_import_attempts']==[]
