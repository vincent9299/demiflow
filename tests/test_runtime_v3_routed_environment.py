"""Declaration-selected routed credentials; values never enter the plan."""
from dataclasses import replace
from pathlib import Path
import time
import pytest

from demiflow import data
from demiflow.data.plan import CallableSpec
from demiflow.execution.operator_bindings.dataflow_model_factory import native_environment_names
from demiflow.execution.hosts.environment import allocation_environment
from demiflow.execution.operator_bindings.dataflow_model_factory import native_model_declaration, PromptFactory, RoutedPromptFactory, CacheFactory
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK


def pipeline():
    return data.from_items([{'item':'test','route':'a'}]).map_prompt_async('enrich',config=parse_prompt_pack(PACK),
        inputs={'payload':'item'},output='answer',concurrency=1,max_requests=1,route='route',
        routes={name:{'config':parse_prompt_pack(PACK.replace('DF_V3_NATIVE_TEST_URL','ROUTE_'+name.upper()+'_URL')
            .replace('DF_V3_NATIVE_TEST_KEY','ROUTE_'+name.upper()+'_KEY')),'concurrency':1,'max_requests':1}
            for name in ('a','b')})


def declaration():
    return native_model_declaration(pipeline()._plan.operations[-1])


@pytest.fixture(autouse=True)
def owned(tmp_path,monkeypatch):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))


def test_exact_routed_children_select_the_union_of_declared_names_without_starting_actors(monkeypatch):
    spec=declaration()
    assert type(spec) is CallableSpec and spec.target is RoutedPromptFactory
    assert all(type(child) is CallableSpec and child.target is PromptFactory for child in spec.constructor_args[0].values())
    def forbidden(*args,**kwargs):raise AssertionError('environment preflight instantiated a user actor')
    monkeypatch.setattr(CallableSpec,'instantiate',forbidden)
    assert native_environment_names(spec)==('ROUTE_A_KEY','ROUTE_A_URL','ROUTE_B_KEY','ROUTE_B_URL')


def test_real_sealed_assignment_projects_only_its_routed_credentials(tmp_path,monkeypatch):
    from test_runtime_v3_real_seams import prepared
    from demiflow.execution.contracts.dataflow import WorkerLease
    names=('ROUTE_A_KEY','ROUTE_A_URL','ROUTE_B_KEY','ROUTE_B_URL')
    for name in names:monkeypatch.setenv(name,'owned-test-value')
    monkeypatch.setenv('UNRELATED_TEST_SECRET','never-forward-test-value')
    with prepared(tmp_path,pipeline(),kind='take_all',arguments={'limit':None}) as (execution,services):
        node=next(n for n in execution._graph.nodes if n.kernel.kind=='prompt')
        stage=next(s for s in execution._physical.stages if node.occurrence_id in s.occurrences)
        # This typed assignment is used only for environment contract checking;
        # actual host/grant/activation proof is the separate real Ray gate.
        lease=WorkerLease('declaration-check','declaration-boot',stage.stage_id,stage.partitions,
            'local','declaration-check',execution._sealed.code_pin_ref,int(time.time()*1000)+60000)
        assignment=execution._assignment(lease,stage)
        first=allocation_environment(assignment,store=services.store,bounds=services.bounds)
        assert set(first)==set(names) and 'UNRELATED_TEST_SECRET' not in first
        sealed=execution._sealed
        monkeypatch.setenv('ROUTE_A_KEY','rotated-owned-test-value')
        second=allocation_environment(assignment,store=services.store,bounds=services.bounds)
        assert second['ROUTE_A_KEY']!=first['ROUTE_A_KEY']
        assert execution._sealed==sealed
        raw=b''.join(services.store.read(sealed.code_pin_ref,max_bytes=services.bounds.max_control_bytes))
        assert b'owned-test-value' not in raw


@pytest.mark.parametrize('fault',['foreign_child','empty','too_many','bad_name','missing_args'])
def test_malformed_routed_environment_declaration_fails_closed(fault):
    spec=declaration();args=list(spec.constructor_args);children=dict(args[0])
    child=next(iter(children.values()))
    if fault=='foreign_child':children['a']=CallableSpec.create(str)
    elif fault=='empty':children={}
    elif fault=='too_many':children={str(i):child for i in range(33)}
    elif fault=='bad_name':children={1:child}
    args[0]=children
    if fault=='missing_args':args=[]
    changed=CallableSpec.create(RoutedPromptFactory,fn_constructor_args=tuple(args))
    with pytest.raises(DataflowError,match='INVALID_CONTRACT'):native_environment_names(changed)


def test_total_routed_environment_union_keeps_the_original_128_name_limit():
    original=declaration();base=next(iter(original.constructor_args[0].values()))
    operation,options=base.constructor_args
    children={}
    for route in range(32):
        prompts=tuple(replace(operation.config.prompts[0],name='p'+str(i),model=replace(
            operation.config.prompts[0].model,base_url_env=f'R{route}_P{i}_URL',api_key_env=f'R{route}_P{i}_KEY')) for i in range(3))
        child=replace(operation,config=replace(operation.config,prompts=prompts))
        children[str(route)]=CallableSpec.create(PromptFactory,fn_constructor_args=(child,options))
    args=list(original.constructor_args);args[0]=children
    spec=CallableSpec.create(RoutedPromptFactory,fn_constructor_args=tuple(args))
    with pytest.raises(DataflowError,match='environment names'):native_environment_names(spec)


def test_nested_native_declarations_keep_the_original_depth_limit():
    spec=declaration()
    for _ in range(5):spec=CallableSpec.create(CacheFactory,fn_constructor_args=(spec,))
    with pytest.raises(DataflowError,match='depth bound'):native_environment_names(spec)


@pytest.mark.parametrize('storage', ['offline_dir', 'offline_store'])
def test_offline_routes_require_no_online_credentials_but_keep_online_siblings(storage, monkeypatch):
    spec = declaration()
    children, *rest = spec.constructor_args
    offline_operation, settings = children['a'].constructor_args
    options = {storage: '/tmp/demiflow-offline-fixture' if storage == 'offline_dir'
               else {'path': '/tmp/demiflow-offline-fixture.sqlite'}}
    offline = CallableSpec.create(PromptFactory, fn_constructor_args=(
        replace(offline_operation, options=options, max_requests=0), settings))
    def forbidden(*args, **kwargs):
        raise AssertionError('environment selection must not instantiate clients or actors')
    monkeypatch.setattr(CallableSpec, 'instantiate', forbidden)
    assert native_environment_names(offline) == ()
    mixed = CallableSpec.create(RoutedPromptFactory, fn_constructor_args=({'a': offline, 'b': children['b']}, *rest))
    assert native_environment_names(mixed) == ('ROUTE_B_KEY', 'ROUTE_B_URL')
    all_offline = CallableSpec.create(RoutedPromptFactory, fn_constructor_args=({'a': offline, 'b': offline}, *rest))
    assert native_environment_names(all_offline) == ()
    cached = CallableSpec.create(CacheFactory, fn_constructor_args=(all_offline,))
    assert native_environment_names(cached) == ()
    # max_requests=0 alone is not an offline declaration: no blanket bypass.
    online_zero = CallableSpec.create(PromptFactory, fn_constructor_args=(
        replace(offline_operation, max_requests=0), settings))
    assert native_environment_names(online_zero) == ('ROUTE_A_KEY', 'ROUTE_A_URL')
