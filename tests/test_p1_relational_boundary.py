"""Pure relational assembly retains existing plans and historic source classes."""
from pathlib import Path
import os
import pickle
import subprocess
import sys

import pytest

from demiflow.data.dataset import Dataset, MaterializedDataset
from demiflow.data.plan import LimitOp, LogicalPlan, SelectColumnsOp
from demiflow.data.relational_declaration import compose_keyed_source, compose_union_sources
from demiflow.data.sources import ItemsSource, KeyedSource, LanceSource, PlanInput, UnionSource
from demiflow.execution.contracts.dataflow_errors import DataflowError


def _input(executor=None):
    return PlanInput(ItemsSource(({'key': 1}, {'key': 1}, {'key': None})), LogicalPlan(), executor)


def test_keyed_source_retains_exact_plan_inputs_without_sort_or_normalizing_rows():
    left, right = _input(object()), _input(object())
    result = compose_keyed_source(left, 'join', 'key', right=right, right_on=[], how='left', suffix='_r')
    assert type(result) is KeyedSource
    assert result.left is left and result.right is right
    assert result.left.source.items == ({'key': 1}, {'key': 1}, {'key': None})
    assert not result.left.plan.operations and not result.right.plan.operations
    assert result.on == result.right_on == ('key',)
    assert (result.how, result.suffix) == ('left', '_r')
    assert result.left.executor is left.executor and result.right.executor is right.executor


@pytest.mark.parametrize('kind', [PlanInput, KeyedSource, UnionSource])
def test_relational_declarations_keep_historical_pickle_names_and_code_pins(kind):
    from demiflow.execution.recovery_identity import describe

    old_module = 'demiflow.execution.pipeline.local_kernel'
    # Protocol 0 GLOBAL is the exact historic module/name lookup, independent
    # of where the candidate's pickler would now choose to store the class.
    old_reference = f'c{old_module}\n{kind.__name__}\n.'.encode('ascii')
    assert pickle.loads(old_reference) is kind
    assert kind.__module__ == 'demiflow.data.sources'
    assert describe(kind)['module'] == old_module
    assert describe(kind.__init__)['module'] == old_module
    assert describe(kind.__repr__)['module'] == old_module
    wrapped = getattr(kind.__repr__, '__wrapped__', None)
    if wrapped is not None:
        assert describe(wrapped)['module'] == old_module

    # An identically named class or method must keep its own module identity.
    unrelated = type(kind.__name__, (), {'__module__': kind.__module__})
    assert describe(unrelated)['module'] == kind.__module__


def test_relational_instance_seal_retains_tags_and_fields():
    from demiflow.execution.recovery_identity import describe

    item = _input()
    keyed = KeyedSource('groups', item, ('key',))
    union = UnionSource((item,))
    assert describe(keyed)['type'] == 'demiflow.execution.local_kernel.KeyedSource'
    assert describe(keyed)['fields']['on'] == ['key']
    assert describe(union)['type'] == 'demiflow.execution.local_kernel.UnionSource'
    assert describe(union)['fields']['inputs'] == [describe(item)]


def test_reduce_retains_declaration_and_copies_only_keyword_container():
    initial, left = object(), _input()
    reducer = lambda state, row, **kwargs: state
    kwargs = {'limit': 3}
    result = compose_keyed_source(left, 'reduce', ['key'], reducer=reducer, initial=initial, reducer_kwargs=kwargs)
    assert result.reducer is reducer and result.initial is initial and result.left is left
    assert result.reducer_kwargs == kwargs and result.reducer_kwargs is not kwargs
    assert result.right is None


@pytest.mark.parametrize('kind,on,options,error,message', [
    ('join', [], {'chunk_bytes': 0}, ValueError, 'nonempty key'),
    ('join', ['key'], {'chunk_bytes': 0, 'how': 'invalid'}, ValueError, 'chunk_bytes'),
    ('join', ['key'], {'how': 'invalid'}, ValueError, 'invalid join type'),
    ('join', ['key'], {'right_on': ['key', 'other']}, ValueError, 'invalid join type'),
    ('groups', ['key'], {'output': 'key'}, ValueError, 'invalid grouping'),
    ('groups', ['key'], {'max_rows': 0}, ValueError, 'invalid grouping'),
    ('reduce', ['key'], {}, TypeError, 'reducer must be callable'),
])
def test_original_parameter_rejections_precede_compiler_validation(monkeypatch, kind, on, options, error, message):
    from demiflow.data import relational_declaration
    calls = []
    monkeypatch.setattr(relational_declaration, 'validate_relational_inputs', lambda *args, **kwargs: calls.append(args))
    if kind == 'join':
        options = {'right': _input(), **options}
    with pytest.raises(error, match=message):
        compose_keyed_source(_input(), kind, on, **options)
    assert calls == []


def test_original_compiler_rejects_missing_or_extra_ports():
    with pytest.raises(DataflowError, match='join requires a right input'):
        compose_keyed_source(_input(), 'join', 'key')
    with pytest.raises(DataflowError, match='takes a single input'):
        compose_keyed_source(_input(), 'groups', 'key', right=_input())


def test_union_flattens_only_empty_plan_and_does_not_add_eager_budget_validation(monkeypatch):
    from demiflow.data import relational_declaration
    inputs = (_input(object()), _input(object()))
    nested = UnionSource(inputs)
    untransformed = PlanInput(nested, LogicalPlan(), object())
    transformed = PlanInput(nested, LogicalPlan((LimitOp(1),)), object())
    monkeypatch.setattr(relational_declaration, 'validate_relational_inputs',
                        lambda *args, **kwargs: pytest.fail('union did not preflight before P1'))
    result = compose_union_sources((untransformed, transformed, inputs[0]))
    assert type(result) is UnionSource
    assert result.inputs == (*inputs, transformed, inputs[0])
    assert all(actual is expected for actual, expected in zip(result.inputs, (*inputs, transformed, inputs[0])))
    assert result.inputs[2].plan is transformed.plan


def test_sdk_assembly_keeps_host_and_original_union_executor_defaults():
    left_host, right_host = object(), object()
    left_input, right_input = _input(left_host), _input(right_host)
    left = Dataset(left_input.source, left_input.plan, left_host)
    right = Dataset(right_input.source, right_input.plan, right_host)
    joined = left.join(right, on='key')
    assert joined._executor is left_host
    assert joined._source.left.executor is left_host and joined._source.right.executor is right_host
    assert joined._source.left.source is left._source and joined._source.right.source is right._source
    assert joined._plan.operations == ()
    union = left.union(right)
    flattened = union.union(left)
    assert flattened._executor is left_host
    assert tuple(item.source for item in flattened._source.inputs) == (left._source, right._source, left._source)
    assert all(item.executor is None for item in flattened._source.inputs)


def test_exclude_keys_preserves_lance_source_and_projects_only_right_keys():
    from demiflow.lance.model import LanceScanSpec
    host, other_host = object(), object()
    source = LanceSource(LanceScanSpec(uri='/tmp/p1-unread.lance', version=1))
    left = Dataset(source, LogicalPlan(), host)
    right = Dataset(_input().source, LogicalPlan(), other_host)
    result = left.exclude_keys(right, on='key')
    assert result._executor is host and result._source.left.source is source
    assert result._source.kind == 'exclude_keys' and result._source.how == 'anti'
    assert result._source.right.executor is other_host
    assert isinstance(result._source.right.plan.operations[-1], SelectColumnsOp)
    assert not right._plan.operations
    with pytest.raises(ValueError, match='excluding _rowid'):
        left.exclude_keys(right, on='_rowid')


def test_replace_plan_returns_original_dataset_type_with_executor_and_stages():
    host, stage = object(), object()
    original = MaterializedDataset(_input().source, LogicalPlan(), host, (stage,))
    source, plan = _input().source, LogicalPlan((LimitOp(1),))
    replacement = original.replace_plan(source, plan)
    assert type(replacement) is Dataset
    assert replacement._source is source and replacement._plan is plan
    assert replacement._executor is host and replacement._stages == (stage,)
    assert original._source is not source and not original._plan.operations


def test_relational_binding_normalizes_key_declarations_once():
    from demiflow.data.relational_declaration import keys
    assert keys('key') == ['key']
    assert keys(('key', 'other')) == ['key', 'other']


def test_cold_compose_and_native_validation_do_not_load_runtime_owners():
    root = Path(__file__).resolve().parents[1]
    program = '''
import sys
from demiflow.data.relational_declaration import compose_keyed_source, compose_union_sources
from demiflow.data.sources import ItemsSource, PlanInput
from demiflow.data.plan import LogicalPlan
from demiflow.planning.dataflow_declaration import validate_native_operation
left = PlanInput(ItemsSource(({'key': 1},)), LogicalPlan())
source = compose_keyed_source(left, 'join', 'key', right=left)
assert source.left is left
assert compose_union_sources((left,)).inputs[0] is left
assert validate_native_operation('lookup', {'key': 'key'}, bindings={'key': 'key'}) is None
for prefix in ('demiflow.execution.local_kernel', 'demiflow.execution.run_context', 'demiflow.execution.checked',
               'demiflow.execution.action_driver.dataflow', 'demiflow.execution.dataflow_api',
               'demiflow.execution.executors', 'demiflow.execution.package_loader',
               'demiflow.data.dataset', 'demiflow.data.local_relational'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
'''
    env = dict(os.environ, PYTHONPATH=str(root), PYTHONDONTWRITEBYTECODE='1')
    result = subprocess.run([sys.executable, '-c', program], cwd=root, env=env,
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
