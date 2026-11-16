"""Pure helper defaults are code declarations; mutable helper state is not."""
import pytest

from demiflow.execution.kernels.task_scope import function_residency_kind


def test_immutable_helper_defaults_do_not_invent_a_state_owner():
    def helper(row, offset=2, *, label='result'):
        return {label: row['n'] + offset}
    def caller(row):
        return helper(row)
    defaults, keywords = helper.__defaults__, helper.__kwdefaults__
    assert function_residency_kind(caller) == 'none'
    assert caller({'n': 3}) == {'result': 5}
    assert helper.__defaults__ is defaults and helper.__kwdefaults__ is keywords


@pytest.mark.parametrize('kind', ['positional', 'keyword', 'attribute', 'closure'])
def test_mutable_helper_data_still_requires_an_explicit_owner(kind):
    if kind == 'positional':
        def helper(row, state=[]):
            state.append(row)
    elif kind == 'keyword':
        def helper(row, *, state={}):
            state.update(row)
    elif kind == 'attribute':
        def helper(row):
            return row
        helper.state = []
    else:
        state = []
        def helper(row):
            state.append(row)
    def caller(row):
        return helper(row)
    assert function_residency_kind(caller) == 'nonrecoverable'
