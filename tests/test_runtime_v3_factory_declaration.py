"""Factory construction bindings cannot hide borrowed caller-owned state."""
from collections.abc import Mapping

import pytest

from demiflow import data
from demiflow.data.plan import CallableSpec
from demiflow.execution.dataflow_seal import _owned_factory_declaration
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan


class NewOnlyFactory:
    def __new__(cls, shared):
        shared.append('construct')
        owner = object.__new__(cls)
        owner.shared = shared
        return owner

    async def astart(self):
        self.shared.append('start')

    def __call__(self, row):
        self.shared.append('call')
        return {**row, 'shared_identity': id(self.shared)}

    async def astop(self):
        self.shared.append('stop')

    async def aclose(self):
        self.shared.append('close')


def _arguments(shared, keyword):
    return {'fn_constructor_kwargs': {'shared': shared}} if keyword else {'fn_constructor_args': [shared]}


@pytest.mark.parametrize('keyword', [False, True])
def test_new_only_factory_constructor_state_is_checked_without_user_code(keyword):
    shared = []
    specification = CallableSpec.create(NewOnlyFactory, **_arguments(shared, keyword))
    assert _owned_factory_declaration(specification) is False
    assert shared == []


@pytest.mark.parametrize('keyword', [False, True])
def test_new_only_factory_keeps_actual_shared_identity_and_lifecycle(tmp_path, keyword):
    shared = []
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(worker_mode='thread'):
        result = data.from_items([{'id': 1}]).map(NewOnlyFactory, execution='thread',
            callable_scope='worker', **_arguments(shared, keyword)).take_all()
        assert result == [{'id': 1, 'shared_identity': id(shared)}]
        assert shared == ['construct', 'start', 'call', 'stop', 'close']
        sealed = load_sealed_plan(run.ref.directory, 'action-1')
        node = next(n for n in sealed['graph'].nodes if n.kernel.kind == 'MapOp')
        assert node.kernel.configuration['thread_attempt'] is True
        assert sealed['physical'].recovery == 'attempt_only'
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_factory_classification_never_iterates_unknown_constructor_mapping():
    calls = []
    class UnknownMapping(Mapping):
        def __iter__(self):
            calls.append('iter')
            raise AssertionError('classification executed user mapping')
        def __getitem__(self, key):
            calls.append('getitem')
            raise AssertionError('classification executed user mapping')
        def __len__(self):
            calls.append('len')
            raise AssertionError('classification executed user mapping')
    specification = CallableSpec(NewOnlyFactory, constructor_kwargs=UnknownMapping())
    assert _owned_factory_declaration(specification) is False
    assert calls == []
