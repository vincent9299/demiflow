"""By-value actor methods carry the same bounded sealed import authority."""
import importlib
import json
import sys

import cloudpickle
import pytest

from test_f7b_closure_dependency import (
    FORMS, _assert_capture_resume_code_pin, _helper_graph,
)
from test_stage2_unified_plan import _map_plan


def _method_capture(transform, trace):
    class MethodCapture:
        def __call__(self, row):
            with trace.open('a') as stream:
                stream.write(str(row['x']) + '\n')
            return transform(row)

        def snapshot_state(self):
            return {}

        def restore_state(self, state):
            assert state == {}

    return MethodCapture()


@pytest.mark.parametrize('form', FORMS)
def test_method_closure_code_edit_rejected_before_call(tmp_path, monkeypatch, form):
    _assert_capture_resume_code_pin(tmp_path, monkeypatch, form, _method_capture)


@pytest.mark.parametrize('member', ['method', 'staticmethod', 'classmethod', 'property', 'inherited'])
def test_serialized_actor_members_pin_same_sources_as_archive(tmp_path, monkeypatch, member):
    from runtime_v3_program_probe import program_store, stage_callable
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.seal.business_sources import plan_code_pin
    from demiflow.execution.pipeline.run_context import RunContext
    helper, paths, name = _helper_graph(tmp_path, monkeypatch, 'actor_' + member)

    class Actor:
        def method(self):
            return helper.delta()

        @staticmethod
        def static():
            return helper.delta()

        @classmethod
        def classmethod(cls):
            return helper.delta()

        @property
        def property(self):
            return helper.delta()

        def __call__(self, row):
            return row

    # Keep only the selected dependency edge. An inherited method is carried
    # through the by-value base; none of these descriptors is invoked to seal.
    for candidate, attr in [('method', 'method'), ('staticmethod', 'static'),
                            ('classmethod', 'classmethod'), ('property', 'property')]:
        if candidate != member and not (member == 'inherited' and candidate == 'method'):
            delattr(Actor, attr)
    if member == 'inherited':
        class Derived(Actor):
            pass
        actor = Derived()
    else:
        actor = Actor()
    plan = _map_plan(actor)
    before = plan_code_pin(plan)
    assert name in before['dependencies']
    carried = cloudpickle.loads(cloudpickle.dumps(plan))
    assert plan_code_pin(carried) == before
    with RunContext.start(tmp_path/'archive') as run:
        with run.action({'kind':'code-pin-fixture'}) as action:
            with program_store(action) as (store, bounds):
                _, manifest = stage_callable(plan.operations[0].callable, store=store, bounds=bounds)
                ref = ArtifactRef(**manifest['business_sources'][name])
                assert ref.digest == before['dependencies'][name]['sha256']
                assert b''.join(store.read(ref, max_bytes=ref.size_bytes)) == paths[name].read_bytes()
            action.complete({'kind':'done'})
    paths[name].write_text('def delta():\n    return 9999\n')
    importlib.reload(sys.modules[name])
    assert plan_code_pin(carried)['dependencies'][name] != before['dependencies'][name]


def test_actor_dependency_walk_never_invokes_instance_or_class_descriptors(tmp_path, monkeypatch):
    from demiflow.execution.seal.business_sources import _by_value_business_modules
    helper, _, name = _helper_graph(tmp_path, monkeypatch, 'passive_actor')
    touched = []

    class Descriptor:
        def __get__(self, obj, owner):
            touched.append('descriptor')
            raise AssertionError('user descriptor executed during code seal')

    class Meta(type):
        def __getattribute__(cls, key):
            touched.append('metaclass:' + key)
            raise AssertionError('metaclass lookup executed during code seal')

    class Actor(metaclass=Meta):
        hostile = Descriptor()
        def __getattribute__(self, key):
            touched.append(key)
            raise AssertionError('user attribute executed during code seal')
        def __call__(self, row):
            return helper.delta()

    modules = _by_value_business_modules(Actor())
    assert name in {m.__name__ for m in modules}
    assert touched == []


@pytest.mark.parametrize('declaration', ['dict', 'private'])
def test_actual_actor_slots_pin_modules_without_getattribute(tmp_path, monkeypatch, declaration):
    from demiflow.execution.seal.business_sources import _by_value_business_modules
    helper, _, name = _helper_graph(tmp_path, monkeypatch, 'passive_slots')
    touched = []
    if declaration == 'dict':
        class Actor:
            __slots__ = {'module': 'Original dependency slot'}
    else:
        class Actor:
            __slots__ = ('__module_capture',)
    actor = Actor()
    slot = 'module' if declaration == 'dict' else '_Actor__module_capture'
    object.__setattr__(actor, slot, helper)
    def forbidden(self, key):
        touched.append(key)
        raise AssertionError('slot walk executed business lookup')
    Actor.__getattribute__ = forbidden
    assert name in {m.__name__ for m in _by_value_business_modules(actor)}
    assert touched == []


def test_importable_static_method_retains_by_reference_source_pin(tmp_path, monkeypatch):
    from demiflow.execution.seal.business_sources import _is_by_reference_udf, _business_dependency_manifest
    from test_stage2_unified_plan import _load_modules
    name = 'static_reference_fixture'
    source = "class Actor:\n    @staticmethod\n    def transform(row):\n        return {'x': row['x'] + 1}\n"
    module, _ = _load_modules(tmp_path, monkeypatch, {name: source}, name)
    function = module.Actor.transform
    assert cloudpickle.loads(cloudpickle.dumps(function)) is function
    assert _is_by_reference_udf(function)
    assert name in _business_dependency_manifest(_map_plan(function))
