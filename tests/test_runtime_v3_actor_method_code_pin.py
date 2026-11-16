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
    from demiflow.execution.chain import plan_code_pin, snapshot_code_sources
    from demiflow.execution.run_context import RunContext
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
            refs = snapshot_code_sources(action, plan)
            helper_refs = [r for r in refs if r['source'] == str(paths[name])]
            assert len(helper_refs) == 1
            assert helper_refs[0]['sha256'] == before['dependencies'][name]['sha256']
            assert (run.directory/helper_refs[0]['path']).read_bytes() == paths[name].read_bytes()
            action.complete({'kind':'done'})
    paths[name].write_text('def delta():\n    return 9999\n')
    importlib.reload(sys.modules[name])
    assert plan_code_pin(carried)['dependencies'][name] != before['dependencies'][name]


def test_actor_dependency_walk_never_invokes_instance_or_class_descriptors(tmp_path, monkeypatch):
    from demiflow.execution.chain import _by_value_business_modules
    helper, _, name = _helper_graph(tmp_path, monkeypatch, 'passive_actor')
    touched = []

    class Descriptor:
        def __get__(self, obj, owner):
            touched.append('descriptor')
            raise AssertionError('user descriptor executed during code seal')

    class Actor:
        hostile = Descriptor()
        def __getattribute__(self, key):
            touched.append(key)
            raise AssertionError('user attribute executed during code seal')
        def __call__(self, row):
            return helper.delta()

    modules = _by_value_business_modules(Actor())
    assert name in {m.__name__ for m in modules}
    assert touched == []
