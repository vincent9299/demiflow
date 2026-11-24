"""Runtime registration cannot impersonate actual bound model implementations."""
import pytest

from demiflow.execution.contracts.dataflow import (
    BoundImageWriter, PrivatePromptJournalPort, PromptJournalPort,
)
from demiflow.operator_llm.model import PromptModel


def masquerading_type(cls, port, kind):
    if kind == 'virtual':
        port.register(cls)
        return cls
    if kind == 'class_attribute':
        return type('Disguised', (cls,), {'__class__': property(lambda self: port)})
    if kind == 'metaclass_attribute':
        class Pretend(type):
            def __getattribute__(self, name):
                return (port, object) if name == '__mro__' else super().__getattribute__(name)
    elif kind == 'metaclass_property':
        class Pretend(type):
            @property
            def __mro__(self):
                return (port, object)
    else:
        class Pretend(type):
            def __eq__(self, other):
                return True
            __hash__ = type.__hash__
    return Pretend('Disguised', (cls,), {})


@pytest.mark.parametrize('port', [PromptJournalPort, PrivatePromptJournalPort])
@pytest.mark.parametrize('target', ['http', 'codex', 'offline'])
@pytest.mark.parametrize('masquerade', ['virtual', 'class_attribute', 'metaclass_attribute', 'metaclass_property', 'metaclass_equality'])
def test_model_clients_refuse_non_inheriting_journal(port, target, masquerade):
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.operator_llm.codex_exec import CodexExecPromptClient
    from demiflow.operator_llm.offline import RemoteOfflinePromptClient

    class Unbound:
        read_only = False
        limit = 1
        declaration = None
        route = None

    value = masquerading_type(Unbound, port, masquerade)()
    assert not any(base is port for base in type.__dict__['__mro__'].__get__(type(value)))
    model = PromptModel('fixture', 'openai_compatible', base_url='http://127.0.0.1:1')
    with pytest.raises(ValueError, match='journal|port'):
        if target == 'http':
            AsyncOperatorLLMClient(model, {'trust_env': False}, native_journal=value)
        elif target == 'codex':
            CodexExecPromptClient(model, {'codex_exec': {}}, native_journal=value)
        else:
            RemoteOfflinePromptClient(model, {'offline_dir': '/unused'}, value)


@pytest.mark.parametrize('masquerade', ['virtual', 'class_attribute', 'metaclass_attribute', 'metaclass_property', 'metaclass_equality'])
def test_image_algorithm_refuses_non_inheriting_writer(masquerade):
    from demiflow.operator_llm.media import store_image
    called = []

    class Unbound:
        def store_image(self, raw, options):
            called.append(raw)

    value = masquerading_type(Unbound, BoundImageWriter, masquerade)()
    with pytest.raises(TypeError, match='bound image writer'):
        store_image(b'not executed', {}, store=value)
    assert called == []
