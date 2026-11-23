"""Runtime registration cannot impersonate actual bound model implementations."""
import pytest

from demiflow.execution.contracts.dataflow import (
    BoundImageWriter, PrivatePromptJournalPort, PromptJournalPort,
)
from demiflow.operator_llm.model import PromptModel


@pytest.mark.parametrize('port', [PromptJournalPort, PrivatePromptJournalPort])
@pytest.mark.parametrize('target', ['http', 'codex', 'offline'])
@pytest.mark.parametrize('masquerade', ['virtual', 'class_attribute'])
def test_model_clients_refuse_non_inheriting_journal(port, target, masquerade):
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.operator_llm.codex_exec import CodexExecPromptClient
    from demiflow.operator_llm.offline import RemoteOfflinePromptClient

    class Unbound:
        read_only = False
        limit = 1
        declaration = None
        route = None

    if masquerade == 'virtual':
        port.register(Unbound)
    else:
        Unbound = type('Disguised', (Unbound,), {'__class__': property(lambda self: port)})
    value = Unbound()
    assert isinstance(value, port)  # The ordinary ABC check is insufficient.
    assert port not in type(value).__mro__
    model = PromptModel('fixture', 'openai_compatible', base_url='http://127.0.0.1:1')
    with pytest.raises(ValueError, match='journal|port'):
        if target == 'http':
            AsyncOperatorLLMClient(model, {'trust_env': False}, native_journal=value)
        elif target == 'codex':
            CodexExecPromptClient(model, {'codex_exec': {}}, native_journal=value)
        else:
            RemoteOfflinePromptClient(model, {'offline_dir': '/unused'}, value)


@pytest.mark.parametrize('masquerade', ['virtual', 'class_attribute'])
def test_image_algorithm_refuses_non_inheriting_writer(masquerade):
    from demiflow.operator_llm.media import store_image
    called = []

    class Unbound:
        def store_image(self, raw, options):
            called.append(raw)

    if masquerade == 'virtual':
        BoundImageWriter.register(Unbound)
    else:
        Unbound = type('Disguised', (Unbound,), {'__class__': property(lambda self: BoundImageWriter)})
    value = Unbound()
    assert isinstance(value, BoundImageWriter)
    with pytest.raises(TypeError, match='bound image writer'):
        store_image(b'not executed', {}, store=value)
    assert called == []
