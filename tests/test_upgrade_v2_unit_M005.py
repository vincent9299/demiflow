"""M005/M009/M010: ctor field, and finite native drain timeout."""
from types import SimpleNamespace

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.embeddings.runtime import EmbeddingActor
from demiflow.embeddings.model import EmbeddingModel
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery


def _actor(**options):
    model = EmbeddingModel(name='m', revision='r', dimensions=4, base_url='http://localhost:9/v1')
    return EmbeddingActor(model=model, inputs={'text': 'text'}, output='vector',
        call_output='call', error_output=None, concurrency=1, label='t',
        options={'timeout_s': 30, 'max_response_bytes': 4096, **options},
        max_requests=None, service=None, request_gate=None)


def _action(run):
    return SimpleNamespace(context=run, action_id='act-1', object_sinks={})


def test_ctor_initializes_native_recovery_none():
    assert _actor()._native_recovery is None


def test_cancel_drain_timeout_no_effect_path_is_zero():
    assert _actor().cancel_drain_timeout_s == 0


def test_cancel_drain_timeout_private_journal_formula():
    actor = _actor(sqlite_journal={'path': '/tmp/demiflow-m010-private.sqlite', 'timeout_s': 10})
    assert actor.cancel_drain_timeout_s == 30 + 3 * 10 + 1


def test_cancel_drain_timeout_native_is_finite(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        actor._native_recovery = NativeEmbeddingRecovery(actor, _action(run), 'node')
        # sqlite_journal is None yet a native journal exists: must not return 0.
        assert actor.options['sqlite_journal'] is None
        assert actor.cancel_drain_timeout_s == 30 + 3 * actor._native_recovery.journal.timeout_s + 1
        assert actor.cancel_drain_timeout_s > 0
