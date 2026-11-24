"""State adapters: the caller-side precheck and the native adapters' snapshot/restore round trip."""
import sys

import pytest

from demiflow.execution.compiler_support.operator_policy import classify_operator_state
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _passthrough(row):
    return row


# --- Caller-side K2 precheck firing (authorization boundary) --------------------

class _StatefulNoAdapter:
    def __init__(self):
        self.value = 0

    def __call__(self, row):
        self.value += 1
        return row


class _AdapterActor:
    def __init__(self):
        self.value = 0

    def __call__(self, row):
        self.value += 1
        return row

    def snapshot_state(self):
        return {"value": self.value}

    def restore_state(self, state):
        self.value = state["value"]


def test_precheck_rejects_stateful_owner_without_adapter():
    policy = classify_operator_state(_StatefulNoAdapter(), phase='instance', ownership='partitionable')
    assert policy.mode == 'unsupported' and not policy.adapter_present


def test_precheck_hookless_state_is_auto_json_not_stateless():
    policy = classify_operator_state(_StatefulNoAdapter(), phase='instance')
    assert policy.mode == 'auto_json' and policy.ownership == 'shared_colocated'


def test_precheck_accepts_stateless_function_and_adapter_actor():
    assert classify_operator_state(_passthrough, phase='declaration').mode == 'function'
    for ownership in ('partitionable', 'none'):
        policy = classify_operator_state(_AdapterActor(), phase='instance', ownership=ownership)
        assert policy.mode == 'hooks' and policy.adapter_present


def test_precheck_plan_accepts_adapter_actor_stream(tmp_path):
    from demiflow import data
    with RunContext.start(tmp_path / "run", limits={"checkpoint_rows": 2,
                                                   "checkpoint_interval_ms": 60000}):
        result = data.from_items([{"id": i} for i in range(6)]).map_async(
            _AdapterActor(), concurrency=1).materialize().take_all()
    assert sorted(result, key=lambda r: r["id"]) == [{"id": i} for i in range(6)]


# --- Native {} adapter round-trips (no resource handle serialized as state) -----

def _bare(cls, **attrs):
    instance = object.__new__(cls)
    for key, value in attrs.items():
        setattr(instance, key, value)
    return instance


@pytest.mark.parametrize("factory", [
    lambda: _bare(__import__("demiflow.objects", fromlist=["ReadLocalObject"]).ReadLocalObject),
    lambda: _bare(__import__("demiflow.collect.wiki_syntax", fromlist=["ParseWikitext"]).ParseWikitext),
    lambda: _bare(__import__("demiflow.collect.documents", fromlist=["DecodeDocument"]).DecodeDocument),
    lambda: _bare(__import__("demiflow.collect.image_resize", fromlist=["EncodeImage"]).EncodeImage),
    lambda: _bare(__import__("demiflow.collect.operators", fromlist=["SearchWeb"]).SearchWeb),
    lambda: _bare(__import__('demiflow.execution.native_queue.raw_fetch', fromlist=["RawFetch"]).RawFetch),
    lambda: _bare(__import__('demiflow.execution.native_queue.queue_producer', fromlist=["QueueProducer"]).QueueProducer),
    lambda: _bare(__import__("demiflow.operator_llm.runtime", fromlist=["PromptActor"]).PromptActor),
    lambda: _bare(__import__("demiflow.embeddings.runtime", fromlist=["EmbeddingActor"]).EmbeddingActor),
    lambda: _bare(__import__('demiflow.operators.lookup', fromlist=["LanceLookup"]).LanceLookup,
                  cache=__import__("collections").OrderedDict(), cache_bytes=0),
])
def test_native_adapter_snapshot_restore_round_trip(factory):
    actor = factory()
    snapshot = actor.snapshot_state()
    assert snapshot == {}
    actor.restore_state(snapshot)
    actor.restore_state({})
    with pytest.raises((ValueError, RecoveryContractError)):
        actor.restore_state({"semantic": "state"})


def test_embedding_adapter_declares_empty_semantic_state():
    from demiflow.embeddings.runtime import EmbeddingActor
    actor = _bare(EmbeddingActor)
    assert actor.snapshot_state() == {}
    actor.restore_state({})
    with pytest.raises(ValueError, match="no mutable semantic state"):
        actor.restore_state({"calls": 1})
