"""Actual prefetch admission and real external effect journal safety."""
import sys
import pytest
from demiflow.planning.dataflow_graph_adapter import LEGACY_PREFETCH, derive_prefetch
from demiflow.execution.contracts.errors import ReservationError
from demiflow.operator_llm.journal import PromptJournal, UncertainPromptCall
pytestmark_linux = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local managed units require Linux")

def test_derive_prefetch_default_equivalence():
    assert derive_prefetch(None) == LEGACY_PREFETCH
    assert LEGACY_PREFETCH == {"append_slots": 64, "source_batch_size": 256,
                               "queue_depth": 4096, "max_pending": 1024}
    assert derive_prefetch(10 ** 9) == LEGACY_PREFETCH  # never above the legacy ceiling


def test_derive_prefetch_caps_to_admitted_budget():
    assert derive_prefetch(8) == {"append_slots": 8, "source_batch_size": 8,
                                  "queue_depth": 8, "max_pending": 8}
    assert derive_prefetch(100) == {"append_slots": 64, "source_batch_size": 100,
                                    "queue_depth": 100, "max_pending": 100}
    for bad in (0, -1, "8", 1.5):
        with pytest.raises(ReservationError) as exc:
            derive_prefetch(bad)
        assert exc.value.code == "INVALID_SPEC"


def test_derive_prefetch_max_pending_protocol_floor():
    # A protocol floor larger than admission must be rejected before sealing.
    with pytest.raises(ReservationError, match="floor exceeds"):
        derive_prefetch(4, max_pending_floor=64)
    assert derive_prefetch(4)["max_pending"] == 4  # default floor is 1
    assert derive_prefetch(200, max_pending_floor=64)["max_pending"] == 200
    with pytest.raises(ReservationError):
        derive_prefetch(4, max_pending_floor=0)


def _request(tag):
    return {"protocol": "openai-chat", "model_contract": {"model": "m"}, "body": {"prompt": tag}}


def _mini_graph_spec(prefetch_slots=None):
    from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
    from demiflow.data.sources import ItemsSource

    def double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    def plan(fn):
        return LogicalPlan((MapOp(CallableSpec.create(fn),
                                  stream_options=MapStreamOptions(concurrency=1)),))

    n = 24
    readers = ["c0", "c1"]
    spec = {
        "source": ItemsSource(tuple({"x": i} for i in range(n))), "source_id": "rsv-source",
        "ancestor": {"plan": plan(double), "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": [{"plan": plan(passthrough), "logical_node_id": "node-" + rid,
                     "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers],
        "routing": "broadcast", "exchange_mode": "memory_incremental",
        "credit_budget": {"rows": 64, "bytes": 8 * 1024 ** 2}, "queue_depth": 4096,
    }
    if prefetch_slots is not None:
        spec["prefetch_slots"] = prefetch_slots
    return spec, readers, n


def _run_graph(tmp_path, spec):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.run_context import RunContext
    executor = LocalDatasetExecutor(workers=1)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 8, "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "rsv"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", workers=result["workers"]))
        from demiflow.execution.seal.sealed_plans import load_sealed_plan
        sealed = load_sealed_plan(run.directory, execution.action_id, version='v3')
    router = next(node.occurrence_id for node in sealed['graph'].nodes if 'graph_route' in node.kernel.configuration)
    routed = {edge.edge_id for edge in sealed['graph'].edges if router in (edge.producer, edge.consumer)}
    return result, [exchange for exchange in sealed['physical'].exchanges if exchange.edge_id in routed]


@pytest.fixture
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


@pytestmark_linux
def test_graph_configured_prefetch_binds_published_credit(tmp_path, pidfds_available):
    spec, readers, n = _mini_graph_spec(prefetch_slots=4)
    with pytest.raises(ReservationError, match="explicit credit rows exceed"):
        _run_graph(tmp_path / "invalid", spec)
    del spec["credit_budget"]
    result, exchanges = _run_graph(tmp_path / "admitted", spec)
    assert result["egress_rows"] == n
    assert result["target_rows"] == dict.fromkeys(readers, n)
    assert exchanges and all(0 < exchange.credit_rows <= 4 for exchange in exchanges)


@pytestmark_linux
def test_graph_default_explicit_credit_visible_in_published_plan(tmp_path, pidfds_available):
    spec, readers, n = _mini_graph_spec()
    result, exchanges = _run_graph(tmp_path, spec)
    assert result["target_rows"] == dict.fromkeys(readers, n)
    assert exchanges and all(0 < exchange.credit_rows <= spec["credit_budget"]["rows"] for exchange in exchanges)


def test_effect_known_completed_reuse_consumes_no_new_budget(tmp_path):
    journal = PromptJournal(tmp_path / 'journal')
    request = _request('hello')
    assert journal.lookup(request) is None
    assert journal.reserve(request) is True
    result = {'text': 'world'}
    journal.response(request, result)
    assert journal.lookup(request) == result
    assert journal.reserve(request) is False
    reopened = PromptJournal(tmp_path / 'journal')
    assert reopened.lookup(request) == result
    assert reopened.reserve(request) is False

def test_effect_uncertain_stays_charged_no_blind_retry(tmp_path):
    journal = PromptJournal(tmp_path / 'journal')
    request = _request('uncertain')
    assert journal.reserve(request) is True
    reopened = PromptJournal(tmp_path / 'journal')
    with pytest.raises(UncertainPromptCall):
        reopened.lookup(request)
    with pytest.raises(UncertainPromptCall):
        reopened.reserve(request)
