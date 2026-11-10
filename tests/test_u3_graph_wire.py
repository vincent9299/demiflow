"""U3 graph-wire conformance: additive D3/D4 schemas (S3).

The graph cut command carries ``final`` outside the closed K3 CheckpointRequest;
cut contributions are K6-shaped fragments (channel_cuts/operator_states/inflight)
ready for full-coverage assembly; capacity requests escalate credit/storage.
All three live in execution/graph_wire.py, never in the byte-frozen contracts/.
"""
import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.worker import validate_payload
from demiflow.execution.graph_wire import (
    CAPACITY_REQUEST_SCHEMA, CUT_CONTRIBUTION_SCHEMA, GRAPH_CHECKPOINT_SCHEMA,
    build_graph_checkpoint, validate_capacity_request, validate_cut_contribution,
    validate_graph_checkpoint,
)


def _code(exc):
    return exc.value.code


def _contribution(**overrides):
    value = {"schema_version": CUT_CONTRIBUTION_SCHEMA, "checkpoint_id": "ck-1",
             "allocation_id": "alloc-1", "channel_cuts": [
                 {"channel_id": "ch-1", "producer_cut_sequence": 3,
                  "consumer_offsets": {"reader-1": 2}}],
             "operator_states": [], "inflight": [], "output_descriptor": None,
             "source_progress": [], "final": False}
    value.update(overrides)
    return value


def test_wire_graph_checkpoint_roundtrip_and_final_flag():
    value = build_graph_checkpoint("ck-1", epoch=1, producer_sequence_bound=4, final=True)
    assert value["schema_version"] == GRAPH_CHECKPOINT_SCHEMA
    assert value["cut"]["producer_sequence_bound"] == 4 and value["final"] is True
    assert validate_graph_checkpoint(value) == value


def test_wire_k3_checkpoint_request_stays_closed_no_final():
    # D3 rationale: K3 payload key set is exactly closed; 'final' cannot ride K3.
    valid = {"checkpoint_id": "ck-1", "epoch": 1, "cut": {"producer_sequence_bound": 4},
             "topology_revision": 0, "deadline_ms": 60000}
    validate_payload("CheckpointRequest", valid)
    with pytest.raises(ProtocolError) as exc:
        validate_payload("CheckpointRequest", dict(valid, final=True))
    assert _code(exc) == "INVALID_SPEC"


def test_wire_graph_checkpoint_rejects_bad_shapes():
    base = build_graph_checkpoint("ck-1", epoch=1, producer_sequence_bound=4)
    with pytest.raises(ProtocolError):
        validate_graph_checkpoint(dict(base, final="yes"))
    with pytest.raises(ProtocolError):
        validate_graph_checkpoint(dict(base, cut={"producer_sequence_bound": -1}))
    with pytest.raises(ProtocolError):
        validate_graph_checkpoint(dict(base, schema_version="demiflow.graph_checkpoint.v9"))
    with pytest.raises(ProtocolError):
        validate_graph_checkpoint(dict(base, extra=1))


def test_wire_cut_contribution_k6_shaped_fragment():
    value = _contribution(
        operator_states=[{"logical_node_id": "n1", "occurrence_id": "o1",
                          "state_ref": {"state_id": "s1", "path": "state/s1.bin",
                                        "bytes": 10, "sha256": "a" * 64}}],
        inflight=[{"request_key": "req-1", "status": "known_completed", "effect_ref": "eff-1"}],
        output_descriptor={"blocks": [], "rows": 0})
    assert validate_cut_contribution(value) == value


def test_wire_cut_contribution_source_progress_k6_shape():
    entry = {"source_id": "src-1", "partition_id": "p0", "position": {"next_ordinal": 60}}
    assert validate_cut_contribution(_contribution(source_progress=[entry]))["source_progress"] == [entry]
    with pytest.raises(ProtocolError):  # position must carry next_ordinal
        validate_cut_contribution(_contribution(source_progress=[
            {"source_id": "src-1", "partition_id": "p0", "position": {}}]))
    with pytest.raises(ProtocolError):  # negative ordinal rejected
        validate_cut_contribution(_contribution(source_progress=[
            {"source_id": "src-1", "partition_id": "p0", "position": {"next_ordinal": -1}}]))
    with pytest.raises(ProtocolError):  # duplicate source participant rejected
        validate_cut_contribution(_contribution(source_progress=[entry, entry]))


def test_wire_cut_contribution_inflight_rules_match_k6():
    with pytest.raises(ProtocolError):  # known completion needs an effect reference
        validate_cut_contribution(_contribution(inflight=[{"request_key": "r", "status": "known_completed"}]))
    with pytest.raises(ProtocolError):  # unknown is allowed but never auto-replayed
        validate_cut_contribution(_contribution(inflight=[{"request_key": "r", "status": "pending"}]))
    assert validate_cut_contribution(_contribution(
        inflight=[{"request_key": "r", "status": "unknown"}]))["inflight"][0]["status"] == "unknown"


def test_wire_cut_contribution_rejects_duplicate_channel_and_bad_descriptor():
    with pytest.raises(ProtocolError):
        validate_cut_contribution(_contribution(channel_cuts=[
            {"channel_id": "ch-1", "producer_cut_sequence": 1, "consumer_offsets": {}},
            {"channel_id": "ch-1", "producer_cut_sequence": 2, "consumer_offsets": {}}]))
    with pytest.raises(ProtocolError):
        validate_cut_contribution(_contribution(output_descriptor={"blocks": []}))
    with pytest.raises(ProtocolError):
        validate_cut_contribution(_contribution(extra=1))


def test_wire_capacity_request_roundtrip_and_validation():
    value = {"schema_version": CAPACITY_REQUEST_SCHEMA, "allocation_id": "alloc-1",
             "channel_id": "ch-1", "rows": 16, "bytes": 4096, "reason": "credit exhausted"}
    assert validate_capacity_request(value) == value
    with pytest.raises(ProtocolError):
        validate_capacity_request(dict(value, rows=-1))
    with pytest.raises(ProtocolError):
        validate_capacity_request(dict(value, schema_version="demiflow.capacity_request.v9"))