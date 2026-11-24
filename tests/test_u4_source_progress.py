"""Historical source positions retain their closed shape, bounds and coverage."""
import pytest

from demiflow.execution.contracts.checkpoint import (
    validate_checkpoint_manifest,
)
from demiflow.execution.contracts.recovery_contract import RecoveryContractError




def _sp(source_id, partition_id, position):
    return {"source_id": source_id, "partition_id": partition_id, "position": position}


def _manifest(**overrides):
    value = {
        "schema_version": "demiflow.checkpoint_manifest.v1",
        "checkpoint_id": "ck1", "run_id": "run-1", "action_id": "action-1",
        "epoch": 1, "topology_revision": 0, "route_epoch": 0, "plan_revision": 0,
        "artifact_manifest_ref": {"manifest_id": "m" * 64, "sha256": "a" * 64},
        "participants": {"sources": [{"source_id": "src-1", "partition_id": "p0"}],
                         "channels": [], "operators": [], "sinks": []},
        "source_progress": [_sp("src-1", "p0", {"next_ordinal": 7})],
        "channel_cuts": [], "operator_states": [], "inflight": [],
        "sink_receipts": [], "seal": {"sealed": False},
    }
    value.update(overrides)
    return value


def _source_manifest(**overrides):
    value = _manifest(**overrides)
    value['participants'] = dict(value['participants'], sources=[
        {'source_id': p['source_id'], 'partition_id': p['partition_id']}
        for p in value['source_progress']])
    return value

# --- historical source positions: cursor_ref positions are accepted -----------------

def test_wire_source_progress_cursor_ref_accepted():
    entry = _sp("lance-src", "p0", {"cursor_ref": "cursor-" + "a" * 64})
    assert validate_checkpoint_manifest(
        _source_manifest(source_progress=[entry]))["source_progress"] == [entry]


@pytest.mark.parametrize("bad", [
    None, 123, "", "a" * 129, "has space", "has/slash", "has.dot", "ünicode",
])
def test_wire_source_progress_cursor_ref_rejects_invalid_identifier(bad):
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(_source_manifest(
            source_progress=[_sp("src-1", "p0", {"cursor_ref": bad})]))


# --- position is exactly one kind, matching the frozen K6 closed rule -----------

@pytest.mark.parametrize("position", [
    {},                                                  # no kind
    {"next_ordinal": 1, "cursor_ref": "c"},              # both kinds
    {"next_ordinal": 1, "extra": 2},                     # extra key
    {"unknown_kind": 1},                                 # neither kind
    {"cursor_ref": "c", "extra": 2},                     # extra key
    7,                                                   # not a dict
    [{"next_ordinal": 1}],                               # not a dict
])
def test_wire_source_progress_position_exactly_one_kind(position):
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(_source_manifest(
            source_progress=[_sp("src-1", "p0", position)]))


@pytest.mark.parametrize("ordinal", [0, 1, 2**63 - 1])
def test_wire_source_progress_next_ordinal_accepts_in_range(ordinal):
    entry = _sp("src-1", "p0", {"next_ordinal": ordinal})
    assert validate_checkpoint_manifest(
        _source_manifest(source_progress=[entry]))["source_progress"] == [entry]


@pytest.mark.parametrize("ordinal", [-1, 2**63, True, 1.0, "1", None])
def test_wire_source_progress_next_ordinal_rejects_out_of_range_or_non_int(ordinal):
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(_source_manifest(
            source_progress=[_sp("src-1", "p0", {"next_ordinal": ordinal})]))


# --- multi-source vector rides the wire; coverage keys stay unique --------------

def test_wire_source_progress_multi_source_vector_mixed_kinds():
    progress = [_sp("fixed-src", "p0", {"next_ordinal": 12}),
                _sp("incr-src", "p0", {"cursor_ref": "c" * 64}),
                _sp("incr-src", "p1", {"cursor_ref": "d" * 64})]   # same source, new partition
    assert validate_checkpoint_manifest(
        _source_manifest(source_progress=progress))["source_progress"] == progress


def test_wire_source_progress_rejects_duplicate_source_partition():
    entry = _sp("incr-src", "p0", {"cursor_ref": "c" * 64})
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(_source_manifest(source_progress=[entry, entry]))


# --- the S4 seam itself: wire and K6 accept/reject every position identically ----

@pytest.mark.parametrize("position", [
    {"next_ordinal": 0},
    {"next_ordinal": 60},
    {"next_ordinal": 2**63 - 1},
    {"cursor_ref": "cursor-" + "a" * 64},
    {"cursor_ref": "c"},
    {"next_ordinal": -1},
    {"next_ordinal": 2**63},
    {"next_ordinal": True},
    {"cursor_ref": ""},
    {"cursor_ref": "a" * 129},
    {"cursor_ref": 5},
    {},
    {"next_ordinal": 1, "cursor_ref": "c"},
    {"unknown": 1},
])
def test_wire_and_k6_position_rules_align(position):
    entry = _sp("src-1", "p0", position)

    def accepts(validate, value):
        try:
            validate(value)
            return True
        except RecoveryContractError:
            return False

    expected = (len(position) == 1 and (
        type(position.get("next_ordinal")) is int and 0 <= position["next_ordinal"] < 2**63
        or type(position.get("cursor_ref")) is str and 1 <= len(position["cursor_ref"]) <= 128))
    k6_ok = accepts(validate_checkpoint_manifest, _manifest(source_progress=[entry]))
    assert k6_ok == expected


# --- cursor_ref vector assembles into a full-coverage K6 manifest ---------------

def _multi_source_manifest():
    participants = {"sources": [{"source_id": "fixed-src", "partition_id": "p0"},
                                {"source_id": "incr-src", "partition_id": "p0"}],
                    "channels": [], "operators": [], "sinks": []}
    progress = [_sp("fixed-src", "p0", {"next_ordinal": 24}),
                _sp("incr-src", "p0", {"cursor_ref": "c" * 64})]
    return _manifest(participants=participants, source_progress=progress)


def test_k6_manifest_cursor_ref_multi_source_full_coverage():
    manifest = _multi_source_manifest()
    assert validate_checkpoint_manifest(manifest) == manifest


def test_k6_manifest_cursor_ref_missing_source_rejected():
    manifest = _multi_source_manifest()
    manifest["source_progress"] = manifest["source_progress"][:1]   # drop incr-src
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(manifest)


def test_k6_manifest_cursor_ref_topology_mismatch_rejected():
    manifest = _multi_source_manifest()
    topology = {"sources": [{"source_id": "fixed-src", "partition_id": "p0"}],
                "channels": []}                              # sealed plan has only one source
    with pytest.raises(RecoveryContractError):
        validate_checkpoint_manifest(manifest, topology=topology)
