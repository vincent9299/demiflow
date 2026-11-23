"""Historical K3 worker payload stays closed."""
import pytest
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.worker import validate_payload

def _code(exc):
    return exc.value.code


def test_wire_k3_checkpoint_request_stays_closed_no_final():
    # D3 rationale: K3 payload key set is exactly closed; 'final' cannot ride K3.
    valid = {"checkpoint_id": "ck-1", "epoch": 1, "cut": {"producer_sequence_bound": 4},
             "topology_revision": 0, "deadline_ms": 60000}
    validate_payload("CheckpointRequest", valid)
    with pytest.raises(ProtocolError) as exc:
        validate_payload("CheckpointRequest", dict(valid, final=True))
    assert _code(exc) == "INVALID_SPEC"
