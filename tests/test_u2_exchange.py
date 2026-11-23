"""Frozen seal rules and real staging descriptors retain their boundaries."""
import pytest
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.exchange import build_data_ref, seal_allowed, validate_seal
from demiflow.execution.exchange.segments import read_domain, descriptor_refs, refs_descriptor
from demiflow.execution.run_context import RunContext

def _code(exc):
    return exc.value.code


@pytest.fixture
def run(tmp_path):
    with RunContext.start(tmp_path / "run") as context:
        yield context


def _ref(domain, location="materializations/a/output/g/0.pkl", sha256="a" * 64, size=10):
    return build_data_ref(kind="file", read_domain=domain, location=location,
                          payload_bytes=size, sha256=sha256, durability="durable",
                          scope_ref="scope-1")


def test_chn_seal_only_success_zero_rows_may_seal():
    assert seal_allowed(["success", "success"]) and seal_allowed([])
    for status in ("failed", "cancelled", "idle"):
        with pytest.raises(ProtocolError):
            seal_allowed(["success", status])
    scope = {"scope_id": "scope-1", "membership": ["p0"], "generation": 1}
    with pytest.raises(ProtocolError):
        validate_seal({"channel_id": "ch-1", "sequence": 9, "seal_scope": scope,
                       "manifest_ref": {"sha256": "c" * 64}, "status": "failed"})


def test_chn_read_domain_mismatch_rejected():
    domain = "file://host-a:/r"
    ref = _ref("file://host-b:/r")  # same path shape, different host domain
    with pytest.raises(ProtocolError) as exc:
        refs_descriptor([ref], rows=1, reader_domain=domain)
    assert _code(exc) == "REF_READ_DOMAIN_MISMATCH"


def test_chn_descriptor_refs_roundtrip(run):
    domain = read_domain(run.directory, "host-a")
    descriptor = {"blocks": [dict(path="materializations/a/output/g/0.pkl", bytes=10, sha256="a" * 64),
                             dict(path="materializations/a/output/g/1.pkl", bytes=20, sha256="b" * 64)],
                  "rows": 3}
    refs = descriptor_refs(run, descriptor, domain=domain, scope_ref="scope-1")
    assert len(refs) == 2 and all(r["kind"] == "file" and r["durability"] == "durable" for r in refs)
    rebuilt = refs_descriptor(refs, rows=3, reader_domain=domain)
    assert rebuilt == descriptor
