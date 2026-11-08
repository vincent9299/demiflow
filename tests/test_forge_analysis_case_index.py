"""Real authenticated row-ID index bytes, fake native scanner; no Lance run."""
import hashlib

import pyarrow as pa
import pytest

from demiflow.workbench.analysis import case_index
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.forge.schema import ForgeError


class Batches:
    def __init__(self, values):
        self.values = iter(values)
    def __iter__(self):return self
    def __next__(self):return next(self.values)
    def close(self):pass


class Snapshot:
    schema = pa.schema([pa.field("value", pa.int64())])
    def count_rows(self):return 3
    def scanner(self, **options):
        assert options["columns"] == [] and options["with_row_id"] is True and options["batch_size"] == 1024
        self.to_batches = lambda:Batches([pa.record_batch([pa.array([2**64-1, 2, 7], type=pa.uint64())], names=["_rowid"])])
        return self


def test_actual_sparse_uint64_membership_not_inferred_from_row_count(tmp_path):
    snapshot = Snapshot(); case_index.build_index(snapshot, tmp_path)
    path = tmp_path/case_index.NAME; path.chmod(0o400)
    entry = dict(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    check = lambda key:case_index.contains(path, entry=entry, rows=3, schema_sha256=schema_digest(snapshot.schema), row_id=key)
    assert check("2") and check("7") and check(str(2**64-1))
    assert not check("0") and not check("1") and not check("3")
    for key in ("02", "-1", "9"*5000):
        with pytest.raises(ForgeError):check(key)
    with pytest.raises(ForgeError, match="schema"):
        case_index.contains(path, entry=entry, rows=3, schema_sha256="a"*64, row_id="2")
    path.chmod(0o600); raw=bytearray(path.read_bytes()); raw[-1]^=1; path.write_bytes(raw);path.chmod(0o400)
    with pytest.raises(ForgeError, match="content"):
        check("2")
