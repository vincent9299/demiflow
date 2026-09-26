"""Lance I/O behavior: managed writes, pinned reads, checkpoints, reconciliation."""
from demiflow.lance.control import control_directory, table_lock_path
import asyncio
import fcntl
import json

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("lance")

from demiflow.data.api import DataAPI
from demiflow.errors import (
    InvalidLanceRequest, LanceExecutionError, LanceWriteConflict,
    LanceWriteError, LanceResourceNotFound,
)


def ds(rows):
    return DataAPI().from_items(rows)


def write_direct(uri, rows, schema=None):
    import lance
    table = pa.Table.from_pylist(rows, schema=schema)
    return lance.write_dataset(table, uri, mode="overwrite")


# ---------------------------------------------------------------- existing write/read behavior

def test_write_lance_create_append_roundtrip(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}, {"k": "b"}]).write_lance(uri)
    ds([{"k": "c"}]).write_lance(uri)
    import lance
    assert lance.dataset(uri).count_rows() == 3


def test_write_lance_rejects_zero_rows(tmp_path):
    with pytest.raises(InvalidLanceRequest):
        ds([]).write_lance(str(tmp_path / "empty.lance"))


def test_write_lance_append_schema_mismatch_is_indeterminate(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    with pytest.raises(LanceWriteError):
        ds([{"other": 1}]).write_lance(uri)


def test_read_lance_pinned_version_ignores_later_appends(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    api = DataAPI()
    import lance
    pinned_version = lance.dataset(uri).version
    pinned = api.read_lance(uri, version=pinned_version)
    ds([{"k": "b"}]).write_lance(uri)
    assert sorted(r["k"] for r in pinned.take_all()) == ["a"]
    assert lance.dataset(uri).count_rows() == 2


def test_write_lance_cas_conflict(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    import lance
    stale = lance.dataset(uri).version
    ds([{"k": "b"}]).write_lance(uri)
    with pytest.raises(LanceWriteConflict):
        ds([{"k": "c"}]).write_lance(uri, expected_version=stale)
    assert lance.dataset(uri).count_rows() == 2


def test_receipt_roundtrip(tmp_path):
    from demiflow.lance.model import LanceWriteReceipt
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    import lance
    version = lance.dataset(uri).version
    receipt = LanceWriteReceipt(
        request_hash="sha256:x", uri=uri, expected_version=None,
        committed_version=version, input_rows=1, written_rows=1,
        schema_hash="sha256:y", status="committed",
    )
    clone = LanceWriteReceipt.from_dict(json.loads(json.dumps(receipt.to_dict())))
    assert clone == receipt


# ---------------------------------------------------------------- checkpoint_lance

SCHEMA = pa.schema([("k", pa.string()), ("n", pa.int64())])


def checkpoint_uri(tmp_path, name="out"):
    return str(tmp_path / f"{name}.lance")


def test_checkpoint_writes_pinned_replayable_snapshot(tmp_path):
    uri = checkpoint_uri(tmp_path)
    api = DataAPI()
    saved = ds([{"k": "a", "n": 1}, {"k": "b", "n": 2}]).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-1",
    )
    assert sorted(r["k"] for r in saved.take_all()) == ["a", "b"]
    import lance
    version = lance.dataset(uri).version
    # later direct writes must not leak into the pinned checkpoint dataset
    write_direct(uri, [{"k": "z", "n": 9}], schema=SCHEMA)
    assert sorted(r["k"] for r in saved.take_all()) == ["a", "b"]
    assert api.read_lance(uri, version=version).take_all()[0]["k"] == "a"


def test_checkpoint_replay_skips_execution(tmp_path):
    uri = checkpoint_uri(tmp_path)
    calls = []

    class Count:
        def __call__(self, row):
            calls.append(row["k"])
            return row

    ds([{"k": "a", "n": 1}]).map(Count).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-1",
    )
    assert calls == ["a"]
    replayed = ds([{"k": "a", "n": 1}]).map(Count).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-1",
    )
    assert calls == ["a"]  # no re-execution
    assert [r["k"] for r in replayed.take_all()] == ["a"]


def test_checkpoint_rejects_changed_fingerprint(tmp_path):
    uri = checkpoint_uri(tmp_path)
    ds([{"k": "a", "n": 1}]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")
    with pytest.raises(InvalidLanceRequest):
        ds([{"k": "a", "n": 1}]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-2")


def test_checkpoint_zero_rows_commits_valid_empty_table(tmp_path):
    uri = checkpoint_uri(tmp_path)
    saved = ds([]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")
    assert saved.take_all() == []
    import lance
    assert lance.dataset(uri).count_rows() == 0
    replayed = ds([]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")
    assert replayed.take_all() == []


def test_checkpoint_enforces_explicit_schema(tmp_path):
    uri = checkpoint_uri(tmp_path)
    with pytest.raises(InvalidLanceRequest) as excinfo:
        ds([{"k": "a", "n": 1, "rogue": True}]).checkpoint_lance(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )
    assert "rogue" in str(excinfo.value)
    assert not __import__("os").path.exists(uri)  # nothing committed on failure

    with pytest.raises(InvalidLanceRequest):
        ds([{"k": "a", "n": "not-an-int"}]).checkpoint_lance(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )


def test_checkpoint_nullable_fields_accept_missing(tmp_path):
    schema = pa.schema([("k", pa.string()), ("note", pa.string())])
    uri = checkpoint_uri(tmp_path)
    saved = ds([{"k": "a"}]).checkpoint_lance(uri, schema=schema, fingerprint="fp-1")
    assert saved.take_all() == [{"k": "a", "note": None}]


def test_checkpoint_rejects_concurrent_writer(tmp_path):
    uri = checkpoint_uri(tmp_path)
    lock_path = str(table_lock_path(uri))
    import os
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(LanceWriteConflict):
            ds([{"k": "a", "n": 1}]).checkpoint_lance(
                uri, schema=SCHEMA, fingerprint="fp-1",
            )


def test_checkpoint_retry_after_partial_attempt(tmp_path):
    # R1: a receiptless target is never assumed to be our debris. The write
    # refuses with an explicit error and the foreign bytes survive untouched.
    import os
    from demiflow.errors import LanceExecutionError
    uri = checkpoint_uri(tmp_path)
    os.makedirs(uri, exist_ok=True)
    with open(os.path.join(uri, "junk.bin"), "wb") as handle:
        handle.write(b"partial attempt without a sidecar")
    with pytest.raises(LanceExecutionError):
        ds([{"k": "a", "n": 1}]).checkpoint_lance(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )
    with open(os.path.join(uri, "junk.bin"), "rb") as handle:
        assert handle.read() == b"partial attempt without a sidecar"


def test_checkpoint_never_overwrites_plain_written_table(tmp_path):
    # R1 repro: a complete table written by a plain Lance writer must not be
    # deleted by a checkpoint targeting the same URI.
    import lance
    uri = checkpoint_uri(tmp_path)
    write_direct(uri, [{"k": "original", "n": 1}], schema=SCHEMA)
    versions_before = lance.dataset(uri).version
    with pytest.raises(LanceExecutionError):
        ds([{"k": "replacement", "n": 2}]).checkpoint_lance(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )
    assert lance.dataset(uri).to_table().column("k").to_pylist() == ["original"]
    assert lance.dataset(uri).version == versions_before


def test_checkpoint_recovers_pending_publish(tmp_path):
    # R1: crash window between rename and sidecar write. A matching pending
    # receipt proves ownership; replay adopts it without re-executing.
    import shutil
    from pathlib import Path
    from demiflow.lance.checkpoint import checkpoint_sidecar_path
    uri = checkpoint_uri(tmp_path)
    ds([{"k": "a", "n": 1}]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")
    sidecar = Path(checkpoint_sidecar_path(uri))
    record = json.loads(sidecar.read_text())
    # Simulate the interrupted publish: sidecar removed, pending left behind.
    sidecar.unlink()
    pending = control_directory(uri) / 'pending-recover.json'
    pending.write_text(json.dumps(record))
    consumed = {"executed": False}

    def factory():
        consumed["executed"] = True
        return iter([{"k": "b", "n": 2}])

    saved = DataAPI().from_iter(factory).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-1",
    )
    assert saved.take_all()[0]["k"] == "a"          # committed rows, not factory rows
    assert consumed["executed"] is False             # upstream plan never executed
    assert not pending.exists()                      # stale pending cleaned
    assert Path(checkpoint_sidecar_path(uri)).exists()


def test_checkpoint_failed_attempt_leaves_target_untouched(tmp_path):
    # R1: a failing attempt (row contract violation) leaves no debris: no
    # table at the target, no attempt directories, no pending receipts.
    from pathlib import Path
    uri = checkpoint_uri(tmp_path)
    with pytest.raises(InvalidLanceRequest):
        ds([{"k": "b", "n": 2}, {"rogue_field": True}]).checkpoint_lance(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )
    assert not Path(uri).exists()
    leftovers = [p.name for p in control_directory(uri).iterdir()
                 if p.name.startswith(("attempt-", "pending-"))]
    assert leftovers == []


def test_checkpoint_replay_binds_caller_uri_after_move(tmp_path):
    # R4 repro: after moving table+sidecar to a new root, replay at the new
    # URI works and never reopens the recorded historical path.
    import shutil
    from pathlib import Path
    from demiflow.lance.checkpoint import checkpoint_sidecar_path
    first = tmp_path / "root1" / "deep" / "out.lance"
    first.parent.mkdir(parents=True)
    ds([{"k": "a", "n": 1}]).checkpoint_lance(str(first), schema=SCHEMA, fingerprint="fp-1")
    second_root = tmp_path / "root2"
    second_root.mkdir()
    control_directory(second_root/'out.lance').mkdir(parents=True)
    shutil.move(str(first), str(second_root / "out.lance"))
    shutil.move(checkpoint_sidecar_path(str(first)),
                str(control_directory(second_root/'out.lance') / 'checkpoint.json'))
    old_recorded = str(first)
    consumed = {"executed": False}

    def factory():
        consumed["executed"] = True
        return iter([{"k": "b", "n": 2}])

    saved = DataAPI().from_iter(factory).checkpoint_lance(
        str(second_root / "out.lance"), schema=SCHEMA, fingerprint="fp-1",
    )
    rows = saved.take_all()
    assert rows[0]["k"] == "a"
    assert consumed["executed"] is False
    # The receipt still carries the historical location as provenance.
    record = json.loads(
        (control_directory(second_root/'out.lance') / 'checkpoint.json').read_text())
    assert record["uri"] == old_recorded
    # A third relocation replays equally well.
    third_root = tmp_path / "root3"
    third_root.mkdir()
    control_directory(third_root/'out.lance').mkdir(parents=True)
    shutil.move(str(second_root / "out.lance"), str(third_root / "out.lance"))
    shutil.move(str(control_directory(second_root/'out.lance') / 'checkpoint.json'),
                str(control_directory(third_root/'out.lance') / 'checkpoint.json'))
    saved3 = DataAPI().from_iter(factory).checkpoint_lance(
        str(third_root / "out.lance"), schema=SCHEMA, fingerprint="fp-1",
    )
    assert saved3.take_all()[0]["k"] == "a"
    assert consumed["executed"] is False


def test_checkpoint_replay_detects_missing_table(tmp_path):
    import shutil
    uri = checkpoint_uri(tmp_path)
    ds([{"k": "a", "n": 1}]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")
    shutil.rmtree(uri)
    with pytest.raises(LanceResourceNotFound):
        ds([{"k": "a", "n": 1}]).checkpoint_lance(uri, schema=SCHEMA, fingerprint="fp-1")


def test_checkpoint_rejects_remote_uri(tmp_path):
    with pytest.raises(InvalidLanceRequest):
        ds([{"k": "a", "n": 1}]).checkpoint_lance(
            "s3://bucket/out.lance", schema=SCHEMA, fingerprint="fp-1",
        )


def test_checkpoint_async(tmp_path):
    uri = checkpoint_uri(tmp_path)
    saved = asyncio.run(
        ds([{"k": "a", "n": 1}]).checkpoint_lance_async(
            uri, schema=SCHEMA, fingerprint="fp-1",
        )
    )
    assert [r["k"] for r in saved.take_all()] == ["a"]


def test_checkpoint_chainable_after_save(tmp_path):
    uri = checkpoint_uri(tmp_path)
    saved = ds([{"k": "a", "n": 1}, {"k": "b", "n": 2}]).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-1",
    )
    doubled = saved.map(lambda row: {"k": row["k"], "n": row["n"] * 10})
    assert sorted(r["n"] for r in doubled.take_all()) == [10, 20]


# ---------------------------------------------------------------- reconciliation

def test_find_committed_append_by_marker(tmp_path):
    import lance
    from demiflow.lance.reconcile import find_committed_append
    from demiflow.lance.write import (
        commit_lance_append, prepare_lance_append, write_lance_fragment,
    )
    from demiflow.lance.model import LanceWriteSpec

    uri = str(tmp_path / "cas.lance")
    write_direct(uri, [{"k": "a"}])
    version = lance.dataset(uri).version

    spec = LanceWriteSpec(uri=uri, expected_version=version)
    prepared = prepare_lance_append(spec)
    fragment = write_lance_fragment(
        prepared, task_index=0, batches=iter([pa.table({"k": ["b", "c"]})]),
    )
    receipt = commit_lance_append(prepared, (fragment,))
    assert receipt.status == "committed"

    found = find_committed_append(uri, operation_id=prepared.operation_id)
    assert found == receipt.committed_version
    assert find_committed_append(uri, operation_id="no-such-operation") is None


def test_reconcile_resolves_indeterminate_receipt(tmp_path):
    import lance
    from demiflow.lance.reconcile import reconcile_lance_append
    from demiflow.lance.write import (
        commit_lance_append, prepare_lance_append, write_lance_fragment,
    )
    from demiflow.lance.model import LanceWriteReceipt, LanceWriteSpec

    uri = str(tmp_path / "cas.lance")
    write_direct(uri, [{"k": "a"}])
    version = lance.dataset(uri).version
    spec = LanceWriteSpec(uri=uri, expected_version=version)
    prepared = prepare_lance_append(spec)
    fragment = write_lance_fragment(
        prepared, task_index=0, batches=iter([pa.table({"k": ["b"]})]),
    )
    receipt = commit_lance_append(prepared, (fragment,))
    assert receipt.status == "committed"

    # simulate an indeterminate copy of the same commit (commit succeeded,
    # caller never learned the outcome)
    from demiflow._compat.error_transport import make_error
    indeterminate = LanceWriteReceipt(
        request_hash=receipt.request_hash, uri=receipt.uri,
        expected_version=receipt.expected_version,
        committed_version=receipt.committed_version,
        input_rows=receipt.input_rows, written_rows=None,
        schema_hash=receipt.schema_hash, status="indeterminate",
        error=make_error(
            module="test", type_name="ConnectionReset", message="reset",
        ),
    )
    resolved = reconcile_lance_append(prepared, (fragment,), indeterminate)
    assert resolved.status == "committed"
    assert resolved.committed_version == receipt.committed_version

    unrelated = LanceWriteReceipt(
        request_hash="sha256:other", uri=uri,
        expected_version=version + 100,
        committed_version=None,
        input_rows=0, written_rows=None,
        schema_hash=receipt.schema_hash, status="indeterminate",
        error=make_error(
            module="test", type_name="Timeout", message="timed out",
        ),
    )
    assert reconcile_lance_append(prepared, (fragment,), unrelated).status == "indeterminate"


def test_checkpoint_supports_async_operator_plan(tmp_path):
    # R6 repro: from_items(...).map_async(async_fn).checkpoint_lance must
    # execute the async plan instead of failing on AsyncMapOp.
    uri = checkpoint_uri(tmp_path)

    async def double(row):
        await asyncio.sleep(0)
        return {**row, "n": row["n"] * 2}

    api = DataAPI()
    saved = api.from_items([{"k": "a", "n": 1}, {"k": "b", "n": 2}]).map_async(double).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-async",
    )
    assert sorted(r["n"] for r in saved.take_all()) == [2, 4]
    # Replay path also survives an async upstream plan.
    replayed = api.from_items([{"k": "x", "n": 9}]).map_async(double).checkpoint_lance(
        uri, schema=SCHEMA, fingerprint="fp-async",
    )
    assert sorted(r["n"] for r in replayed.take_all()) == [2, 4]


def test_checkpoint_async_from_running_loop(tmp_path):
    # R6: awaiting checkpoint_lance_async inside an existing event loop.
    import lance
    uri = checkpoint_uri(tmp_path, "loop")

    async def main():
        await asyncio.sleep(0)
        return await DataAPI().from_items([{"k": "a", "n": 7}]).checkpoint_lance_async(
            uri, schema=SCHEMA, fingerprint="fp-loop",
        )

    saved = asyncio.run(main())
    assert saved.take_all()[0]["n"] == 7
    assert lance.dataset(uri).count_rows() == 1
