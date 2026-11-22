"""L044 · seal_write_plan_ref — stable per-program write-seam pin key.

Binds: the ProgramRef keeps exactly five keys; ``program_id=None`` preserves the
historical single-program key and bytes; an explicit slice id keys on
``identity([action_id, "code-pin-write", program_id])`` with no attempt number
and no pickle bytes; two slices keep two bindings; the same key with different
semantics fail-closes instead of re-binding; ``source`` reaches the pin and the
archive as the same set, so a record sealed before source coverage refuses a
resume that now carries business source callables.

Real RunContext/RunAction journal, real carrier bytes on disk — no mock journal.
"""
import hashlib
import importlib.util
import json
import os
import sys
import uuid
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.chain import seal_write_plan_ref, snapshot_code_sources
from demiflow.execution.seal.business_sources import plan_code_pin
from demiflow.execution.recovery_contract import RecoveryContractError, document, identity
from demiflow.execution.run_context import RunContext, RunAction


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        action = RunAction(run, "write-action", declaration)
        # The executor scope reuses this run (execution_scope), so the source
        # producers and the seal share one journal authority.
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("write-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "write-action", {"kind": "test-complete"})


def _biz(tmp_path, monkeypatch, sources, entry):
    bizdir = tmp_path / ("l044biz_" + uuid.uuid4().hex[:8])
    bizdir.mkdir()
    paths, specs, mods = {}, {}, {}
    for name, src in sources.items():
        path = bizdir / (name + ".py")
        path.write_text(src)
        paths[name] = path
        specs[name] = importlib.util.spec_from_file_location(name, path)
        mods[name] = importlib.util.module_from_spec(specs[name])
        monkeypatch.setitem(sys.modules, name, mods[name])
    monkeypatch.syspath_prepend(str(bizdir))
    monkeypatch.setenv("PYTHONPATH", str(bizdir) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    for name in sources:
        specs[name].loader.exec_module(mods[name])
    return mods[entry], paths


def _source_graph():
    return {
        "l044_helper": "def delta(row):\n    return row['v'] + 1\n",
        "l044_entry": ("import l044_helper\n\n"
                       "def fold(acc, row):\n    return acc + l044_helper.delta(row)\n"),
    }


def _source(entry):
    return data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)._source


def _plan(value=1):
    def stage(row):
        return {**row, "v": row["v"] + value}
    return LogicalPlan((MapOp(CallableSpec.create(stage)),))


def _carrier(run, ref):
    path = run.directory / ref["path"]
    assert path.is_file()
    raw = path.read_bytes()
    assert ref["sha256"] == hashlib.sha256(raw).hexdigest()
    assert ref["bytes"] == len(raw)
    assert ref["path"].startswith("scratch/chain-write-pin/")
    return json.loads(raw)


def test_program_ref_keeps_exactly_five_keys(sealed):
    ref = seal_write_plan_ref(sealed.action, _plan())
    assert set(ref) == {"path", "sha256", "bytes", "code_pin_digest", "code_sources"}
    ref2 = seal_write_plan_ref(sealed.action, _plan(), source=None, program_id="slice-0")
    assert set(ref2) == set(ref)


def test_legacy_key_and_bytes_are_preserved(sealed):
    plan = _plan()
    ref = seal_write_plan_ref(sealed.action, plan)
    legacy_key = sealed.action.action_id + "-code-pin-write"
    assert sealed.run.journal.source_binding(legacy_key) == plan_code_pin(plan)
    assert ref["code_pin_digest"] == identity(plan_code_pin(plan))
    assert _carrier(sealed.run, ref) == plan_code_pin(plan)
    assert ref["bytes"] == len(document(plan_code_pin(plan)))
    assert ref["code_sources"] == snapshot_code_sources(sealed.action, plan)
    # Repeating the historical call is idempotent: same key, same bytes.
    assert seal_write_plan_ref(sealed.action, plan, source=None, program_id=None) == ref
    inert = ItemsSource(items=({"k": "a", "v": 1},))
    assert seal_write_plan_ref(sealed.action, plan, source=inert) == ref


def test_explicit_program_id_uses_the_stable_slice_key(sealed):
    plan = _plan()
    program_id = identity(["write-action", ["source", "left"]])
    ref = seal_write_plan_ref(sealed.action, plan, program_id=program_id)
    expected = identity([sealed.action.action_id, "code-pin-write", program_id])
    assert sealed.run.journal.source_binding(expected) == plan_code_pin(plan)
    assert sealed.run.journal.source_binding(
        sealed.action.action_id + "-code-pin-write") is None
    assert ref["code_pin_digest"] == identity(plan_code_pin(plan))


def test_slice_key_survives_a_real_attempt_bump(tmp_path):
    """The key carries no attempt number: a resumed attempt finds the original
    binding instead of sealing a second one under a fresh key."""
    plan = _plan()
    program_id = identity(["write-action", ["source", "left"]])
    expected = identity(["write-action", "code-pin-write", program_id])
    declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
    with pytest.raises(InterruptedError, match="driver interrupted after seal"):
        with RunContext.start(tmp_path / "run") as run:
            reference = run.ref
            first_attempt = run.attempt
            run.journal.bind_execution_action(run.fence, "write-action", declaration)
            action = RunAction(run, "write-action", declaration)
            first = seal_write_plan_ref(action, plan, program_id=program_id)
            raise InterruptedError("driver interrupted after seal")
    with RunContext.resume(reference) as run:
        assert run.attempt != first_attempt
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        action = RunAction(run, "write-action", declaration)
        again = seal_write_plan_ref(action, plan, program_id=program_id)
        assert run.journal.source_binding(expected) == plan_code_pin(plan)
        assert again["code_pin_digest"] == first["code_pin_digest"]
        run.journal.complete_execution_action(
            run.fence, "write-action", {"kind": "test-complete"})


def test_two_slices_keep_two_bindings(sealed):
    left = seal_write_plan_ref(sealed.action, _plan(1), program_id="slice-left")
    right = seal_write_plan_ref(sealed.action, _plan(2), program_id="slice-right")
    assert left["code_pin_digest"] != right["code_pin_digest"]
    for program_id, plan in (("slice-left", _plan(1)), ("slice-right", _plan(2))):
        key = identity([sealed.action.action_id, "code-pin-write", program_id])
        assert sealed.run.journal.source_binding(key) == plan_code_pin(plan)


def test_same_key_different_semantics_fail_closes(sealed):
    seal_write_plan_ref(sealed.action, _plan(1), program_id="slice-left")
    with pytest.raises(RecoveryContractError, match="Business code changed since the plan was sealed"):
        seal_write_plan_ref(sealed.action, _plan(2), program_id="slice-left")
    # The original binding is untouched: no silent re-bind.
    key = identity([sealed.action.action_id, "code-pin-write", "slice-left"])
    assert sealed.run.journal.source_binding(key) == plan_code_pin(_plan(1))


def test_legacy_key_different_semantics_fail_closes(sealed):
    seal_write_plan_ref(sealed.action, _plan(1))
    with pytest.raises(RecoveryContractError, match="Business code changed since the plan was sealed"):
        seal_write_plan_ref(sealed.action, _plan(2))


@pytest.mark.parametrize("program_id", ["", 0, 7, b"slice", (), ["slice"]])
def test_invalid_program_id_fail_closes(sealed, program_id):
    with pytest.raises(RecoveryContractError, match="nonempty string"):
        seal_write_plan_ref(sealed.action, _plan(), program_id=program_id)


def test_source_reaches_the_pin_and_the_archive_as_one_set(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l044_entry")
    source = _source(entry)
    plan = _plan()
    ref = seal_write_plan_ref(sealed.action, plan, source=source, program_id="slice-0")
    assert ref["code_pin_digest"] == identity(plan_code_pin(plan, source=source))
    assert _carrier(sealed.run, ref) == plan_code_pin(plan, source=source)
    assert ref["code_sources"] == snapshot_code_sources(sealed.action, plan, source=source)
    archived = {item["source"] for item in ref["code_sources"]}
    assert {str(paths["l044_entry"].resolve()), str(paths["l044_helper"].resolve())} <= archived
    key = identity([sealed.action.action_id, "code-pin-write", "slice-0"])
    assert sealed.run.journal.source_binding(key) == plan_code_pin(plan, source=source)


def test_pre_source_record_refuses_a_source_bearing_resume(sealed, tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l044_entry")
    source = _source(entry)
    plan = _plan()
    # Sealed before source coverage existed: the record cannot claim it protected
    # the reducer's imported helper, so the resume must refuse, not guess.
    seal_write_plan_ref(sealed.action, plan, program_id="slice-0")
    with pytest.raises(RecoveryContractError, match="Business code changed since the plan was sealed"):
        seal_write_plan_ref(sealed.action, plan, source=source, program_id="slice-0")
    # With no business source callable the old record still covers everything.
    inert = ItemsSource(items=({"k": "a", "v": 1},))
    assert seal_write_plan_ref(sealed.action, plan, source=inert,
                               program_id="slice-0")["code_pin_digest"] == identity(
        plan_code_pin(plan))


def test_source_helper_edit_after_seal_fail_closes(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l044_entry")
    source = _source(entry)
    plan = _plan()
    seal_write_plan_ref(sealed.action, plan, source=source, program_id="slice-0")
    paths["l044_helper"].write_text("def delta(row):\n    return row['v'] + 9999\n")
    with pytest.raises(RecoveryContractError, match="Business code changed since the plan was sealed"):
        seal_write_plan_ref(sealed.action, plan, source=source, program_id="slice-0")


def test_carrier_is_content_addressed_and_survives_run_exit(tmp_path, monkeypatch):
    plan = _plan()
    with RunContext.start(tmp_path / "audit-run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "audit-write", declaration)
        action = RunAction(run, "audit-write", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l044_entry")
            source = _source(entry)
            ref = seal_write_plan_ref(action, plan, source=source, program_id="slice-0")
            path = run.directory / ref["path"]
            assert path.name == ref["sha256"] + ".json"
            before = path.stat().st_mtime_ns
            assert seal_write_plan_ref(action, plan, source=source, program_id="slice-0") == ref
            assert path.stat().st_mtime_ns == before
        run.journal.complete_execution_action(
            run.fence, "audit-write", {"kind": "test-complete"})
        directory = run.directory
    assert (directory / ref["path"]).is_file()
    for item in ref["code_sources"]:
        assert (directory / item["path"]).is_file()