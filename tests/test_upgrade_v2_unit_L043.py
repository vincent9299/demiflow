"""L043 · snapshot_code_sources — the archive covers exactly what the pin claims.

Binds: the source's own callable files and the identical dependency set the pin
used are archived through the one content-addressed path; a call without
``source`` keeps the historical refs; each ref carries the real sha256/bytes of
the sealed file; a shared helper is archived once; an edited helper produces a
new content-addressed artifact without overwriting the old one; the archive
survives run exit (post-run audit); budget/unpinnable failures fail closed.

Real RunContext/RunAction journal and real importable business modules — no
mock journal, no stand-in artifact authority.
"""
import hashlib
import importlib.util
import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.chain import snapshot_code_sources
from demiflow.execution.seal.business_sources import _DEP_MAX_MODULES, business_dependency_modules
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext, RunAction


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        # The executor scope reuses this run (execution_scope), so the source
        # producers and the seal share one journal authority.
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("main-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "main-action", {"kind": "test-complete"})


def _biz(tmp_path, monkeypatch, sources, entry):
    bizdir = tmp_path / ("l043biz_" + uuid.uuid4().hex[:8])
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
        "l043_helper": "def delta(row):\n    return row['v'] + 1\n",
        "l043_entry": ("import l043_helper\n\n"
                       "def fold(acc, row):\n    return acc + l043_helper.delta(row)\n"),
    }


def _source(entry):
    return data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)._source


def _plan(*functions):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(f)) for f in functions))


def _assert_refs(run, refs):
    for ref in refs:
        assert set(ref) == {"path", "sha256", "bytes", "source"}
        raw = (run.directory / ref["path"]).read_bytes()
        assert ref["sha256"] == hashlib.sha256(raw).hexdigest()
        assert ref["bytes"] == len(raw)
        assert ref["path"].startswith("scratch/chain-code-pin/")
        assert raw == Path(ref["source"]).read_bytes()


def test_archive_covers_the_source_files_and_the_pin_set(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l043_entry")
    source = _source(entry)
    plan = LogicalPlan()
    refs = snapshot_code_sources(sealed.action, plan, source=source)
    _assert_refs(sealed.run, refs)
    archived = {ref["source"] for ref in refs}
    assert archived == {str(paths["l043_entry"].resolve()), str(paths["l043_helper"].resolve())}
    pinned = {str(file.path) for file in business_dependency_modules(plan, source=source).values()}
    assert pinned <= archived


def test_archive_and_pin_collect_the_identical_dependency_set(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, {
        "l043_planhelper": "def bump(row):\n    return row['v'] + 2\n",
        "l043_planentry": ("import l043_planhelper\n\n"
                           "def stage(row):\n    return l043_planhelper.bump(row)\n"),
        **_source_graph(),
    }, "l043_entry")
    plan = _plan(sys.modules["l043_planentry"].stage)
    source = _source(entry)
    refs = snapshot_code_sources(sealed.action, plan, source=source)
    _assert_refs(sealed.run, refs)
    pinned = {str(file.path) for file in business_dependency_modules(plan, source=source).values()}
    archived = {ref["source"] for ref in refs}
    assert pinned <= archived
    assert archived == pinned | {str(paths["l043_planentry"].resolve()),
                                 str(paths["l043_entry"].resolve())}


def test_no_source_call_keeps_the_historical_refs(sealed, tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l043_entry")
    plan = _plan(entry.fold)
    historical = snapshot_code_sources(sealed.action, plan)
    assert snapshot_code_sources(sealed.action, plan, source=None) == historical
    inert = ItemsSource(items=({"k": "a", "v": 1},))
    assert snapshot_code_sources(sealed.action, plan, source=inert) == historical


def test_shared_helper_is_archived_once(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, {
        "l043_shared": "def delta(row):\n    return row['v'] + 1\n",
        "l043_left": ("import l043_shared\n\n"
                      "def fold(acc, row):\n    return acc + l043_shared.delta(row)\n"),
        "l043_right": ("import l043_shared\n\n"
                       "def fold(acc, row):\n    return acc - l043_shared.delta(row)\n"),
    }, "l043_left")
    left = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)
    right = data.from_items([{"k": "a", "v": 2}]).reduce_by_key(
        "k", sys.modules["l043_right"].fold)
    refs = snapshot_code_sources(sealed.action, LogicalPlan(), source=left.union(right)._source)
    _assert_refs(sealed.run, refs)
    sources = [ref["source"] for ref in refs]
    assert len(sources) == len(set(sources)) == 3
    assert str(paths["l043_shared"].resolve()) in sources


def test_edited_helper_gets_a_new_artifact_and_keeps_the_old(sealed, tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l043_entry")
    source = _source(entry)
    first = snapshot_code_sources(sealed.action, LogicalPlan(), source=source)
    _assert_refs(sealed.run, first)
    paths["l043_helper"].write_text("def delta(row):\n    return row['v'] + 9999\n")
    second = snapshot_code_sources(sealed.action, LogicalPlan(), source=source)
    _assert_refs(sealed.run, second)
    old = {ref["sha256"] for ref in first}
    new = {ref["sha256"] for ref in second}
    assert new != old
    for ref in first:
        assert (sealed.run.directory / ref["path"]).is_file()


def test_by_value_source_callable_file_is_archived(sealed, tmp_path, monkeypatch):
    _biz(tmp_path, monkeypatch, {
        "l043_captured": "def delta(row):\n    return row['v'] + 1\n",
    }, "l043_captured")
    captured = sys.modules["l043_captured"]

    def fold(acc, row):
        return acc + captured.delta(row)

    source = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", fold)._source
    refs = snapshot_code_sources(sealed.action, LogicalPlan(), source=source)
    _assert_refs(sealed.run, refs)
    archived = {ref["source"] for ref in refs}
    assert os.path.abspath(__file__) in archived
    assert any(name.endswith("l043_captured.py") for name in archived)


def test_budget_failure_fail_closes(sealed, tmp_path, monkeypatch):
    count = _DEP_MAX_MODULES + 1
    names = ["l043_bulk%02d" % index for index in range(count)]
    sources = {name: "VALUE = %d\n" % index for index, name in enumerate(names)}
    sources["l043_bulkentry"] = (
        "".join("import " + name + "\n" for name in names)
        + "\ndef fold(acc, row):\n    return acc + row['v']\n")
    entry, _ = _biz(tmp_path, monkeypatch, sources, "l043_bulkentry")
    with pytest.raises(RecoveryContractError, match="exceeds budget"):
        snapshot_code_sources(sealed.action, LogicalPlan(), source=_source(entry))


def test_unpinnable_source_callable_fail_closes(sealed):
    class Reducer:
        def __call__(self, acc, row):
            return acc

    source = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", Reducer())._source
    with pytest.raises(RecoveryContractError, match="not a pinnable function"):
        snapshot_code_sources(sealed.action, LogicalPlan(), source=source)


def test_archive_survives_run_exit(tmp_path, monkeypatch):
    with RunContext.start(tmp_path / "audit-run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "audit-action", declaration)
        action = RunAction(run, "audit-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l043_entry")
            refs = snapshot_code_sources(action, LogicalPlan(), source=_source(entry))
        run.journal.complete_execution_action(
            run.fence, "audit-action", {"kind": "test-complete"})
        directory = run.directory
    for ref in refs:
        assert (directory / ref["path"]).is_file()