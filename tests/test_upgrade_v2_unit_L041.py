"""L041 · _business_dependency_modules — source roots seed the SAME module BFS.

The plan side of gap④ already walks each UDF's ``__globals__``. A native source's
reducer/factory imports business helpers the same way, and ``describe(source)``
never walks them. These tests bind: source roots ADD to the one existing BFS
(no second traversal), the plan-derived set is unchanged, a call without
``source`` keeps the historical result exactly, by-value source callables seed
through their captures, an edited source helper changes the manifest, and the
module/value budgets plus an unresolvable business module still fail closed.

Real public producers (``reduce_by_key`` / ``from_iter``) and real importable
business modules — no stand-in module type, no stub classifier.
"""
import importlib.util
import os
import sys
import types
import uuid

import pytest

import demiflow.data as data
from demiflow.data.plan import LogicalPlan
from demiflow.execution.chain import (
    _DEP_MAX_MODULES, _business_dependency_modules,
)
from demiflow.execution.recovery_contract import RecoveryContractError


@pytest.fixture(autouse=True)
def local_executor():
    """Real public local-execution scope; declaration only, no action is run."""
    with data.local_execution(workers=1, worker_mode="thread", partitions=1):
        yield


def _biz(tmp_path, monkeypatch, sources, entry):
    """Write an importable business module graph and import it (helpers first)."""
    bizdir = tmp_path / ("l041biz_" + uuid.uuid4().hex[:8])
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


def _graph(*, reducer="fold", helper="l041_helper", entry="l041_entry"):
    """One helper module plus an entry module whose reducer imports it."""
    return {
        helper: "def delta(row):\n    return row['v'] + 1\n",
        entry: ("import " + helper + "\n\n"
                "calls = 0\n\n"
                "def " + reducer + "(acc, row):\n"
                "    global calls\n"
                "    calls += 1\n"
                "    return acc + " + helper + ".delta(row)\n"),
    }


def _reduce_source(entry, reducer="fold"):
    return data.from_items([{"k": "a", "v": 1}]).reduce_by_key(
        "k", getattr(entry, reducer))._source


def test_source_reducer_helper_enters_the_manifest(tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _graph(), "l041_entry")
    source = _reduce_source(entry)
    plan = LogicalPlan()
    assert _business_dependency_modules(plan) == {}
    found = _business_dependency_modules(plan, source=source)
    assert set(found) == {"l041_entry", "l041_helper"}
    for name, file in found.items():
        assert file.path == paths[name].resolve()
        assert b"".join(file.chunks()) == paths[name].read_bytes()


def test_plan_set_is_unchanged_when_source_is_added(tmp_path, monkeypatch):
    from demiflow.data.plan import CallableSpec, MapOp
    entry, paths = _biz(tmp_path, monkeypatch, {
        "l041_planhelper": "def bump(row):\n    return row['v'] + 2\n",
        "l041_planentry": ("import l041_planhelper\n\n"
                           "def stage(row):\n    return l041_planhelper.bump(row)\n"),
        "l041_srchelper": "def delta(row):\n    return row['v'] + 1\n",
        "l041_srcentry": ("import l041_srchelper\n\n"
                          "def fold(acc, row):\n    return acc + l041_srchelper.delta(row)\n"),
    }, "l041_srcentry")
    plan = LogicalPlan((MapOp(CallableSpec.create(sys.modules["l041_planentry"].stage)),))
    source = _reduce_source(entry)
    plan_only = _business_dependency_modules(plan)
    both = _business_dependency_modules(plan, source=source)
    assert set(plan_only) == {"l041_planentry", "l041_planhelper"}
    assert set(both) == set(plan_only) | {"l041_srcentry", "l041_srchelper"}
    for name in plan_only:
        assert both[name] == plan_only[name]


def test_call_without_source_keeps_the_historical_result(tmp_path, monkeypatch):
    from demiflow.data.plan import CallableSpec, MapOp
    entry, _ = _biz(tmp_path, monkeypatch, _graph(), "l041_entry")
    plan = LogicalPlan((MapOp(CallableSpec.create(entry.fold)),))
    source = _reduce_source(entry)
    historical = _business_dependency_modules(plan)
    assert _business_dependency_modules(plan, source=None) == historical
    # A source with no business callable adds nothing: identical bytes.
    from demiflow.data.sources import ItemsSource
    inert = ItemsSource(items=({"k": "a", "v": 1},))
    assert _business_dependency_modules(plan, source=inert) == historical
    assert _business_dependency_modules(LogicalPlan(), source=inert) == {}


def test_by_value_source_callable_seeds_through_captures(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, {
        "l041_captured": "def delta(row):\n    return row['v'] + 1\n",
    }, "l041_captured")
    captured = sys.modules["l041_captured"]

    def fold(acc, row):
        return acc + captured.delta(row)

    assert "<locals>" in fold.__qualname__
    source = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", fold)._source
    found = _business_dependency_modules(LogicalPlan(), source=source)
    assert set(found) == {"l041_captured"}


def test_editing_the_source_helper_changes_the_manifest(tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _graph(), "l041_entry")
    source = _reduce_source(entry)
    before = _business_dependency_modules(LogicalPlan(), source=source)
    paths["l041_helper"].write_text("def delta(row):\n    return row['v'] + 9999\n")
    after = _business_dependency_modules(LogicalPlan(), source=source)
    assert after["l041_helper"].digest != before["l041_helper"].digest
    assert after["l041_entry"] == before["l041_entry"]


def test_shared_helper_is_collected_once(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, {
        "l041_shared": "def delta(row):\n    return row['v'] + 1\n",
        "l041_left": ("import l041_shared\n\n"
                      "def fold(acc, row):\n    return acc + l041_shared.delta(row)\n"),
        "l041_right": ("import l041_shared\n\n"
                       "def fold(acc, row):\n    return acc - l041_shared.delta(row)\n"),
    }, "l041_left")
    left = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)
    right = data.from_items([{"k": "a", "v": 2}]).reduce_by_key(
        "k", sys.modules["l041_right"].fold)
    found = _business_dependency_modules(LogicalPlan(), source=left.union(right)._source)
    assert set(found) == {"l041_left", "l041_right", "l041_shared"}
    assert list(found) == sorted(found)


def test_module_budget_fail_closes_through_source_roots(tmp_path, monkeypatch):
    count = _DEP_MAX_MODULES + 1
    names = ["l041_bulk%02d" % index for index in range(count)]
    sources = {name: "VALUE = %d\n" % index for index, name in enumerate(names)}
    sources["l041_bulkentry"] = (
        "".join("import " + name + "\n" for name in names)
        + "\ndef fold(acc, row):\n    return acc + row['v']\n")
    entry, _ = _biz(tmp_path, monkeypatch, sources, "l041_bulkentry")
    source = _reduce_source(entry)
    with pytest.raises(RecoveryContractError, match="exceeds budget"):
        _business_dependency_modules(LogicalPlan(), source=source)


def test_unresolvable_business_module_fail_closes(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _graph(), "l041_entry")
    dynamic = types.ModuleType("l041_dynamic")
    monkeypatch.setitem(sys.modules, "l041_dynamic", dynamic)
    monkeypatch.setattr(entry, "dynamic", dynamic, raising=False)
    source = _reduce_source(entry)
    with pytest.raises(RecoveryContractError, match="no resolvable source"):
        _business_dependency_modules(LogicalPlan(), source=source)


def test_walk_is_passive_and_deterministic(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _graph(), "l041_entry")
    source = _reduce_source(entry)
    assert entry.calls == 0
    first = _business_dependency_modules(LogicalPlan(), source=source)
    for _ in range(4):
        assert _business_dependency_modules(LogicalPlan(), source=source) == first
    assert entry.calls == 0


def test_iterable_factory_helper_enters_the_manifest(tmp_path, monkeypatch):
    from demiflow.data.api import DataAPI
    entry, _ = _biz(tmp_path, monkeypatch, {
        "l041_iterhelper": "def rows():\n    return [{'k': 'a', 'v': 1}]\n",
        "l041_iterentry": ("import l041_iterhelper\n\n"
                           "def make():\n    return iter(l041_iterhelper.rows())\n"),
    }, "l041_iterentry")
    source = DataAPI().from_iter(entry.make)._source
    found = _business_dependency_modules(LogicalPlan(), source=source)
    assert set(found) == {"l041_iterentry", "l041_iterhelper"}