"""L042 · plan_code_pin — the source's dependencies join the one code pin.

Binds: the return shape is unchanged (exactly ``operations`` + ``dependencies``),
``operations`` is untouched by ``source``, a call without ``source`` keeps the
historical bytes exactly, a source with business callables extends the same
dependency manifest, editing the source's imported helper changes the pin
digest (the source side of gap④), and the function stays read-only — no journal
write, no artifact, no business call. Budget/unpinnable failures propagate
fail-closed rather than degrading to a weaker pin.
"""
import importlib.util
import os
import sys
import uuid

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.seal.business_sources import _DEP_MAX_MODULES, plan_code_pin
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, document, identity


@pytest.fixture(autouse=True)
def local_executor():
    with data.local_execution(workers=1, worker_mode="thread", partitions=1):
        yield


def _biz(tmp_path, monkeypatch, sources, entry):
    bizdir = tmp_path / ("l042biz_" + uuid.uuid4().hex[:8])
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
        "l042_helper": "def delta(row):\n    return row['v'] + 1\n",
        "l042_entry": ("import l042_helper\n\n"
                       "calls = 0\n\n"
                       "def fold(acc, row):\n"
                       "    global calls\n"
                       "    calls += 1\n"
                       "    return acc + l042_helper.delta(row)\n"),
    }


def _source(entry):
    return data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)._source


def _plan(*functions):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(f)) for f in functions))


def test_return_shape_is_unchanged(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    for pin in (plan_code_pin(LogicalPlan()), plan_code_pin(LogicalPlan(), source=source)):
        assert set(pin) == {"operations", "dependencies"}


def test_operations_are_untouched_by_source(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")

    def stage(row):
        return {**row, "v": row["v"] + 1}

    plan = _plan(stage)
    without = plan_code_pin(plan)
    with_source = plan_code_pin(plan, source=_source(entry))
    assert with_source["operations"] == without["operations"]
    assert set(with_source["dependencies"]) > set(without["dependencies"])


def test_no_source_call_keeps_the_historical_bytes(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    plan = _plan(entry.fold)
    historical = document(plan_code_pin(plan))
    assert document(plan_code_pin(plan, source=None)) == historical
    inert = ItemsSource(items=({"k": "a", "v": 1},))
    assert document(plan_code_pin(plan, source=inert)) == historical
    assert document(plan_code_pin(LogicalPlan(), source=inert)) == document(
        plan_code_pin(LogicalPlan()))


def test_source_dependencies_change_the_pin_digest(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    plan = LogicalPlan()
    assert identity(plan_code_pin(plan)) != identity(plan_code_pin(plan, source=source))
    assert set(plan_code_pin(plan, source=source)["dependencies"]) == {
        "l042_entry", "l042_helper"}


def test_editing_the_source_helper_changes_the_pin(tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    plan = LogicalPlan()
    before = identity(plan_code_pin(plan, source=source))
    # The reducer is carried by reference: its pickle bytes do not change, so only
    # the code-sensitive pin can catch an edit to the helper it imports.
    paths["l042_helper"].write_text("def delta(row):\n    return row['v'] + 9999\n")
    assert identity(plan_code_pin(plan, source=source)) != before


def test_a_pre_source_pin_does_not_cover_the_source(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    plan = LogicalPlan()
    legacy = identity(plan_code_pin(plan))
    assert legacy != identity(plan_code_pin(plan, source=source))


def test_read_only_no_business_call_and_no_artifact(tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    before = {path: path.read_bytes() for path in paths.values()}
    entries = set(tmp_path.iterdir())
    plan_code_pin(LogicalPlan(), source=source)
    assert entry.calls == 0
    assert {path: path.read_bytes() for path in paths.values()} == before
    assert set(tmp_path.iterdir()) == entries


def test_budget_failure_propagates_fail_closed(tmp_path, monkeypatch):
    count = _DEP_MAX_MODULES + 1
    names = ["l042_bulk%02d" % index for index in range(count)]
    sources = {name: "VALUE = %d\n" % index for index, name in enumerate(names)}
    sources["l042_bulkentry"] = (
        "".join("import " + name + "\n" for name in names)
        + "\ndef fold(acc, row):\n    return acc + row['v']\n")
    entry, _ = _biz(tmp_path, monkeypatch, sources, "l042_bulkentry")
    with pytest.raises(RecoveryContractError, match="exceeds budget"):
        plan_code_pin(LogicalPlan(), source=_source(entry))


def test_unpinnable_source_callable_fail_closes():
    class Reducer:
        def __call__(self, acc, row):
            return acc

    source = data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", Reducer())._source
    with pytest.raises(RecoveryContractError, match="not a pinnable function"):
        plan_code_pin(LogicalPlan(), source=source)


def test_pin_is_deterministic(tmp_path, monkeypatch):
    entry, _ = _biz(tmp_path, monkeypatch, _source_graph(), "l042_entry")
    source = _source(entry)
    first = document(plan_code_pin(LogicalPlan(), source=source))
    for _ in range(4):
        assert document(plan_code_pin(LogicalPlan(), source=source)) == first