"""Lane V unit card tests (V001-V005, V036) — the G-V required nodeids.

These exercise the acceptance-fixture mechanics themselves (tree binding,
scope/bounds admission, honest not_run gating, real process crash, type-
preserving semantics, resource-checked close). They do not require the v3
engine modules and therefore run on the current baseline. The 30 integration
scenarios live in test_runtime_v3_acceptance.py.
"""
from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import pytest

import runtime_v3_support as s
from runtime_v3_support import (
    DEFAULT_BOUNDS,
    CrashError,
    NotRunError,
    RuntimeV3Error,
    RuntimeV3Fixture,
    TreeBindingError,
    assert_tree_binding,
    make_scope,
)


def _fixture(provider="local", bounds=None):
    return RuntimeV3Fixture(make_scope(), provider=provider, bounds=bounds or DEFAULT_BOUNDS)


# -- V001 __init__ ----------------------------------------------------------
def test_v001(tmp_path):
    fx = _fixture()
    try:
        assert fx.root.parent == Path("/tmp") and fx.root.name.startswith("demiflow-")
        for sub in ("source", "sink", "evidence", "journal"):
            assert (fx.root / sub).is_dir()
        assert fx.provider == "local"
        assert fx.module_paths["demiflow"].startswith(str(s.WORKTREE))
        assert all(p.startswith(str(s.WORKTREE)) for p in fx.module_paths.values())
        assert fx.environments["local"]["available"] is True
    finally:
        fx.close()

    # Non-scope roots are rejected (never touch business directories).
    with pytest.raises(RuntimeV3Error) as exc:
        RuntimeV3Fixture(tmp_path / "business", provider="local", bounds=DEFAULT_BOUNDS)
    assert exc.value.code == "INVALID_CONTRACT"
    with pytest.raises(RuntimeV3Error):
        RuntimeV3Fixture(Path("/tmp/not-demiflow-scope"), provider="local", bounds=DEFAULT_BOUNDS)

    # Unknown provider rejected. Use a not-yet-created path: validation fails
    # before mkdir, so no scope directory is created or leaked.
    with pytest.raises(RuntimeV3Error):
        RuntimeV3Fixture(Path("/tmp/demiflow-v3-neg-provider"), provider="spark", bounds=DEFAULT_BOUNDS)

    # 0 / negative / non-int bounds are not infinite.
    for bad in ({"max_nodes": 0}, {"max_nodes": -1}, {"max_nodes": True}, {"max_nodes": 1.5}, {}):
        with pytest.raises(RuntimeV3Error):
            RuntimeV3Fixture(Path("/tmp/demiflow-v3-neg-bounds"), provider="local", bounds=bad)
    assert not Path("/tmp/demiflow-v3-neg-provider").exists()
    assert not Path("/tmp/demiflow-v3-neg-bounds").exists()

    # In-process binding must reject a main-repo editable import.
    import demiflow

    original = demiflow.__file__
    try:
        demiflow.__file__ = "/yzp/zhaozy/yangzepeng/0905/demiflow/__init__.py"
        with pytest.raises(TreeBindingError):
            assert_tree_binding()
    finally:
        demiflow.__file__ = original
    assert assert_tree_binding()["demiflow"].startswith(str(s.WORKTREE))

    # A fabricated double with a synthetic __file__ under the tree must NOT
    # impersonate a real file binding (the systemic evidence attack).
    import types

    fake_name = "demiflow.execution._v_fake_double"
    fake = types.ModuleType(fake_name)
    fake.__file__ = str(s.WORKTREE / "demiflow/execution/_v_fake_double.py")  # not on disk
    sys.modules[fake_name] = fake
    try:
        assert s._module_real(fake_name) is False
        with pytest.raises(TreeBindingError):
            assert_tree_binding()
        integrity = s.verify_evidence_integrity({})
        assert integrity["passed"] is False and fake_name in integrity["test_doubles"]
    finally:
        del sys.modules[fake_name]
    assert s.verify_evidence_integrity({})["passed"] is True


# -- V002 run ---------------------------------------------------------------
def test_v002():
    fx = _fixture()
    try:
        # Unknown scenario is an honest contract error, not a silent pass.
        with pytest.raises(RuntimeV3Error) as exc:
            fx.run("does_not_exist")
        assert exc.value.code == "INVALID_CONTRACT"

        # fault must be a string boundary or None.
        with pytest.raises(RuntimeV3Error):
            fx.run("no_ray_data", fault=123)

        # A scenario whose owning lane has not delivered is recorded not_run
        # with lane attribution — never a skip counted as pass.
        with pytest.raises(NotRunError) as exc:
            fx.run("api_matrix")  # this harness is explicitly still incomplete
        assert exc.value.code == "NOT_RUN"
        assert fx.not_run_records, "not_run must be recorded in the ledger"
        record = fx.not_run_records[-1]
        assert record["status"] == "not_run"
        assert (fx.evidence_dir / "not-run.jsonl").is_file()
        if "missing_lanes" in record:
            assert all({"module", "lane"} <= set(m) for m in record["missing_lanes"])
    finally:
        fx.close()

    # A closed fixture refuses to run.
    with pytest.raises(RuntimeV3Error):
        fx.run("no_ray_data")

    # A module present only as a fabricated double is not_run (test_double),
    # never a double-evidence pass — integration gates require real on-disk code.
    import types

    fx2 = _fixture()
    fake_name = "demiflow.execution.dataflow"
    fake = types.ModuleType(fake_name)
    fake.__file__ = str(s.WORKTREE / "demiflow/execution/dataflow.py")  # not on disk
    original_module = sys.modules.get(fake_name)
    sys.modules[fake_name] = fake
    try:
        with pytest.raises(NotRunError) as exc:
            fx2.require_modules("no_ray_data")
        missing = exc.value.missing["missing_lanes"]
        entry = next(m for m in missing if m["module"] == fake_name)
        assert entry["reason"].startswith("test_double")
    finally:
        if original_module is None:
            del sys.modules[fake_name]
        else:
            sys.modules[fake_name] = original_module
        fx2.close()


# -- V003 crash -------------------------------------------------------------
def test_v003():
    fx = _fixture()
    try:
        # A real owned child reaches a durable boundary, then is SIGKILLed.
        body = (
            "import json, time, os; from pathlib import Path; "
            f"j=Path({str(fx.journal_dir)!r}); "
            "(j/'boundary-commit.json').write_text(json.dumps({'boundary':'commit','pid':os.getpid()})); "
            "time.sleep(30)"
        )
        child = fx.spawn_child(body, name="crash-target")
        fx.crash("commit")
        assert child.returncode == -signal.SIGKILL
        record = s.json.loads((fx.evidence_dir / "crash-commit.json").read_text())
        assert record["killed"] is True and record["signal"] == signal.SIGKILL
        assert record["boundary_hit"]["boundary"] == "commit"

        # No child reaching the boundary is a protocol error, not a fake crash.
        with pytest.raises(CrashError):
            fx.crash("never-reached")
        with pytest.raises(RuntimeV3Error):
            fx.crash("")
    finally:
        fx.close()


# -- V004 assert_semantics --------------------------------------------------
def test_v004():
    fx = _fixture()
    try:
        a = fx.assert_semantics
        # Ordered, type-preserving positives.
        a([{"i": 1}], [{"i": 1}], ordering="ordered")
        a((1, 2), (1, 2), ordering="ordered")
        a(b"x", b"x", ordering="ordered")
        # tuple != list (no JSON type destruction).
        with pytest.raises(RuntimeV3Error):
            a([1, 2], (1, 2), ordering="ordered")
        # bytes != str.
        with pytest.raises(RuntimeV3Error):
            a(b"x", "x", ordering="ordered")
        # negative zero is distinguished from positive zero.
        with pytest.raises(RuntimeV3Error):
            a(-0.0, 0.0, ordering="ordered")
        a(-0.0, -0.0, ordering="ordered")
        # int != float.
        with pytest.raises(RuntimeV3Error):
            a(1, 1.0, ordering="ordered")
        # Unordered keeps duplicate multiplicity (no set dedup of join output).
        a([1, 1, 2], [2, 1, 1], ordering="unordered")
        with pytest.raises(RuntimeV3Error):
            a([1, 1, 2], [1, 2], ordering="unordered")
        with pytest.raises(RuntimeV3Error):
            a([{"k": 1}, {"k": 1}], [{"k": 1}], ordering="unordered")
        # nested type preservation under unordered.
        a([(1, 2)], [(1, 2)], ordering="unordered")
        with pytest.raises(RuntimeV3Error):
            a([(1, 2)], [[1, 2]], ordering="unordered")
        with pytest.raises(RuntimeV3Error):
            a([1], [1], ordering="sideways")
    finally:
        fx.close()


# -- V005 close -------------------------------------------------------------
def test_v005():
    fx = _fixture()
    root = fx.root
    child = fx.spawn_child("import time; time.sleep(30)", name="leak-target")
    fx.close()
    assert not root.exists(), "owned scope must be removed"
    assert child.poll() is not None, "owned child must be terminated"
    # Idempotent close.
    fx.close()

    # Cleanup failure must not swallow the original test failure.
    fx2 = _fixture()
    original = ValueError("original business failure")
    fx2.record_original_failure(original)
    fx2._owned_dirs.append(Path("/etc"))  # non-scope: close must refuse, not rm
    with pytest.raises(ValueError) as exc:
        fx2.close()
    assert exc.value is original
    assert Path("/etc").exists()

    # A non-scope owned dir is refused even without an original failure.
    fx3 = _fixture()
    fx3._owned_dirs.append(Path("/tmp"))
    with pytest.raises(RuntimeV3Error):
        fx3.close()
    assert Path("/tmp").exists()


# -- V036 runtime_v3_fixture ------------------------------------------------
class _Marker:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _Node:
    def __init__(self, marker):
        self._marker = marker

    def get_closest_marker(self, name):
        return self._marker


class _Request:
    def __init__(self, marker=None):
        self.node = _Node(marker)


def test_v036(runtime_v3_fixture, tmp_path):
    import conftest

    # Positive path through the real shared fixture.
    fx = runtime_v3_fixture
    assert isinstance(fx, RuntimeV3Fixture)
    assert fx.provider == "local"
    assert fx.root.parent == Path("/tmp") and fx.root.name.startswith("demiflow-")
    assert fx.module_paths["demiflow"].startswith(str(s.WORKTREE))
    scope = fx.root

    raw = conftest.runtime_v3_fixture.__wrapped__

    # finally.close runs when the generator completes normally.
    gen = raw(tmp_path, _Request())
    yielded = next(gen)
    owned = yielded.root
    gen.close()
    assert not owned.exists()

    # A same-host two_hosts claim fails as not_run (never a skip-as-pass).
    gen = raw(tmp_path, _Request(_Marker(environment="two_hosts")))
    with pytest.raises(NotRunError):
        next(gen)

    # A ray_core provider without ray fails rather than silently falling back.
    gen = raw(tmp_path, _Request(_Marker(provider="ray_core")))
    with pytest.raises(NotRunError):
        next(gen)

    # The original test failure propagates through teardown (not swallowed).
    gen = raw(tmp_path, _Request())
    next(gen)
    boom = RuntimeError("test body failed")
    with pytest.raises(RuntimeError) as exc:
        gen.throw(boom)
    assert exc.value is boom