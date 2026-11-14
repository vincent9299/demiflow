"""L018 unit: run_ray_graph prefetch wiring is identical to run_local_graph (Q008).

Ray is not installed in this worktree, so run_ray_graph cannot be executed here
(its function body imports ray before the wiring). Per the dispatch, missing Ray
evidence is kept unverified, not fabricated: this module pins the wiring and the
recovery byte-grant guard structurally (AST-identical to the Local route, which
L017 executes), and records the Ray end-to-end execution as an unverified
integration gate. No Ray claim is made.
"""
import ast
import pathlib

import pytest

import demiflow

assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(
    pathlib.Path(__file__).resolve().parents[1]), demiflow.__file__

from demiflow.execution import graph

_SRC = pathlib.Path(graph.__file__).read_text()
_TREE = ast.parse(_SRC)


def _func(name):
    return next(n for n in _TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)


def _prefetch_block(fn):
    body = fn.body
    idx = next(i for i, s in enumerate(body) if isinstance(s, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "prefetch_slots" for t in s.targets))
    # inflight_bytes assign, bound If, queue_depth assign, queue_depth If.
    return [ast.dump(s) for s in body[idx + 1:idx + 5]]


def _recovery_guard(fn):
    return next(ast.dump(s) for s in fn.body if isinstance(s, ast.If)
                and isinstance(s.test, ast.Compare)
                and isinstance(s.test.left, ast.Name) and s.test.left.id == "restore"
                and isinstance(s.test.ops[0], ast.IsNot))


def test_ray_prefetch_wiring_identical_to_local():
    assert _prefetch_block(_func("run_ray_graph")) == _prefetch_block(_func("run_local_graph"))


def test_ray_recovery_guard_identical_to_local():
    assert _recovery_guard(_func("run_ray_graph")) == _recovery_guard(_func("run_local_graph"))


def test_ray_wiring_consumes_both_resolved_fields():
    fn = _func("run_ray_graph")
    bound = next(s for s in fn.body if isinstance(s, ast.If)
                 and isinstance(s.test, ast.Compare)
                 and isinstance(s.test.left, ast.Name) and s.test.left.id == "prefetch_slots")
    assert any(isinstance(n, ast.ImportFrom)
               and any(a.name == "resolve_prefetch_credit" for a in n.names)
               for n in ast.walk(bound))
    assigned = {t.id for n in ast.walk(bound) if isinstance(n, ast.Assign)
                for t in n.targets if isinstance(t, ast.Name)}
    assert {"prefetch", "budget"} <= assigned


def test_ray_end_to_end_execution_unverified():
    pytest.importorskip("ray", reason="Ray absent: run_ray_graph execution is an unverified integration gate")
    pytest.skip("Ray present but full graph orchestration is an integration-tree gate, not a unit test")
