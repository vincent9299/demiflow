"""F003 verify_preserve: _catch_facts keeps the exact null/[] empty-value history.

Body byte-identical to baseline bbd93ff (SHA
75a4013106efddc70407c8ced4b53766e9c6b6b83c0a30d6e55b013799303eaa). Locks: None->None,
empty->[], deterministic sorted module.qualname names, and cancellation is never
dropped from the declared whitelist.
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
from pathlib import Path

import demiflow
from demiflow.execution.plan_facts import _catch_facts

_BASELINE_SHA = "75a4013106efddc70407c8ced4b53766e9c6b6b83c0a30d6e55b013799303eaa"


def test_module_source_is_worktree():
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def test_function_body_matches_baseline_sha():
    path = Path(demiflow.__file__).parent / "execution" / "plan_facts.py"
    source = path.read_text()
    node = next(n for n in ast.walk(ast.parse(source))
                if isinstance(n, ast.FunctionDef) and n.name == "_catch_facts")
    assert hashlib.sha256(ast.get_source_segment(source, node).encode()).hexdigest() == _BASELINE_SHA


def test_none_stays_none():
    assert _catch_facts(None) is None


def test_empty_tuple_becomes_empty_list():
    # null vs [] is a real historical distinction; empty must serialize as [].
    assert _catch_facts(()) == []
    assert _catch_facts([]) == []


def test_sorted_module_qualname_names():
    result = _catch_facts((TypeError, ValueError, KeyError))
    assert result == ["builtins.KeyError", "builtins.TypeError", "builtins.ValueError"]


def test_order_independent_determinism():
    a = _catch_facts((ValueError, TypeError, KeyError))
    b = _catch_facts((KeyError, ValueError, TypeError))
    assert a == b


def test_cancellation_is_not_swallowed():
    result = _catch_facts((asyncio.CancelledError, ValueError))
    name = asyncio.CancelledError.__module__ + "." + asyncio.CancelledError.__qualname__
    assert name in result
    assert result == sorted(result)


def test_custom_exception_class_name():
    class MyError(Exception):
        pass

    result = _catch_facts((MyError,))
    assert result == [MyError.__module__ + "." + MyError.__qualname__]


def test_repeated_call_is_stable():
    catch = (ValueError, KeyError)
    assert _catch_facts(catch) == _catch_facts(catch)