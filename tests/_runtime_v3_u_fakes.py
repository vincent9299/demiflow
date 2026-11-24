"""Test-domain spec-bound fakes for cross-lane modules not yet landed in this tree.

Lane U migrates the Dataset public entries to the unified v3 facade. The facade
(E015/E016/E017 ``demiflow.execution.dataflow_api``) and the C action DTO
(``demiflow.execution.contracts.dataflow``) are not present in this worktree's
baseline, so they are injected in-process here as strict recording doubles that
enforce the frozen interfaces.pyi contract (provider mapping, admitted action
kinds, bounded Json declarations). Production code never imports these fakes and
never falls back to them; the integration tree runs the real E/C implementations.

Every other module exercised by these tests (checked.action, the local executor,
local_kernel, the model/document/Lance actors) is the real baseline implementation
from this worktree. ``assert_tree_binding`` proves that in-process.
"""
from __future__ import annotations

import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_TREE = Path(__file__).resolve().parents[1]

FAKE_MODULE_NAMES = (
    'demiflow.execution.executors.dataflow_api',
    "demiflow.execution.contracts.dataflow",
)

# Frozen E015/E016 admission facts (interfaces.pyi / dataflow_api).
_ACTION_KINDS = frozenset({
    "take", "take_all", "take_batch", "iter_rows", "iter_batches", "count",
    "aggregate", "schema", "columns", "size_bytes", "materialize",
    "write_datasink", "write_lance", "write_file", "run_stream", "collect_summary",
})
_PROVIDER = {"local": "local", "ray": "ray_core"}
_MAX_DECLARATION_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ActionProgram:
    name: str
    kind: str
    value: Any


@dataclass
class NativeOperation:
    kind: str
    configuration: dict
    bindings: dict


@dataclass
class ActionRequest:
    kind: str
    source: Any
    plan: Any
    arguments: dict
    sink: Any
    programs: tuple
    provider: str


class Record:
    """In-process call log shared by the recording doubles; reset per test."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.native: list[NativeOperation] = []
        self.requests: list[ActionRequest] = []
        self.runs: list[ActionRequest] = []
        self.results: dict[str, Any] = {}

    def native_kinds(self) -> list[str]:
        return [op.kind for op in self.native]

    def request_kinds(self) -> list[str]:
        return [req.kind for req in self.requests]

    def last_request(self) -> ActionRequest:
        return self.requests[-1]


RECORD = Record()


def _json_mapping(value: object, label: str) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError(label + " must be a mapping")
    try:
        raw = json.dumps(dict(value), sort_keys=True, allow_nan=False, default=str)
    except (TypeError, ValueError) as exc:
        raise ValueError(label + " must be bounded Json") from exc
    if len(raw.encode("utf-8")) > _MAX_DECLARATION_BYTES:
        raise ValueError(label + " exceeds declaration budget")
    return dict(value)


def make_native_operation(kind: str, configuration: Mapping[str, object], *,
                          bindings: Mapping[str, object]) -> NativeOperation:
    if type(kind) is not str or not kind:
        raise ValueError("native operation kind must be a non-empty str")
    config = _json_mapping(configuration, "configuration")
    binds = _json_mapping(bindings, "bindings")
    operation = NativeOperation(kind=kind, configuration=config, bindings=binds)
    RECORD.native.append(operation)
    return operation


def make_action_request(executor, source, plan, *, kind, arguments,
                        sink=None, programs=()) -> ActionRequest:
    provider = _PROVIDER.get(getattr(executor, "NAME", ""))
    if provider is None:
        raise ValueError("unsupported executor provider")
    if type(kind) is not str or kind not in _ACTION_KINDS:
        raise ValueError("unknown action kind: " + str(kind))
    args = _json_mapping(arguments, "arguments")
    request = ActionRequest(kind=kind, source=source, plan=plan, arguments=args,
                            sink=sink, programs=tuple(programs), provider=provider)
    RECORD.requests.append(request)
    return request


def run_dataset_action(request: ActionRequest) -> Any:
    RECORD.runs.append(request)
    return RECORD.results.get(request.kind)


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def install_fakes() -> None:
    if _real_module_available('demiflow.execution.executors.dataflow_api'):
        return
    api = types.ModuleType('demiflow.execution.executors.dataflow_api')
    api.make_native_operation = make_native_operation
    api.make_action_request = make_action_request
    api.run_dataset_action = run_dataset_action
    api._ACTION_KINDS = _ACTION_KINDS
    sys.modules['demiflow.execution.executors.dataflow_api'] = api

    dto = types.ModuleType("demiflow.execution.contracts.dataflow")
    dto.ActionProgram = ActionProgram
    sys.modules["demiflow.execution.contracts.dataflow"] = dto


def assert_tree_binding() -> dict:
    """In-process assertion that every real demiflow module comes from this tree."""
    import demiflow
    root = Path(demiflow.__file__).resolve()
    assert root.is_relative_to(_TREE), f"demiflow not from worktree: {root}"
    loaded = {}
    for name, module in list(sys.modules.items()):
        if not name.startswith("demiflow"):
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            assert name in FAKE_MODULE_NAMES, f"unexpected file-less module: {name}"
            continue
        resolved = Path(path).resolve()
        assert resolved.is_relative_to(_TREE), f"module outside worktree: {name} -> {resolved}"
        loaded[name] = str(resolved)
    return loaded