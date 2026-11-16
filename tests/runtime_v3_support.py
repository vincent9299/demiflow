"""Lane V test-domain foundation for the unified runtime v3 acceptance gates.

This module is test infrastructure only (contract C15). It never modifies
production code. It binds every run to this worktree, creates scoped resources
under ``/tmp/demiflow-*``, drives the *real* public Dataset/Pipeline/workbench
entries, and records honest evidence. Missing environments or undelivered lanes
are recorded as ``not_run`` (never a skip counted as pass).

Owned symbols: RuntimeV3Fixture.__init__/.run/.crash/.assert_semantics/.close
(V001-V005). The pytest fixture lives in tests/conftest.py (V036).
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import types
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

# The worktree that owns this test file. tests/runtime_v3_support.py -> root.
WORKTREE = Path(__file__).resolve().parent.parent
TASK_PACKAGE = WORKTREE / "docs" / "unified-runtime-v3-20261114"

PROVIDERS = ("local", "ray_core")
ENVIRONMENTS = ("local", "ray", "gpu", "two_hosts", "browser")


class RuntimeV3Error(AssertionError):
    """A real acceptance failure with a contract error code."""

    def __init__(self, code: str, message: str, **context: Any) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.context = context


class TreeBindingError(RuntimeV3Error):
    def __init__(self, message: str) -> None:
        super().__init__("INVALID_CONTRACT", message)


class NotRunError(RuntimeV3Error):
    """Environment or lane unavailable: recorded not_run, never a pass."""

    def __init__(self, scenario: str, missing: Mapping[str, Any]) -> None:
        super().__init__("NOT_RUN", f"scenario {scenario} not run: {missing}", **missing)
        self.scenario = scenario
        self.missing = dict(missing)


class CrashError(RuntimeV3Error):
    def __init__(self, message: str) -> None:
        super().__init__("INTERNAL_PROTOCOL", message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_scenarios() -> dict[str, dict[str, Any]]:
    """Read the frozen scenario table from tools/scenarios.py (single source).

    We load the design tool as data so the acceptance口径 cannot drift into a
    hand-written second table.
    """
    path = TASK_PACKAGE / "tools" / "scenarios.py"
    spec = importlib.util.spec_from_file_location("_runtime_v3_scenarios", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    table: dict[str, dict[str, Any]] = {}
    for name, intents, must, counter, envs in module.SCENARIOS:
        table[name] = {
            "intents": intents,
            "must": must,
            "counterexample": counter,
            "environments": tuple(e.strip() for e in envs.split(",")),
        }
    return table


SCENARIOS = load_scenarios()

# v3 modules each scenario consumes, with the owning implementation lane. When
# a module is absent the lane has not delivered; the scenario is recorded
# not_run with that attribution (it is still fully implemented and will run once
# the module lands). Presence of the module is NOT sufficient: the scenario then
# really runs and really fails if the default path is not unified.
SCENARIO_MODULES: dict[str, tuple[tuple[str, str], ...]] = {
    "no_ray_data": (("demiflow.execution.dataflow", "E"), ("demiflow.data.dataset", "U")),
    "api_matrix": (("demiflow.execution.dataflow", "E"), ("demiflow.data.dataset", "U")),
    "shared_diamond": (("demiflow.execution.dataflow", "E"), ("demiflow.planning.dataflow_compile", "G")),
    "incremental_first_row": (("demiflow.execution.dataflow", "E"), ("demiflow.execution.source_readers", "S")),
    "three_exchange_modes": (("demiflow.execution.exchange.dataflow_channels", "X"), ("demiflow.execution.dataflow", "E")),
    "cpu_gpu_isolation": (("demiflow.planning.dataflow_place", "P"), ("demiflow.execution.dataflow", "E")),
    "fairness_deadlock": (("demiflow.execution.dataflow_scheduler", "E"), ("demiflow.execution.dataflow", "E")),
    "relational_semantics": (("demiflow.execution.kernels.relational", "K"), ("demiflow.execution.dataflow", "E")),
    "sort_shuffle_aggregate": (("demiflow.execution.kernels.ordering", "K"), ("demiflow.execution.kernels.aggregate", "K")),
    "source_snapshots": (("demiflow.execution.source_readers", "S"), ("demiflow.execution.dataflow_sources", "S")),
    "multiworker_barrier": (("demiflow.execution.dataflow_checkpoint", "D"), ("demiflow.execution.worker.dataflow_execution", "B")),
    "route_recovery": (("demiflow.execution.exchange.dataflow_state", "X"), ("demiflow.execution.dataflow_checkpoint", "D")),
    "commit_crash_matrix": (("demiflow.execution.dataflow_checkpoint", "D"), ("demiflow.execution.commit", "D")),
    "model_effect_recovery": (("demiflow.execution.dataflow_models", "M"), ("demiflow.execution.dataflow_model_journal", "M")),
    "actor_state_lifecycle": (("demiflow.execution.kernels.operator", "K"), ("demiflow.execution.worker.runtime", "B")),
    "cache_materialize": (("demiflow.execution.dataflow_models", "M"), ("demiflow.execution.dataflow_handles", "T")),
    "queue_full_business": (("demiflow.execution.dataflow_queue", "Q"), ("demiflow.execution.queue_execution", "Q")),
    "memory_peak": (("demiflow.execution.dataflow", "E"), ("demiflow.execution.payload_limits", "E")),
    "disk_gc": (("demiflow.execution.exchange.dataflow_channels", "X"), ("demiflow.execution.dataflow_store", "H")),
    "remote_journal": (("demiflow.execution.dataflow_authority", "H"), ("demiflow.execution.worker.dataflow_host", "H")),
    "two_hosts": (("demiflow.execution.dataflow_hosts", "H"), ("demiflow.execution.dataflow_store", "H")),
    "node_loss": (("demiflow.execution.dataflow_hosts", "H"), ("demiflow.execution.dataflow_checkpoint", "D")),
    "driver_restart": (("demiflow.execution.dataflow_authority", "H"), ("demiflow.execution.dataflow", "E")),
    "history_identity": (("demiflow.execution.dataflow_history", "W"), ("demiflow.execution.recovery_identity", "W")),
    "workbench_real_plan": (("demiflow.workbench.runs.facts", "W"), ("demiflow.workbench.graph.model", "W")),
    "product_regression": (("demiflow.workbench.runs.manager", "W"), ("demiflow.workbench.graph.model", "W")),
    "bounded_terminal": (("demiflow.execution.dataflow_terminals", "T"), ("demiflow.execution.dataflow", "E")),
    "artifact_security": (("demiflow.execution.dataflow_artifacts", "H"), ("demiflow.execution.dataflow_store", "H")),
    "performance": (("demiflow.execution.dataflow", "E"), ("demiflow.execution.dataflow_scheduler", "E")),
    "all_regressions": (("demiflow.execution.dataflow", "E"),),
}


def imported_demiflow_paths() -> dict[str, str]:
    paths: dict[str, str] = {}
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            file = getattr(module, "__file__", None)
            if file:
                paths[name] = str(Path(file).resolve())
    return paths


def _module_realness(name: str, module: Any) -> tuple[bool, str]:
    """Distinguish a real on-disk module from a test-domain double.

    The systemic evidence attack: a lane fabricates ``types.ModuleType(...)`` and
    assigns a synthetic ``__file__`` under the tree, so a naive
    ``is_relative_to(tree)`` check is vacuously satisfied even though the real
    implementation is absent. A real module must (a) have a ``__file__`` that
    actually exists on disk, (b) carry a genuine import ``__spec__`` whose origin
    matches that file, and (c) live under this worktree. A real namespace package
    (no ``__file__`` but a spec with search locations) is also accepted.
    """
    spec = getattr(module, "__spec__", None)
    file = getattr(module, "__file__", None)
    if file:
        path = Path(file)
        if not path.is_file():
            return False, f"synthetic __file__ not present on disk: {file}"
        if not path.resolve().is_relative_to(WORKTREE):
            return False, f"outside worktree: {file}"
        if spec is None:
            return False, "no __spec__ (manually fabricated ModuleType)"
        origin = getattr(spec, "origin", None)
        if origin and Path(origin).resolve() != path.resolve():
            return False, f"__spec__.origin {origin} != __file__ {file}"
        return True, "real"
    if spec is not None and getattr(spec, "submodule_search_locations", None):
        return True, "real namespace package"
    return False, "no __file__ and no namespace spec (fabricated)"


def classify_modules() -> dict[str, Any]:
    """Split every loaded demiflow module into real implementations vs test
    doubles, with a reason. Reports must carry this so double evidence is never
    mistaken for a real integration-tree run."""
    real: dict[str, str] = {}
    doubles: dict[str, str] = {}
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            ok, reason = _module_realness(name, module)
            (real if ok else doubles)[name] = reason
    return {"real_modules": real, "test_doubles": doubles}


def assert_tree_binding() -> dict[str, str]:
    """In-process assertion that demiflow and every loaded submodule come from
    this worktree (IL012) AND are real on-disk modules, not synthetic-__file__
    doubles. A fabricated module fails closed here."""
    import demiflow

    root = Path(demiflow.__file__).resolve()
    if not root.is_relative_to(WORKTREE):
        raise TreeBindingError(
            f"demiflow imported from {root}, expected under worktree {WORKTREE}; "
            "run with PYTHONPATH=\"$PWD\" from the worktree (editable install points at main repo)"
        )
    paths = imported_demiflow_paths()
    for name, path in paths.items():
        if not Path(path).is_relative_to(WORKTREE):
            raise TreeBindingError(f"submodule {name} imported from outside worktree: {path}")
    classification = classify_modules()
    if classification["test_doubles"]:
        detail = "; ".join(f"{n}: {r}" for n, r in classification["test_doubles"].items())
        raise TreeBindingError(f"synthetic/double demiflow modules detected (real binding required): {detail}")
    return paths


def _module_real(module: str) -> bool:
    """True only if the module is a real on-disk implementation in this tree.

    A module present only as a fabricated double is NOT real: scenario gates use
    this so isolated-tree double evidence cannot satisfy an integration gate."""
    try:
        if module in sys.modules:
            ok, _ = _module_realness(module, sys.modules[module])
            return ok
        spec = importlib.util.find_spec(module)
        if spec is None or spec.origin is None:
            return False
        path = Path(spec.origin)
        return path.is_file() and path.resolve().is_relative_to(WORKTREE)
    except (ImportError, ValueError, ModuleNotFoundError):
        return False


def scan_production_doubles() -> list[dict[str, Any]]:
    """Static check (requirement 3): production code must hard-import real
    modules at top level, never fabricate doubles. Flag ``types.ModuleType``
    fabrication and synthetic ``__file__`` assignment anywhere under demiflow/."""
    import re

    patterns = (
        (re.compile(r"types\.ModuleType\s*\("), "types.ModuleType fabrication"),
        (re.compile(r"__file__\s*=\s*[^=]"), "synthetic __file__ assignment"),
    )
    violations: list[dict[str, Any]] = []
    for path in sorted((WORKTREE / "demiflow").rglob("*.py")):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            for pattern, label in patterns:
                if pattern.search(line):
                    violations.append(
                        {"file": str(path.relative_to(WORKTREE)), "line": lineno, "pattern": label, "text": line.strip()[:160]}
                    )
    return violations


def verify_evidence_integrity(report: Mapping[str, Any]) -> dict[str, Any]:
    """Executable evidence-integrity gate (controller requirements 1-3).

    Fails if any loaded demiflow module is a test double (synthetic __file__ /
    fabricated ModuleType), so a report built on double evidence cannot pass.
    Also surfaces production-side fabrication violations for the integration
    tree to act on. Real modules and doubles are explicitly separated."""
    classification = classify_modules()
    errors: list[str] = []
    for name, reason in classification["test_doubles"].items():
        errors.append(f"test_double in evidence: {name} ({reason})")
    production = scan_production_doubles()
    return {
        "passed": not errors,
        "errors": errors,
        "real_modules": classification["real_modules"],
        "test_doubles": classification["test_doubles"],
        "production_double_violations": production,
    }


def detect_environments() -> dict[str, dict[str, Any]]:
    """Explicit, honest environment availability. Never infer two_hosts from a
    single machine; it requires an explicit descriptor with two distinct hosts."""
    env: dict[str, dict[str, Any]] = {"local": {"available": True, "reason": "this machine"}}

    ray_spec = importlib.util.find_spec("ray") is not None
    env["ray"] = {"available": ray_spec, "reason": "ray importable" if ray_spec else "ray not installed"}

    gpu = False
    gpu_reason = "no CUDA device"
    try:
        import torch

        if torch.cuda.is_available():
            gpu = True
            gpu_reason = f"torch.cuda devices={torch.cuda.device_count()}"
    except Exception as exc:  # torch missing is a legitimate unavailable env
        gpu_reason = f"torch unavailable: {exc}"
    env["gpu"] = {"available": gpu, "reason": gpu_reason}

    two_hosts = False
    two_reason = "DEMIFLOW_V3_TWO_HOSTS descriptor not set"
    descriptor = os.environ.get("DEMIFLOW_V3_TWO_HOSTS")
    if descriptor:
        try:
            info = json.loads(Path(descriptor).read_text())
            hosts = {h["host_id"] for h in info["hosts"]}
            nodes = {h["node_id"] for h in info["hosts"]}
            if len(hosts) >= 2 and len(nodes) >= 2 and ray_spec:
                two_hosts = True
                two_reason = f"distinct hosts={sorted(hosts)}"
            else:
                two_reason = "descriptor must list >=2 distinct host_id and node_id with ray"
        except Exception as exc:
            two_reason = f"invalid two_hosts descriptor: {exc}"
    env["two_hosts"] = {"available": two_hosts, "reason": two_reason}

    browser = False
    browser_reason = "playwright chromium not installed"
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            exe = Path(p.chromium.executable_path)
            if exe.exists():
                browser = True
                browser_reason = f"chromium at {exe}"
    except Exception as exc:
        browser_reason = f"playwright unavailable: {exc}"
    env["browser"] = {"available": browser, "reason": browser_reason}
    return env


class _RayDataBlocker:
    """Meta-path finder that refuses ray.data imports and records attempts."""

    def __init__(self) -> None:
        self.attempts: list[str] = []

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if fullname == "ray.data" or fullname.startswith("ray.data."):
            self.attempts.append(fullname)
            raise ImportError(f"ray.data is blocked by the unified-runtime acceptance gate: {fullname}")
        return None

    def __enter__(self) -> "_RayDataBlocker":
        sys.meta_path.insert(0, self)
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            sys.meta_path.remove(self)
        except ValueError:
            pass


class _ExecuteActionRecorder:
    """Observe (never replace) the real execute_action; record plan identity.

    This wraps the real production facade and delegates to it. It is evidence
    collection, not a fake compiler/allocator/barrier/journal/sink.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._original: Any = None
        self._module: Any = None
        self._aliases = []

    def __enter__(self) -> "_ExecuteActionRecorder":
        try:
            self._module = importlib.import_module("demiflow.execution.dataflow")
        except ImportError:
            self._module = None
            return self
        self._original = getattr(self._module, "execute_action", None)
        if self._original is None:
            return self
        recorder = self

        def observe(observation_id: str, request: Any, *args: Any, **kwargs: Any) -> Any:
            # Only nested observer wrappers share this ID. A real Dataset call
            # made inside the production facade still enters wrapped() afresh.
            # No context/global token can conflate two independent invocations.
            delegate = getattr(recorder._original, "_runtime_v3_observe", None)
            handle = (delegate(observation_id, request, *args, **kwargs) if delegate is not None
                      else recorder._original(request, *args, **kwargs))
            entry: dict[str, Any] = {
                "observation_id": observation_id,
                "observer_pid": os.getpid(),
                "provider": getattr(request, "provider", None),
                "terminal_kind": getattr(request, "kind", None),
                "action_id": getattr(getattr(request, "action", None), "action_id", None),
                "plan_id": _extract_plan_id(handle, request),
            }
            recorder.calls.append(entry)
            return handle

        def wrapped(request: Any, *args: Any, **kwargs: Any) -> Any:
            return observe(uuid.uuid4().hex, request, *args, **kwargs)

        wrapped._runtime_v3_observe = observe
        aliases = [importlib.import_module(name) for name in
                   ("demiflow.execution.dataflow_api", "demiflow.execution.dataflow_stream")]
        self._module.execute_action = wrapped
        for api in aliases:
            if getattr(api, "execute_action", None) is self._original:
                self._aliases.append((api, self._original))
                api.execute_action = wrapped
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._module is not None and self._original is not None:
            self._module.execute_action = self._original
        for module, original in self._aliases:
            module.execute_action = original


def _merge_execution_observations(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge observations of one invocation, never equal business identities.

    Historical child receipts without an observation ID remain separate: their
    equality is not proof that the same physical invocation was observed twice.
    """
    merged: list[dict[str, Any]] = []
    known: dict[str, dict[str, Any]] = {}
    for group in groups:
        for entry in group:
            observation_id = entry.get("observation_id")
            if observation_id is None:
                merged.append(entry)
                continue
            if not isinstance(observation_id, str) or not observation_id:
                raise RuntimeV3Error("INVALID_CONTRACT", "Invalid execution observation ID")
            if observation_id in known:
                if entry != known[observation_id]:
                    raise RuntimeV3Error("INVALID_CONTRACT", "Conflicting execution observation facts")
                continue
            known[observation_id] = entry
            merged.append(entry)
    return merged


def _extract_plan_id(handle: Any, request: Any) -> Any:
    for obj in (getattr(getattr(handle, "_execution", None), "_physical", None),
                handle, getattr(request, "action", None)):
        if obj is None:
            continue
        for attr in ("plan_id", "physical", "plan"):
            value = getattr(obj, attr, None)
            if isinstance(value, str) and value:
                return value
            if value is not None and hasattr(value, "plan_id"):
                return value.plan_id
    return None


# Child bootstrap: every spawned test process re-asserts the worktree binding.
_CHILD_BOOTSTRAP = (
    "import sys; from pathlib import Path; import demiflow; "
    f"root=Path({str(WORKTREE)!r}); "
    "assert Path(demiflow.__file__).resolve().is_relative_to(root), 'child tree binding'; "
)


class RuntimeV3Fixture:
    """Scoped, tree-bound acceptance fixture (V001-V005)."""

    def __init__(self, root: Path, *, provider: str, bounds: Mapping[str, int]) -> None:
        if provider not in PROVIDERS:
            raise RuntimeV3Error("INVALID_CONTRACT", f"unknown provider {provider!r}; expected {PROVIDERS}")
        root = Path(root)
        # Only /tmp/demiflow-* scopes are permitted (V001 counterexample: never
        # touch business directories).
        if root.parent != Path("/tmp") or not root.name.startswith("demiflow-"):
            raise RuntimeV3Error(
                "INVALID_CONTRACT", f"fixture root must be /tmp/demiflow-*, got {root}"
            )
        if not isinstance(bounds, Mapping) or not bounds:
            raise RuntimeV3Error("INVALID_CONTRACT", "bounds must be a non-empty mapping")
        for key, value in bounds.items():
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise RuntimeV3Error("INVALID_CONTRACT", f"bound {key} must be a positive int (0/negative != infinite)")
        self.provider = provider
        self.bounds = dict(bounds)
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.source_dir = self.root / "source"
        self.sink_dir = self.root / "sink"
        self.evidence_dir = self.root / "evidence"
        self.journal_dir = self.root / "journal"
        for sub in (self.source_dir, self.sink_dir, self.evidence_dir, self.journal_dir):
            sub.mkdir(exist_ok=True)
        self.module_paths = assert_tree_binding()
        self.environments = detect_environments()
        self._children: list[subprocess.Popen] = []
        self._owned_dirs: list[Path] = [self.root]
        self._not_run: list[dict[str, Any]] = []
        self._closed = False
        self._original_failure: BaseException | None = None
        self._fd_baseline = self._count_fds()

    # -- environment / lane gating ----------------------------------------
    def require_environment(self, *envs: str) -> None:
        for env in envs:
            if env not in ENVIRONMENTS:
                raise RuntimeV3Error("INVALID_CONTRACT", f"unknown environment {env!r}")
            info = self.environments.get(env, {"available": False, "reason": "unknown"})
            if not info["available"]:
                self._record_not_run("environment", {"environment": env, "reason": info["reason"]})
                raise NotRunError("environment", {"missing_environment": env, "reason": info["reason"]})

    def require_modules(self, scenario: str) -> None:
        missing = []
        for module, lane in SCENARIO_MODULES.get(scenario, ()):
            if not _module_real(module):
                reason = "absent"
                if module in sys.modules:
                    _, why = _module_realness(module, sys.modules[module])
                    reason = f"test_double: {why}"
                missing.append({"module": module, "lane": lane, "reason": reason})
        if missing:
            self._record_not_run(scenario, {"missing_lanes": missing})
            raise NotRunError(scenario, {"missing_lanes": missing})

    def _record_not_run(self, scenario: str, detail: Mapping[str, Any]) -> None:
        entry = {"scenario": scenario, "status": "not_run", **detail, "ts": time.time()}
        self._not_run.append(entry)
        path = self.evidence_dir / "not-run.jsonl"
        with path.open("a") as stream:
            stream.write(json.dumps(entry, ensure_ascii=False) + "\n")

    @property
    def not_run_records(self) -> list[dict[str, Any]]:
        return list(self._not_run)

    # -- real execution ----------------------------------------------------
    def run(self, scenario: str, *, fault: str | None = None) -> Mapping[str, Any]:
        if self._closed:
            raise RuntimeV3Error("INVALID_CONTRACT", "fixture is closed")
        self.module_paths = assert_tree_binding()
        if scenario not in SCENARIOS:
            raise RuntimeV3Error("INVALID_CONTRACT", f"unknown scenario {scenario!r}")
        if fault is not None and not isinstance(fault, str):
            raise RuntimeV3Error("INVALID_CONTRACT", "fault must be a string boundary or None")
        envs = SCENARIOS[scenario]["environments"]
        # "all" means the gate aggregates every environment; the scenario itself
        # runs locally and declares any specific need via require_environment.
        selected = "ray" if self.provider == "ray_core" else "local"
        self.require_environment(selected)
        # Environment lists describe the scenario matrix, not simultaneous
        # prerequisites. A local driver-restart proof needs no second host;
        # dedicated multi-host scenarios must still prove actual two hosts.
        optional = {"local", "ray", "all"}
        # V027 explicitly permits loss of an actual producer actor owner.
        # Physical two-host placement remains mandatory for its separate gate.
        if scenario != 'two_hosts':
            optional.add('two_hosts')
        self.require_environment(*(e for e in envs if e not in optional))
        self.require_modules(scenario)
        func = _SCENARIO_FUNCS[scenario]
        from demiflow.data.api import _use_executor
        from demiflow.execution.executors.local import LocalDatasetExecutor
        from demiflow.execution.executors.ray import RayDatasetExecutor
        from demiflow.execution.run_context import RunContext
        executor = LocalDatasetExecutor() if self.provider == "local" else RayDatasetExecutor()
        from contextlib import nullcontext
        self_managed = scenario in {'commit_crash_matrix', 'model_effect_recovery', 'remote_journal', 'api_matrix',
            'multiworker_barrier', 'route_recovery', 'driver_restart', 'actor_state_lifecycle', 'performance',
            'history_identity', 'workbench_real_plan', 'product_regression', 'cpu_gpu_isolation', 'node_loss'}
        scope = nullcontext() if self_managed else RunContext.start(self.journal_dir / scenario)
        try:
            with scope, _use_executor(executor):
                with _RayDataBlocker() as blocker, _ExecuteActionRecorder() as recorder:
                    evidence = func(self, fault)
        finally:
            executor.close()
        evidence = dict(evidence)
        evidence["scenario"] = scenario
        evidence["fault"] = fault
        evidence["ray_data_import_attempts"] = list(blocker.attempts) + evidence.pop('child_ray_data_import_attempts', [])
        outer_calls = list(recorder.calls)
        child_calls = evidence.pop('child_execute_action_calls', [])
        evidence["execute_action_observations"] = {"outer": outer_calls, "case_or_child": child_calls}
        evidence["execute_action_calls"] = _merge_execution_observations(outer_calls, child_calls)
        evidence["provider"] = self.provider
        evidence["module_paths"] = assert_tree_binding()
        self._save_evidence(scenario if fault is None else scenario + '-' + fault, evidence)
        return evidence

    def _save_evidence(self, name: str, evidence: Mapping[str, Any]) -> Path:
        path = self.evidence_dir / f"{name}.json"
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=1, default=str))
        return path

    # -- real process crash at a boundary (V003) ---------------------------
    def spawn_child(self, body: str, *, name: str = "worker") -> subprocess.Popen:
        """Spawn an owned test child running real code in this worktree."""
        code = _CHILD_BOOTSTRAP + body
        proc = subprocess.Popen(
            [sys.executable, "-B", "-c", code],
            cwd=str(WORKTREE),
            env={**os.environ, "PYTHONPATH": str(WORKTREE)},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._children.append(proc)
        return proc

    def crash(self, boundary: str) -> None:
        """Kill an owned child at a real boundary; persist boundary-hit evidence.

        A plain thrown exception is NOT a process crash. We wait for the child
        to durably mark that it reached ``boundary``, then SIGKILL it and record
        the signal exit. Only owned children are killed; never external pids.
        """
        if not isinstance(boundary, str) or not boundary:
            raise RuntimeV3Error("INVALID_CONTRACT", "boundary must be a non-empty string")
        marker = self.journal_dir / f"boundary-{boundary}.json"
        target: subprocess.Popen | None = None
        deadline = time.time() + self.bounds.get("timeout_ms", 30000) / 1000.0
        while time.time() < deadline:
            if not any(proc.poll() is None for proc in self._children):
                break  # exited children cannot reach a future boundary
            for proc in self._children:
                if proc.poll() is None and marker.exists():
                    target = proc
                    break
            if target is not None:
                break
            time.sleep(0.01)
        if target is None:
            raise CrashError(f"no owned child reached boundary {boundary!r} before deadline")
        hit = json.loads(marker.read_text())
        pid = target.pid
        target.send_signal(signal.SIGKILL)
        try:
            target.wait(timeout=10)
        except subprocess.TimeoutExpired:
            raise CrashError(f"owned child {pid} did not exit after SIGKILL")
        record = {
            "boundary": boundary,
            "pid": pid,
            "killed": True,
            "signal": signal.SIGKILL,
            "returncode": target.returncode,
            "boundary_hit": hit,
            "ts": time.time(),
        }
        (self.evidence_dir / f"crash-{boundary}.json").write_text(json.dumps(record, indent=1))
        if target.returncode != -signal.SIGKILL:
            raise CrashError(f"child {pid} exit {target.returncode} is not a SIGKILL process crash")

    # -- type-preserving semantic comparison (V004) ------------------------
    def assert_semantics(self, expected: object, actual: object, *, ordering: str) -> None:
        if ordering not in ("ordered", "unordered"):
            raise RuntimeV3Error("INVALID_CONTRACT", f"ordering must be ordered/unordered, got {ordering!r}")
        if ordering == "ordered":
            _assert_deep_equal(expected, actual, path="root")
        else:
            _assert_multiset_equal(expected, actual, path="root")

    # -- resource-checked close (V005) -------------------------------------
    def _count_fds(self) -> int:
        try:
            return len(os.listdir("/proc/self/fd"))
        except OSError:
            return -1

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        problems: list[str] = []
        # Terminate only owned children; preserve their evidence.
        for proc in self._children:
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    problems.append(f"owned child {proc.pid} survived SIGKILL")
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
        # FD leak check (best-effort; baseline captured at construction).
        fd_now = self._count_fds()
        if fd_now >= 0 and self._fd_baseline >= 0 and fd_now > self._fd_baseline + 8:
            problems.append(f"FD leak: baseline={self._fd_baseline} now={fd_now}")
        # Classify owned dirs; only /tmp/demiflow-* scopes may be removed.
        removable: list[Path] = []
        for directory in self._owned_dirs:
            resolved = directory.resolve()
            if resolved.parent == Path("/tmp") and resolved.name.startswith("demiflow-"):
                removable.append(resolved)
            else:
                problems.append(f"refusing to remove non-scope directory {resolved}")
        # Preserve failure evidence before removing anything, so a close problem
        # is recorded even when the scope itself is later cleaned up.
        if problems:
            detail = {"problems": problems, "not_run": self._not_run}
            (self.root / "close-failure.json").write_text(json.dumps(detail, indent=1))
        for resolved in removable:
            _rmtree_owned(resolved, problems)
        if problems:
            # Cleanup failure must not swallow an original test failure.
            if self._original_failure is not None:
                raise self._original_failure
            raise RuntimeV3Error("INTERNAL_PROTOCOL", "fixture close left resources: " + "; ".join(problems))

    def record_original_failure(self, exc: BaseException) -> None:
        self._original_failure = exc


def _rmtree_owned(path: Path, problems: list[str]) -> None:
    import shutil

    try:
        shutil.rmtree(path)
    except OSError as exc:
        problems.append(f"failed to remove owned scope {path}: {exc}")


# -- deep, type-preserving equality -----------------------------------------
def _type_tag(value: Any) -> str:
    return type(value).__name__


def _assert_deep_equal(expected: Any, actual: Any, *, path: str) -> None:
    if type(expected) is not type(actual):
        raise RuntimeV3Error(
            "INVALID_CONTRACT", f"{path}: type mismatch expected {_type_tag(expected)} got {_type_tag(actual)}"
        )
    if isinstance(expected, float):
        # Preserve negative zero and reject NaN/Infinity drift.
        if expected == 0.0 and actual == 0.0:
            if _signbit(expected) != _signbit(actual):
                raise RuntimeV3Error("INVALID_CONTRACT", f"{path}: negative-zero mismatch {expected!r} vs {actual!r}")
        if expected != actual:
            raise RuntimeV3Error("INVALID_CONTRACT", f"{path}: float mismatch {expected!r} vs {actual!r}")
        return
    if isinstance(expected, dict):
        if set(expected) != set(actual):
            raise RuntimeV3Error("INVALID_CONTRACT", f"{path}: key set mismatch {set(expected)} vs {set(actual)}")
        for key in expected:
            _assert_deep_equal(expected[key], actual[key], path=f"{path}.{key}")
        return
    if isinstance(expected, (list, tuple)):
        if len(expected) != len(actual):
            raise RuntimeV3Error("INVALID_CONTRACT", f"{path}: length {len(expected)} vs {len(actual)}")
        for index, (e, a) in enumerate(zip(expected, actual)):
            _assert_deep_equal(e, a, path=f"{path}[{index}]")
        return
    if expected != actual:
        raise RuntimeV3Error("INVALID_CONTRACT", f"{path}: value mismatch {expected!r} vs {actual!r}")


def _signbit(value: float) -> bool:
    import math

    return math.copysign(1.0, value) < 0.0


def _freeze_key(value: Any) -> Any:
    """A hashable, type-sensitive key that keeps multiplicity (no set dedup of
    the outer collection) and distinguishes -0.0, tuple/list, bytes/str."""
    if isinstance(value, dict):
        return ("dict", tuple(sorted((k, _freeze_key(v)) for k, v in value.items())))
    if isinstance(value, list):
        return ("list", tuple(_freeze_key(v) for v in value))
    if isinstance(value, tuple):
        return ("tuple", tuple(_freeze_key(v) for v in value))
    if isinstance(value, float):
        return ("float", value, _signbit(value))
    if isinstance(value, (bytes, bytearray)):
        return (type(value).__name__, bytes(value))
    return (type(value).__name__, value)


def _assert_multiset_equal(expected: Any, actual: Any, *, path: str) -> None:
    if not isinstance(expected, (list, tuple)) or not isinstance(actual, (list, tuple)):
        _assert_deep_equal(expected, actual, path=path)
        return
    from collections import Counter

    left = Counter(_freeze_key(v) for v in expected)
    right = Counter(_freeze_key(v) for v in actual)
    if left != right:
        raise RuntimeV3Error(
            "INVALID_CONTRACT",
            f"{path}: unordered multiplicity mismatch; missing={left - right} extra={right - left}",
        )


# -- scenario implementations (real public entries) -------------------------
# Each builds and runs a real pipeline through the public Dataset/data API and
# returns evidence. Semantic assertions use assert_semantics (type preserving).
# The unified-engine evidence (execute_action recorded, ray.data blocked) is
# attached by .run and asserted by the acceptance tests.

def _data() -> Any:
    from demiflow import data

    return data


def _s_no_ray_data(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    data = _data()
    rows = [{"id": i, "v": i * 2} for i in range(24)]
    src = fx.source_dir / "items.jsonl"
    src.write_text("".join(json.dumps(r) + "\n" for r in rows))
    ds = data.read_json(str(src)).map(lambda r: {**r, "w": r["v"] + 1}).filter(lambda r: r["w"] % 2 == 1)
    batched = ds.map_batches(lambda batch: batch, batch_size=4, batch_format="pyarrow")
    out = batched.take_all()
    fx.assert_semantics(24, len(out), ordering="ordered")
    sorted_out = ds.sort("id").take_all()
    fx.assert_semantics([r["id"] for r in sorted_out], sorted(r["id"] for r in rows), ordering="ordered")
    return {"rows": len(out), "sorted_ids": [r["id"] for r in sorted_out], "terminal": "take_all+sort"}


def _s_api_matrix(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_api_scenarios import api_matrix
    if fx.provider == 'ray_core':
        from runtime_v3_ray_matrix_process import run_ray_api_matrix
        evidence = run_ray_api_matrix(fx, fault)
    else:
        evidence = api_matrix(fx, fault)
    assert evidence['api_count'] == 98 and len(evidence['api']) == 98
    assert not evidence['failed'], {key: value['failed'] for key, value in evidence['failed'].items()}
    if evidence['not_run']:
        detail = {'unverified_api': evidence['not_run'], 'executed': evidence['executed']}
        fx._record_not_run('api_matrix', detail)
        raise NotRunError('api_matrix', detail)
    assert evidence['complete'] and len(evidence['tested']) == 98
    assert set(evidence['executed']) == set(evidence['tested']) == set(evidence['api'])
    assert evidence['cases'] and all(case['status'] == 'passed' for case in evidence['cases'])
    return evidence


def _api_positive_and_negative(data: Any, symbol: str, fx: RuntimeV3Fixture) -> str:
    """Exercise a positive semantic example and an error-argument example for a
    public entry, using the real method. Returns a compact status string."""
    name = symbol.split(".")[-1]
    owner = symbol.split(".")[0]
    try:
        if owner == "Dataset":
            ds = data.from_items([{"i": 1, "k": "a"}, {"i": 2, "k": "b"}])
            method = getattr(ds, name)
            if name in ("take", "take_all", "count", "take_batch", "iter_rows", "iter_batches",
                        "schema", "columns", "size_bytes", "collect_summary", "materialize",
                        "execution_metadata", "stats"):
                _invoke_terminal(name, method, fx)
                positive = "ok"
            elif name in ("map", "filter", "flat_map", "select_columns", "drop_columns",
                          "rename_columns", "add_column", "limit"):
                _invoke_transform(name, method)
                positive = "ok"
            else:
                positive = "surface_present"
            negative = _api_negative(ds, name)
            return f"{positive}/{negative}"
        if owner == "MaterializedDataset":
            mat = data.from_items([{"i": 1}])
            if name == "num_blocks":
                mat.num_blocks()
            elif name == "release":
                mat.release()
            return "ok/ok"
        # module-level read entries
        func = getattr(data, name, None)
        if func is None:
            return "MISSING"
        _invoke_read(name, func, fx)
        return "ok/ok"
    except NotRunError:
        raise
    except Exception as exc:
        return f"error:{type(exc).__name__}:{exc}"


def _invoke_terminal(name: str, method: Any, fx: RuntimeV3Fixture) -> None:
    if name == "take":
        method(1)
    elif name == "take_all":
        method()
    elif name == "take_batch":
        method(max_batch=1)
    elif name == "count":
        method()
    elif name in ("iter_rows", "iter_batches"):
        for _ in method():
            break
    elif name == "collect_summary":
        method(max_rows=2)
    elif name == "materialize":
        method().release()
    else:
        method()


def _invoke_transform(name: str, method: Any) -> None:
    if name == "map":
        method(lambda r: r).take_all()
    elif name == "filter":
        method(lambda r: True).take_all()
    elif name == "flat_map":
        method(lambda r: [r]).take_all()
    elif name == "select_columns":
        method(["i"]).take_all()
    elif name == "drop_columns":
        method(["k"]).take_all()
    elif name == "rename_columns":
        method({"i": "j"}).take_all()
    elif name == "add_column":
        method("z", lambda r: 0).take_all()
    elif name == "limit":
        method(1).take_all()


def _invoke_read(name: str, func: Any, fx: RuntimeV3Fixture) -> None:
    if name == "from_items":
        func([{"i": 1}]).take_all()
    elif name == "from_iter":
        func(lambda: iter([{"i": 1}])).take_all()
    elif name == "range":
        func(2).take_all()
    elif name == "read_records":
        p = fx.source_dir / "m.jsonl"
        p.write_text('{"i": 1}\n')
        func(str(p)).take_all()
    elif name in ("read_json", "read_csv", "read_text", "read_parquet"):
        p = fx.source_dir / f"m.{name.split('_')[1]}"
        if name == "read_json":
            p.write_text('{"i": 1}\n')
        elif name == "read_csv":
            p.write_text("i\n1\n")
        elif name == "read_text":
            p.write_text("line\n")
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq

            pq.write_table(pa.table({"i": [1]}), str(p))
        func(str(p)).take(1)
    elif name == "from_arrow":
        import pyarrow as pa

        func(pa.table({"i": [1]})).take_all()
    elif name == "from_numpy":
        import numpy as np

        func(np.array([1, 2])).take_all()
    elif name == "from_pandas":
        import pandas as pd

        func(pd.DataFrame({"i": [1]})).take_all()
    elif name == "read_lance":
        p = fx.sink_dir / "m.lance"
        import lance

        lance.write_dataset([{"i": 1}], str(p))
        func(str(p)).take_all()
    else:
        # remaining reads need specific resources; presence + signature check
        import inspect

        inspect.signature(func)


def _api_negative(ds: Any, name: str) -> str:
    try:
        if name == "take":
            ds.take(-1)
        elif name == "sort":
            ds.sort([])
        elif name == "repartition":
            ds.repartition()
        elif name == "join":
            ds.join(ds, on=[])
        elif name == "select_columns":
            ds.select_columns([])
        else:
            return "n/a"
        return "NOT_REJECTED"
    except Exception as exc:
        return f"rejected:{type(exc).__name__}"


def _s_shared_diamond(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    log = str(fx.evidence_dir / 'shared-calls.jsonl')
    def observe(row):
        with open(log, 'a') as stream:
            stream.write(str(row['n']) + '\n')
        return row
    shared = _data().from_items([{'n': i} for i in range(3)]).map(observe)
    result = shared.map(lambda row: {**row, 'branch': 'left'}).union(
        shared.map(lambda row: {**row, 'branch': 'right'})).take_all()
    fx.assert_semantics([{'n': i, 'branch': b} for b in ('left', 'right') for i in range(3)],
                        result, ordering='unordered')
    first = Path(log).read_text().splitlines()
    fx.assert_semantics(['0', '1', '2'], first, ordering='unordered')
    base = _data().from_items([{'n': 3}])
    result = base.map(observe).union(base.map(observe)).take_all()
    fx.assert_semantics([{'n': 3}, {'n': 3}], result, ordering='unordered')
    last = Path(log).read_text().splitlines()
    fx.assert_semantics(['3', '3'], last[len(first):], ordering='unordered')
    return {'shared_calls': len(first), 'independent_calls': len(last) - len(first)}


def _s_incremental_first_row(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    released = str(fx.evidence_dir / 'consumer-received')
    exhausted = str(fx.evidence_dir / 'source-exhausted')
    def source():
        from pathlib import Path
        import time
        yield {'n': 0}
        deadline = time.monotonic() + 10
        while not Path(released).exists():
            if time.monotonic() > deadline:
                raise AssertionError('First row was held until source completion')
            time.sleep(.01)
        yield {'n': 1}
        Path(exhausted).write_text('done')
    rows = _data().from_iter(source).iter_rows()
    try:
        first = next(rows)
        fx.assert_semantics({'n': 0}, first, ordering='ordered')
        before_eoi = not Path(exhausted).exists()
        assert before_eoi
        Path(released).write_text('received')
        fx.assert_semantics([{'n': 1}], list(rows), ordering='ordered')
        assert Path(exhausted).exists()
    finally:
        Path(released).touch()
        rows.close()
    return {'first_row_before_eoi': before_eoi}


def _s_three_exchange_modes(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from dataclasses import replace
    from pytest import MonkeyPatch
    from demiflow.execution import dataflow_api as api
    from demiflow.execution.dataflow import DataflowExecution
    resolve, prepare = api.resolve_runtime_policy, DataflowExecution.prepare
    observed, results = [], {}
    for mode in ('memory_incremental', 'durable_incremental', 'finite_sealed'):
        def policy(options, limits, *, provider):
            policy, bounds = resolve(options, limits, provider=provider)
            return replace(policy, fusion=False,
                durability='durable' if mode == 'durable_incremental' else 'ephemeral'), bounds
        def capture(execution):
            sealed = prepare(execution)
            observed.append(execution)
            return sealed
        with MonkeyPatch.context() as patch:
            patch.setattr(api, 'resolve_runtime_policy', policy)
            patch.setattr(DataflowExecution, 'prepare', capture)
            ds = _data().from_items([{'n': i} for i in range(6)]).map(lambda row: row)
            if mode == 'finite_sealed':
                ds = ds.sort('n')
            rows = ds.take_all()
        fx.assert_semantics([{'n': i} for i in range(6)], rows, ordering='unordered')
        execution = observed[-1]
        channels = [edge for edge in execution._physical.exchanges if edge.mode == mode]
        assert channels and execution.finish().complete
        if mode == 'finite_sealed':
            for edge in channels:
                assert edge.channel_id in execution._finite_seals
                # The consumer ran only after every real producer succeeded.
                ref = execution._finite_seals[edge.channel_id]
                assert execution._store.inspect(ref)
                seal = json.loads(b''.join(execution._store.read(ref, max_bytes=execution._bounds.max_control_bytes)))
                assert set(seal['membership']) == set(edge.producer_partitions)
                assert all(member['status'] == 'succeeded' for member in seal['producers'])
        results[mode] = len(rows)
    return {'modes': results}


def _s_cpu_gpu_isolation(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_gpu_scenarios import cpu_gpu_isolation, validate_evidence
    evidence = cpu_gpu_isolation(fx, fault)
    validate_evidence(evidence)
    return evidence


def _s_fairness_deadlock(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_resource_scenarios import fairness_deadlock
    return fairness_deadlock(fx, fault)


def _s_relational_semantics(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    data = _data()
    left = data.from_items([{"k": 1, "a": "x"}, {"k": 1, "a": "y"}, {"k": None, "a": "n"}, {"k": 2, "a": "z"}])
    right = data.from_items([{"k": 1, "b": "p"}, {"k": 1, "b": "q"}, {"k": 3, "b": "r"}])
    inner = left.join(right, on="k", how="inner").take_all()
    # null does not match; duplicate key 1 yields 2x2=4 pairs.
    fx.assert_semantics(4, len(inner), ordering="unordered")
    semi = left.join(right, on="k", how="semi").take_all()
    fx.assert_semantics(2, len(semi), ordering="unordered")
    anti = left.join(right, on="k", how="anti").take_all()
    fx.assert_semantics(2, len(anti), ordering="unordered")
    leftj = left.join(right, on="k", how="left").take_all()
    fx.assert_semantics(6, len(leftj), ordering="unordered")

    def non_assoc(acc: list, row: dict) -> list:
        return (acc or []) + [row["k"]]

    grouped = left.reduce_by_key("k", non_assoc, initial=[]).take_all()
    return {"inner": len(inner), "semi": len(semi), "anti": len(anti), "left": len(leftj), "groups": len(grouped)}


def _s_sort_shuffle_aggregate(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    data = _data()
    rows = [{"a": i % 3, "b": i, "v": float(i)} for i in range(30)]
    ds = data.from_items(rows)
    sorted_out = ds.sort(["a", "b"]).take_all()
    keys = [(r["a"], r["b"]) for r in sorted_out]
    fx.assert_semantics(sorted(keys), keys, ordering="ordered")
    shuffled = ds.random_shuffle(seed=7).take_all()
    fx.assert_semantics(rows, shuffled, ordering="unordered")
    again = ds.random_shuffle(seed=7).take_all()
    fx.assert_semantics([r["b"] for r in shuffled], [r["b"] for r in again], ordering="ordered")
    total = ds.aggregate(data.Sum("v", alias_name="total"), data.Count("v", alias_name="n"))
    return {"sorted": len(sorted_out), "shuffle_stable": True, "aggregate": total}


def _s_source_snapshots(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    data = _data()
    import lance

    p = fx.sink_dir / "snap.lance"
    lance.write_dataset([{"i": 1}], str(p))
    v1 = data.read_lance(str(p)).take_all()
    lance.write_dataset([{"i": 2}], str(p), mode="append")
    pinned = data.read_lance(str(p), version=1).take_all()
    fx.assert_semantics([{"i": 1}], pinned, ordering="unordered")
    head = data.read_lance(str(p)).take_all()
    fx.assert_semantics(2, len(head), ordering="unordered")
    return {"pinned_version": len(pinned), "head": len(head), "v1": len(v1)}


def _s_multiworker_barrier(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_residency_scenarios import multiworker_barrier
    return multiworker_barrier(fx, fault)


def _s_route_recovery(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_residency_scenarios import route_recovery
    return route_recovery(fx, fault)


def _s_commit_crash_matrix(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_recovery_scenarios import commit_crash_matrix
    return commit_crash_matrix(fx, fault)


def _s_model_effect_recovery(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_recovery_scenarios import model_effect_recovery
    return model_effect_recovery(fx, fault)


def _s_actor_state_lifecycle(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_residency_scenarios import actor_state_lifecycle
    return actor_state_lifecycle(fx, fault)


def _s_cache_materialize(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    # Re-verify the 7 historical cache/async-materialize defects for real.
    nodeids = [
        "tests/test_bound_result_cache.py::test_unbound_async_none_and_list_keep_map_semantics",
        "tests/test_bound_result_cache.py::test_only_computation_result_is_shared[None-False]",
        "tests/test_bound_result_cache.py::test_only_computation_result_is_shared[answer1-False]",
        "tests/test_bound_result_cache.py::test_only_computation_result_is_shared[answer2-False]",
        "tests/test_bound_result_cache.py::test_shared_wait_cancellation_and_actor_hooks",
        "tests/test_async_materialize.py::test_async_materialize_spills_reuses_and_writes",
        "tests/test_async_materialize.py::test_wide_rows_spill_before_row_limit_and_restore_individually",
    ]
    results = run_nodeids(nodeids, fx.root / "cache-reverify")
    return {"historical_defects": results}


def _s_queue_full_business(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_queue_business_scenarios import queue_full_business
    return queue_full_business(fx, fault)


def _s_memory_peak(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_resource_scenarios import memory_peak
    return memory_peak(fx, fault)


def _s_disk_gc(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_gc_scenarios import disk_gc
    return disk_gc(fx, fault)


def _s_remote_journal(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_recovery_scenarios import remote_journal
    return remote_journal(fx, fault)


def _s_two_hosts(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    detail = {"missing_semantic_harness": 'host names in a JSON file do not prove actual Ray node placement or shared-store access'}
    fx._record_not_run('two_hosts', detail)
    raise NotRunError('two_hosts', detail)


def _s_node_loss(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_node_loss_scenarios import node_loss, validate_evidence
    evidence = node_loss(fx, fault)
    validate_evidence(evidence)
    return evidence


def _s_driver_restart(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_recovery_scenarios import driver_restart
    return driver_restart(fx, fault)


def _s_history_identity(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_product_scenarios import history_identity
    return history_identity(fx, fault)


def _s_workbench_real_plan(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_product_scenarios import workbench_real_plan
    return workbench_real_plan(fx, fault)


def _s_product_regression(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_product_scenarios import product_regression
    return product_regression(fx, fault)


def _s_bounded_terminal(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    data = _data()
    ds = data.from_items([{"i": i} for i in range(10)])
    fx.assert_semantics([], ds.take(0), ordering="ordered")
    fx.assert_semantics(1, len(ds.take(1)), ordering="ordered")
    fx.assert_semantics(10, len(ds.take_all()), ordering="ordered")
    fx.assert_semantics(10, ds.count(), ordering="ordered")
    try:
        ds.take_all(limit=5)
    except ValueError:
        pass
    else:
        raise AssertionError("take_all(limit=5) truncated instead of rejecting overflow")
    it = ds.iter_rows()
    next(it)
    it.close()
    return {"take0": 0, "take1": 1, "take_all": 10, "limit5": "overflow_rejected"}


def _s_artifact_security(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_artifact_security_scenarios import artifact_security, validate_evidence
    evidence = artifact_security(fx, fault)
    validate_evidence(evidence)
    return evidence


def _s_performance(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    from runtime_v3_performance_scenarios import performance
    return performance(fx, fault)


def _s_all_regressions(fx: RuntimeV3Fixture, fault: str | None) -> dict[str, Any]:
    detail = {"missing_semantic_harness": 'must execute classified historical nodeids; collection is inventory, not a passing regression run'}
    fx._record_not_run('all_regressions', detail)
    raise NotRunError('all_regressions', detail)


_SCENARIO_FUNCS: dict[str, Callable[[RuntimeV3Fixture, str | None], dict[str, Any]]] = {
    "no_ray_data": _s_no_ray_data,
    "api_matrix": _s_api_matrix,
    "shared_diamond": _s_shared_diamond,
    "incremental_first_row": _s_incremental_first_row,
    "three_exchange_modes": _s_three_exchange_modes,
    "cpu_gpu_isolation": _s_cpu_gpu_isolation,
    "fairness_deadlock": _s_fairness_deadlock,
    "relational_semantics": _s_relational_semantics,
    "sort_shuffle_aggregate": _s_sort_shuffle_aggregate,
    "source_snapshots": _s_source_snapshots,
    "multiworker_barrier": _s_multiworker_barrier,
    "route_recovery": _s_route_recovery,
    "commit_crash_matrix": _s_commit_crash_matrix,
    "model_effect_recovery": _s_model_effect_recovery,
    "actor_state_lifecycle": _s_actor_state_lifecycle,
    "cache_materialize": _s_cache_materialize,
    "queue_full_business": _s_queue_full_business,
    "memory_peak": _s_memory_peak,
    "disk_gc": _s_disk_gc,
    "remote_journal": _s_remote_journal,
    "two_hosts": _s_two_hosts,
    "node_loss": _s_node_loss,
    "driver_restart": _s_driver_restart,
    "history_identity": _s_history_identity,
    "workbench_real_plan": _s_workbench_real_plan,
    "product_regression": _s_product_regression,
    "bounded_terminal": _s_bounded_terminal,
    "artifact_security": _s_artifact_security,
    "performance": _s_performance,
    "all_regressions": _s_all_regressions,
}


# -- historical regression machinery (V035) ---------------------------------
def historical_dispositions() -> list[dict[str, Any]]:
    doc = json.loads((TASK_PACKAGE / "evidence" / "historical-dispositions.json").read_text())
    return doc["cases"]


def collect_repository(out_dir: Path) -> dict[str, Any]:
    """Collect every test file/nodeid in the repository via real pytest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "--collect-only", "-q", "tests"],
        cwd=str(WORKTREE),
        env={**os.environ, "PYTHONPATH": str(WORKTREE)},
        capture_output=True,
        text=True,
        timeout=1200,
    )
    (out_dir / "collect-stdout.txt").write_text(proc.stdout)
    (out_dir / "collect-stderr.txt").write_text(proc.stderr)
    nodeids = [line.strip() for line in proc.stdout.splitlines() if "::" in line and not line.startswith("=")]
    files = sorted({n.split("::")[0] for n in nodeids})
    baseline = json.loads((TASK_PACKAGE / "evidence" / "regression-files.json").read_text())
    baseline_files = {f["file"] for f in baseline["files"]}
    new_files = sorted(set(files) - baseline_files)
    baseline_only = sorted(baseline_files - set(files))
    return {
        "exit_code": proc.returncode,
        "file_count": len(files),
        "nodeid_count": len(nodeids),
        "baseline_file_count": baseline["count"],
        "new_files": new_files,
        "baseline_only": baseline_only,
        "baseline_only_reason": "module-level pytest.importorskip('ray'); not collectable without ray",
        "files": files,
    }


def run_nodeids(nodeids: list[str], out_dir: Path) -> dict[str, Any]:
    """Run specific nodeids in an isolated subprocess and record real outcomes."""
    out_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "--no-header", "-rA", *nodeids],
        cwd=str(WORKTREE),
        env={**os.environ, "PYTHONPATH": str(WORKTREE)},
        capture_output=True,
        text=True,
        timeout=1800,
    )
    (out_dir / "run-stdout.txt").write_text(proc.stdout)
    (out_dir / "run-stderr.txt").write_text(proc.stderr)
    outcomes: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        for status in ("PASSED", "FAILED", "ERROR", "SKIPPED", "XFAIL"):
            if line.startswith(status) and "::" in line:
                outcomes[line.split(" ")[1]] = status
    return {"exit_code": proc.returncode, "outcomes": outcomes}


def build_disposition_table() -> list[dict[str, Any]]:
    """Map each of the 44 historical failures to its current-behavior assertion,
    change rationale, replacement assertion and owner. The 37 old-contract cases
    keep a current-behavior assertion plus a mapped replacement; the 7 real
    defects retain the original case and must be fixed to pass (no xfail/delete).
    """
    table = []
    for case in historical_dispositions():
        nodeid = case["nodeid"]
        cls = case["historical_class"]
        if cls == "preexisting_real_defect":
            entry = {
                "nodeid": nodeid,
                "historical_class": cls,
                "owner": case["owner"],
                "current_behavior_assertion": "original case retained verbatim; must pass after M/T fix",
                "change": "real cache/async-materialize defect; fix production, do not weaken test",
                "replacement_assertion": nodeid,
                "required": case["required"],
            }
        else:
            entry = {
                "nodeid": nodeid,
                "historical_class": cls,
                "owner": case["owner"],
                "current_behavior_assertion": _old_contract_current_assertion(nodeid),
                "change": _old_contract_change(nodeid),
                "replacement_assertion": _old_contract_replacement(nodeid),
                "required": case["required"],
            }
        table.append(entry)
    return table


def _old_contract_current_assertion(nodeid: str) -> str:
    if "checkpoint_bridges" in nodeid or "stream_sink_checkpoint" in nodeid:
        return "retired checkpoint/checkpoint_lance entries must reject (not revive old protocol)"
    if "every_explicit_option_rejects" in nodeid:
        return "streaming options now translate to the unified contract; assert the new admission behavior"
    if "nested_relations_and_lazy_workers" in nodeid:
        return "nested relations/lazy workers admission follows unified preflight; assert new rejection point"
    if "lance_checkpoint_writer_failure" in nodeid:
        return "writer failure joins stream and closes workers under unified sink protocol"
    return "assert current unified behavior for this entry"


def _old_contract_change(nodeid: str) -> str:
    if "checkpoint" in nodeid:
        return "checkpoint/checkpoint_async/checkpoint_lance/checkpoint_lance_async retired in v3 (C11)"
    return "v2 streaming-option/lazy-action rejection replaced by unified ActionRequest admission (C02/C11)"


def _old_contract_replacement(nodeid: str) -> str:
    if "checkpoint_bridges" in nodeid or "stream_sink_checkpoint" in nodeid:
        return "tests/test_runtime_v3_u.py retired-checkpoint rejection cases (U lane)"
    return "tests/test_runtime_v3_u.py / _l.py / _y.py unified admission cases (U/L/Y lanes)"


# -- evidence_guard-schema report export ------------------------------------
def source_digests() -> dict[str, str]:
    digests: dict[str, str] = {}
    for base in ("demiflow", "tests"):
        for path in sorted((WORKTREE / base).rglob("*.py")):
            digests[str(path.relative_to(WORKTREE))] = _sha256(path)
    digests["pyproject.toml"] = _sha256(WORKTREE / "pyproject.toml")
    return digests


class ReportPlugin:
    """pytest plugin capturing setup/call/teardown per nodeid for the report."""

    def __init__(self) -> None:
        self.results: list[dict[str, Any]] = []
        self.collected: list[str] = []
        self.collection_errors: list[str] = []
        self.deselected: list[str] = []
        self.exit_code: int | None = None

    def pytest_collection_modifyitems(self, session: Any, config: Any, items: list) -> None:
        self.collected = [item.nodeid for item in items]

    def pytest_collectreport(self, report: Any) -> None:
        if report.failed:
            self.collection_errors.append(str(report.longrepr))

    def pytest_deselected(self, items: list) -> None:
        self.deselected.extend(item.nodeid for item in items)

    def pytest_runtest_logreport(self, report: Any) -> None:
        self.results.append(
            {
                "nodeid": report.nodeid,
                "when": report.when,
                "outcome": report.outcome,
                "wasxfail": hasattr(report, "wasxfail"),
                "rerun": getattr(report, "rerun", 0) or 0,
            }
        )

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        self.exit_code = int(exitstatus)


def run_and_export(nodeids: list[str], report_path: Path, required_nodeids: list[str]) -> dict[str, Any]:
    """Run pytest in-process with the report plugin and export an
    evidence_guard.evaluate-compatible report. Returns the evaluation result.

    The report explicitly separates real modules from test doubles and runs the
    executable evidence-integrity gate: a report built on synthetic-__file__
    doubles cannot pass, even if a naive is_relative_to(tree) check is satisfied."""
    import pytest

    before = source_digests()
    plugin = ReportPlugin()
    args = ["-p", "no:cacheprovider", "--no-header", "-q", *nodeids]
    exit_code = pytest.main(args, plugins=[plugin])
    after = source_digests()
    classification = classify_modules()
    report = {
        "exit_code": int(exit_code),
        "collection_errors": plugin.collection_errors,
        "deselected": plugin.deselected,
        "collected": plugin.collected,
        "source_before": before,
        "source_after": after,
        "module_paths": imported_demiflow_paths(),
        "real_modules": classification["real_modules"],
        "test_doubles": classification["test_doubles"],
        "results": plugin.results,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=1))
    guard = _load_evidence_guard()
    evaluation = guard.evaluate(report, required_nodeids=required_nodeids, expected_tree=WORKTREE, expected_digests=before)
    integrity = verify_evidence_integrity(report)
    (report_path.parent / (report_path.stem + ".integrity.json")).write_text(json.dumps(integrity, indent=1))
    evaluation = dict(evaluation)
    evaluation["integrity"] = integrity
    if not integrity["passed"]:
        evaluation["passed"] = False
        evaluation["errors"] = list(evaluation.get("errors", [])) + integrity["errors"]
    (report_path.parent / (report_path.stem + ".evaluation.json")).write_text(json.dumps(evaluation, indent=1))
    return evaluation


def _load_evidence_guard() -> Any:
    path = TASK_PACKAGE / "tools" / "evidence_guard.py"
    spec = importlib.util.spec_from_file_location("_runtime_v3_evidence_guard", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_scope(prefix: str = "demiflow-v3-") -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix, dir="/tmp"))


DEFAULT_BOUNDS: dict[str, int] = {
    "max_nodes": 4096,
    "max_edges": 16384,
    "max_partitions": 64,
    "max_depth": 64,
    "max_control_bytes": 65536,
    "max_row_bytes": 8 * 1024 * 1024,
    "max_batch_bytes": 32 * 1024 * 1024,
    "max_resident_bytes": 256 * 1024 * 1024,
    "max_spill_bytes": 512 * 1024 * 1024,
    "max_state_bytes": 64 * 1024 * 1024,
    "max_pending_events": 1024,
    "max_open_files": 256,
    "max_output_rows": 100000,
    "timeout_ms": 30000,
}
