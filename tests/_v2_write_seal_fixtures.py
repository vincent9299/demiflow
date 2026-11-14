"""Contract samples for the v2 native write-seam interfaces (C08).

``execution/native_plan.py`` and ``planning/dataset_dag.py`` are L-contract /
L-native deliverables and are not present in this worktree. The samples here
reproduce the *declared* C08 shapes so the L-chain wiring can be tested against
stable signatures. They are test-domain only: nothing in production imports them.

Evidence built on these samples is interface-level. It proves what chain.py
binds, dispatches and derives; it does not prove the integrated v2 write path,
which the integration tree must gate once L005/L007/L008/L031 land.
"""
from dataclasses import dataclass, field
from types import MappingProxyType, SimpleNamespace

NATIVE_ROUTES = ("native_relational", "arrow_batch", "lance_scan")
V2_SCHEMA = "demiflow.execution_bindings.v2"


@dataclass(frozen=True)
class SampleExecution:
    """C08 ExecutionPlacement: programs/stage_limits are read-only views."""
    execution_id: str
    placement: str = "driver"
    group_id: str | None = None
    backend: str = "datafusion"
    stage_ids: tuple = ()
    profile: object = field(default_factory=lambda: MappingProxyType({}))
    programs: tuple = ()
    stage_limits: object = field(default_factory=lambda: MappingProxyType({}))
    recovery: str = "native_checkpoint"


@dataclass(frozen=True)
class SampleLayout:
    """The ExecutionLayout fields the write seal reads (checkpoint/executions)."""
    checkpoint: object
    executions: tuple = ()
    artifact_manifest_ref: object = field(default_factory=lambda: {"manifest_id": "m", "sha256": "s"})
    bindings_ref: object = field(default_factory=lambda: {"path": "b.json", "sha256": "s"})
    supported_slice: str = "native_driver_v1"
    workers: tuple = ()


def program_binding(*, program_id, input_path, plan_ref=None, sealed_operators=(),
                    operator_bindings=(), source_identity_ref="src-identity"):
    return {"program_id": program_id, "input_path": list(input_path),
            "plan_ref": plan_ref if plan_ref is not None else {
                "path": "p.json", "sha256": "s", "code_pin_digest": "pin",
                "code_sources": [], "format": "code-pin-v1"},
            "source_identity_ref": source_identity_ref,
            "sealed_operators": [dict(op) for op in sealed_operators],
            "operator_bindings": [dict(ob) for ob in operator_bindings]}


def freeze_programs(programs):
    return tuple(MappingProxyType(dict(p)) for p in programs)


def decision(route, *, backend="datafusion", recovery="native_checkpoint",
             action_kind="write_lance"):
    """C08 ActionDecision sample: route/backend/recovery are already admitted."""
    return SimpleNamespace(action_kind=action_kind, backend=backend, route=route,
                           recovery=recovery)


class NativePlanRecorder:
    """Records the exact arguments chain.py passes to the native_plan seam."""

    def __init__(self, *, layout=None, placement=None, logical_dag=None, physical=None,
                 bindings_ref=None, verify_error=None):
        self.layout = layout
        self.placement = placement
        self.logical_dag = logical_dag if logical_dag is not None else {
            "dag_id": "dag", "sources": [{"source_id": "fixed"}]}
        self.physical = physical if physical is not None else {
            "plan_revision": 0, "channels": [{"channel_id": "chan"}]}
        self.bindings_ref = bindings_ref if bindings_ref is not None else {
            "path": "b.json", "sha256": "s"}
        self.verify_error = verify_error
        self.compile_calls = []
        self.load_calls = []
        self.verify_calls = []

    def compile_native_topology(self, action, source, plan, *, decision):
        self.compile_calls.append({"action_id": action.action_id, "source": source,
                                   "plan": plan, "decision": decision})
        return self.logical_dag, self.physical, self.bindings_ref

    def load_native_execution(self, run, action_id, *, execution_id):
        self.load_calls.append({"run_id": run.ref.run_id, "action_id": action_id,
                                "execution_id": execution_id})
        return self.layout, self.placement

    def verify_native_programs(self, source, plan, *, programs, action_id):
        self.verify_calls.append({"source": source, "plan": plan,
                                  "programs": programs, "action_id": action_id})
        if self.verify_error is not None:
            raise self.verify_error

    def install(self, monkeypatch):
        import sys
        module = SimpleNamespace(
            compile_native_topology=self.compile_native_topology,
            load_native_execution=self.load_native_execution,
            verify_native_programs=self.verify_native_programs)
        monkeypatch.setitem(sys.modules, "demiflow.execution.native_plan", module)
        return self


def install_dataset_dag(monkeypatch, logical_dag, *, slices=()):
    """Sample planning.dataset_dag.compile_dataset_logical_dag (returns a pair)."""
    import sys
    calls = []

    def compile_dataset_logical_dag(*, action_id, source, plan, terminal_kind, sink_ids):
        calls.append({"action_id": action_id, "source": source, "plan": plan,
                      "terminal_kind": terminal_kind, "sink_ids": sink_ids})
        return logical_dag, tuple(slices)

    monkeypatch.setitem(sys.modules, "demiflow.planning.dataset_dag",
                        SimpleNamespace(compile_dataset_logical_dag=compile_dataset_logical_dag))
    return calls