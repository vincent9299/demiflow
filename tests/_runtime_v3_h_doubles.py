"""Test-domain spec-bound doubles for lane H leaf tests (test_doubles).

These fakes stand in for absent peer lanes only: the C-lane frozen DTO/codec
modules (field-compatible with the delivered C contracts), the D-lane seal
loader, and the external Ray carrier. They are never used by production code and
are explicitly marked test_doubles in the evidence report. The real demiflow
modules under test always bind to this worktree.
"""
import importlib.machinery
import sys
import types
from dataclasses import dataclass
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def assert_tree():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(ROOT)), demiflow.__file__
    return demiflow


def _real_module_available(name):
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def install():
    names = ('demiflow.execution.contracts.dataflow',
             'demiflow.execution.contracts.dataflow_errors',
             'demiflow.execution.contracts.dataflow_codec')
    if all(_real_module_available(n) for n in names):
        return importlib.import_module(names[0])
    df = types.ModuleType('demiflow.execution.contracts.dataflow')

    @dataclass(frozen=True)
    class ArtifactRef:
        digest: str; size_bytes: int; store_id: str; key: str; media_type: str

    @dataclass(frozen=True)
    class Bounds:
        max_nodes: int = 8; max_edges: int = 8; max_partitions: int = 8; max_depth: int = 8
        max_control_bytes: int = 1 << 20; max_row_bytes: int = 1 << 20; max_batch_bytes: int = 1 << 20
        max_resident_bytes: int = 1 << 20; max_spill_bytes: int = 1 << 20; max_state_bytes: int = 1 << 20
        max_pending_events: int = 64; max_open_files: int = 64; max_output_rows: int = 1 << 20
        timeout_ms: int = 2000

    @dataclass(frozen=True)
    class ArtifactCommand:
        operation_id: str; kind: str; ref: object; owner: str; size_bytes: int

    @dataclass(frozen=True)
    class ArtifactReply:
        operation_id: str; present: object; charged_bytes: int

    @dataclass(frozen=True)
    class ControlReply:
        operation_id: str; status: str; payload: object; payload_ref: object; error_code: object

    @dataclass(frozen=True)
    class CompletedControl:
        allocation_id: str; boot_id: str; reply: object

    @dataclass(frozen=True)
    class EffectCommand:
        operation_id: str; kind: str; effect_id: str; actor_id: str; request_digest: str
        payload_ref: object; budget: dict

    @dataclass(frozen=True)
    class EffectReply:
        operation_id: str; status: str; result_ref: object; receipt_ref: object

    @dataclass(frozen=True)
    class QueueBinding:
        action_id: str; source_id: object; mode: str; contract_digest: str; generation: object
        pool: object; source_occurrence: object; fetch_occurrence: str; final_occurrence: object

    @dataclass(frozen=True)
    class QueueCommand:
        operation_id: str; operation: str; binding: object; record_id: object
        delivery_ref: object; row_ref: object

    @dataclass(frozen=True)
    class QueueReply:
        operation_id: str; status: str; delivery_ref: object; row_ref: object
        native_fact_ref: object; retry_after_ms: int

    @dataclass(frozen=True)
    class NativeJournalCommand:
        operation_id: str; operation: str; action_id: str; node_id: str; request_key: str
        request_ref: object; value_ref: object; reservation: object; options: dict

    @dataclass(frozen=True)
    class NativeJournalReply:
        operation_id: str; value: object; value_ref: object

    @dataclass(frozen=True)
    class WorkerEvent:
        schema: str; kind: str; operation_id: str; run_id: str; attempt: int; fence: str
        allocation_id: str; boot_id: str; plan_revision: int; scope_id: str
        controller_epoch: int; payload_ref: object; payload: object

    @dataclass(frozen=True)
    class WorkerLease:
        allocation_id: str; boot_id: str; stage_id: str; partition_ids: tuple; host_id: str
        endpoint_ref: str; grant_ref: object; expires_at_ms: int

    @dataclass(frozen=True)
    class SealedExecution:
        logical_ref: object; physical_ref: object; programs: tuple; code_pin_ref: object
        identity_bridge_ref: object; source_refs: tuple; constants_ref: object

    @dataclass(frozen=True)
    class HostDescriptor:
        host_id: str; provider: str; runtime_revision: str; store_domains: tuple
        logical_resources: dict; isolation_capabilities: tuple

    @dataclass(frozen=True)
    class DomainProof:
        host_id: str; store_id: str; challenge_digest: str; namespace_digest: str; durable: bool

    @dataclass(frozen=True)
    class DataLocation:
        domain: str; temporary_locator: object; durable_ref: object

    @dataclass(frozen=True)
    class RuntimePolicy:
        provider: str; durability: str; default_partitions: int; max_replicas: int; fusion: bool
        checkpoint_rows: int; checkpoint_interval_ms: int; fairness_quantum_bytes: int
        control_timeout_ms: int; store_id: str; required_recovery: str

    @dataclass(frozen=True)
    class LoadedProgram:
        kind: str; value: object; digest: str

    @dataclass(frozen=True)
    class GraphNode:
        occurrence_id: str; program_ref: object = None

    @dataclass(frozen=True)
    class GraphEdge:
        source: str; target: str

    @dataclass(frozen=True)
    class DataflowGraph:
        schema: str; action_id: str; dag_id: str; nodes: tuple; edges: tuple
        terminals: tuple; sources: tuple

    @dataclass(frozen=True)
    class PhysicalDataflow:
        schema: str; dag_id: str; plan_id: str; plan_revision: int; policy: object; bounds: object

    @dataclass(frozen=True)
    class WorkerAssignment:
        schema: str; run_id: str; action_id: str; attempt: int; fence: str; plan_id: str
        plan_revision: int; lease: object; sealed: object; stages: tuple; inputs: tuple
        outputs: tuple; source_splits: tuple; restore_refs: tuple; bounds: object

    @dataclass(frozen=True)
    class DriverServices:
        action: object; journal_service: object; store: object; resources: object; policy: object
        bounds: object; hosts: tuple; clock_ms: object

    @dataclass(frozen=True)
    class RuntimeServices:
        store: object; journal: object; bounds: object; assignment: object; clock_ms: object
        check_stop: object; constants: object; programs: object

    for name, obj in dict(
            ArtifactRef=ArtifactRef, Bounds=Bounds, ArtifactCommand=ArtifactCommand,
            ArtifactReply=ArtifactReply, ControlReply=ControlReply, CompletedControl=CompletedControl,
            EffectCommand=EffectCommand, EffectReply=EffectReply, QueueBinding=QueueBinding,
            QueueCommand=QueueCommand, QueueReply=QueueReply, NativeJournalCommand=NativeJournalCommand,
            NativeJournalReply=NativeJournalReply, WorkerEvent=WorkerEvent, WorkerLease=WorkerLease,
            SealedExecution=SealedExecution, HostDescriptor=HostDescriptor, DomainProof=DomainProof,
            DataLocation=DataLocation, RuntimePolicy=RuntimePolicy, LoadedProgram=LoadedProgram,
            GraphNode=GraphNode, GraphEdge=GraphEdge, DataflowGraph=DataflowGraph,
            PhysicalDataflow=PhysicalDataflow, WorkerAssignment=WorkerAssignment,
            DriverServices=DriverServices, RuntimeServices=RuntimeServices).items():
        setattr(df, name, obj)

    err = types.ModuleType('demiflow.execution.contracts.dataflow_errors')

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            super().__init__(message)
            self.code = code; self.retryable = retryable; self.context = context

    err.DataflowError = DataflowError; df.DataflowError = DataflowError

    import json as _json
    from dataclasses import asdict as _asdict
    codec = types.ModuleType('demiflow.execution.contracts.dataflow_codec')
    _REF_KEYS = {'digest', 'size_bytes', 'store_id', 'key', 'media_type'}

    def _revive(v):
        if isinstance(v, dict):
            if _REF_KEYS.issubset(v.keys()):
                return ArtifactRef(**{k: v[k] for k in _REF_KEYS})
            return {k: _revive(x) for k, x in v.items()}
        if isinstance(v, list):
            return [_revive(x) for x in v]
        return v

    def encode_wire(value, *, type_name, bounds):
        raw = _json.dumps(_asdict(value), sort_keys=True, separators=(',', ':')).encode()
        if len(raw) > bounds.max_control_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'frame exceeds budget')
        return raw

    def decode_wire(raw, *, type_name, bounds):
        if len(raw) > bounds.max_control_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'frame exceeds budget')
        data = _revive(_json.loads(raw))
        return getattr(df, type_name)(**data)

    codec.encode_wire = encode_wire; codec.decode_wire = decode_wire
    import demiflow.execution.contracts as pkg
    sys.modules['demiflow.execution.contracts.dataflow'] = df
    sys.modules['demiflow.execution.contracts.dataflow_errors'] = err
    sys.modules['demiflow.execution.contracts.dataflow_codec'] = codec
    pkg.dataflow = df; pkg.dataflow_errors = err; pkg.dataflow_codec = codec
    return df


def build_fake_ray():
    """Spec-bound fake of the external Ray carrier; caller scopes the binding."""
    if 'ray' in sys.modules:
        return sys.modules['ray']
    ray = types.ModuleType('ray')
    ray.__spec__ = importlib.machinery.ModuleSpec('ray', None)

    class GetTimeoutError(Exception):
        pass

    class RayActorError(Exception):
        pass

    exc = types.ModuleType('ray.exceptions')
    exc.GetTimeoutError = GetTimeoutError; exc.RayActorError = RayActorError
    ray.exceptions = exc

    class _RemoteRef:
        def __init__(self, value):
            self.value = value

    class _ActorClass:
        def __init__(self, cls, options):
            self._cls = cls; self.remote_options = dict(options); self.remote_calls = []

        def options(self, **kw):
            self.remote_options.update(kw); return self

        def remote(self, *args):
            self.remote_calls.append(args); return _RemoteRef(args)

    def remote(*dargs, **dkw):
        def deco(cls):
            return _ActorClass(cls, dkw)
        if dargs and callable(dargs[0]):
            return _ActorClass(dargs[0], {})
        return deco

    ray.remote = remote
    ray.get = lambda ref, timeout=None: ref.value if isinstance(ref, _RemoteRef) else ref
    ray.kill = lambda actor, no_restart=False: None
    ray.is_initialized = lambda: True
    ray.nodes = lambda: []
    ray.init = lambda *a, **k: None
    ray.__version__ = '0.0-test-double'
    ray._ActorClass = _ActorClass
    return ray


SEAL_STATE = {'sealed': None, 'plan_id': None, 'plan_revision': None, 'program_ref': None}


def install_fake_seal():
    """Spec-bound fake of the absent D-lane seal loader (test_doubles)."""
    if _real_module_available('demiflow.execution.dataflow_seal'):
        return importlib.import_module('demiflow.execution.dataflow_seal')
    import hashlib
    import json
    df = sys.modules['demiflow.execution.contracts.dataflow']
    seal = types.ModuleType('demiflow.execution.dataflow_seal')

    def verify_dataflow_program(ref, *, expected_pin, store, bounds):
        return df.LoadedProgram(kind='callable', value={'digest': ref.digest}, digest=ref.digest)

    def load_dataflow(directory, action_id, *, store, bounds):
        manifest = json.loads((Path(directory) / 'manifest.json').read_bytes())
        assert manifest['schema'] == 'demiflow.runtime_bundle.v1'
        s = SEAL_STATE['sealed']
        graph = df.DataflowGraph(
            schema='demiflow.dataflow_graph.v3', action_id=action_id,
            dag_id=hashlib.sha256(b'dag').hexdigest(),
            nodes=(df.GraphNode(occurrence_id=hashlib.sha256(b'occ').hexdigest(),
                                program_ref=SEAL_STATE['program_ref']),),
            edges=(), terminals=(), sources=())
        physical = df.PhysicalDataflow(
            schema='demiflow.physical_dataflow.v3', dag_id=graph.dag_id,
            plan_id=SEAL_STATE['plan_id'], plan_revision=SEAL_STATE['plan_revision'],
            policy=None, bounds=None)
        return graph, physical, s

    seal.verify_dataflow_program = verify_dataflow_program
    seal.load_dataflow = load_dataflow
    sys.modules['demiflow.execution.dataflow_seal'] = seal
    import demiflow.execution.contracts as pkg
    pkg.dataflow_seal = seal
    return seal