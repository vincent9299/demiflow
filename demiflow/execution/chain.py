"""E-side vertical chain orchestration and the driver commit terminal (U2/S4).

The operator chain runs whole inside one independent worker host (D3); the
driver owns only the control plane plus the existing source capture and the
single result-commit authority (D). The worker stages result blocks under the
granted fence and requests a durable commit over the control channel with a
``CommitCut``; the driver re-reads that staging descriptor (no re-write), builds
the K6 checkpoint manifest, commits through the existing ``CommitCoordinator``
and replies ``CommitReceipt``. The worker then advances its confirmed ledger.

This is a new wire message family, not a change to the frozen K3 kind set. The
first batch is single-source/single-partition: the K6 source-progress vector
degenerates to one component (``legacy_checkpoint_fragment`` shape); it never
pretends one ordinal covers a multi-source DAG (U3+). The ordered-channel
protocol itself is proven by the CHN group; the single-worker real chain commits
in lock-step, so a barrier can never overtake its pre-cut output descriptor.
"""
import hashlib
import logging

from .artifact_manifest import entries
from .block_sink import BlockSink
from .commit import CommitCoordinator
from .contracts.checkpoint import (
    legacy_checkpoint_fragment, validate_checkpoint_manifest,
)
from .recovery_contract import (
    RecoveryContractError, SourcePosition, document, identifier, identity,
)
from .run_context import RunAction
from .state_artifacts import StateArtifacts

COMMIT_CUT_SCHEMA = "demiflow.commit_cut.v1"
COMMIT_RECEIPT_SCHEMA = "demiflow.commit_receipt.v1"
WORKER_FAILURE_SCHEMA = "demiflow.worker_failure.v1"
_MAX_CONTROL_BYTES = 1 << 20
_DEP_MAX_MODULES = 64
_DEP_MAX_BYTES = 8 * 1024 ** 2
_DEP_MAX_FUNCTIONS = 256
_DEP_MAX_NODES = 4096
SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2

logger = logging.getLogger(__name__)


def build_commit_cut(operation_id, checkpoint_id, *, source_id, next_ordinal,
                     sequence, channel_id, channel_cut_sequence,
                     output_descriptor, states, inflight=(), final=False):
    identifier(operation_id)
    identifier(checkpoint_id)
    identifier(source_id)
    identifier(channel_id)
    if type(next_ordinal) is not int or not 0 <= next_ordinal < 2**63:
        raise RecoveryContractError("CommitCut source ordinal is invalid")
    if type(sequence) is not int or sequence < 1:
        raise RecoveryContractError("CommitCut sequence must be positive")
    if type(channel_cut_sequence) is not int or channel_cut_sequence < 0:
        raise RecoveryContractError("CommitCut channel sequence is invalid")
    if type(final) is not bool:
        raise RecoveryContractError("CommitCut final flag must be a bool")
    value = {
        "schema_version": COMMIT_CUT_SCHEMA, "operation_id": operation_id,
        "checkpoint_id": checkpoint_id, "source_id": source_id,
        "next_ordinal": next_ordinal, "sequence": sequence,
        "channel_id": channel_id, "channel_cut_sequence": channel_cut_sequence,
        "output_descriptor": output_descriptor, "states": states,
        "inflight": list(inflight), "final": final,
    }
    validate_commit_cut(value)
    return value


def validate_commit_cut(value):
    if type(value) is not dict or value.get("schema_version") != COMMIT_CUT_SCHEMA:
        raise RecoveryContractError("Invalid CommitCut schema")
    required = {"schema_version", "operation_id", "checkpoint_id", "source_id",
                "next_ordinal", "sequence", "channel_id", "channel_cut_sequence",
                "output_descriptor", "states", "inflight", "final"}
    if set(value) != required:
        raise RecoveryContractError("CommitCut key set is unsupported")
    for name in ("operation_id", "checkpoint_id", "source_id", "channel_id"):
        identifier(value[name])
    descriptor = value["output_descriptor"]
    if type(descriptor) is not dict or set(descriptor) != {"blocks", "rows"}:
        raise RecoveryContractError("CommitCut output descriptor is invalid")
    if type(descriptor["rows"]) is not int or descriptor["rows"] < 0:
        raise RecoveryContractError("CommitCut row count is invalid")
    if type(value["states"]) is not dict:
        raise RecoveryContractError("CommitCut states must be a mapping")
    if type(value["inflight"]) is not list:
        raise RecoveryContractError("CommitCut inflight must be a list")
    for entry in value["inflight"]:
        if type(entry) is not dict or not {"request_key", "status"} <= set(entry) \
                or set(entry) - {"request_key", "status", "effect_ref"}:
            raise RecoveryContractError("CommitCut inflight entry is invalid")
        identifier(entry["request_key"])
        if entry["status"] not in ("known_completed", "unknown"):
            raise RecoveryContractError("CommitCut inflight status is unsupported")
        if entry["status"] == "known_completed" and "effect_ref" not in entry:
            raise RecoveryContractError("Known completion needs an effect reference")
        if "effect_ref" in entry:
            identifier(entry["effect_ref"])
    if type(value["final"]) is not bool:
        raise RecoveryContractError("CommitCut final flag must be a bool")
    document(value, _MAX_CONTROL_BYTES)
    return value


def build_commit_receipt(operation_id, commit_id, sequence, status, receipt_descriptor):
    identifier(operation_id)
    identifier(commit_id)
    if status not in ("acked", "data_committed", "checkpoint_committed"):
        raise RecoveryContractError("CommitReceipt status is unsupported")
    value = {"schema_version": COMMIT_RECEIPT_SCHEMA, "operation_id": operation_id,
             "commit_id": commit_id, "sequence": sequence, "status": status,
             "receipt_descriptor": receipt_descriptor}
    validate_commit_receipt(value)
    return value


def validate_commit_receipt(value):
    if type(value) is not dict or value.get("schema_version") != COMMIT_RECEIPT_SCHEMA:
        raise RecoveryContractError("Invalid CommitReceipt schema")
    if set(value) != {"schema_version", "operation_id", "commit_id", "sequence",
                      "status", "receipt_descriptor"}:
        raise RecoveryContractError("CommitReceipt key set is unsupported")
    identifier(value["operation_id"])
    identifier(value["commit_id"])
    if type(value["sequence"]) is not int or value["sequence"] < 1:
        raise RecoveryContractError("CommitReceipt sequence must be positive")
    document(value, _MAX_CONTROL_BYTES)
    return value


class ChainResultSink(BlockSink):
    """Driver-side adapter that commits a worker staging descriptor verbatim.

    The worker already wrote and registered the blocks under the granted fence
    (staging_only), so the driver never re-encodes rows: ``prepare`` validates the
    descriptor against the shared child-action root, charges the driver output
    budget exactly once, and returns the descriptor as the prepared evidence.
    commit/inspect/abort/iter_rows keep the native BlockSink receipt semantics.
    """

    def prepare(self, descriptor, context):
        if type(descriptor) is not dict or set(descriptor) != {"blocks", "rows"}:
            raise RecoveryContractError("Chain result preparation needs a descriptor")
        self._validate(descriptor)
        if not self.input_capture and not self.staging_only:
            for block in self.block_entries(descriptor):
                reservation = 'rows-' + hashlib.sha256(block['path'].encode()).hexdigest()
                self.run.reserve_output('row_bytes', block['bytes'], reservation_id=reservation)
            self.run.reserve('output_rows', descriptor['rows'],
                             ceiling=self.run.limits['max_output_rows'])
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return descriptor


def chain_checkpoint_manifest(topology, cut, *, state_refs, sink_receipts, sealed):
    """Build the K6 manifest for a single-source chain cut (degenerate vector).

    Coverage is the manifest's own participant set: one source component, the
    result-out channel cut, the chain operator participants and the output sink.
    Keyed operator-state indexing beyond pure transforms is U3/U4; operator_states
    only carries entries that have a durable state ref in this cut.
    """
    source_id = cut["source_id"]
    position = {"source_id": source_id, "next_ordinal": cut["next_ordinal"]}
    fragment = legacy_checkpoint_fragment(position, source_ids=[source_id],
                                          channel_ids=(), partition_count=1)
    participants = {
        "sources": fragment["participants"]["sources"],
        "channels": [topology["channel_id"]],
        "operators": [dict(logical_node_id=node["logical_node_id"],
                           occurrence_id=node["occurrence_id"])
                      for node in topology["operators"]],
        "sinks": [topology["sink_id"]],
    }
    channel_cuts = [{"channel_id": topology["channel_id"],
                     "producer_cut_sequence": cut["channel_cut_sequence"],
                     "consumer_offsets": {"driver": cut["channel_cut_sequence"]}}]
    operator_states = []
    for index, node in enumerate(topology["operators"]):
        ref = state_refs.get("node-" + str(index))
        if ref is not None:
            operator_states.append({"logical_node_id": node["logical_node_id"],
                                    "occurrence_id": node["occurrence_id"],
                                    "state_ref": ref})
    value = {
        "schema_version": "demiflow.checkpoint_manifest.v1",
        "checkpoint_id": cut["checkpoint_id"], "run_id": topology["run_id"],
        "action_id": topology["action_id"], "epoch": topology["epoch"],
        "topology_revision": topology["topology_revision"],
        "route_epoch": topology["route_epoch"], "plan_revision": topology["plan_revision"],
        "artifact_manifest_ref": topology["artifact_manifest_ref"],
        "participants": participants, "source_progress": fragment["source_progress"],
        "channel_cuts": channel_cuts, "operator_states": operator_states,
        "inflight": cut["inflight"], "sink_receipts": sink_receipts,
        "seal": {"sealed": bool(sealed)},
    }
    if sealed:
        value["seal"]["digest"] = identity({k: v for k, v in value.items() if k != "seal"})
    return validate_checkpoint_manifest(value)


class DriverStreamTerminal:
    """The driver's single result-commit authority for one worker chain.

    Reconciles any prepared/partial commit before resume (existing
    ``CommitCoordinator.recover``), then serves ``CommitCut`` requests: re-read
    the worker staging descriptor, commit through the one native protocol, record
    the K6 checkpoint manifest and reply ``CommitReceipt``. Idempotency is the
    journal's: commit_id derives from (run, action, sequence); begin_commit
    dedups an identical intent and rejects a differing one.
    """

    def __init__(self, action, source_reference, *, output_child_id, topology,
                 sink_id="output"):
        self.run = action.context
        self.source_id = source_reference["source_id"]
        self.topology = dict(topology)
        self.sink_id = identifier(sink_id)
        if self.topology["sink_id"] != self.sink_id:
            raise RecoveryContractError("Topology sink differs from the terminal sink")
        declaration = dict(kind="chain-result", source=action.declaration["source"],
                           parent=action.semantic_id, protocol="commit-cut-v1")
        previous = self.run.journal.execution_action(output_child_id)
        self.run.journal.bind_execution_action(self.run.fence, output_child_id, declaration)
        self.child = RunAction(self.run, output_child_id, declaration)
        self.child.replaying = previous is not None
        self.sink = ChainResultSink(self.child, sink_id)
        self.coordinator = CommitCoordinator(self.child)
        self.checkpoints = {}
        self.inputs = self.rows = self.sequence = 0
        facts = self.coordinator.recover({sink_id: self.sink})
        latest = facts[-1] if facts else None
        if latest is not None and latest["status"] != "pending":
            position = latest["intent"]["position"]
            if position["source_id"] != self.source_id:
                raise RecoveryContractError("Chain source progress differs")
            self.inputs = position["next_ordinal"]
            self.sequence = latest["sequence"]
            summary = StateArtifacts(self.run).read(
                latest["prepared"]["state"]["stream-progress"])
            self.rows = summary["rows"]
        self.complete = self.child.record["result"] is not None

    def restore_point(self):
        """The latest acked cut the driver hands the worker on Restore."""
        latest = self.run.journal.latest_execution_commit(self.child.action_id, confirmed=True)
        if latest is None or latest["status"] == "pending":
            return None
        return {"position": latest["intent"]["position"], "sequence": latest["sequence"],
                "rows": self.rows, "state": latest["prepared"]["state"],
                "checkpoint_id": latest["commit_id"]}

    def replay_descriptors(self):
        """Forward already committed output descriptors without actor replay."""
        for fact in self.run.journal.iter_execution_commits(
                self.child.action_id, through_sequence=self.sequence):
            if fact["status"] != "pending":
                yield fact["receipts"][self.sink_id]["data"]

    def commit_cut(self, cut):
        validate_commit_cut(cut)
        if cut["source_id"] != self.source_id:
            raise RecoveryContractError("CommitCut belongs to another source")
        if cut["channel_id"] != self.topology["channel_id"]:
            raise RecoveryContractError("CommitCut belongs to another channel")
        # sequence == self.sequence is an idempotent retry of an accepted cut:
        # begin_commit dedups an identical intent and rejects a differing one.
        if cut["sequence"] not in (self.sequence, self.sequence + 1):
            raise RecoveryContractError("CommitCut sequence is not the next cut")
        descriptor = cut["output_descriptor"]
        fact = self.coordinator.commit(cut["sequence"],
            outputs={self.sink_id: (self.sink, descriptor)},
            position=SourcePosition(self.source_id, cut["next_ordinal"]),
            states=cut["states"])
        data = fact["receipts"][self.sink_id]["data"]
        manifest = chain_checkpoint_manifest(self.topology, cut,
            state_refs=fact["prepared"]["state"],
            sink_receipts=[{"sink_id": self.sink_id, "commit_id": fact["commit_id"],
                            "receipt_ref": fact["commit_id"]}],
            sealed=cut["final"])
        ref = StateArtifacts(self.run).put(cut["checkpoint_id"], manifest)
        self.checkpoints[cut["checkpoint_id"]] = {"manifest": manifest, "ref": ref}
        self.inputs = cut["next_ordinal"]
        self.sequence = fact["sequence"]
        self.rows = self._summary_rows(fact)
        return build_commit_receipt(cut["operation_id"], fact["commit_id"],
                                    fact["sequence"], fact["status"], data)

    def _summary_rows(self, fact):
        summary = StateArtifacts(self.run).read(fact["prepared"]["state"]["stream-progress"])
        return summary["rows"]

    def finish(self, source_rows):
        if self.inputs != source_rows:
            raise RecoveryContractError("Unconfirmed source at chain completion")
        self.child.complete(dict(kind="materialized", rows=self.rows,
                                 inputs=self.inputs, commits=self.sequence))
        self.complete = True


def unified_chain_eligible(plan):
    """The in-run pickle delivery (E2) requires a cloudpickle-serializable plan.

    Plans embedding a live process-bound actor runtime (prompt/model actors hold
    asyncio/HTTP/journal locks) cannot cross to an independent worker host; they
    stay on the byte-unchanged legacy in-driver path. This is the controlled MVP-1
    seam boundary: the probe only tests serializability (a pure dumps), so a real
    error inside a serializable plan still raises along the unified path and is
    never masked here. Fallback hits are logged at debug for observability.
    """
    import cloudpickle
    import pickle
    try:
        cloudpickle.dumps(plan)
    except (pickle.PicklingError, TypeError, AttributeError) as exc:
        logger.debug("unified chain ineligible: plan not cloudpickle-serializable "
                     "(%s: %s); routing to legacy in-driver path", type(exc).__name__, exc)
        return False
    return True


def _stable_code_identity(value):
    """Normalize ``describe`` output for cross-process cloudpickle stability.

    cloudpickle reconstructs a function-local class with a stripped ``__qualname__``
    (the enclosing ``<locals>`` scope does not exist in the worker), so describe's
    qualified-name label is not round-trip stable. The code-pin binds the actually
    executed code (``source_hash`` + ``code_record`` + closure), not that label, so
    reduce each function/class ``name`` to its final component. Structural plan
    identity is bound separately by the sealed logical_dag operator recheck.
    """
    if isinstance(value, dict):
        return {k: (v.rsplit(".", 1)[-1]
                    if k == "name" and isinstance(v, str) and "module" in value
                    else _stable_code_identity(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_stable_code_identity(v) for v in value]
    return value


def _classify_dependency_module(module):
    """Classify a ``__globals__`` module: return a business source Path to pin, or
    None to skip (platform / stdlib / site-packages third-party / compiled ext).

    Passive — reads only module metadata and the source path, never executes code.
    A module that is neither stdlib nor platform nor third-party and has no
    resolvable Python source (dynamic, no ``__file__``) fail-closes: the code-pin
    refuses rather than silently leaving an unpinned business dependency.
    """
    import inspect
    import sys
    import sysconfig
    from pathlib import Path
    name = getattr(module, "__name__", "") or ""
    if name.split(".", 1)[0] in sys.stdlib_module_names:
        return None
    if name == "demiflow" or name.startswith("demiflow."):
        return None
    try:
        filename = inspect.getsourcefile(module)
    except (TypeError, OSError):
        filename = None
    filename = filename or getattr(module, "__file__", None)
    if filename:
        path = Path(filename)
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        if resolved.is_relative_to(Path(__file__).resolve().parents[1]):
            return None
        for key in ("purelib", "platlib"):
            base = sysconfig.get_path(key)
            if base and resolved.is_relative_to(Path(base).resolve()):
                return None
        if resolved.suffix == ".py" and resolved.is_file():
            return resolved
        return None
    if getattr(module, "__path__", None) is not None:
        return None
    raise RecoveryContractError(
        "Business dependency module has no resolvable source; code-pin refuses (fail-closed): " + name)


def _is_by_reference_udf(function):
    """True if cloudpickle carries this UDF by reference (importable module +
    qualname), so the worker re-imports the identical module namespace.

    By-value UDFs (lambdas, ``<locals>`` closures, ``__main__`` scripts) reconstruct
    a minimal ``__globals__`` on the worker that differs from the driver's full
    module namespace, so traversing them would diverge the manifest; their code is
    pinned by ``describe`` (source/bytecode/closure) and the cloudpickle payload.
    """
    import sys
    module_name = getattr(function, "__module__", None)
    if not module_name or module_name == "__main__":
        return False
    module = sys.modules.get(module_name)
    if module is None:
        return False
    obj = module
    for part in getattr(function, "__qualname__", "").split("."):
        if part == "<locals>":
            return False
        obj = getattr(obj, part, None)
        if obj is None:
            return False
    return obj is function


def _is_by_reference_class(cls):
    """True if cloudpickle carries this class by reference (importable module +
    qualname), so the worker re-imports it; its defining module is then pinnable.
    A by-value class (``<locals>``/``__main__``) is reconstructed on the worker, so
    its defining module is never pinned (R4) — only its captured state is walked.
    """
    import sys
    module_name = getattr(cls, "__module__", None)
    if not module_name or module_name == "__main__":
        return False
    module = sys.modules.get(module_name)
    if module is None:
        return False
    obj = module
    for part in getattr(cls, "__qualname__", "").split("."):
        if part == "<locals>":
            return False
        obj = getattr(obj, part, None)
        if obj is None:
            return False
    return obj is cls


def _code_global_names(code):
    """Global names a code object (and its nested code constants) references.

    ``co_names`` excludes freevars (closure cells), so captured modules/nested
    functions are reached separately through ``__closure__``; this is the
    referenced-globals subset cloudpickle carries by value (driver == worker).
    """
    import types
    names = set(code.co_names)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _code_global_names(const)
    return names


def _by_value_reachable(function):
    """Values a by-value function carries: referenced globals subset + closure cells.

    Passive — reads ``__globals__``/``__closure__`` only, never executes code. An
    empty cell (ValueError) is skipped, mirroring ``describe``.
    """
    namespace = function.__globals__
    values = [namespace[name] for name in _code_global_names(function.__code__)
              if name in namespace]
    for cell in function.__closure__ or ():
        try:
            values.append(cell.cell_contents)
        except ValueError:
            pass
    return values


def _declared_slots(cls):
    """Slot names declared across a class MRO (passive), excluding dict/weakref."""
    slots = []
    for base in getattr(cls, "__mro__", ()):
        declared = getattr(base, "__slots__", ())
        if isinstance(declared, str):
            declared = (declared,)
        for slot in declared:
            if slot not in ("__dict__", "__weakref__"):
                slots.append(slot)
    return slots


def _is_walkable_value(value):
    """True if a value is a composite the dependency walk recurses (not a leaf).

    Filtering leaves before enqueue keeps the value budget meaningful: primitives,
    bytes and opaque C objects without introspectable state carry no by-name
    business module and are skipped passively.
    """
    import functools
    import inspect
    import types
    if value is None or isinstance(value, (str, int, float, bool, bytes, bytearray)):
        return False
    if isinstance(value, (types.ModuleType, functools.partial,
                          list, tuple, set, frozenset, dict)):
        return True
    if inspect.isfunction(value) or inspect.isclass(value) or inspect.ismethod(value):
        return True
    if getattr(value, "__dict__", None) is not None:
        return True
    return bool(_declared_slots(type(value)))


def _by_value_business_modules(function):
    """Business modules reachable from a by-value UDF's captured state (gap④ R2).

    cloudpickle carries a by-value closure's captured modules BY NAME (the worker
    re-imports them), but ``describe`` records them name-only and the by-reference
    walk excludes by-value UDFs entirely. This walks the closure's referenced
    globals + cells and recurses composite capture forms — nested by-value
    functions/classes, bound methods, ``functools.partial`` (func/args/keywords),
    containers (list/tuple/dict/set/frozenset) and instances (type module if
    by-reference + ``__dict__`` + declared slots) — to reach every captured business
    module. The returned modules seed the SAME module BFS, which pins their source
    and recurses their full namespace (consistent worker-side once re-imported by
    name). Bounded (function/value budgets), cycle-safe (visited ids), passive (no
    code execution) and fail-closed (over-budget refuses). ``__main__`` and by-value
    defining modules are never pinned (R4); their own code is bound by ``describe``.
    """
    import functools
    import inspect
    import sys
    import types
    from collections import deque

    modules, pending, visited = [], deque([function]), set()
    functions = nodes = 0

    def enqueue(values):
        pending.extend(v for v in values if _is_walkable_value(v))

    while pending:
        value = pending.popleft()
        marker = id(value)
        if marker in visited:
            continue
        visited.add(marker)
        nodes += 1
        if nodes > _DEP_MAX_NODES:
            raise RecoveryContractError(
                "Business dependency walk exceeds value budget; code-pin refuses (fail-closed)")
        if isinstance(value, types.ModuleType):
            name = getattr(value, "__name__", "")
            if name and name != "__main__":
                modules.append(value)
            continue
        if inspect.ismethod(value):
            enqueue((value.__self__, value.__func__))
            continue
        if inspect.isfunction(value):
            functions += 1
            if functions > _DEP_MAX_FUNCTIONS:
                raise RecoveryContractError(
                    "Business dependency walk exceeds function budget; code-pin refuses (fail-closed)")
            if _is_by_reference_udf(value):
                module = sys.modules.get(value.__module__)
                if module is not None:
                    pending.append(module)
            else:
                enqueue(_by_value_reachable(value))
            continue
        if inspect.isclass(value):
            if _is_by_reference_class(value):
                module = sys.modules.get(value.__module__)
                if module is not None:
                    pending.append(module)
            else:
                enqueue(vars(value).values())
            continue
        if isinstance(value, functools.partial):
            enqueue((value.func,))
            enqueue(value.args)
            enqueue(value.keywords.values())
            continue
        if isinstance(value, dict):
            enqueue(value.values())
            continue
        if isinstance(value, (list, tuple, set, frozenset)):
            enqueue(value)
            continue
        cls = type(value)
        if _is_by_reference_class(cls):
            module = sys.modules.get(cls.__module__)
            if module is not None:
                pending.append(module)
        state = getattr(value, "__dict__", None)
        if isinstance(state, dict):
            enqueue(state.values())
        for slot in _declared_slots(cls):
            try:
                slot_value = getattr(value, slot)
            except AttributeError:
                continue
            enqueue((slot_value,))
    return modules


def _business_dependency_modules(plan):
    """Transitive closure of business modules a plan's pure UDF maps import.

    ``describe`` binds a UDF's own source/bytecode/closure but never ``__globals__``,
    so an imported business helper escapes the code-pin (gap④). This walks each
    by-reference pure-UDF ``__globals__`` for business modules and recurses through
    their own namespaces, pinning every business module's source. A by-value front-end
    additionally reaches the business modules a closure/lambda/``__main__`` UDF
    CAPTURES (cells + instance/partial/container state, R2), seeding the same BFS.
    Platform (``demiflow.*``), stdlib and site-packages third-party modules are
    skipped — platform code is pinned by ``platform_revision``; third-party env
    resolution is deferred (D5). Bounded (≤64 modules, ≤8 MiB total source, plus
    function/value walk budgets), deterministic (sorted by name), cycle-safe (visited
    set) and passive (no code execution). Over-budget or an unresolvable business
    source fail-closes at seal. The worker recomputes this from its own actual imports,
    so an edited helper changes the manifest and the load gate refuses. Returns
    ``{name: (Path, raw_bytes)}`` sorted by name.
    """
    import inspect
    import sys
    import types
    from collections import deque
    from .plan_facts import operation_facts

    def udfs():
        for operation in plan.operations:
            try:
                if operation_facts(operation).get("native") is not None:
                    continue
            except (RuntimeError, ValueError, TypeError):
                pass
            target = getattr(getattr(operation, "callable", None), "target", None)
            function = getattr(target, "__self__", target)
            if inspect.isfunction(function) and _is_by_reference_udf(function):
                yield function

    def candidates(namespace):
        for value in list(namespace.values()):
            if isinstance(value, types.ModuleType):
                yield value
            elif inspect.isfunction(value) or inspect.isclass(value):
                module = sys.modules.get(getattr(value, "__module__", None))
                if module is not None:
                    yield module

    def by_value_udfs():
        for operation in plan.operations:
            try:
                if operation_facts(operation).get("native") is not None:
                    continue
            except (RuntimeError, ValueError, TypeError):
                pass
            target = getattr(getattr(operation, "callable", None), "target", None)
            function = getattr(target, "__self__", target)
            if inspect.isfunction(function) and not _is_by_reference_udf(function):
                yield function

    pending = deque()
    for function in udfs():
        pending.extend(candidates(function.__globals__))
    # By-value front-end (gap④ R2): a closure/lambda/__main__ UDF is excluded above,
    # yet cloudpickle carries its captured business modules by name. Reach them
    # through cells + composite state and seed the SAME module BFS (unchanged).
    for function in by_value_udfs():
        pending.extend(_by_value_business_modules(function))
    visited, found, total = set(), {}, 0
    while pending:
        module = pending.popleft()
        name = getattr(module, "__name__", "")
        if not name or name in visited:
            continue
        visited.add(name)
        path = _classify_dependency_module(module)
        if path is None:
            continue
        raw = path.read_bytes()
        total += len(raw)
        if len(found) >= _DEP_MAX_MODULES or total > _DEP_MAX_BYTES:
            raise RecoveryContractError(
                "Business dependency manifest exceeds budget; code-pin refuses (fail-closed)")
        found[name] = (path, raw)
        pending.extend(candidates(vars(module)))
    return {name: found[name] for name in sorted(found)}


def _business_dependency_manifest(plan):
    """Code-pin dependency digest: business module source identities, no paths.

    Paths are excluded so the driver-sealed and worker-recomputed manifests are
    identical for unedited code regardless of how each side resolved the module.
    """
    return {name: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            for name, (path, raw) in _business_dependency_modules(plan).items()}


def plan_code_pin(plan):
    """Code-sensitive identity of the plan's actually-executed operations.

    Mirrors ``plan_identity``'s operation identity: native domain facts for native
    actors (engine code, separately pinned by ``platform_revision``), ``describe``
    for pure UDF maps so each business function's ``source_hash`` + ``code_record``
    is bound. Editing business code changes this pin even when cloudpickle carries
    an importable UDF by reference (pickle bytes unchanged). The ``dependencies``
    key pins the transitive business modules each UDF imports (``describe`` never
    walks ``__globals__``), so editing an imported helper also changes the pin.
    The worker recomputes it from the loaded plan alone, so it is the load-time
    code-pin authority.
    """
    from .plan_facts import operation_facts
    from .recovery_identity import describe
    operations = []
    for operation in plan.operations:
        native = None
        try:
            native = operation_facts(operation).get("native")
        except (RuntimeError, ValueError, TypeError):
            pass
        if native is not None:
            operations.append({"native": native,
                "policy": {name: describe(getattr(operation, name)) for name in
                    ("concurrency", "queue_depth", "catch", "hard_timeout", "execution")
                    if hasattr(operation, name)}})
        else:
            operations.append(describe(operation))
    return {"operations": _stable_code_identity(operations),
            "dependencies": _business_dependency_manifest(plan)}


def snapshot_code_sources(action, plan):
    """Seal the business UDF source files as immutable run artifacts for audit.

    The code-pin digest already binds each UDF's source bytes (``describe`` carries
    ``source_hash``) and its transitive business dependencies; this preserves the
    actual sealed source so a refused resume can be audited against the code that
    was pinned. Best-effort for plain module functions; native actors are engine
    code pinned by ``platform_revision``. Dependency modules archive through the
    same content-addressed path — no second artifact authority.
    """
    import inspect
    from pathlib import Path
    from .state_artifacts import atomic_bytes
    run = action.context
    seen, refs = set(), []

    def archive(source):
        source = Path(source)
        if not source.is_file() or source in seen:
            return
        seen.add(source)
        raw = source.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        path = run.directory / "scratch" / "chain-code-pin" / (digest + ".py")
        run.allocate_artifact(path, size=len(raw))
        with run.journal.publication_guard(run.fence):
            atomic_bytes(path, raw)
        run.release_temporaries([path.with_name(path.name + ".staging")])
        refs.append({"path": str(path.relative_to(run.directory)), "sha256": digest,
                     "bytes": len(raw), "source": str(source)})

    for operation in plan.operations:
        target = getattr(getattr(operation, "callable", None), "target", None)
        function = getattr(target, "__self__", target)
        if not inspect.isfunction(function):
            continue
        filename = inspect.getsourcefile(function)
        if filename:
            archive(filename)
    for name, (path, raw) in _business_dependency_modules(plan).items():
        archive(path)
    return refs


def stage_plan_artifact(action, plan):
    """Content-address the in-run pickled plan; the worker verifies before load.

    Persistent protocol never carries an arbitrary cloudpickle document (E2):
    this is the in-run pickle delivery path, sha256-bound into the K1 manifest.
    The pickle alone does not pin importable business UDFs (carried by reference),
    so a code-sensitive code-pin is sealed alongside it: the per-plan key keeps
    graph's multiple plans per action distinct, and a resume whose current business
    code differs from the sealed pin fail-closes (history immutable, D4).
    """
    import cloudpickle
    from .state_artifacts import atomic_bytes
    run = action.context
    raw = cloudpickle.dumps(plan)
    digest = hashlib.sha256(raw).hexdigest()
    path = run.directory / "scratch" / "chain-plans" / (digest + ".pkl")
    run.allocate_artifact(path, size=len(raw))
    with run.journal.publication_guard(run.fence):
        atomic_bytes(path, raw)
    run.release_temporaries([path.with_name(path.name + ".staging")])
    code_pin = plan_code_pin(plan)
    pin_key = action.action_id + "-code-pin-" + digest
    sealed = run.journal.source_binding(pin_key)
    if sealed is not None and sealed != code_pin:
        raise RecoveryContractError(
            "Business code changed since the plan was sealed; resume refused (fail-closed)")
    run.journal.bind_source(run.fence, pin_key, code_pin)
    sources = snapshot_code_sources(action, plan)
    return {"path": str(path.relative_to(run.directory)), "sha256": digest, "bytes": len(raw),
            "code_pin_digest": identity(code_pin), "code_sources": sources}


def verify_loaded_plan(plan, plan_ref, sealed_operators):
    """Worker load-time gate: the loaded plan must equal the sealed plan contract.

    Two complementary bindings, both recomputed from the loaded plan alone: the
    code-pin (``describe`` identities, code-sensitive) fail-closes if business code
    changed since seal; the operator ``logical_node_id``s (frozen ``dag.py``
    semantics) bind the loaded plan structure to the sealed logical_dag. The worker
    holds no commit authority — this is a pure load check, not a second authority.
    """
    from ..planning.dag import _operation_semantics
    if identity(plan_code_pin(plan)) != plan_ref["code_pin_digest"]:
        raise RecoveryContractError(
            "Worker plan code-pin differs; business code edited since seal (fail-closed)")
    if sealed_operators is not None:
        recomputed = [identity([_operation_semantics(op)[0], index])
                      for index, op in enumerate(plan.operations)]
        sealed = [node["logical_node_id"] for node in sealed_operators]
        if recomputed != sealed:
            raise RecoveryContractError(
                "Loaded plan operator identity differs from the sealed logical_dag")


def _worker_failure(message):
    """Turn a worker-level failure frame into an identifiable driver error (F8).

    A load/init failure (or a row error outside an outstanding cut) happens before
    any CommitCut, so it cannot ride a cut response; the worker sends this frame
    instead and the driver ends immediately rather than blocking to control timeout.
    """
    detail = message.get("traceback")
    if detail:
        logger.error("Worker failed:\n%s", detail)
    error = message.get("error") or {}
    return RecoveryContractError("Worker failed: %s: %s"
                                 % (error.get("type"), error.get("message")))


def seal_plan_facts(run, action, logical_dag, physical, manifest_ref, sealed_at):
    """Persist the executed sealed plan as durable content-addressed facts (D1a).

    gap⑤: the compiled logical_dag/physical were returned but never persisted, so
    the workbench display graph had no authoritative execution plan to read. Both
    documents are sealed under ``scratch/chain-sealed-plans/<sha256>.json`` (atomic,
    bounded, idempotent) and an attempt-independent F1 reference is bound through the
    existing ``runtime_sources`` slot (zero journal schema change). The single compile
    seam covers the local/ray/queue callers; the return signature is unchanged.

    F1 omits ``attempt`` (bind_source is immutable; an attempt field would make resume
    re-compilation differ and falsely refuse recovery) and omits ``identity_bridge_ref``
    (the D1b bridge is a separate side-car binding key, so it never mutates this F1).
    """
    from .state_artifacts import atomic_bytes

    def archive(value):
        raw = document(value, MAX_SEALED_PLAN_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        path = run.directory / "scratch" / "chain-sealed-plans" / (digest + ".json")
        # Unregistered, mirroring the K1 manifest: clean_orphans deletes registered
        # protected=0 artifacts at run exit, but the workbench audits these post-run.
        with run.journal.publication_guard(run.fence):
            if not path.exists():
                atomic_bytes(path, raw)
            elif path.read_bytes() != raw:
                raise RecoveryContractError("Existing sealed-plan artifact is corrupt")
        return {"sha256": digest, "path": str(path.relative_to(run.directory)),
                "bytes": len(raw)}

    facts = {"schema": SEALED_PLAN_FACTS_SCHEMA, "run_id": run.ref.run_id,
             "action_id": action.action_id, "dag_id": logical_dag["dag_id"],
             "plan_id": physical["plan_id"], "artifact_manifest_ref": manifest_ref,
             "logical_dag_ref": archive(logical_dag),
             "physical_plan_ref": archive(physical), "sealed_at": sealed_at}
    run.journal.bind_source(run.fence, action.action_id + "-sealed-plan", facts)
    return facts


def compile_chain_topology(action, source, plan, source_reference, *,
                           output_child_id, host_id, plan_ref):
    """Compile the E2 linear DAG/physical plan and seal the K1 manifest (D3).

    The whole operator chain runs in one worker host, so the source-progress
    vector is a single component and the result-out channel is the terminal edge.
    Worker counts live only in the physical worker group, never in logical ids.
    """
    from ..planning.dag import compile_logical_dag, compile_physical_execution_plan
    from .contracts.artifact import build_manifest, save_manifest, seal_manifest
    from .exchange import read_domain
    run = action.context
    config = run.journal.recovery_identity()
    domain = read_domain(run.directory, host_id)
    logical_dag = compile_logical_dag(action_id=action.action_id, source=source,
                                      operations=plan.operations,
                                      terminal_kind="materialize", sink_ids=("output",))
    # The entrypoint definition_ref binds the code-pin (the actually-executed
    # business code), not the mutable pickle reference; location points at the
    # sealed source artifact. bundle_digest stays the plan-bundle pickle digest.
    code_pin_digest = plan_ref["code_pin_digest"]
    code_location = (plan_ref["code_sources"][0]["path"] if plan_ref["code_sources"]
                     else plan_ref["path"])
    manifest = build_manifest(
        run_id=run.ref.run_id,
        platform={"source_digest": config["platform_revision"], "distribution": "demiflow",
                  "version": "u2", "runtime_identity_ref": identity([run.ref.run_id, "runtime"])},
        business={"bundle_digest": "sha256:" + plan_ref["sha256"],
                  "entrypoints": [{"name": "chain", "module_key": "p_" + plan_ref["sha256"][:24],
                                   "definition_ref": {"sha256": code_pin_digest,
                                                      "location": code_location}}]},
        config={"resolved_config_digest": identity([run.ref.run_id, "config"]),
                "model_config_refs": []},
        environment={"requirements_sha256": identity(["requirements"]),
                     "lock_digest": identity(["lock"]), "resolution_digest": identity(["resolution"])},
        protocol_versions={"worker": {"major": 1, "minor": 0},
                           "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1,
                           "resource_protocol": "1.0"},
        secret_refs=[])
    # Seal with the run's persisted birth epoch so the ref is stable across
    # driver restarts (manifest_id excludes sealed_at; the storage digest binds it).
    manifest = seal_manifest(manifest, int(config["started_at"] * 1000))
    ref = save_manifest(run.directory, manifest)
    mref = {"manifest_id": ref["manifest_id"], "sha256": ref["sha256"]}
    stage_refs = [identity([logical_dag["dag_id"], node["occurrence_id"]])
                  for node in logical_dag["nodes"]]
    physical = compile_physical_execution_plan(
        logical_dag=logical_dag, plan_revision=0, artifact_manifest_ref=mref,
        worker_groups=[{"group_id": "chain-group", "role": "cpu_prepare",
                        "profile_ref": "profile-local", "stage_refs": stage_refs,
                        "min_workers": 1, "initial_workers": 1, "max_workers": 1}],
        recovery={"checkpoint_interval_ms": run.limits["checkpoint_interval_ms"],
                  "unconfirmed_rows": run.limits["checkpoint_unconfirmed_rows"],
                  "unconfirmed_bytes": run.limits["checkpoint_unconfirmed_bytes"],
                  "terminals": []},
        feasibility_snapshot_ref=identity([run.ref.run_id, "feasibility"]),
        read_domain=domain, durability_declared="durable",
        partition_count=1, scheme_kind="single")
    operators = [{"logical_node_id": node["logical_node_id"],
                  "occurrence_id": node["occurrence_id"]}
                 for node in logical_dag["nodes"][1:-1]]
    topology = {"run_id": run.ref.run_id, "action_id": output_child_id,
                "epoch": run.attempt, "topology_revision": 0, "route_epoch": 0,
                "plan_revision": 0, "artifact_manifest_ref": mref,
                "source_id": source_reference["source_id"],
                "channel_id": physical["channels"][-1]["channel_id"],
                "operators": operators, "sink_id": "output"}
    seal_plan_facts(run, action, logical_dag, physical, mref, manifest["sealed_at"])
    return topology, logical_dag, physical


class _ChainBlocks:
    """Repeatable lazy block paths over the driver's committed chain receipts."""

    def __init__(self, terminal):
        self.terminal = terminal

    def __iter__(self):
        from .executors.local import _SpilledBlock
        run = self.terminal.run
        for descriptor in self.terminal.replay_descriptors():
            for block in self.terminal.sink.block_entries(descriptor):
                yield _SpilledBlock(str(run.directory / block["path"]), durable=True)

    def __len__(self):
        return sum(1 for _ in self)


def run_local_chain(executor, dataset, *, source=None, plan=None, host_id="local",
                    resource_root=None, control_timeout_s=120):
    """Drive one whole operator chain in an independent Local worker host (D3).

    The driver captures the fixed source, compiles the E2 DAG/physical plan and
    K1 manifest, provisions one managed worker through the real ResourceManager,
    runs the frozen K3 lifecycle over an inherited socketpair, then serves the
    worker's ``CommitCut`` requests through the single ``DriverStreamTerminal``
    commit authority and replies ``CommitReceipt``. The worker stages result
    blocks under the granted fence; the driver never re-encodes rows. After the
    final cut is acked the driver drains, finishes the child action and retires
    the worker through confirmed exit evidence. Default in-driver path is
    untouched: this is selected only by the platform-side executor seam (D5).
    """
    import sys
    import time
    import uuid

    from ..data.plan import LogicalPlan
    from ..resources.capacity import LocalCapacityPool
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.local import LocalResourceProvider
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.process_unit import ManagedProcessUnit
    from .contracts.envelope import WORKER_PROTOCOL_VERSION
    from .contracts.worker import WorkerProtocolModel
    from .executors.local import _LocalMaterializedHandle
    from .resource_state import RunResourceState
    from .run_context import admit_stream, current_action
    from .source_snapshot import capture_source, freeze_source
    from .worker.bootstrap import (
        LocalControlConnector, recv_message, send_message,
    )

    action = current_action()
    if action is None:
        raise RuntimeError("Chain materialization requires the native action context")
    run = action.context
    plan = dataset._plan if plan is None else plan
    source = dataset._source if source is None else source
    admit_stream(plan)
    selected = freeze_source(action, source)
    source_sink, source_reference = capture_source(
        action, lambda: executor.iter_rows(selected, LogicalPlan()))
    source_rows = source_reference["data"]["rows"]
    output_child_id = action.action_id + "-chain"
    source_child_id = action.action_id + "-source"
    plan_ref = stage_plan_artifact(action, plan)
    topology, logical_dag, physical = compile_chain_topology(
        action, selected, plan, source_reference, output_child_id=output_child_id,
        host_id=host_id, plan_ref=plan_ref)
    terminal = DriverStreamTerminal(action, source_reference,
                                    output_child_id=output_child_id, topology=topology)
    executor._run_ref = run.ref
    if terminal.complete:
        return _LocalMaterializedHandle(
            _ChainBlocks(terminal), terminal.rows,
            recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                    semantic_id=action.semantic_id))

    ids = {k: uuid.uuid4().hex for k in
           ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
    scope_id, allocation_id, boot_id = ids["scope_id"], ids["allocation_id"], ids["boot_id"]
    grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
             "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
             "boot_id": boot_id, "controller_epoch": run.attempt,
             "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
    capabilities = ["cpu-map", "stream"]
    restore = terminal.restore_point()
    launch_spec = {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": capabilities, "grant": grant,
        "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"],
        "execution": {"source_child_id": source_child_id, "output_child_id": output_child_id,
                      "plan_ref": plan_ref, "source_reference": source_reference,
                      "channel_id": topology["channel_id"], "restore": restore,
                      "sealed_operators": topology["operators"]},
    }

    root = run.directory.with_name(run.directory.name + "-resources") if resource_root is None \
        else __import__("pathlib").Path(resource_root)
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    connector = LocalControlConnector()
    provider = LocalResourceProvider(pool, unit, control=connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(allocation_id, launch_spec)
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "local"}, run.fence,
        {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": plan_ref["sha256"],
                                "location": "file://" + str(run.directory / plan_ref["path"])}},
        entrypoint="demiflow-worker",
        argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap"),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(ids["worker_id"], allocation_id,
                            ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3}),
                            launch, provision_deadline_ms=60000)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                        uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, (request,), ()))
    sock = None
    drained = False
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise RecoveryContractError("Local chain worker did not reach RUNNING: " + str(phase))
        sock = connector.driver_socks[allocation_id]
        sock.settimeout(control_timeout_s)
        model = WorkerProtocolModel(
            scope_id=scope_id, run_id=run.ref.run_id, allocation_id=allocation_id,
            boot_id=boot_id, controller_epoch=run.attempt, grant=grant,
            plan_revision=0, capabilities=capabilities)

        def now_ms():
            return int(time.time() * 1000)

        def command(kind, payload):
            op = uuid.uuid4().hex
            send_message(sock, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": allocation_id, "boot_id": boot_id, "payload": payload})
            return op

        def read_response(op):
            while True:
                message = recv_message(sock)
                if message is None:
                    raise RecoveryContractError("Worker closed before responding")
                if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                    raise _worker_failure(message)
                if message.get("response_to") == op:
                    if "error" in message:
                        raise RecoveryContractError("Worker command failed: " + str(message["error"]))
                    return message["receipt"]
                if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    model.handle(message, now_ms=now_ms())

        register = recv_message(sock)
        if register is None or register.get("kind") != "Register":
            raise RecoveryContractError("Worker did not register")
        model.handle(register, now_ms=now_ms())
        read_response(command("VerifyArtifactAndCapabilities",
                              {"artifact_manifest_ref": topology["artifact_manifest_ref"],
                               "required_capabilities": capabilities}))
        assignment_id = uuid.uuid4().hex
        read_response(command("Restore", {"assignment_ref": assignment_id,
                                          "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
                                          "state_refs": []}))
        read_response(command("Activate", {"grant_ref": ids["grant_id"],
                                           "controller_epoch": run.attempt,
                                           "operation_id": uuid.uuid4().hex}))
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
            "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "worker_id": ids["worker_id"], "controller_epoch": run.attempt,
            "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": topology["artifact_manifest_ref"],
            "stages": [node["occurrence_id"] for node in logical_dag["nodes"]],
            "partitions": ["p0"], "channels": {"inputs": [], "outputs": [topology["channel_id"]]},
            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
            "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
        assign_op = command("Assign", {"assignment": assignment})
        final_done = False
        while not final_done:
            message = recv_message(sock)
            if message is None:
                raise RecoveryContractError("Worker closed before the final commit cut")
            if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                raise _worker_failure(message)
            if message.get("response_to") == assign_op:
                if "error" in message:
                    raise RecoveryContractError("Worker assign failed: " + str(message["error"]))
                continue
            if message.get("schema_version") == COMMIT_CUT_SCHEMA:
                receipt = terminal.commit_cut(message)
                send_message(sock, receipt)
                final_done = message["final"]
                continue
            if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                model.handle(message, now_ms=now_ms())
        read_response(command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 5000,
                                        "reason": "chain drained"}))
        terminal.finish(source_rows)
        drained = True
    finally:
        # Unified resource terminal (F6, mirrors the carrier N3 pattern): success,
        # failure and cancel all convert confirmed exit to capacity release through
        # the same ResourceManager close+reconcile protocol. Only an unconfirmed exit
        # keeps its charge; the kill below is a backstop for survivors the protocol
        # could not confirm.
        try:
            manager.close(
                MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                                uuid.uuid4().hex, accepted["desired_revision"]),
                {"mode": "DRAINED" if drained else "ABANDON",
                 "engine_receipt_ref": allocation_id + ("-drained" if drained else "-abandoned"),
                 "terminate_deadline_ms": 5000,
                 "reason": "chain drained" if drained else "chain terminal cleanup"})
            manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
        except Exception:
            logger.exception("Chain resource terminal failed; falling back to process kill")
        if sock is not None:
            sock.close()
        connector.close(allocation_id)
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    return _LocalMaterializedHandle(
        _ChainBlocks(terminal), terminal.rows,
        recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                semantic_id=action.semantic_id))


def run_ray_chain(executor, dataset, *, source=None, plan=None, host_id="local",
                  resource_root=None, control_timeout_s=120):
    """Drive one whole operator chain in an independent Ray Core actor host (D3).

    Same driver orchestration as ``run_local_chain`` (fixed source capture, E2
    DAG/physical plan, K1 manifest, single DriverStreamTerminal commit authority,
    frozen K3 lifecycle, CommitCut/CommitReceipt handshake, final drain). Only the
    worker host and control transport differ: a named Ray actor (max_restarts=0)
    instead of a managed process, actor method calls plus a bounded outbox pull
    instead of an inherited socketpair (design §8). Ray Core places/reserves the
    actor; there is no LocalCapacityPool double-charge. Result blocks still hand
    off as staging descriptors on the shared same-host run directory (D3). The
    default Ray Data path is untouched: this is selected only by the platform-side
    executor seam (D5). Single-host Ray is semantic-adaptation evidence only.
    """
    import sys
    import time
    import uuid

    from ..data.plan import LogicalPlan
    from ..resources.contracts import (
        RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan, ResourceProfile,
        ResourceScope, WorkerLaunchSpec, WorkerRequest,
    )
    from ..resources.manager import ManagerBounds, ResourceManager
    from ..resources.ray import RayResourceProvider
    from .contracts.envelope import WORKER_PROTOCOL_VERSION
    from .contracts.worker import WorkerProtocolModel
    from .executors.local import _LocalMaterializedHandle
    from .resource_state import RunResourceState
    from .run_context import admit_stream, current_action
    from .source_snapshot import capture_source, freeze_source
    from .worker.ray_host import RayControlConnector

    action = current_action()
    if action is None:
        raise RuntimeError("Chain materialization requires the native action context")
    run = action.context
    plan = dataset._plan if plan is None else plan
    source = dataset._source if source is None else source
    admit_stream(plan)
    selected = freeze_source(action, source)
    # Driver-side source capture is backend-neutral (D3): read the fixed source
    # with the local reader, never Ray Data (D1 env is Ray Core only).
    from .executors.local import LocalDatasetExecutor
    capture_executor = LocalDatasetExecutor(workers=1)
    source_sink, source_reference = capture_source(
        action, lambda: capture_executor.iter_rows(selected, LogicalPlan()))
    source_rows = source_reference["data"]["rows"]
    output_child_id = action.action_id + "-chain"
    source_child_id = action.action_id + "-source"
    plan_ref = stage_plan_artifact(action, plan)
    topology, logical_dag, physical = compile_chain_topology(
        action, selected, plan, source_reference, output_child_id=output_child_id,
        host_id=host_id, plan_ref=plan_ref)
    terminal = DriverStreamTerminal(action, source_reference,
                                    output_child_id=output_child_id, topology=topology)
    executor._run_ref = run.ref
    if terminal.complete:
        return _LocalMaterializedHandle(
            _ChainBlocks(terminal), terminal.rows,
            recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                    semantic_id=action.semantic_id))

    ids = {k: uuid.uuid4().hex for k in
           ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
    scope_id, allocation_id, boot_id = ids["scope_id"], ids["allocation_id"], ids["boot_id"]
    grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
             "fence": run.fence, "scope_id": scope_id, "allocation_id": allocation_id,
             "boot_id": boot_id, "controller_epoch": run.attempt,
             "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
    capabilities = ["cpu-map", "stream"]
    restore = terminal.restore_point()
    launch_spec = {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": capabilities, "grant": grant,
        "artifact_digest_observed": run.journal.recovery_identity()["platform_revision"],
        "execution": {"source_child_id": source_child_id, "output_child_id": output_child_id,
                      "plan_ref": plan_ref, "source_reference": source_reference,
                      "channel_id": topology["channel_id"], "restore": restore,
                      "sealed_operators": topology["operators"]},
    }

    connector = RayControlConnector(command_timeout_s=control_timeout_s)
    provider = RayResourceProvider(connector)
    state = RunResourceState(run.journal)
    manager = ResourceManager(provider, state, bounds=ManagerBounds())
    connector.stage(allocation_id, launch_spec)
    state.open_scope(run.fence, ResourceScope(
        scope_id, run.ref.run_id, {"provider": "ray"}, run.fence,
        {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": plan_ref["sha256"],
                                "location": "file://" + str(run.directory / plan_ref["path"])}},
        entrypoint="demiflow-worker-actor",
        argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.ray_host"),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = WorkerRequest(ids["worker_id"], allocation_id,
                            ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3}),
                            launch, provision_deadline_ms=60000)
    accepted = manager.apply(
        MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                        uuid.uuid4().hex, 0),
        ResourcePlan(scope_id, (request,), ()))
    try:
        manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise RecoveryContractError("Ray chain worker did not reach RUNNING: " + str(phase))
        model = WorkerProtocolModel(
            scope_id=scope_id, run_id=run.ref.run_id, allocation_id=allocation_id,
            boot_id=boot_id, controller_epoch=run.attempt, grant=grant,
            plan_revision=0, capabilities=capabilities)

        def now_ms():
            return int(time.time() * 1000)

        def command(kind, payload):
            op = uuid.uuid4().hex
            result = connector.handle(allocation_id, {
                "protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": scope_id, "run_id": run.ref.run_id, "operation_id": op,
                "controller_epoch": run.attempt, "authority_ref": ids["grant_id"],
                "allocation_id": allocation_id, "boot_id": boot_id, "payload": payload})
            if "error" in result:
                raise RecoveryContractError("Worker command failed: " + str(result["error"]))
            return result["receipt"]

        def await_event(kind, deadline):
            while True:
                events = connector.pull(allocation_id)
                for message in events:
                    if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                        raise _worker_failure(message)
                    if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                        model.handle(message, now_ms=now_ms())
                    if message.get("kind") == kind:
                        return message
                if time.monotonic() > deadline:
                    raise RecoveryContractError("Ray worker did not emit " + kind)
                if not events:
                    time.sleep(0.01)

        lifecycle_deadline = time.monotonic() + control_timeout_s
        await_event("Register", lifecycle_deadline)
        command("VerifyArtifactAndCapabilities",
                {"artifact_manifest_ref": topology["artifact_manifest_ref"],
                 "required_capabilities": capabilities})
        assignment_id = uuid.uuid4().hex
        command("Restore", {"assignment_ref": assignment_id,
                            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
                            "state_refs": []})
        await_event("Ready", lifecycle_deadline)
        command("Activate", {"grant_ref": ids["grant_id"],
                             "controller_epoch": run.attempt,
                             "operation_id": uuid.uuid4().hex})
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1", "assignment_id": assignment_id,
            "operation_id": uuid.uuid4().hex, "scope_id": scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "worker_id": ids["worker_id"], "controller_epoch": run.attempt,
            "grant_ref": ids["grant_id"], "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": topology["artifact_manifest_ref"],
            "stages": [node["occurrence_id"] for node in logical_dag["nodes"]],
            "partitions": ["p0"], "channels": {"inputs": [], "outputs": [topology["channel_id"]]},
            "restore_point": {"checkpoint_id": restore["checkpoint_id"]} if restore else None,
            "route_epoch": 0, "topology_revision": 0, "ready_deadline_ms": 60000}
        command("Assign", {"assignment": assignment})
        final_done = False
        deadline = time.monotonic() + control_timeout_s
        while not final_done:
            events = connector.pull(allocation_id)
            for message in events:
                if message.get("schema_version") == WORKER_FAILURE_SCHEMA:
                    raise _worker_failure(message)
                if message.get("schema_version") == COMMIT_CUT_SCHEMA:
                    receipt = terminal.commit_cut(message)
                    connector.deliver_receipt(allocation_id, receipt)
                    final_done = message["final"]
                elif message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    model.handle(message, now_ms=now_ms())
            if not events:
                if time.monotonic() > deadline:
                    raise RecoveryContractError("Ray worker closed before the final commit cut")
                time.sleep(0.01)
        command("Drain", {"deadline_ms": int(run.limits["cancel_grace_s"] * 1000) + 5000,
                          "reason": "chain drained"})
        terminal.finish(source_rows)
        manager.close(
            MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence,
                            uuid.uuid4().hex, accepted["desired_revision"]),
            {"mode": "DRAINED", "engine_receipt_ref": allocation_id + "-drained",
             "terminate_deadline_ms": 5000, "reason": "chain drained"})
        manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
    finally:
        connector.terminate(allocation_id)
        connector.close(allocation_id)
    return _LocalMaterializedHandle(
        _ChainBlocks(terminal), terminal.rows,
        recovery_reference=dict(run_id=run.ref.run_id, action_id=action.action_id,
                                semantic_id=action.semantic_id))