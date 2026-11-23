"""Semantic identities for native plans; never Python object IDs or row keys."""
from dataclasses import fields, is_dataclass
import hashlib
import inspect
from pathlib import Path
import types
from contextvars import ContextVar

from demiflow.execution.contracts.recovery_sdk_v3 import code_record
from .recovery_contract import identity, document, RecoveryContractError, MAX_DOCUMENT_BYTES
from .recovery_sdk_v3 import class_method_pins, function_pin, persistent_source_module, persistent_source_name

_SOURCE_WALK = ContextVar('demiflow_source_identity_walk', default=None)


def _persistent_module(value):
    """Preserve the v3 names for the relocated relational classes and their methods."""
    module = persistent_source_module(value)
    if type(module) is not str or module != 'demiflow.data.sources':
        return module
    from ..data.sources import KeyedSource, PlanInput, UnionSource
    for declaration in (KeyedSource, PlanInput, UnionSource):
        if value is declaration:
            return 'demiflow.execution.local_kernel'
        if inspect.isfunction(value):
            method = vars(declaration).get(value.__name__)
            # The generated dataclass repr also captures its wrapped function.
            if value is method or value is getattr(method, '__wrapped__', None):
                return 'demiflow.execution.local_kernel'
    return module


def describe(value, depth=0):
    if depth > 32:
        raise RecoveryContractError("Plan identity exceeds depth budget")
    if value is None or type(value) in (str, int, float, bool):
        return value
    from enum import Enum
    if isinstance(value, Enum):
        # Previously unsupported typed declaration; retain enum type as well as
        # its value. Existing native prompt JSON identities never enter here.
        return {'enum': type(value).__module__ + '.' + type(value).__qualname__,
                'value': describe(value.value, depth + 1)}
    if type(value) is bytes:
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "bytes": len(value)}
    if type(value) in (tuple, list):
        if len(value) > 100000:
            raise RecoveryContractError("Plan declaration exceeds item budget")
        # Large literal inputs are reduced to bounded semantic identities.
        if len(value) > 1024:
            digest = hashlib.sha256()
            for item in value:
                digest.update(document(describe(item, depth + 1)))
                digest.update(b"\n")
            return {"items": len(value), "sha256": digest.hexdigest()}
        return [describe(v, depth + 1) for v in value]
    if type(value) is dict:
        return {str(k): describe(v, depth + 1) for k, v in value.items()}
    if isinstance(value, types.MappingProxyType):
        return {str(k): describe(v, depth + 1) for k, v in value.items()}
    if type(value) in (set, frozenset):
        # Canonicalize only unordered value identities, never Dataset rows.
        # Retain at most 16384 fixed-size hashes and one bounded descriptor.
        if len(value) > 16384:
            raise RecoveryContractError('Set identity exceeds item budget')
        parts, size = [], 0
        for item in value:
            encoded = document(describe(item, depth + 1))
            size += len(encoded)
            if size > MAX_DOCUMENT_BYTES:
                raise RecoveryContractError('Set identity exceeds byte budget')
            parts.append(hashlib.sha256(encoded).digest())
        parts.sort()
        digest = hashlib.sha256()
        for part in parts:
            digest.update(part)
        return {'collection_type': type(value).__name__, 'items': len(value),
                'sha256': digest.hexdigest()}
    # Only newly supported data-only scalar types enter this branch. Keep the
    # accepted JSON/set/enum and native model identity representations intact.
    from .contracts.scalar_values import scalar_node
    try:
        scalar = scalar_node(value, max_bytes=MAX_DOCUMENT_BYTES)
    except (ValueError, OverflowError) as exc:
        raise RecoveryContractError('Unsupported scalar identity: ' + str(exc)) from exc
    if scalar is not None:
        return {'data_scalar': scalar}
    if isinstance(value, Path):
        return {"path": str(value.absolute())}
    if isinstance(value, types.ModuleType):
        return {"module": value.__name__}
    if inspect.ismethod(value):
        return {"method": value.__name__, "owner": describe(value.__self__, depth + 1)}
    if inspect.isfunction(value):
        closure = []
        for cell in value.__closure__ or ():
            try:
                item = cell.cell_contents
            except ValueError:
                item = None
            closure.append(describe(item, depth + 1))
        filename = inspect.getsourcefile(value)
        source = None
        source_hash = None
        if filename and Path(filename).is_file():
            if Path(filename).stat().st_size > 8 * 1024**2:
                raise RecoveryContractError("Callable source exceeds identity byte budget")
            source = Path(filename).read_bytes()
            source_hash = hashlib.sha256(source).hexdigest()
        code = identity(code_record(value.__code__))
        source_hash, code = function_pin(value, source, source_hash, code)
        return dict(module=_persistent_module(value), name=persistent_source_name(value), source_hash=source_hash,
                    code=code,
                    defaults=describe(value.__defaults__, depth + 1),
                    kwdefaults=describe(value.__kwdefaults__, depth + 1), closure=closure)
    if inspect.isclass(value):
        methods = {}
        for name, method in vars(value).items():
            if inspect.isfunction(method):
                methods[name] = identity(code_record(method.__code__))
        return dict(module=_persistent_module(value), name=value.__qualname__,
                    methods=class_method_pins(value, methods))
    from ..data.sources import PlanInput
    if isinstance(value, PlanInput):
        return plan_identity('input', value.source, value.plan)
    if type(value).__module__.startswith('numpy') and hasattr(value, 'dtype'):
        if getattr(value.dtype, 'hasobject', False):
            raise RecoveryContractError('Object arrays need a fixed source adapter')
        import numpy as np
        digest = hashlib.sha256()
        iterator = np.nditer(value, flags=['external_loop', 'buffered', 'zerosize_ok'],
                             order='C', buffersize=65536)
        for chunk in iterator:
            digest.update(chunk.tobytes())
        return {'array': str(value.dtype), 'shape': list(value.shape), 'sha256': digest.hexdigest()}
    if is_dataclass(value):
        # Runtime validators/observation coordinates are not semantic parameters.
        return {"type": _persistent_module(type(value)) + "." + type(value).__qualname__,
                "fields": {f.name: describe(getattr(value, f.name), depth + 1)
                           for f in fields(value) if f.name not in {"runtime_contract", "label"}}}
    native = getattr(value, "recovery_identity", None)
    if callable(native):
        return {"native": describe(type(value), depth + 1), "declaration": native()}
    # Arrow schemas are declarations; Arrow buffers/data are never encoded here.
    if type(value).__module__.startswith("pyarrow") and type(value).__name__ == "Schema":
        return {"arrow_schema": value.serialize().to_pybytes().hex()}
    # Native CSV option objects are previously unsupported declarations. Bind
    # every native field, preserving the old identity path for all other types.
    if type(value).__module__ == "pyarrow._csv":
        import functools
        import pyarrow as pa
        import pyarrow.csv as pacsv
        declarations = {
            pacsv.ReadOptions: ('use_threads', 'block_size', 'skip_rows',
                'skip_rows_after_names', 'column_names', 'autogenerate_column_names', 'encoding'),
            pacsv.ParseOptions: ('delimiter', 'quote_char', 'double_quote', 'escape_char',
                'newlines_in_values', 'ignore_empty_lines', 'invalid_row_handler'),
            pacsv.ConvertOptions: ('check_utf8', 'column_types', 'null_values',
                'true_values', 'false_values', 'decimal_point', 'timestamp_parsers',
                'strings_can_be_null', 'quoted_strings_can_be_null', 'auto_dict_encode',
                'auto_dict_max_cardinality', 'include_columns', 'include_missing_columns',
                *(('default_column_type',) if hasattr(pacsv.ConvertOptions, 'default_column_type') else ())),
        }
        names = declarations.get(type(value))
        if names is not None:
            # Fail closed if a future native library adds a data field we have
            # not included, instead of certifying an incomplete declaration.
            public = {n for n in dir(value) if not n.startswith('_')
                      and not callable(getattr(value, n))}
            if public - set(names):
                raise RecoveryContractError('Unbound native CSV option fields')
            state = {}
            for name in names:
                item = getattr(value, name)
                if name == 'column_types':
                    state[name] = {key: describe(pa.schema([('value', dtype)]), depth + 1)
                                   for key, dtype in item.items()}
                elif name == 'default_column_type' and item is not None:
                    state[name] = describe(pa.schema([('value', item)]), depth + 1)
                elif name == 'timestamp_parsers':
                    state[name] = [{'arrow_csv_timestamp_parser': 'ISO8601'}
                                   if parser is pacsv.ISO8601 else describe(parser, depth + 1)
                                   for parser in item]
                elif name == 'invalid_row_handler' and type(item) is functools.partial:
                    state[name] = {'partial': describe(item.func, depth + 1),
                        'args': describe(item.args, depth + 1),
                        'keywords': describe(item.keywords, depth + 1),
                        'attributes': describe(vars(item), depth + 1)}
                else:
                    state[name] = describe(item, depth + 1)
            return {'arrow_csv_options': type(value).__name__, 'fields': state}
    # Nested native prompts were previously unrepresentable (live runtime).
    # Reuse their guarded native facts; existing operation identities are unchanged.
    from ..operator_llm.native_actor import is_native_prompt_actor
    if is_native_prompt_actor(value):
        from ..operator_llm.flow_contract import agent_facts, prompt_facts
        return {'native_prompt': prompt_facts(value) if value.environment is None
                else agent_facts(value, value.environment)}
    if callable(value):
        # C04 identity rule 1: a plain callable's config/constructor args/
        # initial_state must not disappear for lack of hooks. Instance data
        # initial values participate in semantic identity, matching the accepted
        # historical bytes; unadapted deps inside the state still fail closed.
        state = getattr(value, "__dict__", {})
        return {"actor": describe(type(value), depth + 1), "initial_state": describe(state, depth + 1)}
    import threading
    import contextvars
    if isinstance(value, contextvars.ContextVar):
        # C04 identity rule 2: a ContextVar carries a business value. Erasing it
        # to a type name would collide distinct configurations on resume.
        try:
            current = value.get()
        except LookupError:
            current = None
        return {"contextvar": value.name, "value": describe(current, depth + 1)}
    if isinstance(value, (threading.Event, threading.Barrier, threading.Condition,
                          threading.Semaphore, threading.BoundedSemaphore)) \
            or (type(value).__module__ == '_thread'
                and type(value).__qualname__ in ('lock', 'RLock')):
        # C04 identity rule 3: only synchronization primitives proven to carry no
        # business semantics get a stable, address-free, rebuildable declaration.
        # Thread / thread-local / opaque handles are NOT swallowed by type here.
        return {"runtime_resource": type(value).__module__ + "." + type(value).__qualname__}
    raise RecoveryContractError("No native semantic identity for " + type(value).__name__)


def source_identity(source):
    prior = _SOURCE_WALK.get()
    stack, count = prior if prior is not None else ((), [0])
    count[0] += 1
    if id(source) in stack or len(stack) >= 32 or count[0] > 4096:
        raise RecoveryContractError('Source identity cycle or graph budget exceeded')
    token = _SOURCE_WALK.set((stack + (id(source),), count))
    try:
        return _source_identity(source)
    finally:
        _SOURCE_WALK.reset(token)


def _source_identity(source):
    from ..data.sources import (
        DatasourceSource, FileSource, ItemsSource, KeyedSource, MaterializedSource, UnionSource,
    )
    # Composite plans bind child identities, never runtime handles/executors.
    # In particular a captured materialization is identified by its receipts,
    # not by serializing the lazy _CheckpointBlocks reader into a declaration.
    if isinstance(source, UnionSource):
        return {'union': [plan_identity('input', p.source, p.plan) for p in source.inputs]}
    if isinstance(source, KeyedSource):
        import dataclasses
        return {'keyed': {f.name: describe(getattr(source, f.name))
                          for f in dataclasses.fields(source) if f.name not in {'left', 'right'}},
                'left': plan_identity('input', source.left.source, source.left.plan),
                'right': (plan_identity('input', source.right.source, source.right.plan)
                          if source.right is not None else None)}
    if isinstance(source, ItemsSource):
        from demiflow.execution.contracts.literal_source_identity import literal_source_digest
        return {'literal_source': literal_source_digest(source), 'rows': len(source.items)}
    if isinstance(source, MaterializedSource):
        ref = getattr(source.handle, "recovery_reference", None)
        if ref is not None:
            return {"materialized": ref}
        from demiflow.execution.contracts.literal_source_identity import literal_source_digest
        return {'literal_materialization': literal_source_digest(source.handle.blocks),
                'rows': source.handle.row_count}
    if isinstance(source, DatasourceSource):
        from .queue_source import QueueDatasource
        from .queue_history import QueueHistoryDatasource
        if type(source.datasource) in {QueueDatasource,QueueHistoryDatasource}:
            return source.datasource.declaration()
        facts = source.datasource.snapshot_identity()
        return {"datasource": describe(type(source.datasource)),
                "snapshot": facts, "declaration": identity(describe(vars(source.datasource)))}
    if isinstance(source, FileSource):
        # Bind actual local contents, not just a mutable filename/mtime.
        files = []
        for raw in source.paths:
            path = Path(raw)
            if not path.is_file() or path.is_symlink():
                raise RecoveryContractError("File source needs a fixed native file manifest")
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            files.append({"path": str(path.absolute()), "sha256": digest.hexdigest()})
        return {"source": describe(source), "files": files}
    return describe(source)


def operation_identity(operation):
    """One semantic projection for action identity and code pins.

    Newly supported stream specializations include their complete batch policy.
    Existing prompt identities retain their historical representation. Invalid
    native declarations must never fall back to describing a mutable actor.
    """
    from ..data import plan as ops
    if type(operation) is ops.PredictOp:
        from ..data.training import ModelSelection
        def fit_identity(decl, ancestors):
            if decl in ancestors or len(ancestors) >= 32:
                raise RecoveryContractError("training declaration cycle or excessive depth")
            parent = decl.parent
            if isinstance(parent, ModelSelection):
                parent = {"fit": fit_identity(parent.declaration, ancestors | {decl}), "step": parent.steps[0]}
            return {"inputs": {name: plan_identity("input", source, plan) for name, source, plan in decl.inputs},
                    "recipe": describe(decl.recipe), "retrain_id": decl.retrain_id, "parent": describe(parent)}
        return {"prediction": {"fit": fit_identity(operation.fit, set()), "steps": list(operation.steps),
                               "inputs": describe(operation.inputs), "parameters": describe(operation.parameters)}}
    from .plan_facts import operation_facts
    specialized = type(operation) in {ops.EmbeddingMapOp, ops.VectorSearchOp,
        ops.StreamGroupBatchesOp, ops.DeduplicateOp, ops.StreamPrependOp}
    try:
        fact = operation_facts(operation)
    except (RuntimeError, ValueError, TypeError):
        if specialized or type(operation) is ops.OperatorLLMMapOp:
            raise
        return describe(operation)
    native = fact.get('native')
    if native is None:
        if type(operation) is ops.OperatorLLMMapOp:
            # UUID node coordinates are execution ownership, not prompt semantics.
            return {'model_map': fact['arguments']}
        return describe(operation)
    names = ('concurrency', 'queue_depth', 'catch', 'hard_timeout', 'execution')
    if specialized:
        names += ('max_batch', 'flush_interval')
    # Native producers already own their canonical JSON declaration. Running it
    # through describe would hash long enums/lists and rewrite accepted history.
    # New domains must normalize their own non-JSON configuration at the producer.
    return {'native': native, 'policy': {name: describe(getattr(operation, name))
                                        for name in names if hasattr(operation, name)}}


def plan_identity(kind, source, plan, spec=None, arguments=None):
    operations = []
    for operation in plan.operations if plan is not None else ():
        # Use existing native domain facts for clients/resources, not their live
        # sockets, locks, caches or constructor object identities.
        operations.append(operation_identity(operation))
    return dict(kind=kind, source=source_identity(source) if source is not None else None,
                operations=operations, sink=describe(spec), arguments=describe(arguments))


def platform_identity():
    """Bounded source revision of the installed native execution implementation."""
    root = Path(__file__).resolve().parents[1]
    digest, count, size = hashlib.sha256(), 0, 0
    for path in sorted(root.rglob('*.py')):
        count += 1
        if count > 4096 or path.is_symlink():
            raise RecoveryContractError('Platform source inventory exceeds bounds')
        size += path.stat().st_size
        if size > 64 * 1024**2:
            raise RecoveryContractError('Platform source bytes exceed bounds')
        digest.update(str(path.relative_to(root)).encode())
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(65536), b''):
                digest.update(chunk)
    return digest.hexdigest()
