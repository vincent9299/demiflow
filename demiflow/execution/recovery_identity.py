"""Semantic identities for native plans; never Python object IDs or row keys."""
from dataclasses import fields, is_dataclass
import hashlib
import inspect
from pathlib import Path
import types

from .artifacts import code_record
from .recovery_contract import identity, document, RecoveryContractError, MAX_DOCUMENT_BYTES


def describe(value, depth=0):
    if depth > 32:
        raise RecoveryContractError("Plan identity exceeds depth budget")
    if value is None or type(value) in (str, int, float, bool):
        return value
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
        source_hash = None
        if filename and Path(filename).is_file():
            if Path(filename).stat().st_size > 8 * 1024**2:
                raise RecoveryContractError("Callable source exceeds identity byte budget")
            source_hash = hashlib.sha256(Path(filename).read_bytes()).hexdigest()
        return dict(module=value.__module__, name=value.__qualname__, source_hash=source_hash,
                    code=identity(code_record(value.__code__)),
                    defaults=describe(value.__defaults__, depth + 1),
                    kwdefaults=describe(value.__kwdefaults__, depth + 1), closure=closure)
    if inspect.isclass(value):
        methods = {}
        for name, method in vars(value).items():
            if inspect.isfunction(method):
                methods[name] = identity(code_record(method.__code__))
        return dict(module=value.__module__, name=value.__qualname__, methods=methods)
    from .local_kernel import PlanInput
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
        return {"type": type(value).__module__ + "." + type(value).__qualname__,
                "fields": {f.name: describe(getattr(value, f.name), depth + 1)
                           for f in fields(value) if f.name not in {"runtime_contract", "label"}}}
    native = getattr(value, "recovery_identity", None)
    if callable(native):
        return {"native": describe(type(value), depth + 1), "declaration": native()}
    # Arrow schemas are declarations; Arrow buffers/data are never encoded here.
    if type(value).__module__.startswith("pyarrow") and type(value).__name__ == "Schema":
        return {"arrow_schema": value.serialize().to_pybytes().hex()}
    if callable(value):
        state = getattr(value, "__dict__", {})
        return {"actor": describe(type(value), depth + 1), "initial_state": describe(state, depth + 1)}
    raise RecoveryContractError("No native semantic identity for " + type(value).__name__)


def source_identity(source):
    from ..data.sources import MaterializedSource, DatasourceSource, FileSource, ItemsSource
    if isinstance(source, ItemsSource):
        return {'literal_source': identity(describe(source)), 'rows': len(source.items)}
    if isinstance(source, MaterializedSource):
        ref = getattr(source.handle, "recovery_reference", None)
        if ref is not None:
            return {"materialized": ref}
        return {'literal_materialization': identity(describe(source.handle.blocks)),
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


def plan_identity(kind, source, plan, spec=None, arguments=None):
    operations = []
    for operation in plan.operations if plan is not None else ():
        # Use existing native domain facts for clients/resources, not their live
        # sockets, locks, caches or constructor object identities.
        native = None
        try:
            from .plan_facts import operation_facts
            fact = operation_facts(operation)
            native = fact.get("native")
        except (RuntimeError, ValueError, TypeError):
            pass
        if native is not None:
            operations.append({"native": native,
                "policy": {name: describe(getattr(operation, name)) for name in
                    ("concurrency", "queue_depth", "catch", "hard_timeout", "execution")
                    if hasattr(operation, name)}})
        else:
            operations.append(describe(operation))
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
