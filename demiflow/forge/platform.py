"""Read-only facts about explicitly public platform namespaces.

This module discovers objects; it neither registers operators nor grants their
use. Inspection never constructs exported classes or evaluates descriptors.
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import json
import math
from pathlib import Path
import sys
import sysconfig
import types

from .schema import ForgeError


PUBLIC_MODULES = (
    "demiflow", "demiflow.data", "demiflow.operator_llm", "demiflow.agent",
    "demiflow.inference", "demiflow.services", "demiflow.objects",
    "demiflow.collect.raw_contract", "demiflow.secrets", "demiflow.queue",
)
LIMITS = {
    "entries": 8192, "depth": 32, "file_bytes": 32 * 1024**2,
    "source_bytes": 128 * 1024**2, "distributions": 512,
    "metadata_file_bytes": 8 * 1024**2, "metadata_bytes": 32 * 1024**2,
    "objects": 2048, "members": 512, "inheritance": 32,
    "doc_bytes": 256 * 1024, "signature_bytes": 32 * 1024,
    "catalog_bytes": 16 * 1024**2, "snapshot_bytes": 32 * 1024**2,
    "timeout_seconds": 60, "stderr_bytes": 1024**2,
}


def limits_with(overrides=None):
    result = dict(LIMITS)
    for key, value in (overrides or {}).items():
        if key not in result or type(value) is not int or not 0 < value <= LIMITS[key]:
            raise ForgeError(f"Invalid or expanded platform limit: {key}")
        result[key] = value
    return result


def module_scope(modules=None):
    values = tuple(PUBLIC_MODULES if modules is None else modules)
    if not values or len(values) > len(PUBLIC_MODULES) or len(set(values)) != len(values):
        raise ForgeError("Platform scope must be nonempty and unique")
    if any(value not in PUBLIC_MODULES for value in values):
        raise ForgeError("Platform namespace is not public in this discovery scope")
    return tuple(sorted(values))


def bounded_json(value, maximum):
    output = bytearray()
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    for part in encoder.iterencode(value):
        encoded = part.encode("utf-8")
        if len(output) + len(encoded) > maximum:
            raise ForgeError("Platform JSON exceeds byte budget", code='resource_limit')
        output.extend(encoded)
    return bytes(output)


def content_digest(value):
    return hashlib.sha256(bounded_json(value, LIMITS["snapshot_bytes"])).hexdigest()


def _type_name(value):
    kind = type(value)
    return f"{kind.__module__}.{kind.__qualname__}"


def _literal(value, depth=0):
    """Render only known builtins. In particular never call a default's repr."""
    if type(value) in (str, bytes):
        if len(value) > 4096:
            return f"<{_type_name(value)} length={len(value)}>"
        return repr(value)
    if value is None or type(value) in (bool, int):
        try:
            return repr(value)
        except ValueError:
            return f"<{_type_name(value)}>"
    if type(value) is float:
        return repr(value) if math.isfinite(value) else f"<float:{str(value)}>"
    if value is Ellipsis:
        return "..."
    if depth < 1 and type(value) in (tuple, list) and len(value) <= 16:
        items = ", ".join(_literal(item, depth + 1) for item in value)
        if type(value) is tuple:
            return "(" + items + ("," if len(value) == 1 else "") + ")"
        return "[" + items + "]"
    if isinstance(value, type):
        module = inspect.getattr_static(value, "__module__", None)
        name = type.__getattribute__(value, "__qualname__")
        return module + "." + name if isinstance(module, str) and isinstance(name, str) else "<class>"
    return f"<{_type_name(value)}>"


def _signature(value, kind, maximum):
    if kind not in {"function", "method", "staticmethod", "classmethod", "class"}:
        return {"status": "not_applicable"}
    # inspect.signature can invoke metaclass properties (notably
    # __signature__). Members are still discovered statically below.
    if kind == "class" and type(value) is not type:
        return {"status": "unavailable"}
    try:
        signature = inspect.signature(value, eval_str=False, follow_wrapped=False)
    except (TypeError, ValueError):
        return {"status": "unavailable"}
    parts, parameters = [], []
    positional_only = [p.name for p in signature.parameters.values() if p.kind == p.POSITIONAL_ONLY]
    has_star = False
    for p in signature.parameters.values():
        if p.kind == p.KEYWORD_ONLY and not has_star:
            parts.append("*")
            has_star = True
        prefix = "*" if p.kind == p.VAR_POSITIONAL else "**" if p.kind == p.VAR_KEYWORD else ""
        if prefix:
            has_star = True
        label = prefix + p.name
        item = {"name": p.name, "kind": p.kind.name,
                "required": p.default is p.empty and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)}
        if p.annotation is not p.empty:
            item["annotation"] = _literal(p.annotation)
            label += ": " + item["annotation"]
        if p.default is not p.empty:
            from ..data.callable_contract import default_value

            item["default"] = _literal(p.default)
            item["default_value"] = default_value(p.default)
            label += " = " + item["default"]
        parts.append(label)
        if positional_only and p.name == positional_only[-1]:
            parts.append("/")
        parameters.append(item)
        if len(parameters) > 512:
            raise ForgeError("Signature exceeds parameter budget")
    display = "(" + ", ".join(parts) + ")"
    if signature.return_annotation is not signature.empty:
        display += " -> " + _literal(signature.return_annotation)
    result = {"status": "available", "text": display, "parameters": parameters}
    bounded_json(result, maximum)
    return result


def _origin(value, owner, root):
    module_name = inspect.getattr_static(value, "__module__", None) if isinstance(value, type) else getattr(value, "__module__", None) if isinstance(value, (types.FunctionType, types.BuiltinFunctionType)) else None
    if not isinstance(module_name, str):
        module_name = owner
    module = sys.modules.get(module_name)
    raw = vars(module).get("__file__") if isinstance(module, types.ModuleType) else None
    result = {"module": module_name}
    if not raw:
        if module_name in sys.builtin_module_names or module_name == "builtins":
            return {**result, "kind": "builtin"}
        raise ForgeError(f"Object owner has no inspectable origin: {module_name}")
    path = Path(raw)
    if path.is_symlink():
        raise ForgeError(f"Symlink in platform owner: {module_name}")
    path = path.resolve()
    try:
        relative = path.relative_to(root)
        return {**result, "kind": "platform", "path": relative.as_posix()}
    except ValueError:
        if module_name == "demiflow" or module_name.startswith("demiflow."):
            raise ForgeError(f"Platform object is shadowed: {module_name}")
    # Installed dependency/stdlib locations are diagnostics relative to their
    # trusted interpreter roots, never an arbitrary caller-provided path.
    for key in ("purelib", "platlib", "stdlib", "platstdlib"):
        base = sysconfig.get_path(key)
        if base:
            try:
                relative = path.relative_to(Path(base).resolve())
                return {**result, "kind": "dependency" if key.endswith("lib") and key in {"purelib", "platlib"} else "stdlib", "path": relative.as_posix()}
            except ValueError:
                pass
    raise ForgeError(f"Object owner is outside trusted interpreter roots: {module_name}")


def _record(identifier, value, owner_module, root, limits, member_owner=None, *, version=3):
    raw = value
    if isinstance(raw, staticmethod):
        kind, value = "staticmethod", raw.__func__
    elif isinstance(raw, classmethod):
        kind, value = "classmethod", raw.__func__
    elif isinstance(raw, property):
        kind, value = "property", raw.fget
    elif isinstance(raw, type):
        kind = "class"
    elif isinstance(raw, (types.FunctionType, types.BuiltinFunctionType)):
        kind = "method" if member_owner else "function"
    elif inspect.ismethoddescriptor(raw) or inspect.isgetsetdescriptor(raw):
        kind = "descriptor"
    else:
        kind = "attribute"
    if kind == "class":
        doc = type.__getattribute__(value, "__dict__").get("__doc__")
    elif isinstance(value, (types.FunctionType, types.BuiltinFunctionType)):
        doc = value.__doc__
    elif isinstance(raw, property):
        doc = raw.__doc__
    else:
        doc = None
    if not isinstance(doc, str):
        doc = ""
    if len(doc.encode("utf-8")) > limits["doc_bytes"]:
        raise ForgeError(f"Public documentation exceeds budget: {identifier}")
    doc = inspect.cleandoc(doc)
    origin = _origin(value, owner_module, root)
    qualname = getattr(value, "__qualname__", None) if isinstance(value, (types.FunctionType, types.BuiltinFunctionType, type)) else None
    signature = _signature(value, kind, limits["signature_bytes"])
    diagnostics = []
    if not doc:
        diagnostics.append("documentation_missing")
    if signature["status"] == "unavailable":
        diagnostics.append("signature_unavailable")
    if kind == "descriptor":
        diagnostics.append("descriptor_not_evaluated")
    semantics = {"status": "unknown"}
    if version >= 2 and (isinstance(value,types.FunctionType) or type(value) is type):
        from ..data.flow_contract import validate_semantics
        semantics = validate_semantics(vars(value).get("__demiflow_flow__", semantics), signature.get("parameters", []))
        if version == 2 and "format" in semantics:
            ports = semantics['callable_ports']
            semantics = {key: semantics[key] for key in ("format", "kind", "datasets", "effects", "execution")}
            semantics['callable'] = ports[0]['name'] if len(ports) == 1 and ports[0]['role'] == 'transform' else None
            semantics["format"] = "demiflow_api_flow_v1"
    return {
        "id": identifier, "kind": kind, "origin": origin,
        "defined_name": qualname or member_owner or identifier.split(":", 1)[1],
        "member_owner": member_owner, "signature": signature,
        "documentation": {"status": "available" if doc else "missing", "text": doc},
        "native_semantics": semantics, "diagnostics": diagnostics,
    }


def _public_export(module, label, root, *, depth=0):
    """Resolve the two owned lazy facades without calling arbitrary getters.

    Public names and canonical targets are data declarations. Underlying module
    imports remain trusted platform imports; class descriptors are not evaluated.
    """
    namespace = vars(module)
    if label in namespace:
        return namespace[label]
    expected = {"demiflow": root / "__init__.py",
                "demiflow.data": root / "data/__init__.py"}
    name = namespace.get("__name__")
    path = Path(namespace.get("__file__", ""))
    if (depth >= 3 or type(module) is not types.ModuleType or type(name) is not str or name not in expected
            or path.is_symlink() or path.resolve() != expected[name].resolve()):
        raise ForgeError(f"Unresolved public export: {name}:{label}")
    targets = namespace.get("_EXPORTS")
    target = targets.get(label) if type(targets) is dict else None
    if (type(target) is not tuple or len(target) != 2 or type(target[0]) is not str
            or len(target[0]) > 256
            or target[1] is not None and (type(target[1]) is not str or not target[1].isidentifier())):
        raise ForgeError(f"Invalid lazy public export: {name}:{label}")
    target_name = importlib.util.resolve_name(target[0], name)
    if not target_name.startswith("demiflow."):
        raise ForgeError(f"Lazy public target is outside the platform: {name}:{label}")
    provider = importlib.import_module(target_name)
    provider_path = Path(vars(provider).get("__file__", ""))
    if provider_path.is_symlink() or not provider_path.resolve().is_relative_to(root):
        raise ForgeError(f"Lazy public target is shadowed: {name}:{label}")
    return provider if target[1] is None else _public_export(provider, target[1], root, depth=depth + 1)


def inspect_modules(modules, root, limits, *, importer=importlib.import_module, version=3):
    """Worker implementation. importer injection is only used by trusted tests."""
    root = Path(root).resolve()
    records, diagnostics = [], []
    size = 0

    def append(record):
        nonlocal size
        encoded = bounded_json(record, limits["catalog_bytes"])
        size += len(encoded)
        if len(records) >= limits["objects"] or size > limits["catalog_bytes"]:
            raise ForgeError("Platform catalogue exceeds object/byte budget")
        records.append(record)

    for name in modules:
        try:
            module = importer(name)
            path = Path(vars(module).get("__file__", ""))
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ForgeError(f"Platform module is shadowed: {name}")
            exported = vars(module).get("__all__")
            if not isinstance(exported, (list, tuple)) or len(exported) > limits["objects"] or any(
                not isinstance(item, str) or not item.isidentifier() or item.startswith("_") for item in exported
            ) or len(set(exported)) != len(exported):
                raise ForgeError(f"Invalid public export declaration: {name}")
            for label in sorted(exported):
                value = _public_export(module, label, root)
                append(_record(f"{name}:{label}", value, name, root, limits, version=version))
                if not isinstance(value, type):
                    continue
                mro = type.__getattribute__(value, "__mro__")
                if len(mro) > limits["inheritance"]:
                    raise ForgeError("Public class exceeds inheritance budget")
                seen = set()
                for parent in mro:
                    namespace = type.__getattribute__(parent, "__dict__")
                    for member, raw in namespace.items():
                        if member.startswith("_") or member in seen:
                            continue
                        seen.add(member)
                        if len(seen) > limits["members"]:
                            raise ForgeError("Public class exceeds member budget")
                        owner = type.__getattribute__(parent, "__module__")
                        owner_name = owner + "." + type.__getattribute__(parent, "__qualname__")
                        append(_record(f"{name}:{label}.{member}", raw, owner, root, limits, owner_name, version=version))
        except (ImportError, OSError, ForgeError, ValueError, TypeError) as exc:
            diagnostics.append({"module": name, "code": "inspection_incomplete", "detail": str(exc)[:2048]})
    result = {
        "format": f"demiflow_forge_platform_catalog_v{version}", "modules": list(modules),
        "inspection_complete": not diagnostics,
        "objects": sorted(records, key=lambda item: item["id"]), "diagnostics": diagnostics,
    }
    bounded_json(result, limits["catalog_bytes"])
    return result


def discover_catalog(modules=None, *, limits=None, version=3):
    from .platform_worker import run_inspection

    return run_inspection(module_scope(modules), limits_with(limits), version=version)


def native_handlers(record):
    """Resolve handlers only from the real, trusted platform API declaration.

    Handler names in catalogue JSON are evidence to compare, never import paths.
    The enclosing inspection binds and rechecks the complete platform snapshot.
    """
    from functools import partial
    from ..data.flow_contract import handler_identity

    root = Path(__file__).resolve().parents[1]
    origin = record["origin"]
    if origin.get("kind") != "platform" or not origin["module"].startswith("demiflow."):
        raise ForgeError("Contract owner is not a native platform module")
    module = importlib.import_module(origin["module"])
    if Path(module.__file__).resolve() != root / origin["path"]:
        raise ForgeError("Native contract module is shadowed")
    value = module
    for component in record["defined_name"].split("."):
        if not component.isidentifier() or component.startswith("_"):
            raise ForgeError("Native API path is not public")
        value = inspect.getattr_static(value, component)
    if isinstance(value, (staticmethod, classmethod)):
        value = value.__func__
    if not (isinstance(value,types.FunctionType) or type(value) is type) or type(value) is type and record["native_semantics"].get("role")!="value":
        raise ForgeError("Native API has no function or pure declaration contract")
    if vars(value).get("__demiflow_flow__") != record["native_semantics"]:
        raise ForgeError("Native declaration differs from the inspected catalogue")
    handlers = vars(value).get("__demiflow_flow_handlers__", {})
    if set(handlers) != {"normalize", "resource_references", "analyze"}:
        raise ForgeError("Native contract handlers are incomplete")
    for name, handler in handlers.items():
        function = handler.func if isinstance(handler, partial) else handler
        if (not isinstance(function, types.FunctionType)
                or not Path(inspect.getfile(function)).resolve().is_relative_to(root)
                or handler_identity(handler) != record["native_semantics"]["handlers"][name]):
            raise ForgeError("Native handler differs from the inspected platform")
    return handlers
