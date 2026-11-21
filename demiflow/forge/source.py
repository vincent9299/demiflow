"""Bounded static source lookup. Candidate modules are never imported."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
from pathlib import Path
import time

from .io import no_symlinks
from .platform_environment import read_bounded

SOURCE_LIMITS = {
    "files": 1024,
    "file_bytes": 1024**2,
    "total_bytes": 16 * 1024**2,
    "ast_nodes": 200000,
    "file_nodes": 50000,
    "depth": 32,
    "calls": 256,
    "expansion": 32,
    "nodes": 1024,
    "edges": 4096,
    "seconds": 60,
}


class InspectionError(ValueError):
    def __init__(self, rule, detail, *, node=None, path="", status="invalid", cause="unknown"):
        super().__init__(detail)
        self.diagnostic = {
            "rule": rule,
            "status": status,
            "category": ("unknown" if status == "incomplete" else "unsupported" if rule == "FL-08"
                         else "denied" if rule in {"FL-07", "FL-09", "OP-07", "OP-08"} else "invalid"),
            "detail": detail,
            "path": path,
            "line": getattr(node, "lineno", None),
            "column": getattr(node, "col_offset", None),
            "cause": cause,
            "code": rule,
            "scope": "source" if path else "flow",
            "related_nodes": [],
        }


@dataclass(frozen=True)
class Symbol:
    name: str


@dataclass
class Definition:
    module: str
    node: ast.AST


class SourceTree:
    def __init__(self, root, limits=None, *, exclude=()):
        self.root = Path(root).absolute()
        no_symlinks(self.root)
        self.excluded = frozenset(Path(path) for path in exclude)
        self.limits = dict(SOURCE_LIMITS)
        for key, value in (limits or {}).items():
            if (
                key not in self.limits
                or type(value) is not int
                or not 0 < value <= self.limits[key]
            ):
                raise ValueError(f"Invalid or expanded inspection limit: {key}")
            self.limits[key] = value
        self.modules, self.files, self.loading = {}, {}, set()
        self.imports = {}
        self.bytes = self.nodes = self.calls = 0
        self.started = time.monotonic()

    def guard(self):
        if time.monotonic() - self.started > self.limits["seconds"]:
            raise InspectionError(
                "FL-10", "Inspection time budget exceeded", status="incomplete", cause="resource"
            )

    def fail(self, rule, detail, node=None, module="", *, incomplete=False, path=None, cause=None):
        if path is None:
            path = module.replace(".", "/") + ".py" if module else ""
        raise InspectionError(
            rule,
            detail,
            node=node,
            path=path,
            status="incomplete" if incomplete else "invalid",
            cause=cause or ("unknown" if incomplete else "source"),
        )

    def require_visible(self, relative):
        path = Path(relative)
        if self.excluded.intersection((path, *path.parents)):
            self.fail("FL-10", "Source is outside the visible scope", incomplete=True)

    def load(self, module):
        import sys
        self.guard()
        if module in self.modules:
            return self.modules[module]
        parts = module.split(".")
        if parts[0] in sys.stdlib_module_names or parts[0] == 'demiflow':
            self.fail('FL-02', 'Business module shadows a trusted namespace: ' + parts[0])
        if any(not p.isidentifier() for p in parts):
            self.fail(
                "OP-01", f"Business definition must name a local Python module: {module}"
            )
        if module in self.loading or len(self.loading) >= self.limits["depth"]:
            self.fail(
                "OP-06",
                "Cyclic/deep module dependency is not supported",
                module=module,
                incomplete=True,
            )
        self.loading.add(module)
        try:
            # Package initialization participates in the closure even though no
            # import is performed here.
            for index in range(1, len(parts)):
                parent = ".".join(parts[:index])
                if (
                    parent not in self.loading
                    and (self.root / "/".join(parts[:index]) / "__init__.py").exists()
                ):
                    self.load(parent)
            path = self.root.joinpath(*parts).with_suffix(".py")
            if not path.exists():
                path = self.root.joinpath(*parts) / "__init__.py"
            relative = str(path.relative_to(self.root))
            self.require_visible(relative)
            if len(self.files) >= self.limits["files"]:
                self.fail("FL-10", "Source file budget exceeded", path=relative, incomplete=True)
            try:
                raw = read_bounded(path, self.limits["file_bytes"])
            except (OSError, ValueError) as exc:
                self.fail("FL-01", str(exc), path=relative, incomplete=True)
            # Read evidence is independent of successful parsing/inspection.
            # Failed inputs must also participate in report freshness checks.
            self.files[relative] = hashlib.sha256(raw).hexdigest()
            self.bytes += len(raw)
            if self.bytes > self.limits["total_bytes"]:
                self.fail("FL-10", "Source byte budget exceeded", path=relative, incomplete=True)
            try:
                tree = ast.parse(raw, filename=str(path))
            except SyntaxError as exc:
                self.fail("FL-01", str(exc), exc, path=relative, incomplete=True)
            count = sum(1 for _ in ast.walk(tree))
            self.nodes += count
            if (
                count > self.limits["file_nodes"]
                or self.nodes > self.limits["ast_nodes"]
            ):
                self.fail("FL-10", "AST budget exceeded", path=relative, incomplete=True)
            namespace = {}
            for statement in tree.body:
                self.guard()
                if (
                    isinstance(statement, ast.Expr)
                    and isinstance(statement.value, ast.Constant)
                    and isinstance(statement.value.value, str)
                ):
                    continue
                if isinstance(statement, (ast.Import, ast.ImportFrom)):
                    self._imports(
                        module, statement, namespace, path.name == "__init__.py"
                    )
                elif isinstance(
                    statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                ):
                    if statement.name in namespace:
                        self.fail(
                            "FL-02",
                            "Definition shadows an existing symbol",
                            statement,
                            module,
                        )
                    if statement.decorator_list:
                        self.fail(
                            "OP-06",
                            "Candidate decorators are unsupported",
                            statement,
                            module,
                            incomplete=True,
                        )
                    self._definition_initialization(statement, namespace, module)
                    if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        for default in [
                            *statement.args.defaults,
                            *statement.args.kw_defaults,
                        ]:
                            if default is not None:
                                self.literal(default, module)
                    namespace[statement.name] = Definition(module, statement)
                elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
                    targets = (
                        statement.targets
                        if isinstance(statement, ast.Assign)
                        else [statement.target]
                    )
                    if any(
                        not isinstance(t, ast.Name) or t.id in namespace
                        for t in targets
                    ):
                        self.fail(
                            "FL-01",
                            "Module assignment must define a new constant",
                            statement,
                            module,
                        )
                    if isinstance(statement, ast.AnnAssign):
                        self._annotation_initialization(
                            statement.annotation, namespace, module
                        )
                    value = (
                        namespace[statement.value.id]
                        if isinstance(statement.value, ast.Name)
                        and isinstance(
                            namespace.get(statement.value.id), (Symbol, Definition)
                        )
                        else self.literal(statement.value, module)
                    )
                    for target in targets:
                        namespace[target.id] = value
                else:
                    self.fail(
                        "FL-01",
                        "Executable module initialization is forbidden",
                        statement,
                        module,
                    )
            self.modules[module] = namespace
            # Even an unused local import runs its package/module initialization
            # in Python. Include that source in the inspected closure as well.
            for value in [*namespace.values(), *self.imports.get(module, ())]:
                if isinstance(value, Symbol) and self.is_local(value.name):
                    names = value.name.split(".")
                    for end in range(len(names), 0, -1):
                        dependency = ".".join(names[:end])
                        base = self.root.joinpath(*names[:end])
                        if (
                            base.with_suffix(".py").exists()
                            or (base / "__init__.py").exists()
                        ):
                            if dependency != module:
                                self.load(dependency)
                            break
            return namespace
        finally:
            self.loading.remove(module)

    def _definition_initialization(self, node, namespace, module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.decorator_list:
                self.fail(
                    "OP-06",
                    "Candidate decorators are unsupported",
                    node,
                    module,
                    incomplete=True,
                )
            for default in [*node.args.defaults, *node.args.kw_defaults]:
                if default is not None:
                    self.literal(default, module)
            annotations = [
                p.annotation
                for p in [
                    *node.args.posonlyargs,
                    *node.args.args,
                    *node.args.kwonlyargs,
                ]
            ] + [node.returns]
            annotations.extend(
                p.annotation
                for p in (node.args.vararg, node.args.kwarg)
                if p is not None
            )
            for annotation in annotations:
                if annotation is not None:
                    self._annotation_initialization(annotation, namespace, module)
            return
        typed = (
            len(node.bases) == 1
            and isinstance(node.bases[0], ast.Name)
            and namespace.get(node.bases[0].id) == Symbol("typing.TypedDict")
        )
        if (node.bases and not typed) or any(
            k.arg != "total" or not typed for k in node.keywords
        ):
            self.fail(
                "OP-06",
                "Candidate inheritance/metaclass is unsupported",
                node,
                module,
                incomplete=True,
            )
        for keyword in node.keywords:
            self.literal(keyword.value, module)
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._definition_initialization(item, namespace, module)
            elif isinstance(item, ast.AnnAssign) and typed and item.value is None:
                self._annotation_initialization(item.annotation, namespace, module)
            elif (
                isinstance(item, ast.Pass)
                or isinstance(item, ast.Expr)
                and isinstance(item.value, ast.Constant)
                and isinstance(item.value.value, str)
            ):
                pass
            else:
                self.fail(
                    "OP-07",
                    "Executable class initialization is forbidden",
                    item,
                    module,
                )

    def _annotation_initialization(self, node, namespace, module):
        """Do not admit annotations that can invoke a candidate protocol."""
        for item in ast.walk(node):
            if isinstance(
                item,
                (
                    ast.Call,
                    ast.Lambda,
                    ast.NamedExpr,
                    ast.Await,
                    ast.ListComp,
                    ast.DictComp,
                    ast.GeneratorExp,
                ),
            ):
                self.fail("OP-06", "Executable annotation is forbidden", item, module)
            if isinstance(item, ast.Subscript):
                base = item.value
                while isinstance(base, ast.Attribute):
                    base = base.value
                value = namespace.get(base.id) if isinstance(base, ast.Name) else None
                safe = (
                    isinstance(base, ast.Name)
                    and value is None
                    and base.id in {"list", "dict", "tuple", "set", "frozenset", "type"}
                )
                safe |= isinstance(value, Symbol) and (
                    value.name.startswith("typing.") or value.name == "typing"
                )
                if not safe:
                    self.fail(
                        "OP-06",
                        "Annotation subscription could invoke an unsupported candidate protocol",
                        item,
                        module,
                        incomplete=True,
                    )

    def literal(self, node, module=""):
        try:
            value = ast.literal_eval(node)
            if isinstance(value, (str, bytes)) and len(value) > 65536:
                raise ValueError("Literal exceeds budget")
            if type(value) is int and value.bit_length() > 256:
                raise ValueError("Integer exceeds budget")
            return value
        except (ValueError, TypeError, RecursionError):
            self.fail(
                "FL-01",
                "Only bounded literal defaults/constants are allowed",
                node,
                module,
            )

    def _imports(self, module, node, namespace, package):
        if isinstance(node, ast.Import):
            self.imports.setdefault(module, []).extend(
                Symbol(a.name) for a in node.names
            )
            pairs = [
                (
                    a.asname or a.name.split(".")[0],
                    a.name if a.asname else a.name.split(".")[0],
                )
                for a in node.names
            ]
        else:
            base = node.module or ""
            if node.level:
                parents = module.split(".") if package else module.split(".")[:-1]
                if node.level > len(parents):
                    self.fail("FL-01", "Relative import leaves workspace", node, module)
                base = ".".join(
                    parents[: len(parents) - node.level + 1] + ([base] if base else [])
                )
            pairs = [(a.asname or a.name, base + "." + a.name) for a in node.names]
        for name, target in pairs:
            if name == "*" or name in namespace:
                self.fail(
                    "FL-02", "Wildcard/shadowing imports are forbidden", node, module
                )
            root = target.split(".")[0]
            if root in {"demiflow", "typing", "__future__", "json", "hashlib", "math", "re", "html", "torch"}:
                if (self.root / (root + ".py")).exists() or (self.root / root).exists():
                    self.fail(
                        "FL-02",
                        f"Local module shadows trusted namespace: {root}",
                        node,
                        module,
                    )
            elif not self.is_local(target):
                known_effect = root in {
                    "os",
                    "sys",
                    "subprocess",
                    "requests",
                    "httpx",
                    "urllib",
                    "socket",
                    "openai",
                    "threading",
                    "concurrent",
                    "multiprocessing",
                    "random",
                    "time",
                }
                self.fail(
                    "OP-05" if known_effect else "OP-06",
                    f"Dependency has no admitted compute boundary: {target}",
                    node,
                    module,
                    incomplete=not known_effect,
                )
            namespace[name] = Symbol(target)

    def is_local(self, name):
        """Local module origin, independent of business directory names."""
        parts = name.split('.')
        if any(not part.isidentifier() for part in parts):
            return False
        for end in range(len(parts), 0, -1):
            base = self.root.joinpath(*parts[:end])
            for candidate in (base.with_suffix('.py'), base / '__init__.py'):
                no_symlinks(candidate)
                if candidate.is_file():
                    return True
        return False

    def resolve(self, symbol, seen=()):
        if not isinstance(symbol, Symbol) or not self.is_local(symbol.name):
            return symbol
        if symbol.name in seen or len(seen) >= self.limits["depth"]:
            self.fail("OP-06", "Cyclic/deep symbol aliases", incomplete=True)
        parts = symbol.name.split(".")
        if len(parts) > 1:
            module, name = ".".join(parts[:-1]), parts[-1]
            path = self.root.joinpath(*parts[:-1]).with_suffix(".py")
            package = self.root.joinpath(*parts[:-1]) / "__init__.py"
            if path.exists() or package.exists():
                namespace = self.load(module)
                if name in namespace:
                    value = namespace[name]
                    if isinstance(value, Symbol) and value.name == symbol.name:
                        self.fail(
                            "OP-06",
                            "Recursive import alias",
                            module=module,
                            incomplete=True,
                        )
                    return self.resolve(value, (*seen, symbol.name))
        return symbol

    def unchanged(self):
        for name, digest in self.files.items():
            if (
                hashlib.sha256(
                    read_bounded(self.root / name, self.limits["file_bytes"])
                ).hexdigest()
                != digest
            ):
                self.fail(
                    "FL-10",
                    f"Source changed during inspection: {name}",
                    incomplete=True,
                )


def parameters(node, *, drop_self=False):
    args = node.args
    result = []
    positional = [*args.posonlyargs, *args.args]
    required = len(positional) - len(args.defaults)
    for i, value in enumerate(positional):
        if drop_self and i == 0 and value.arg == "self":
            continue
        result.append(
            {
                "name": value.arg,
                "kind": (
                    "POSITIONAL_ONLY"
                    if i < len(args.posonlyargs)
                    else "POSITIONAL_OR_KEYWORD"
                ),
                "required": i < required,
                "annotation": value.annotation,
            }
        )
    if args.vararg:
        result.append(
            {
                "name": args.vararg.arg,
                "kind": "VAR_POSITIONAL",
                "required": False,
                "annotation": args.vararg.annotation,
            }
        )
    for value, default in zip(args.kwonlyargs, args.kw_defaults):
        result.append(
            {
                "name": value.arg,
                "kind": "KEYWORD_ONLY",
                "required": default is None,
                "annotation": value.annotation,
            }
        )
    if args.kwarg:
        result.append(
            {
                "name": args.kwarg.arg,
                "kind": "VAR_KEYWORD",
                "required": False,
                "annotation": args.kwarg.annotation,
            }
        )
    return result


def defaults(node, tree, module):
    positional = [*node.args.posonlyargs, *node.args.args]
    result = {
        p.arg: tree.literal(v, module)
        for p, v in zip(
            positional[len(positional) - len(node.args.defaults) :], node.args.defaults
        )
    }
    result.update(
        {
            p.arg: tree.literal(v, module)
            for p, v in zip(node.args.kwonlyargs, node.args.kw_defaults)
            if v is not None
        }
    )
    return result
