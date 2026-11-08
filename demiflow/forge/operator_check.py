"""Static adapters for ordinary typed functions/actors, never their execution."""

from __future__ import annotations

import ast
import copy
import re
from dataclasses import dataclass

from ..data.callable_contract import bind_parameters
from ..data.flow_contract import compatible, field, scalar, union_type, literal_type
from .source import Definition, Symbol, defaults, parameters

_UNKNOWN = object()

@dataclass
class Value:
    type: dict
    origin: str = "local"
    literal: object = _UNKNOWN
    owns_container: bool = False


def container_origin(values):
    """A new container can still borrow mutable objects from a caller."""
    return next(
        (
            v.origin
            for v in values
            if v.origin != "local" and v.type["kind"] in {"record", "list", "dict", "set"}
        ),
        "local",
    )


class OperatorChecker:
    def __init__(self, tree):
        self.tree = tree
        self.active = []
        self.generators = []
        self.type_active = set()
        self.records = []

    def fail(self, rule, detail, node, module, *, incomplete=False):
        self.tree.fail(rule, detail, node, module, incomplete=incomplete)

    def lookup(self, name, module, env):
        if name in env:
            return env[name]
        namespace = self.tree.load(module)
        if name in namespace:
            value = self.tree.resolve(namespace[name])
            return (
                value
                if isinstance(value, (Symbol, Definition))
                else Value(literal_type(value), "shared")
            )
        return Symbol("builtins." + name)

    def symbol(self, node, module):
        if isinstance(node, ast.Name):
            return self.lookup(node.id, module, {})
        if isinstance(node, ast.Attribute):
            parent = self.symbol(node.value, module)
            if isinstance(parent, Symbol):
                return self.tree.resolve(Symbol(parent.name + "." + node.attr))
        self.fail(
            "OP-06",
            "Symbol source is not statically known",
            node,
            module,
            incomplete=True,
        )

    def annotation(self, node, module, depth=0):
        if node is None or depth > 16:
            self.fail(
                "OP-03",
                "Missing or deeply nested public type",
                node,
                module,
                incomplete=True,
            )
        if isinstance(node, ast.Constant):
            if node.value is None:
                return scalar("none")
            if isinstance(node.value, str):
                # Parsing a forward annotation is not evaluating it.
                return self.annotation(
                    ast.parse(node.value, mode="eval").body, module, depth + 1
                )
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            a, b = self.annotation(node.left, module, depth + 1), self.annotation(
                node.right, module, depth + 1
            )
            try:
                return union_type(a, b)
            except ValueError as exc:
                self.fail("OP-03", str(exc), node, module, incomplete=True)
        if isinstance(node, ast.Subscript):
            target = self.symbol(node.value, module)
            name = target.name if isinstance(target, Symbol) else ""
            if name in {"typing.Optional", "typing.Required", "typing.NotRequired"}:
                result = self.annotation(node.slice, module, depth + 1)
                return (
                    dict(result, nullable=True) if name == "typing.Optional" else result
                )
            args = (
                list(node.slice.elts)
                if isinstance(node.slice, ast.Tuple)
                else [node.slice]
            )
            if name == "typing.Union":
                try:
                    return union_type(*(self.annotation(arg, module, depth + 1) for arg in args))
                except ValueError as exc:
                    self.fail("OP-03", str(exc), node, module, incomplete=True)
            kinds = {
                "builtins.list": "list",
                "typing.List": "list",
                "builtins.tuple": "tuple",
                "typing.Tuple": "tuple",
                "builtins.dict": "dict",
                "typing.Dict": "dict",
                "typing.Iterator": "iterator",
                "typing.Iterable": "iterator",
                "collections.abc.Iterator": "iterator",
                "collections.abc.Iterable": "iterator",
            }
            if name in kinds:
                kind = kinds[name]
                if kind == "dict" and len(args) == 2:
                    return {
                        **scalar(kind),
                        "key": self.annotation(args[0], module, depth + 1),
                        "value": self.annotation(args[1], module, depth + 1),
                    }
                if kind in {"list", "tuple", "iterator"} and (
                    len(args) == 1
                    or (
                        len(args) == 2
                        and isinstance(args[1], ast.Constant)
                        and args[1].value is Ellipsis
                    )
                ):
                    return {
                        **scalar(kind),
                        "item": self.annotation(args[0], module, depth + 1),
                    }
            self.fail(
                "OP-03", "Unsupported generic port type", node, module, incomplete=True
            )
        target = self.symbol(node, module)
        if isinstance(target, Symbol) and target.name in {
            f"builtins.{x}" for x in ("str", "bytes", "int", "float", "bool")
        }:
            return scalar(target.name.split(".")[-1])
        if isinstance(target, Definition) and isinstance(target.node, ast.ClassDef):
            key = (target.module, target.node.name)
            if key in self.type_active:
                self.fail(
                    "OP-03", "Recursive type declaration", node, module, incomplete=True
                )
            self.type_active.add(key)
            try:
                cls = target.node
                if len(cls.bases) != 1 or self.symbol(
                    cls.bases[0], target.module
                ) != Symbol("typing.TypedDict"):
                    self.fail(
                        "OP-03",
                        "Port class must be a TypedDict",
                        cls,
                        target.module,
                        incomplete=True,
                    )
                total = True
                for keyword in cls.keywords:
                    if (
                        keyword.arg != "total"
                        or not isinstance(keyword.value, ast.Constant)
                        or type(keyword.value.value) is not bool
                    ):
                        self.fail(
                            "OP-03",
                            "Unsupported TypedDict options",
                            cls,
                            target.module,
                            incomplete=True,
                        )
                    total = keyword.value.value
                fields = {}
                for item in cls.body:
                    if isinstance(item, ast.Expr) and isinstance(
                        item.value, ast.Constant
                    ):
                        continue
                    if (
                        not isinstance(item, ast.AnnAssign)
                        or not isinstance(item.target, ast.Name)
                        or item.value is not None
                    ):
                        self.fail(
                            "OP-03",
                            "TypedDict must only declare fields",
                            item,
                            target.module,
                        )
                    required = total
                    if isinstance(item.annotation, ast.Subscript):
                        wrapper = self.symbol(item.annotation.value, target.module)
                        if wrapper == Symbol("typing.Required"):
                            required = True
                        elif wrapper == Symbol("typing.NotRequired"):
                            required = False
                    fields[item.target.id] = field(
                        self.annotation(item.annotation, target.module, depth + 1),
                        required=required,
                    )
                    if len(fields) > 2048:
                        self.fail(
                            "OP-03",
                            "TypedDict field budget exceeded",
                            cls,
                            target.module,
                            incomplete=True,
                        )
                return {**scalar("record"), "fields": fields}
            finally:
                self.type_active.remove(key)
        self.fail(
            "OP-03",
            "Unknown/Any port type is not an explicit contract",
            node,
            module,
            incomplete=True,
        )

    def describe(self, definition, *, method=False):
        if not isinstance(definition, Definition):
            self.fail(
                "OP-01",
                "Operator must resolve to an actual local definition",
                getattr(definition, "node", None),
                getattr(definition, "module", ""),
            )
        cls = definition.node if isinstance(definition.node, ast.ClassDef) else None
        node = definition.node
        if cls:
            if cls.bases or cls.keywords:
                self.fail(
                    "OP-06",
                    "Actor inheritance/metaclass is not in the supported subset",
                    cls,
                    definition.module,
                    incomplete=True,
                )
            node = next(
                (
                    n
                    for n in cls.body
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and n.name == "__call__"
                ),
                None,
            )
            if node is None:
                self.fail("OP-02", "Actor requires __call__", cls, definition.module)
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.fail("OP-02", "Expected a function or actor", node, definition.module)
        params = parameters(node, drop_self=bool(cls) or method)
        for p in params:
            if p["kind"] in {"VAR_POSITIONAL", "VAR_KEYWORD", "POSITIONAL_ONLY"}:
                self.fail(
                    "OP-02",
                    "Variadic/positional-only operator parameters are unsupported",
                    node,
                    definition.module,
                )
            p["type"] = self.annotation(p.pop("annotation"), definition.module)
        return {
            "definition": definition,
            "node": node,
            "parameters": params,
            "defaults": defaults(node, self.tree, definition.module),
            "returns": self.annotation(node.returns, definition.module),
            "async": isinstance(node, ast.AsyncFunctionDef),
            "generator": any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in ast.walk(node)),
            "class": cls,
        }

    def check(
        self,
        description,
        *,
        constructor_args=(),
        constructor_kwargs=None,
        state=None,
        methods=None,
    ):
        definition, node = description["definition"], description["node"]
        module = definition.module
        identity = (module, node.name, getattr(node, "lineno", 0))
        if identity in self.active or len(self.active) >= self.tree.limits["depth"]:
            self.fail(
                "OP-06",
                "Recursive/deep callable closure",
                node,
                module,
                incomplete=True,
            )
        self.tree.calls += 1
        if self.tree.calls > self.tree.limits["calls"]:
            self.fail(
                "FL-10",
                "Callable context budget exceeded",
                node,
                module,
                incomplete=True,
            )
        self.active.append(identity)
        self.generators.append(description.get("generator", False))
        try:
            if description.get("generator") and (description["async"] or description["returns"]["kind"] != "iterator"):
                self.fail("OP-03", "Generators require a synchronous Iterator/Iterable return contract", node, module)
            state = {} if state is None else state
            cls = description["class"]
            if cls:
                methods = {
                    n.name: Definition(module, n)
                    for n in cls.body
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
                init = methods.get("__init__")
                for item in cls.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if item.name.startswith('__') and item.name not in {'__init__', '__call__'}:
                            self.fail('OP-06', 'Actor object protocol overrides are unsupported', item, module, incomplete=True)
                        if item.decorator_list:
                            self.fail(
                                "OP-06",
                                "Actor decorators are unsupported",
                                item,
                                module,
                                incomplete=True,
                            )
                        defaults(item, self.tree, module)
                    elif isinstance(item, ast.Expr) and isinstance(
                        item.value, ast.Constant
                    ):
                        pass
                    elif isinstance(item, ast.Pass):
                        pass
                    else:
                        self.fail(
                            "OP-07",
                            "Actor class body must contain methods only",
                            item,
                            module,
                        )
                if init:
                    ps = parameters(init.node, drop_self=True)
                    try:
                        values = bind_parameters(
                            ps,
                            constructor_args,
                            constructor_kwargs or {},
                            defaults=defaults(init.node, self.tree, module),
                        )
                    except TypeError as exc:
                        self.fail("OP-02", str(exc), init.node, module)
                    env = {
                        k: Value(literal_type(v), "config", v) for k, v in values.items()
                    }
                    self._statements(
                        init.node.body, env, module, scalar("none"), state, methods
                    )
                elif constructor_args or constructor_kwargs:
                    self.fail(
                        "OP-02", "Actor has no constructor parameters", cls, module
                    )
            else:
                methods = methods or {}
            constructor_state = (
                {name: copy.deepcopy(value.literal) for name, value in state.items()}
                if cls and all(isinstance(value, Value) and value.literal is not _UNKNOWN for value in state.values())
                else None
            )
            env = {
                p["name"]: Value(p["type"], "input") for p in description["parameters"]
            }
            for key, value in description["defaults"].items():
                expected = next(
                    p["type"] for p in description["parameters"] if p["name"] == key
                )
                if not compatible(literal_type(value), expected):
                    self.fail(
                        "OP-03",
                        f"Default conflicts with annotation: {key}",
                        node,
                        module,
                    )
            terminated = self._statements(
                node.body, env, module, description["returns"], state, methods
            )
            if (
                not terminated
                and description["returns"]["kind"] != "none"
                and not description["returns"].get("nullable")
                and not (description["generator"] and description["returns"]["kind"] == "iterator" and not description["async"])
            ):
                self.fail(
                    "OP-03",
                    "Function can fall through without a declared return",
                    node,
                    module,
                )
            for name in ("astart", "astop", "aclose"):
                if name in methods:
                    hook = methods[name]
                    if parameters(hook.node, drop_self=True):
                        self.fail(
                            "OP-07",
                            f"Unsupported lifecycle signature: {name}",
                            hook.node,
                            module,
                        )
                    self._statements(
                        hook.node.body, {}, module, scalar("none"), state, methods
                    )
            self.records.append(
                {
                    "source": module.replace(".", "/") + ".py",
                    "line": node.lineno,
                    "name": definition.node.name,
                    "parameters": description["parameters"],
                    "returns": description["returns"],
                    "actor": bool(cls),
                    "async": description["async"],
                    "hooks": sorted(name for name in methods if name in {"astart", "astop", "aclose", "close", "start", "stop"}),
                    "behavior": "not_run",
                    "lifecycle_runtime": "not_run",
                }
            )
            return constructor_state
        finally:
            self.active.pop()
            self.generators.pop()

    def _expr(self, node, env, module, state, methods):
        self.tree.guard()

        def visit(n):
            return self._expr(n, env, module, state, methods)

        if isinstance(node, ast.Constant):
            return Value(literal_type(node.value), literal=node.value)
        if isinstance(node, ast.Name):
            return self.lookup(node.id, module, env)
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                if node.attr in state:
                    return state[node.attr]
                if node.attr in methods:
                    return methods[node.attr]
                self.fail(
                    "OP-06",
                    f"Unknown actor attribute: {node.attr}",
                    node,
                    module,
                    incomplete=True,
                )
            parent = visit(node.value)
            if isinstance(parent, Symbol):
                return self.tree.resolve(Symbol(parent.name + "." + node.attr))
            self.fail(
                "OP-06",
                "Non-call attribute access is outside the supported value protocol",
                node,
                module,
                incomplete=True,
            )
        if isinstance(node, ast.Subscript):
            parent = visit(node.value)
            if isinstance(node.slice, ast.Slice):
                if not isinstance(parent, Value) or parent.type['kind'] not in {'list', 'tuple', 'str', 'bytes'}:
                    self.fail('OP-06', 'Slice requires a known sequence', node, module, incomplete=True)
                for bound in (node.slice.lower, node.slice.upper, node.slice.step):
                    if bound is not None:
                        value = visit(bound)
                        if not isinstance(value, Value) or value.type != scalar('int'):
                            self.fail('OP-03', 'Slice bounds require non-null integers', node, module)
                return Value(parent.type, parent.origin, owns_container=True)
            key = visit(node.slice)
            if not isinstance(parent, Value):
                self.fail(
                    "OP-06", "Unknown subscript receiver", node, module, incomplete=True
                )
            kind = parent.type["kind"]
            if kind == "record" and isinstance(node.slice, ast.Constant):
                value = parent.type["fields"].get(node.slice.value)
                if not value:
                    self.fail(
                        "OP-03",
                        f"Undeclared input key: {node.slice.value}",
                        node,
                        module,
                    )
                if not value["required"]:
                    self.fail(
                        "OP-03",
                        "Optional keys require an explicit get/default",
                        node,
                        module,
                    )
                return Value(value["type"], parent.origin)
            if kind in {"list", "tuple", "dict", "str", "bytes"}:
                return Value(
                    parent.type.get(
                        "item",
                        parent.type.get(
                            "value", scalar("str" if kind == "str" else "int")
                        ),
                    ),
                    parent.origin,
                )
            self.fail(
                "OP-06", "Subscript type is unresolved", node, module, incomplete=True
            )
        if isinstance(node, ast.Dict):
            fields = {}
            values = []
            for key, value in zip(node.keys, node.values):
                actual = visit(value)
                if not isinstance(actual, Value):
                    self.fail(
                        "OP-06",
                        "Callable objects are not row values",
                        value,
                        module,
                        incomplete=True,
                    )
                values.append(actual)
                if key is None:
                    if actual.type["kind"] != "record":
                        self.fail(
                            "OP-03",
                            "Unknown dictionary expansion",
                            value,
                            module,
                            incomplete=True,
                        )
                    fields.update(actual.type["fields"])
                elif isinstance(key, ast.Constant) and isinstance(key.value, str):
                    fields[key.value] = field(actual.type)
                else:
                    self.fail(
                        "OP-03",
                        "Row result keys must be explicit",
                        key,
                        module,
                        incomplete=True,
                    )
            return Value(
                {**scalar("record"), "fields": fields}, container_origin(values), owns_container=True
            )
        if isinstance(node, ast.JoinedStr):
            for part in node.values:
                if isinstance(part, ast.Constant) and type(part.value) is str:
                    continue
                if not isinstance(part, ast.FormattedValue):
                    self.fail('OP-06', 'Unknown formatted value', node, module, incomplete=True)
                value = visit(part.value)
                if not isinstance(value, Value) or value.type['kind'] not in {'str', 'int', 'float', 'bool', 'none'}:
                    self.fail('OP-03', 'Formatted values require primitive ports', part, module)
                if part.conversion not in {-1, ord('s'), ord('r'), ord('a')}:
                    self.fail('OP-03', 'Unknown string conversion', part, module)
                if part.format_spec is not None:
                    if not isinstance(part.format_spec, ast.JoinedStr) or any(
                            not isinstance(v, ast.Constant) or type(v.value) is not str for v in part.format_spec.values):
                        self.fail('OP-06', 'Format spec must be literal', part, module, incomplete=True)
                    spec = ''.join(v.value for v in part.format_spec.values)
                    match = re.fullmatch(r'[+ -]?0?([0-9]{0,4})(?:\.([0-9]{1,2}))?([sdefg%]?)', spec)
                    if not match or int(match[1] or 0) > 1024 or int(match[2] or 0) > 64:
                        self.fail('OP-03', 'Format spec exceeds supported bounds', part, module)
                    kind = 'str' if part.conversion != -1 else value.type['kind']
                    if spec and (value.type.get('nullable') or kind == 'none'
                            or match[3] in {'e', 'f', 'g', '%'} and kind not in {'int', 'float', 'bool'}
                            or match[3] == 'd' and kind not in {'int', 'bool'}
                            or match[3] == 's' and kind != 'str'):
                        self.fail('OP-03', 'Format spec conflicts with value type', part, module)
            return Value(scalar('str'))
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            values = [visit(n) for n in node.elts]
            if any(not isinstance(v, Value) for v in values):
                self.fail(
                    "OP-06",
                    "Dynamic callable containers are unsupported",
                    node,
                    module,
                    incomplete=True,
                )
            item = union_type(*(v.type for v in values)) if values else scalar('never')
            return Value(
                {
                    **scalar("set" if isinstance(node, ast.Set) else "tuple" if isinstance(node, ast.Tuple) else "list"),
                    "item": item,
                },
                container_origin(values), owns_container=True,
            )
        if isinstance(node, ast.BoolOp):
            local = dict(env)
            values = []
            for index, operand in enumerate(node.values):
                value = self._expr(operand, local, module, state, methods)
                if not isinstance(value, Value) or value.type['kind'] not in {
                        'str', 'bytes', 'int', 'float', 'bool', 'none', 'record',
                        'list', 'tuple', 'dict', 'set', 'json', 'union'}:
                    self.fail('OP-06', 'Boolean operand protocol is unknown', node, module, incomplete=True)
                # An earlier operand of `or` is returned only when truthy.
                # `and` can return a false/null operand, including before the end.
                if isinstance(node.op, ast.Or) and index < len(node.values) - 1:
                    if value.type['kind'] != 'none':
                        values.append(Value(dict(value.type, nullable=False), value.origin,
                                            owns_container=value.owns_container))
                else:
                    values.append(value)
                self._narrow(operand, local, isinstance(node.op, ast.And), module)
            result = values[0]
            for value in values[1:]:
                result = self._merge_values(result, value)
            return result
        if isinstance(node, (ast.Compare, ast.UnaryOp, ast.BinOp)):
            operands = (
                [node.left, *node.comparators]
                if isinstance(node, ast.Compare)
                else (
                    node.values
                    if isinstance(node, ast.BoolOp)
                    else (
                        [node.operand]
                        if isinstance(node, ast.UnaryOp)
                        else [node.left, node.right]
                    )
                )
            )
            values = [visit(n) for n in operands]
            if any(
                not isinstance(v, Value)
                or v.type["kind"]
                not in {
                    "str",
                    "bytes",
                    "int",
                    "float",
                    "bool",
                    "none",
                    "record",
                    "list",
                    "tuple",
                    "dict",
                    "json",
                    "set",
                }
                for v in values
            ):
                self.fail(
                    "OP-06",
                    "Operator operand protocol is unknown",
                    node,
                    module,
                    incomplete=True,
                )
            if (
                isinstance(node, ast.Compare)
                or isinstance(node, ast.UnaryOp)
                and isinstance(node.op, ast.Not)
            ):
                return Value(scalar("bool"))
            kinds = [v.type["kind"] for v in values]
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.BitOr, ast.BitAnd, ast.Sub)) and kinds == ['set', 'set']:
                if not compatible(values[0].type['item'], values[1].type['item']):
                    self.fail('OP-03', 'Set element types differ', node, module)
                return Value(values[0].type, owns_container=True)
            if all(k in {"int", "float", "bool"} for k in kinds):
                kind = (
                    "float"
                    if "float" in kinds
                    or isinstance(getattr(node, "op", None), ast.Div)
                    else "int"
                )
                return Value(
                    scalar(kind, nullable=any(v.type.get("nullable") for v in values))
                )
            if (
                isinstance(node, ast.BinOp)
                and isinstance(node.op, ast.Add)
                and kinds[0] == kinds[1]
                and kinds[0] in {"str", "bytes", "list", "tuple"}
            ):
                return Value(values[0].type, container_origin(values), owns_container=True)
            if (
                isinstance(node, ast.BinOp)
                and isinstance(node.op, ast.Mult)
                and kinds[0] in {"str", "bytes"}
                and kinds[1] == "int"
            ):
                return Value(values[0].type)
            self.fail(
                "OP-06",
                "Arithmetic value transfer is not supported for these operands",
                node,
                module,
                incomplete=True,
            )
        if isinstance(node, ast.IfExp):
            visit(node.test)
            left, right = dict(env), dict(env)
            self._narrow(node.test, left, True, module)
            self._narrow(node.test, right, False, module)
            a = self._expr(node.body, left, module, state, methods)
            b = self._expr(node.orelse, right, module, state, methods)
            if isinstance(a, Value) and isinstance(b, Value):
                return self._merge_values(a, b)
            self.fail('OP-03', 'Conditional values must have known types', node, module, incomplete=True)
        if isinstance(node, ast.Await):
            return visit(node.value)
        if isinstance(node, (ast.ListComp, ast.GeneratorExp, ast.DictComp)):
            local = dict(env)
            for gen in node.generators:
                source = self._expr(gen.iter, local, module, state, methods)
                if (
                    gen.is_async
                    or not isinstance(gen.target, ast.Name)
                    or not isinstance(source, Value)
                    or source.type["kind"] not in {"list", "tuple", "str", "iterator"}
                ):
                    self.fail(
                        "OP-06",
                        "Unsupported comprehension source",
                        node,
                        module,
                        incomplete=True,
                    )
                local[gen.target.id] = Value(
                    source.type.get("item", scalar("str")), source.origin
                )
                for condition in gen.ifs:
                    self._expr(condition, local, module, state, methods)
                    self._narrow(condition, local, True, module)
            if isinstance(node, ast.DictComp):
                key = self._expr(node.key, local, module, state, methods)
                value = self._expr(node.value, local, module, state, methods)
                if (not isinstance(key, Value) or key.type['kind'] not in {'str', 'int', 'float', 'bool'}
                        or key.type.get('nullable') or not isinstance(value, Value)):
                    self.fail('OP-03', 'Dictionary comprehension requires known hashable keys and values', node, module)
                return Value({**scalar('dict'), 'key': key.type, 'value': value.type},
                             container_origin([value]), owns_container=True)
            value = self._expr(node.elt, local, module, state, methods)
            return Value(
                {**scalar("iterator" if isinstance(node, ast.GeneratorExp) else "list"), "item": value.type}, container_origin([value]), owns_container=True
            )
        if isinstance(node, ast.Call):
            if any(k.arg is None for k in node.keywords) or any(
                isinstance(a, ast.Starred) for a in node.args
            ):
                self.fail(
                    "OP-06",
                    "Dynamic call argument expansion is unsupported",
                    node,
                    module,
                    incomplete=True,
                )
            # sorted's key is inspected as a pure expression over the item port;
            # it is not admitted as an arbitrary callable value.
            target_symbol = self.symbol(node.func, module) if isinstance(node.func, ast.Name) and node.func.id not in env else None
            if target_symbol == Symbol('builtins.sorted'):
                if len(node.args) != 1 or any(k.arg not in {'key', 'reverse'} for k in node.keywords):
                    self.fail('OP-02', 'Unsupported sorted signature', node, module)
                source = visit(node.args[0])
                if not isinstance(source, Value) or source.type['kind'] not in {'list', 'tuple', 'set', 'iterator'}:
                    self.fail('OP-03', 'sorted requires a declared sequence', node, module)
                if not any(k.arg == 'key' for k in node.keywords) and (source.type['item']['kind'] not in {'str', 'int', 'float', 'bool', 'never'} or source.type['item'].get('nullable')):
                    self.fail('OP-03', 'sorted requires a scalar item or explicit key', node, module)
                for keyword in node.keywords:
                    if keyword.arg == 'reverse':
                        if not isinstance(keyword.value, ast.Constant) or type(keyword.value.value) is not bool:
                            self.fail('OP-03', 'sorted reverse must be a literal boolean', node, module)
                    else:
                        fn = keyword.value
                        if not isinstance(fn, ast.Lambda) or len(fn.args.args) != 1 or fn.args.posonlyargs or fn.args.kwonlyargs or fn.args.vararg or fn.args.kwarg or fn.args.defaults:
                            self.fail('OP-06', 'sorted key requires one pure item expression', node, module, incomplete=True)
                        local = dict(env, **{fn.args.args[0].arg: Value(source.type['item'], source.origin)})
                        key = self._expr(fn.body, local, module, state, methods)
                        if not isinstance(key, Value) or key.type['kind'] not in {'str', 'int', 'float'} or key.type.get('nullable'):
                            self.fail('OP-03', 'sorted key must be a non-null scalar', node, module)
                return Value({**scalar('list'), 'item': source.type['item']}, source.origin, owns_container=True)
            args = [visit(a) for a in node.args]
            kwargs = {k.arg: visit(k.value) for k in node.keywords}
            if isinstance(node.func, ast.Attribute) and not (
                isinstance(node.func.value, ast.Name) and node.func.value.id == "self"
            ):
                parent = visit(node.func.value)
                if isinstance(parent, Value):
                    return self._method(
                        parent, node.func.attr, args, kwargs, node, module
                    )
            target = visit(node.func)
            if isinstance(target, Definition):
                bound_method = target in methods.values()
                description = self.describe(target, method=bound_method)
                try:
                    actual = bind_parameters(
                        description["parameters"],
                        args,
                        kwargs,
                        defaults={
                            k: Value(literal_type(v))
                            for k, v in description["defaults"].items()
                        },
                    )
                except TypeError as exc:
                    self.fail("OP-02", str(exc), node, module)
                for p in description["parameters"]:
                    value = actual[p["name"]]
                    if not isinstance(value, Value) or not compatible(
                        value.type, p["type"]
                    ):
                        self.fail(
                            "OP-03",
                            "Helper argument conflicts with declared port: "
                            + p["name"],
                            node,
                            module,
                        )
                self.check(
                    description,
                    state=state if bound_method else None,
                    methods=methods if bound_method else None,
                )
                return Value(
                    description["returns"], container_origin(list(actual.values()))
                )
            if isinstance(target, Symbol):
                name = target.name
                pure = self._pure_call(name, args, kwargs, node, module)
                if pure is not None:
                    return pure
                if name.startswith("demiflow."):
                    self.fail(
                        "OP-04",
                        "Dataset/platform orchestration cannot run inside a compute operator",
                        node,
                        module,
                    )
                if name in {
                    "builtins.eval",
                    "builtins.exec",
                    "builtins.compile",
                    "builtins.__import__",
                    "builtins.getattr",
                    "builtins.setattr",
                    "builtins.globals",
                    "builtins.locals",
                }:
                    self.fail(
                        "OP-06",
                        f"Dynamic execution/reflection is forbidden: {name}",
                        node,
                        module,
                    )
                if name in {"builtins.open", "builtins.print", "builtins.input"}:
                    self.fail("OP-05", "Undeclared external effect", node, module)
                builtin = name.removeprefix("builtins.")
                if name.startswith("builtins.") and builtin in {
                    "len",
                    "str",
                    "int",
                    "float",
                    "bool",
                    "bytes",
                    "abs",
                    "min",
                    "max",
                    "round",
                    "sum",
                    "all",
                    "any",
                    "range",
                    "ValueError",
                    "TypeError",
                    "RuntimeError",
                    "KeyError",
                    "Exception",
                    "BaseException",
                }:
                    if any(not isinstance(a, Value) for a in [*args, *kwargs.values()]):
                        self.fail(
                            "OP-06",
                            "Unknown builtin argument protocol",
                            node,
                            module,
                            incomplete=True,
                        )
                    result = (
                        "int"
                        if builtin == "len"
                        else (
                            "bool"
                            if builtin in {"all", "any"}
                            else (
                                "exception"
                                if builtin.endswith("Error")
                                or builtin.endswith("Exception")
                                else (
                                    builtin
                                    if builtin
                                    in {"str", "int", "float", "bool", "bytes"}
                                    else args[0].type["kind"] if args else "unknown"
                                )
                            )
                        )
                    )
                    if builtin == "range":
                        return Value({**scalar("list"), "item": scalar("int")})
                    if builtin == "sum":
                        if not args or args[0].type["kind"] not in {"list", "tuple"}:
                            self.fail(
                                "OP-03",
                                "sum requires a declared sequence",
                                node,
                                module,
                                incomplete=True,
                            )
                        return Value(args[0].type["item"])
                    if (
                        builtin in {"min", "max"}
                        and len(args) == 1
                        and args[0].type["kind"] in {"list", "tuple"}
                    ):
                        return Value(args[0].type["item"], args[0].origin)
                    return Value(scalar(result))
            self.fail(
                "OP-06",
                "Call target/effect has no admitted boundary",
                node,
                module,
                incomplete=True,
            )
        self.fail(
            "OP-06",
            f"Unsupported expression: {type(node).__name__}",
            node,
            module,
            incomplete=True,
        )

    def _pure_call(self, name, args, kwargs, node, module):
        """Closed standard-library value protocols, never candidate execution.

        JSON parsed values remain opaque until a builtin type guard narrows
        them. They cannot satisfy an annotated business port by assertion.
        Hooks, arbitrary classes, codecs and mutable hash state are excluded.
        Resource/termination guarantees remain those of the execution worker.
        """
        def signature(names, optional=(), defaults=None):
            if len(args) > len(names) or set(kwargs) - set(names) or set(names[:len(args)]) & kwargs.keys():
                self.fail('OP-02', 'Invalid pure call signature: ' + name, node, module)
            values = dict(defaults or {})
            values.update(zip(names, args))
            values.update(kwargs)
            if set(names) - set(optional) - values.keys():
                self.fail('OP-02', 'Missing pure call argument: ' + name, node, module)
            return values

        def typed(value, kinds):
            if not isinstance(value, Value) or value.type['kind'] not in kinds or value.type.get('nullable'):
                self.fail('OP-03', 'Invalid pure value argument: ' + name, node, module)

        def json_value(value, depth=0):
            if depth > 16:
                return False
            kind = value['kind']
            if kind in {'str', 'int', 'float', 'bool', 'none', 'never', 'json'}:
                return True
            if kind in {'list', 'tuple'}:
                return json_value(value['item'], depth + 1)
            if kind == 'union':
                return all(json_value(t, depth + 1) for t in value['options'])
            if kind == 'record':
                return all(json_value(f['type'], depth + 1) for f in value['fields'].values())
            return kind == 'dict' and value['key']['kind'] == 'str' and json_value(value['value'], depth + 1)

        if name == 'builtins.isinstance':
            if len(args) != 2 or kwargs or not isinstance(args[0], Value) or not isinstance(args[1], Symbol) or args[1].name not in {
                    'builtins.str', 'builtins.int', 'builtins.float', 'builtins.bool', 'builtins.bytes', 'builtins.list', 'builtins.dict'}:
                self.fail('OP-06', 'isinstance admits only explicit builtin value types', node, module, incomplete=True)
            return Value(scalar('bool'))
        if name in {'builtins.list', 'builtins.tuple', 'builtins.set', 'builtins.dict'}:
            if len(args) != 1 or kwargs or not isinstance(args[0], Value):
                self.fail('OP-02', 'Container copy needs one typed input', node, module)
            source, kind = args[0], name.removeprefix('builtins.')
            if kind == 'dict':
                if source.type['kind'] not in {'record', 'dict'}:
                    self.fail('OP-03', 'dict copy requires a mapping', node, module)
                return Value(source.type, source.origin, owns_container=True)
            if source.type['kind'] not in {'list', 'tuple', 'set', 'iterator'}:
                self.fail('OP-03', 'Sequence copy requires a known iterable', node, module)
            if kind == 'set' and (source.type['item']['kind'] not in {'str', 'int', 'float', 'bool', 'never'} or source.type['item'].get('nullable')):
                self.fail('OP-03', 'Set values must be hashable non-null scalars', node, module)
            return Value({**scalar(kind), 'item': source.type['item']}, source.origin, owns_container=True)
        if name == 'json.loads':
            value = signature(['s'])['s']
            typed(value, {'str'})
            return Value(scalar('json', nullable=True))
        if name == 'json.dumps':
            values = signature(['obj', 'skipkeys', 'ensure_ascii', 'check_circular', 'allow_nan', 'indent', 'separators', 'sort_keys'],
                               optional=['skipkeys', 'ensure_ascii', 'check_circular', 'allow_nan', 'indent', 'separators', 'sort_keys'])
            if len(args) != 1 or not isinstance(values['obj'], Value) or not json_value(values['obj'].type):
                self.fail('OP-03', 'json.dumps requires plain JSON-compatible values', node, module)
            for keyword in node.keywords:
                try:
                    option = ast.literal_eval(keyword.value)
                except (ValueError, TypeError):
                    self.fail('OP-06', 'JSON formatting options must be literal', node, module, incomplete=True)
                valid = (type(option) is bool if keyword.arg in {'skipkeys', 'ensure_ascii', 'check_circular', 'allow_nan', 'sort_keys'}
                         else option is None or type(option) is int and 0 <= option <= 8 if keyword.arg == 'indent'
                         else type(option) is tuple and len(option) == 2 and all(type(s) is str and len(s) <= 8 for s in option))
                if not valid or keyword.arg == 'skipkeys' and option or keyword.arg == 'check_circular' and not option:
                    self.fail('OP-03', 'Unsafe or excessive JSON formatting option', node, module)
            return Value(scalar('str'))
        if name == 'hashlib.sha256':
            value = signature(['string'])['string']
            typed(value, {'bytes'})
            return Value(scalar('sha256'))
        if name in {'math.isfinite', 'math.isnan', 'math.isinf', 'math.floor', 'math.ceil'}:
            value = signature(['x'])['x']
            typed(value, {'int', 'float'})
            return Value(scalar('bool' if name.startswith(('math.is',)) else 'int'))
        if name == 'html.unescape':
            value = signature(['s'])['s']
            typed(value, {'str'})
            return Value(scalar('str'))
        if name in {'re.sub', 're.split'}:
            names = ['pattern', 'repl', 'string', 'count', 'flags'] if name == 're.sub' else ['pattern', 'string', 'maxsplit', 'flags']
            values = signature(names, optional=names[-2:])
            for key in names[:-2]:
                typed(values[key], {'str'})
            raw = {**dict(zip(names, node.args)), **{k.arg: k.value for k in node.keywords}}
            for key in ['pattern', *(k for k in names[-2:] if k in values)]:
                item = raw[key]
                if not isinstance(item, ast.Constant) or (not (type(item.value) is str and len(item.value) <= 512) if key == 'pattern' else type(item.value) is not int or not 0 <= item.value <= 65536):
                    self.fail('OP-06', 'Regex pattern/options must be bounded literals', node, module, incomplete=True)
            return Value(scalar('str') if name == 're.sub' else {**scalar('list'), 'item': scalar('str')}, owns_container=name == 're.split')
        return None

    def _method(self, parent, name, args, kwargs, node, module):
        kind = parent.type["kind"]
        if any(not isinstance(a, Value) for a in [*args, *kwargs.values()]):
            self.fail(
                "OP-06",
                "Unknown method argument protocol",
                node,
                module,
                incomplete=True,
            )
        if kind == 'sha256' and name in {'hexdigest', 'digest'} and not args and not kwargs:
            return Value(scalar('str' if name == 'hexdigest' else 'bytes'))
        if kind == 'str' and name == 'encode' or kind == 'bytes' and name == 'decode':
            if len(args) > 1 or set(kwargs) - {'encoding'} or args and kwargs:
                self.fail('OP-02', 'Encoding admits one explicit codec and strict errors', node, module)
            codec = args[0] if args else kwargs.get('encoding')
            if codec is not None and codec.literal not in {'utf-8', 'ascii'}:
                self.fail('OP-06', 'Only fixed UTF-8/ASCII codecs are admitted', node, module, incomplete=True)
            return Value(scalar('bytes' if name == 'encode' else 'str'))
        if kind == 'dict' and name in {'get', 'keys', 'values'}:
            if name != 'get':
                if args or kwargs:
                    self.fail('OP-02', 'Mapping view takes no arguments', node, module)
                return Value({**scalar('iterator'), 'item': parent.type['key' if name == 'keys' else 'value']}, parent.origin)
            if not 1 <= len(args) <= 2 or kwargs or not compatible(args[0].type, parent.type['key']):
                self.fail('OP-03', 'Mapping get needs a declared key', node, module)
            value = parent.type['value']
            if value['kind'] == 'json':
                if len(args) == 2 and args[1].type['kind'] not in {'str', 'int', 'float', 'bool', 'none', 'list', 'dict', 'record'}:
                    self.fail('OP-03', 'JSON default must be a plain value', node, module)
                return Value(value, parent.origin)
            return Value(union_type(value, args[1].type if len(args) == 2 else scalar('none')), parent.origin)
        if kind == 'list' and name in {'append', 'extend'}:
            if parent.origin != 'local' and not parent.owns_container:
                self.fail('OP-07', 'Mutation of shared input/configuration is forbidden', node, module)
            if len(args) != 1 or kwargs:
                self.fail('OP-02', 'List mutation needs one value', node, module)
            value = args[0].type
            if name == 'extend':
                if value['kind'] not in {'list', 'tuple'}:
                    self.fail('OP-03', 'extend requires a declared sequence', node, module)
                value = value['item']
            if not compatible(value, parent.type['item']):
                self.fail('OP-03', 'List mutation conflicts with declared item', node, module)
            parent.origin = container_origin([parent, args[0]])
            return Value(scalar('none'))
        if kind == "str" and name in {
            "strip",
            "lstrip",
            "rstrip",
            "lower",
            "upper",
            "casefold",
            "replace",
            "join",
            "split",
            "rsplit",
            "startswith",
            "endswith",
            "isdigit",
            "isalpha",
        }:
            if name in {"split", "rsplit"}:
                return Value({**scalar("list"), "item": scalar("str")})
            return Value(
                scalar(
                    "bool"
                    if name in {"startswith", "endswith", "isdigit", "isalpha"}
                    else "str"
                )
            )
        if (
            kind == "record"
            and name == "get"
            and 1 <= len(node.args) <= 2
            and isinstance(node.args[0], ast.Constant)
        ):
            item = parent.type["fields"].get(node.args[0].value)
            if item is None:
                self.fail("OP-03", "get references undeclared key", node, module)
            default = args[1].type if len(args) == 2 else scalar("none")
            result = item["type"]
            if not item["required"]:
                if default["kind"] == "none":
                    result = dict(result, nullable=True)
                elif not compatible(default, result):
                    self.fail(
                        "OP-03", "get default conflicts with value type", node, module
                    )
            return Value(result, parent.origin)
        if name in {
            "append",
            "extend",
            "update",
            "pop",
            "clear",
            "sort",
            "remove",
            "setdefault",
        }:
            if parent.origin != "local":
                self.fail(
                    "OP-07",
                    "Mutation of shared input/configuration is forbidden",
                    node,
                    module,
                )
            self.fail(
                "OP-06",
                "Local mutation transfer is not covered; construct an explicit result",
                node,
                module,
                incomplete=True,
            )
        self.fail(
            "OP-06",
            f"Unsupported value method: {kind}.{name}",
            node,
            module,
            incomplete=True,
        )

    def _merge_values(self, left, right):
        if compatible(left.type, right.type):
            shape = right.type
        elif compatible(right.type, left.type):
            shape = left.type
        else:
            shape = union_type(left.type, right.type)
        return Value(shape, container_origin([left, right]),
                     owns_container=left.owns_container and right.owns_container)

    def _narrow(self, test, env, positive, module):
        if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
            self._narrow(test.operand, env, not positive, module)
        elif isinstance(test, ast.Name) and positive:
            before = env.get(test.id)
            if isinstance(before, Value) and before.type['kind'] != 'none':
                env[test.id] = Value(dict(before.type, nullable=False), before.origin,
                                     owns_container=before.owns_container)
        elif (isinstance(test, ast.Compare) and isinstance(test.left, ast.Name)
              and len(test.ops) == 1 and isinstance(test.ops[0], (ast.Is, ast.IsNot))
              and isinstance(test.comparators[0], ast.Constant) and test.comparators[0].value is None):
            before = env.get(test.left.id)
            if isinstance(before, Value):
                nonnull = positive == isinstance(test.ops[0], ast.IsNot)
                shape = dict(before.type, nullable=False) if nonnull else scalar('none')
                env[test.left.id] = Value(shape, before.origin, owns_container=before.owns_container)
        elif isinstance(test, ast.BoolOp) and ((isinstance(test.op, ast.And) and positive) or (isinstance(test.op, ast.Or) and not positive)):
            for item in test.values:
                self._narrow(item, env, positive, module)
        elif positive and isinstance(test, ast.Call) and isinstance(test.func, ast.Name) and self.lookup(test.func.id, module, env) == Symbol('builtins.isinstance'):
            if len(test.args) != 2 or not isinstance(test.args[0], ast.Name):
                return
            name = test.args[0].id
            before = env.get(name)
            target = self.symbol(test.args[1], module)
            if not isinstance(before, Value) or not isinstance(target, Symbol):
                return
            kind = target.name.removeprefix('builtins.')
            shape = dict(before.type, nullable=False)
            if before.type['kind'] == 'json':
                shape = ({**scalar('dict'), 'key': scalar('str'), 'value': scalar('json', nullable=True)} if kind == 'dict'
                         else {**scalar('list'), 'item': scalar('json', nullable=True)} if kind == 'list' else scalar(kind))
            elif before.type['kind'] == 'union':
                choices = [v for v in before.type['options'] if v['kind'] == kind or kind == 'dict' and v['kind'] == 'record']
                if choices:
                    shape = union_type(*choices)
            env[name] = Value(shape, before.origin, owns_container=before.owns_container)

    def _statements(self, body, env, module, returns, state, methods):
        terminated = False
        for node in body:
            self.tree.guard()
            if terminated:
                break

            def expr(n):
                return self._expr(n, env, module, state, methods)

            if isinstance(node, ast.Return):
                value = expr(node.value) if node.value else Value(scalar("none"))
                if node.value is None and returns["kind"] == "iterator" and self.generators[-1]:
                    terminated = True
                    continue
                if not isinstance(value, Value) or not compatible(value.type, returns):
                    self.fail(
                        "OP-03",
                        "Return expression conflicts with declared port",
                        node,
                        module,
                    )
                alternatives = returns["options"] if returns["kind"] == "union" else [returns]
                records = [r for r in alternatives if r["kind"] == "record" and compatible(value.type, r)]
                if value.type["kind"] == "record" and records and all(
                    set(value.type["fields"]) - set(r["fields"]) for r in records
                ):
                    self.fail(
                        "OP-03", "Return contains undeclared fields", node, module
                    )
                terminated = True
            elif isinstance(node, ast.Raise):
                if node.exc:
                    expr(node.exc)
                if node.cause:
                    expr(node.cause)
                terminated = True
            elif isinstance(node, ast.Expr):
                if isinstance(node.value, (ast.Yield, ast.YieldFrom)):
                    yielded = expr(node.value.value)
                    actual = yielded.type if isinstance(yielded, Value) else scalar("unknown")
                    if isinstance(node.value, ast.YieldFrom):
                        actual = actual.get("item", scalar("unknown"))
                    if returns["kind"] != "iterator" or not compatible(actual, returns["item"]):
                        self.fail("OP-03", "Yield conflicts with the declared iterator item", node, module)
                else:
                    expr(node.value)
            elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                value = expr(node.value) if node.value is not None else None
                if isinstance(node, ast.AnnAssign):
                    declared = self.annotation(node.annotation, module)
                    if value is None or not isinstance(value, Value) or not compatible(value.type, declared):
                        self.fail('OP-03', 'Local annotation must match its initialized value', node, module)
                    value = Value(declared, value.origin, value.literal, value.owns_container)
                if isinstance(node, ast.AugAssign):
                    before = expr(node.target)
                    if not isinstance(before, Value) or before.type["kind"] not in {
                        "int",
                        "float",
                        "str",
                    }:
                        self.fail(
                            "OP-07",
                            "In-place mutation of mutable or unknown values is forbidden",
                            node,
                            module,
                        )
                    if not isinstance(value, Value) or not compatible(
                        value.type, before.type
                    ):
                        self.fail(
                            "OP-03",
                            "Augmented assignment changes the known type",
                            node,
                            module,
                            incomplete=True,
                        )
                for target in (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                ):
                    if isinstance(target, ast.Name):
                        env[target.id] = value
                    elif (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == "self"
                    ):
                        before = state.get(target.attr)
                        if before is not None and (
                            not isinstance(value, Value)
                            or not compatible(value.type, before.type)
                        ):
                            self.fail(
                                "OP-07",
                                "Actor state changes its declared configuration type",
                                node,
                                module,
                                incomplete=True,
                            )
                        state[target.attr] = value
                    else:
                        parent = (
                            expr(target.value)
                            if isinstance(target, (ast.Subscript, ast.Attribute))
                            else None
                        )
                        if not isinstance(parent, Value) or parent.origin != "local":
                            self.fail(
                                "OP-07",
                                "Mutation of shared/unknown values is forbidden",
                                node,
                                module,
                            )
                        self.fail(
                            "OP-06",
                            "Indexed assignment requires an explicit supported value protocol",
                            node,
                            module,
                            incomplete=True,
                        )
            elif isinstance(node, ast.If):
                expr(node.test)
                left, right = dict(env), dict(env)
                self._narrow(node.test, left, True, module)
                self._narrow(node.test, right, False, module)
                lt = self._statements(
                    node.body, left, module, returns, dict(state), methods
                )
                rt = self._statements(
                    node.orelse, right, module, returns, dict(state), methods
                )
                if lt and not rt:
                    env.update(right)
                elif rt and not lt:
                    env.update(left)
                else:
                    for key in left.keys() & right.keys():
                        a, b = left[key], right[key]
                        if isinstance(a, Value) and isinstance(b, Value):
                            env[key] = self._merge_values(a, b)
                        elif a == b:
                            env[key] = a
                        elif key in env:
                            env.pop(key)
                terminated = lt and rt
            elif isinstance(node, ast.For):
                source = expr(node.iter)
                if (
                    not isinstance(node.target, ast.Name)
                    or not isinstance(source, Value)
                    or source.type["kind"] not in {"list", "tuple", "str", "iterator"}
                ):
                    self.fail(
                        "OP-06",
                        "Unsupported loop source",
                        node,
                        module,
                        incomplete=True,
                    )
                local = dict(env)
                local[node.target.id] = Value(
                    source.type.get("item", scalar("str")), source.origin
                )
                self._statements(node.body, local, module, returns, state, methods)
                self._statements(
                    node.orelse, dict(env), module, returns, state, methods
                )
                for item in ast.walk(node):
                    if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store):
                        before, after = env.get(item.id), local.get(item.id)
                        if (
                            isinstance(before, Value)
                            and isinstance(after, Value)
                            and compatible(before.type, after.type)
                            and compatible(after.type, before.type)
                        ):
                            env[item.id] = Value(
                                before.type, container_origin([before, after]), owns_container=before.owns_container and after.owns_container
                            )
                        elif before != after:
                            env.pop(item.id, None)
            elif isinstance(node, ast.Try):
                for n in node.finalbody:
                    if any(
                        isinstance(x, (ast.Return, ast.Break, ast.Continue))
                        for x in ast.walk(n)
                    ):
                        self.fail(
                            "OP-08",
                            "finally must not suppress an active failure",
                            n,
                            module,
                        )
                bt = self._statements(
                    node.body, dict(env), module, returns, state, methods
                )
                handlers = []
                for handler in node.handlers:
                    broad = handler.type is None
                    if handler.type:
                        types = (
                            list(handler.type.elts)
                            if isinstance(handler.type, ast.Tuple)
                            else [handler.type]
                        )
                        for item in types:
                            resolved = self.symbol(item, module)
                            if not isinstance(
                                resolved, Symbol
                            ) or not resolved.name.startswith("builtins."):
                                self.fail(
                                    "OP-06",
                                    "Exception type source is unknown",
                                    item,
                                    module,
                                    incomplete=True,
                                )
                            broad |= resolved.name in {
                                "builtins.Exception",
                                "builtins.BaseException",
                            }
                    if broad and (
                        not handler.body
                        or not isinstance(handler.body[-1], ast.Raise)
                        or any(
                            isinstance(x, (ast.Return, ast.Break, ast.Continue))
                            for item in handler.body
                            for x in ast.walk(item)
                        )
                    ):
                        self.fail(
                            "OP-08",
                            "Broad exception suppression has no explicit failure channel",
                            handler,
                            module,
                        )
                    local = dict(env)
                    if handler.name:
                        local[handler.name] = Value(scalar("exception"))
                    handlers.append(
                        self._statements(
                            handler.body, local, module, returns, state, methods
                        )
                    )
                self._statements(
                    node.orelse, dict(env), module, returns, state, methods
                )
                ft = self._statements(
                    node.finalbody, env, module, returns, state, methods
                )
                terminated = ft or (bt and bool(handlers) and all(handlers))
                for item in ast.walk(node):
                    if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store):
                        env.pop(item.id, None)
            elif isinstance(node, (ast.Pass, ast.Break, ast.Continue)):
                pass
            else:
                self.fail(
                    "OP-06",
                    f"Unsupported statement: {type(node).__name__}",
                    node,
                    module,
                    incomplete=True,
                )
        return terminated
