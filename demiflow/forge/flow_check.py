"""Interpret the restricted wiring language into a diagnostic report, not a plan."""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json

from ..data.callable_contract import bind_parameters, validate_field_binding
from ..data.flow_contract import (
    compatible,
    scalar,
    transfer_fields,
    require_fields,
    union_type,
    field,
    literal_type,
)
from .operator_check import OperatorChecker
from .platform import bounded_json, content_digest, native_handlers
from .schema import ForgeError
from .source import (
    Definition,
    InspectionError,
    SourceTree,
    Symbol,
    defaults,
    parameters,
)


@dataclass
class DatasetPort:
    node: str
    fields: dict
    stream: bool = False


@dataclass
class API:
    record: dict
    receiver: DatasetPort | DeclaredValue | None = None


@dataclass
class Action:
    node: str


@dataclass
class RuntimeValue:
    expression: dict
    type: dict


@dataclass
class DeclaredValue:
    value: dict
    type: dict


@dataclass
class Construction:
    definition: Definition
    args: tuple
    kwargs: dict
    state: dict
    identity: str


class FlowChecker:
    def __init__(self, root, catalog, tables, targets, *, limits=None, resource_resolver=None, exclude=()):
        self.tree = SourceTree(root, limits, exclude=exclude)
        self.operators = OperatorChecker(self.tree)
        self.catalog, self.tables, self.targets = catalog, tables, targets
        self.symbols, self.methods = {}, {}
        for record in catalog["objects"]:
            self.symbols[record["id"].replace(":", ".")] = record
            self.symbols[record["origin"]["module"] + "." + record["defined_name"]] = (
                record
            )
            if (record.get("member_owner") or "").endswith(".Dataset"):
                self.methods[record["id"].rsplit(".", 1)[-1]] = record
        self.nodes, self.edges, self.actions, self.consumed = [], [], [], set()
        self.node_index, self.parent_index = {}, {}
        self.capability_cache = {}
        self.stack, self.writes = [], set()
        self.parameter_scopes = []
        self.constructions = 0
        self.resource_resolver = resource_resolver
        self.resource_bindings = {}
        self.resource_count = 0
        self.value_edges = []
        self.checkpoint_targets = set()
        self.used_checkpoints = set()

    def fail(self, rule, detail, node, module, *, incomplete=False, cause=None):
        self.tree.fail(rule, detail, node, module, incomplete=incomplete,cause=cause)

    def inspect(self, entry, arguments):
        try:
            if (
                self.catalog.get("format") != "demiflow_forge_platform_catalog_v3"
                or not self.catalog["inspection_complete"]
            ):
                self.fail(
                    "FL-02",
                    "Complete v3 native catalogue is required; historical catalogues are read-only",
                    None,
                    "",
                    incomplete=True,
                )
            path, separator, function = entry.partition(":")
            if (
                not separator
                or not path.endswith(".py")
                or not function.isidentifier()
                or function.startswith('_')
                or any(not p.isidentifier() for p in path[:-3].split('/'))
            ):
                self.fail("FL-01", "Entry must be a project-relative Python file and public function", None, "")
            module = path[:-3].replace("/", ".")
            definition = self.tree.resolve(self.tree.load(module).get(function))
            if not isinstance(definition, Definition) or not isinstance(
                definition.node, ast.FunctionDef
            ):
                self.fail(
                    "FL-01",
                    "Entry must be a top-level synchronous flow function",
                    None,
                    module,
                )
            self._helper(definition, [], arguments)
            if not self.actions:
                self.fail(
                    "FL-04",
                    "Complete flow must declare a native terminal action",
                    definition.node,
                    module,
                )
            dangling = [
                n["id"]
                for n in self.nodes
                if n["result"] == "dataset"
                and n["id"] not in self.consumed
                and n["id"] not in self.actions
            ]
            if dangling:
                self.fail(
                    "FL-04",
                    f"Unconsumed Dataset branch(es): {dangling}",
                    definition.node,
                    module,
                )
            self.tree.unchanged()
            diagnostics = []
        except InspectionError as exc:
            diagnostics = [exc.diagnostic]
        except (
            ValueError,
            TypeError,
            KeyError,
            SyntaxError,
            OSError,
            RecursionError,
        ) as exc:
            diagnostics = [
                {
                    "rule": "FL-01",
                    "status": "incomplete",
                    "category": "unknown",
                    "detail": str(exc),
                    "path": "",
                    "line": None,
                    "column": None,
                }
            ]
        status = diagnostics[0]["status"] if diagnostics else "checked"
        binding = {
            "entry": entry,
            "arguments": arguments,
            "sources": dict(sorted(self.tree.files.items())),
            "catalog": content_digest(self.catalog),
            "tables": self.tables,
            "targets": self.targets,
            "resources": self.resource_bindings,
        }
        report = {
            "format": "demiflow_forge_flow_inspection_v3",
            "status": status,
            "entry": entry,
            "binding": binding,
            "diagnostics": diagnostics,
            "nodes": self.nodes,
            "edges": self.edges,
            "value_edges": self.value_edges,
            "action_edges": [{"from": a, "to": b} for a, b in zip(self.actions, self.actions[1:])],
            "operators": self.operators.records,
            "actions": self.actions,
            "coverage": {
                "wiring": status,
                "operator_shape": status,
                "operator_policy": status,
                "data_schema": status,
                "operator_behavior": "not_run",
                "execution": "not_run",
                "resource_runtime": "not_run",
                "permissions_runtime": "not_run",
            },
            "execution_context": "native local default; not executed",
        }
        report["digest"] = content_digest(report)
        bounded_json(report, 8 * 1024**2)
        return report

    def _resolve(self, value):
        value = self.tree.resolve(value)
        if isinstance(value, Symbol) and value.name in self.symbols:
            return API(self.symbols[value.name])
        return value

    def _parameter_origin(self, node):
        """A sidecar proof of direct configuration references; never compare values."""
        if isinstance(node, ast.Name) and self.parameter_scopes:
            return self.parameter_scopes[-1].get(node.id)
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            origin = self._parameter_origin(node.value)
            key = node.slice.value
            return self._origin_index(origin,key)
        if isinstance(node, ast.Dict) and all(isinstance(k,ast.Constant) and type(k.value) is str for k in node.keys):
            return {k.value:self._parameter_origin(v) for k,v in zip(node.keys,node.values)}
        if isinstance(node, (ast.List, ast.Tuple)):
            return [self._parameter_origin(v) for v in node.elts]
        return None

    @staticmethod
    def _is_origin(value):
        return (type(value) is dict and set(value)=={'argument','path'}
                and type(value['argument']) is str and type(value['path']) is list)

    def _origin_index(self,origin,key):
        if self._is_origin(origin) and type(key) in {str,int} and len(origin['path'])<16:
            return {**origin,'path':origin['path']+[key]}
        if type(origin) is dict and type(key) is str:return origin.get(key)
        if type(origin) is list and type(key) is int and -len(origin)<=key<len(origin):return origin[key]
        return None

    def _origin_ports(self,origins):
        result={}
        def visit(path,value,depth=0):
            if depth>16 or len(result)>=256:return
            if self._is_origin(value):result[path]=value
            elif type(value) is dict:
                for key,item in value.items():visit(path+'/'+str(key).replace('~','~0').replace('/','~1'),item,depth+1)
            elif type(value) in {tuple,list}:
                for index,item in enumerate(value):visit(path+'/'+str(index),item,depth+1)
        for key,value in origins.items():visit(key,value)
        return result

    def _helper(self, definition, args, kwargs, origins=None):
        node, module = definition.node, definition.module
        if not isinstance(node, ast.FunctionDef):
            self.fail(
                "FL-02",
                "Flow helpers must be synchronous local definitions",
                node,
                module,
            )
        identity = f"{module}:{node.name}"
        if identity in self.stack or len(self.stack) >= self.tree.limits["depth"]:
            self.fail(
                "FL-10", "Recursive/deep flow helper", node, module, incomplete=True
            )
        self.tree.calls += 1
        if self.tree.calls > self.tree.limits["calls"]:
            self.fail(
                "FL-10", "Flow call budget exceeded", node, module, incomplete=True
            )
        ps = parameters(node)
        if any(
            p["kind"] in {"VAR_POSITIONAL", "VAR_KEYWORD", "POSITIONAL_ONLY"}
            for p in ps
        ):
            self.fail("FL-01", "Flow parameters must be keyword-bindable", node, module)
        try:
            env = bind_parameters(
                ps, args, kwargs, defaults=defaults(node, self.tree, module)
            )
        except TypeError as exc:
            self.fail("FL-03", str(exc), node, module)
        if origins is None:
            origins = ([], {key: {'argument': key, 'path': []} for key in kwargs})
        origin_env = bind_parameters(ps, *origins)
        self.parameter_scopes.append(origin_env)
        self.stack.append(identity)
        try:
            _, value = self._body(node.body, env, module)
            return value
        finally:
            self.stack.pop()
            self.parameter_scopes.pop()

    def _body(self, body, env, module):
        for node in body:
            self.tree.guard()
            if isinstance(node, ast.Return):
                return True, self._expr(node.value, env, module) if node.value else None
            if isinstance(node, ast.Expr):
                self._expr(node.value, env, module)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                value = self._expr(node.value, env, module)
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                for target in targets:
                    if not isinstance(target, ast.Name):
                        self.fail("FL-07", "Flow mutation is not wiring", node, module)
                    if target.id in self.tree.load(module):
                        self.fail(
                            "FL-02",
                            "Flow variable shadows an imported/defined symbol",
                            node,
                            module,
                        )
                    env[target.id] = value
                    self.parameter_scopes[-1][target.id] = self._parameter_origin(node.value)
            elif isinstance(node, ast.If):
                test = self._expr(node.test, env, module)
                if isinstance(test, (RuntimeValue, DatasetPort, Action)):
                    self.fail(
                        "FL-07",
                        "Topology condition must be determined by bound configuration",
                        node,
                        module,
                    )
                if type(test) is not bool:
                    if type(test) in (dict, list, tuple, str, int, float, type(None)):
                        test = bool(test)
                    else:
                        self.fail(
                            "FL-07",
                            "Topology condition must be determined by bound configuration",
                            node,
                            module,
                        )
                returned, value = self._body(
                    node.body if test else node.orelse, env, module
                )
                if returned:
                    return True, value
            elif isinstance(node, ast.For):
                values = self._expr(node.iter, env, module)
                if (
                    not isinstance(values, (list, tuple))
                    or len(values) > self.tree.limits["expansion"]
                    or not isinstance(node.target, ast.Name)
                ):
                    self.fail(
                        "FL-10",
                        "Flow loop requires a bounded configured sequence",
                        node,
                        module,
                        incomplete=True,
                    )
                iterable_origin = self._parameter_origin(node.iter)
                for index, value in enumerate(values):
                    env[node.target.id] = value
                    origin = iterable_origin
                    self.parameter_scopes[-1][node.target.id] = self._origin_index(origin,index)
                    self.stack.append(f"loop:{node.lineno}:{index}")
                    try:
                        returned, result = self._body(node.body, env, module)
                    finally:
                        self.stack.pop()
                    if returned:
                        return True, result
                returned, result = self._body(node.orelse, env, module)
                if returned:
                    return True, result
            elif isinstance(node, ast.Pass):
                pass
            else:
                self.fail(
                    "FL-07",
                    f"Unsupported wiring statement: {type(node).__name__}",
                    node,
                    module,
                    incomplete=True,
                )
        return False, None

    def _expr(self, node, env, module):
        self.tree.guard()

        def visit(n):
            return self._expr(n, env, module)

        if isinstance(node, ast.Constant):
            return self.tree.literal(node, module)
        if isinstance(node, ast.Name):
            if node.id in env:
                return env[node.id]
            namespace = self.tree.load(module)
            if node.id in namespace:
                return self._resolve(namespace[node.id])
            if node.id in {
                "ValueError", "TypeError", "RuntimeError", "Exception",
                "type", "isinstance", "len", "str", "int", "float", "bool",
                "min", "max", "list", "tuple", "dict", "set", "sorted",
                "any", "all", "sum", "range", "enumerate", "zip",
            }:
                return Symbol("builtins." + node.id)
            self.fail("FL-01", f"Unbound name: {node.id}", node, module)
        if isinstance(node, ast.Attribute):
            parent = visit(node.value)
            if isinstance(parent, Action) and self.node_index[parent.node]['kind'] == 'write_table':
                writer = self.node_index[parent.node]
                if node.attr == 'uri':
                    return writer['parameters']['uri']
                if node.attr in {'committed_version', 'written_rows', 'input_rows', 'status'}:
                    return RuntimeValue({'runtime_value': {'node': parent.node, 'field': node.attr}},
                                        scalar('str' if node.attr == 'status' else 'int'))
            if isinstance(parent, Symbol):
                return self._resolve(Symbol(parent.name + "." + node.attr))
            if isinstance(parent, DeclaredValue):
                owner = {"fit": "FitRef", "model_selection": "ModelSelection"}.get(parent.type["kind"])
                if owner == "FitRef" and node.attr == "checkpoints":
                    return parent
                if (owner, node.attr) in {("FitRef", "select"), ("ModelSelection", "predict")}:
                    member = self.symbols.get("demiflow.data.training." + owner + "." + node.attr)
                    if member:
                        return API(member, parent)
            if isinstance(parent, DatasetPort) and node.attr in self.methods:
                return API(self.methods[node.attr], parent)
            if type(parent) is dict and node.attr in {'get', 'keys', 'values', 'items'}:
                return ('__bound_pure_method__', getattr(parent, node.attr))
            if type(parent) is str and node.attr in {'strip', 'lstrip', 'rstrip', 'startswith', 'endswith', 'lower', 'upper', 'replace', 'split', 'join'}:
                return ('__bound_pure_method__', getattr(parent, node.attr))
            self.fail(
                "FL-02",
                "Attribute does not resolve to a public platform API",
                node,
                module,
                incomplete=True,
            )
        if isinstance(node, ast.Subscript):
            parent, key = visit(node.value), visit(node.slice)
            if type(parent) not in (dict, list, tuple) or type(key) not in (str, int):
                self.fail(
                    "FL-07",
                    "Only bound configuration values may be indexed",
                    node,
                    module,
                )
            return parent[key]
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod)):
            left, right = visit(node.left), visit(node.right)
            if isinstance(left, RuntimeValue) or isinstance(right, RuntimeValue):
                if not isinstance(node.op, (ast.Add, ast.Sub)):
                    self.fail('FL-07', 'Unsupported native value arithmetic', node, module)
                types = [self._value_type(value) for value in (left, right)]
                if any(value['kind'] not in {'int', 'float'} or value.get('nullable') for value in types) or types[0]['kind'] != types[1]['kind']:
                    self.fail('FL-06', 'Native value arithmetic requires matching non-null numeric types', node, module)
                return RuntimeValue({'value_expression': 'add' if isinstance(node.op, ast.Add) else 'sub',
                                     'arguments': self._json_value([left, right])}, types[0])
            if isinstance(node.op, ast.Div) and isinstance(left, Path) and isinstance(right, (str, Path)):
                return left / right
            if (isinstance(left, Symbol) and left.opaque) or (isinstance(right, Symbol) and right.opaque):
                self.fail('FL-07', 'Opaque module constant used in wiring expression', node, module, incomplete=True)
            if isinstance(node.op, (ast.Add, ast.Sub)):
                if type(left) is not type(right) or type(left) not in (int, float, str) or type(left) is str and isinstance(node.op, ast.Sub):
                    self.fail('FL-07', 'Unsupported configured arithmetic', node, module)
                result = left + right if isinstance(node.op, ast.Add) else left - right
            else:
                if type(left) not in (int, float) or type(right) not in (int, float):
                    self.fail('FL-07', 'Unsupported configured arithmetic', node, module)
                if isinstance(node.op, (ast.Div, ast.FloorDiv, ast.Mod)) and right == 0:
                    self.fail('FL-07', 'Division by zero in configured arithmetic', node, module)
                result = (left * right if isinstance(node.op, ast.Mult) else
                          left / right if isinstance(node.op, ast.Div) else
                          left // right if isinstance(node.op, ast.FloorDiv) else
                          left % right)
            bounded_json(result, 1024 * 1024)
            return result
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            is_and = isinstance(node.op, ast.And)
            last = None
            for operand in node.values:
                val = visit(operand)
                if isinstance(val, (RuntimeValue, DatasetPort, Action)):
                    self.fail('FL-07', 'Runtime value used in topology', node, module)
                last = val
                if is_and and not val:
                    return val
                if not is_and and val:
                    return val
            return last
        if isinstance(node, (ast.List, ast.Tuple)):
            if len(node.elts) > 1024:
                self.fail(
                    "FL-10", "Sequence exceeds budget", node, module, incomplete=True
                )
            items = [visit(n) for n in node.elts]
            return items if isinstance(node, ast.List) else tuple(items)
        if isinstance(node, ast.Dict):
            if any(k is None for k in node.keys):
                self.fail(
                    "FL-03",
                    "Dynamic mapping expansion is unsupported",
                    node,
                    module,
                    incomplete=True,
                )
            keys = [visit(k) for k in node.keys]
            if any(type(k) is not str for k in keys) or len(set(keys)) != len(keys):
                self.fail("FL-03", "Mapping keys must be unique strings", node, module)
            return dict(zip(keys, [visit(v) for v in node.values]))
        if isinstance(node, ast.UnaryOp) and isinstance(
            node.op, (ast.Not, ast.USub, ast.UAdd)
        ):
            value = visit(node.operand)
            if isinstance(value, RuntimeValue) and isinstance(node.op, ast.Not) and value.type == scalar('bool'):
                return RuntimeValue({'value_expression': 'not', 'arguments': [value.expression]}, scalar('bool'))
            if isinstance(node.op, ast.Not) and type(value) in (bool, int, float, str, list, tuple, dict, set, type(None)):
                return not value
            if type(value) not in (bool, int, float):
                self.fail("FL-07", "Runtime value used in topology", node, module)
            return (
                not value
                if isinstance(node.op, ast.Not)
                else -value if isinstance(node.op, ast.USub) else value
            )
        if isinstance(node, ast.Compare):
            values = [visit(node.left), *[visit(n) for n in node.comparators]]
            if (len(node.ops) == 1 and isinstance(node.ops[0], (ast.Is, ast.IsNot))
                    and any(v is None for v in values)
                    and all(type(v) in (dict, list, tuple, type(None)) for v in values)):
                # Presence of a bounded optional configuration is static. Native
                # query/port values (including nested ones) are not declarations.
                bounded_json(values, 1024 * 1024)
                same = values[0] is values[1]
                return same if isinstance(node.ops[0], ast.Is) else not same
            if any(isinstance(v, RuntimeValue) for v in values):
                operators = {ast.Eq: 'eq', ast.NotEq: 'ne', ast.Lt: 'lt', ast.LtE: 'le', ast.Gt: 'gt', ast.GtE: 'ge'}
                if len(node.ops) != 1 or type(node.ops[0]) not in operators:
                    self.fail('FL-07', 'Native value comparisons require one supported comparison', node, module)
                types = [v.type if isinstance(v, RuntimeValue) else literal_type(v) for v in values]
                if any(t['kind'] not in {'bool', 'int', 'float', 'str'} or t.get('nullable') for t in types) or types[0]['kind'] != types[1]['kind']:
                    self.fail('FL-06', 'Native comparison requires matching non-null scalar types', node, module)
                return RuntimeValue({'value_expression': operators[type(node.ops[0])], 'arguments': self._json_value(values)}, scalar('bool'))
            if any(isinstance(v, (RuntimeValue, DatasetPort, Action)) for v in values):
                self.fail("FL-07", "Runtime value used in topology", node, module)
            type_map = {'builtins.int': int, 'builtins.str': str, 'builtins.float': float,
                        'builtins.bool': bool, 'builtins.list': list, 'builtins.dict': dict,
                        'builtins.tuple': tuple, 'builtins.set': set}
            values = [type_map.get(v.name, v) if isinstance(v, Symbol) else v for v in values]
            result = True
            for a, op, b in zip(values, node.ops, values[1:]):
                if isinstance(op, (ast.In, ast.NotIn)):
                    if not isinstance(b, (tuple, list, set, dict, str)):
                        self.fail("FL-07", "Unsupported configured membership test", node, module)
                    present = a in b
                    result &= present if isinstance(op, ast.In) else not present
                    continue
                if isinstance(op, (ast.Is, ast.IsNot)):
                    same = a is b
                    result &= same if isinstance(op, ast.Is) else not same
                    continue
                if type(a) not in (str, int, float, bool, type(None), type) or type(b) not in (str, int, float, bool, type(None), type):
                    self.fail("FL-07", "Runtime value used in topology", node, module)
                if isinstance(op, ast.Eq):
                    result &= a == b
                elif isinstance(op, ast.NotEq):
                    result &= a != b
                elif isinstance(op, ast.Lt):
                    result &= a < b
                elif isinstance(op, ast.LtE):
                    result &= a <= b
                elif isinstance(op, ast.Gt):
                    result &= a > b
                elif isinstance(op, ast.GtE):
                    result &= a >= b
                else:
                    self.fail(
                        "FL-07",
                        "Unsupported configured comparison",
                        node,
                        module,
                        incomplete=True,
                    )
            return result
        if isinstance(node, ast.Call):
            target = visit(node.func)
            if any(k.arg is None for k in node.keywords) or any(
                isinstance(a, ast.Starred) for a in node.args
            ):
                self.fail(
                    "FL-03",
                    "Dynamic argument expansion is unsupported",
                    node,
                    module,
                    incomplete=True,
                )
            if len({k.arg for k in node.keywords}) != len(node.keywords):
                self.fail("FL-03", "Repeated keyword argument", node, module)
            args = [visit(a) for a in node.args]
            kwargs = {k.arg: visit(k.value) for k in node.keywords}
            if (isinstance(target, tuple) and len(target) == 2 and target[0] == '__bound_pure_method__'
                    and not any(isinstance(a, (RuntimeValue, DatasetPort, Action)) for a in (*args, *kwargs.values()))):
                try:
                    val = target[1](*args, **kwargs)
                    return list(val) if isinstance(val, (type({}.keys()), type({}.values()), type({}.items()))) else val
                except (TypeError, ValueError, KeyError) as exc:
                    self.fail("FL-07", str(exc), node, module)
            if isinstance(target, Symbol):
                def _bounded_range(*rargs):
                    r = range(*rargs)
                    if len(r) > 4096:
                        raise ValueError("Static range exceeds 4096 elements")
                    return list(r)
                pure_builtins = {
                    'builtins.type': type,
                    'builtins.isinstance': isinstance,
                    'builtins.len': len,
                    'builtins.str': str,
                    'builtins.int': int,
                    'builtins.float': float,
                    'builtins.bool': bool,
                    'builtins.min': min,
                    'builtins.max': max,
                    'builtins.list': list,
                    'builtins.tuple': tuple,
                    'builtins.dict': dict,
                    'builtins.set': set,
                    'builtins.sorted': sorted,
                    'builtins.any': any,
                    'builtins.all': all,
                    'builtins.sum': sum,
                    'builtins.range': _bounded_range,
                    'builtins.enumerate': lambda it, start=0: list(enumerate(it, start)),
                    'builtins.zip': lambda *its: list(zip(*its)),
                    'pathlib.Path': Path,
                    'pathlib.PurePosixPath': Path,
                }
                fn = pure_builtins.get(target.name)
                if fn is not None and not any(isinstance(a, (RuntimeValue, DatasetPort, Action)) for a in (*args, *kwargs.values())):
                    try:
                        # Map Symbol('builtins.int') etc. when passed as type argument to isinstance/type comparisons.
                        type_map = {'builtins.int': int, 'builtins.str': str, 'builtins.float': float,
                                    'builtins.bool': bool, 'builtins.list': list, 'builtins.dict': dict,
                                    'builtins.tuple': tuple, 'builtins.set': set}
                        norm_args = [type_map.get(a.name, a) if isinstance(a, Symbol) else a for a in args]
                        return fn(*norm_args, **kwargs)
                    except (TypeError, ValueError) as exc:
                        self.fail("FL-07", str(exc), node, module)
            if isinstance(target, API):
                return self._native(target, args, kwargs, node, module)
            if isinstance(target, Definition):
                if isinstance(target.node, ast.ClassDef):
                    try:
                        # Constructors only receive bounded literal configuration.
                        bounded_json([args, kwargs], 1024 * 1024)
                    except (ForgeError, TypeError, ValueError):
                        self.fail("OP-06", "Actor construction requires literal configuration", node, module)
                    description = self.operators.describe(target)
                    state = self.operators.check(description, constructor_args=args, constructor_kwargs=kwargs)
                    if state is None:
                        self.fail("OP-07", "Constructed actor state is not statically recoverable; use the native callable-class constructor arguments", node, module, incomplete=True)
                    self.constructions += 1
                    if self.constructions > self.tree.limits["calls"]:
                        self.fail("FL-10", "Actor construction budget exceeded", node, module, incomplete=True)
                    identity = content_digest([module, node.lineno, node.col_offset, self.stack, self.constructions])[:16]
                    return Construction(target, tuple(args), kwargs, state, identity)
                return self._helper(target, args, kwargs, origins=(
                    [self._parameter_origin(a) for a in node.args],
                    {k.arg: self._parameter_origin(k.value) for k in node.keywords},
                ))
            self.fail(
                "FL-02",
                "Wiring calls must resolve to native APIs or checked flow helpers",
                node,
                module,
                incomplete=True,
            )
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            return self._static_comprehension(node, env, module)
        if isinstance(node, ast.IfExp):
            cond = visit(node.test)
            if isinstance(cond, (RuntimeValue, DatasetPort, Action)):
                self.fail("FL-07", "Runtime value used in topology", node.test, module)
            return visit(node.body) if bool(cond) else visit(node.orelse)
        self.fail(
            "FL-07",
            f"Unsupported wiring expression: {type(node).__name__}",
            node,
            module,
            incomplete=True,
        )

    def _static_comprehension(self, node, env, module):
        def _bind_target(target, value, scope):
            if isinstance(target, ast.Name):
                scope[target.id] = value
                return
            if isinstance(target, (ast.Tuple, ast.List)):
                if not isinstance(value, (tuple, list)) or len(value) != len(target.elts):
                    self.fail("FL-07", "Comprehension target unpack mismatch", target, module)
                for elt, item in zip(target.elts, value):
                    _bind_target(elt, item, scope)
                return
            self.fail("FL-07", "Unsupported comprehension target", target, module, incomplete=True)

        results = {} if isinstance(node, ast.DictComp) else []
        steps = 0

        def _walk(gen_index, scope):
            nonlocal steps
            if gen_index == len(node.generators):
                if isinstance(node, ast.DictComp):
                    k = self._expr(node.key, scope, module)
                    v = self._expr(node.value, scope, module)
                    if isinstance(k, (RuntimeValue, DatasetPort, Action)) or isinstance(v, (RuntimeValue, DatasetPort, Action)):
                        self.fail("FL-07", "Runtime value used in static comprehension", node, module)
                    results[k] = v
                else:
                    val = self._expr(node.elt, scope, module)
                    if isinstance(val, (RuntimeValue, DatasetPort, Action)):
                        self.fail("FL-07", "Runtime value used in static comprehension", node, module)
                    results.append(val)
                return
            gen = node.generators[gen_index]
            if gen.is_async:
                self.fail("FL-07", "Async comprehension unsupported in wiring", gen, module, incomplete=True)
            iterable = self._expr(gen.iter, scope, module)
            if isinstance(iterable, (RuntimeValue, DatasetPort, Action)) or not isinstance(iterable, (list, tuple, set, dict, str)):
                self.fail("FL-07", "Comprehension requires a static configured iterable", gen.iter, module)
            for item in iterable:
                steps += 1
                if steps > 4096:
                    self.fail("FL-10", "Static comprehension exceeded 4096 iterations", node, module, incomplete=True)
                inner = dict(scope)
                _bind_target(gen.target, item, inner)
                keep = True
                for cond in gen.ifs:
                    flag = self._expr(cond, inner, module)
                    if isinstance(flag, (RuntimeValue, DatasetPort, Action)):
                        self.fail("FL-07", "Runtime value used in comprehension filter", cond, module)
                    if not bool(flag):
                        keep = False
                        break
                if keep:
                    _walk(gen_index + 1, inner)

        _walk(0, dict(env))
        if isinstance(node, ast.DictComp):
            return results
        if isinstance(node, ast.SetComp):
            return set(results)
        return results

    def _native(self, api, args, kwargs, node, module):
        record = api.record
        sem = record["native_semantics"]
        kind = sem.get("kind")
        try:
            handlers = native_handlers(record)
        except (ForgeError, AttributeError, KeyError, ValueError) as exc:
            self.fail(
                "FL-02",
                "Native connection semantics are not supported: " + str(exc),
                node,
                module,
                incomplete=True,
            )
        try:
            from ..data.callable_contract import bind_native_parameters

            values = bind_native_parameters(
                record["signature"]["parameters"],
                ([api.receiver] if api.receiver else []) + args,
                kwargs,
            )
            parameter_origins = bind_parameters(
                record['signature']['parameters'],
                ([None] if api.receiver else []) + [self._parameter_origin(a) for a in node.args],
                {k.arg: self._parameter_origin(k.value) for k in node.keywords},
            )
        except (ValueError, TypeError, SyntaxError) as exc:
            self.fail("FL-03", str(exc), node, module)
        if kind in {"training_recipe", "predictor_spec"}:
            return self._training_declaration(kind, values, node, module)
        if kind == "select_model":
            from ..data.training import training_steps
            fitted = values["self"]
            if not isinstance(fitted, DeclaredValue) or fitted.type != scalar("fit"):
                self.fail("FL-04", "select requires the declared fit result", node, module)
            return DeclaredValue({**fitted.value, "steps": list(training_steps(values["steps"]))}, scalar("model_selection"))
        from ..execution.capabilities import check_action
        capability = check_action(kind, values)
        if not capability['supported']:
            self.fail('FL-07', capability['reason'], node, module,cause='unsupported')
        callable_ports = {p['name']: p for p in sem['callable_ports']}
        primary = next((p['name'] for p in callable_ports.values() if p['role'] == 'transform'), None)
        for name, value in values.items():
            if name not in callable_ports and name != 'catch' and self._has_callback(value):
                self.fail('OP-02', 'Callback is not a declared native callable port: ' + name, node, module)
        inputs = []
        if kind == 'embeddings' and not (isinstance(values.get('model'), DeclaredValue)
                and values['model'].type == scalar('embedding_model')):
            self.fail('FL-06', 'Embedding model must come from the native EmbeddingModel value', node, module)
        # Value provenance is established by this interpreter, never by accepting
        # a user-written JSON marker or a caller-supplied action identity.
        if kind == 'require' and not (type(values.get('condition')) is bool or
                isinstance(values.get('condition'), RuntimeValue) and values['condition'].type == scalar('bool')):
            self.fail('FL-06', 'Guard requires a boolean expression from checked native values', node, module)
        runtime_values = self._runtime_values(values)
        consumers = {'require': {'condition'}, 'read_table': {'version'}, 'from_items': {'items'},
                     'read_checkpoint': {'checkpoint'}, 'run_stream': {'checkpoint'},
                     'map': {'fn_kwargs'}, 'filter': {'fn_kwargs'}, 'flat_map': {'fn_kwargs'}, 'batch_map': {'fn_kwargs'}}
        if runtime_values and self._runtime_values({k: v for k, v in values.items() if k not in consumers.get(kind, set())}):
            self.fail('FL-07', 'This native parameter has no runtime value consumer contract', node, module)
        if set(sem["effects"]) - {"read_table", "write_table", "materialize", "network", "ledger", "checkpoint", "objects"}:
            self.fail(
                "FL-07",
                "Native effects require an additional managed contract",
                node,
                module,
                incomplete=True,
            )
        if (
            values.get("checkpoint") is not None and kind not in {'run_stream', 'read_checkpoint'}
            or values.get("checkpoint_operator") is not None
        ):
            self.fail(
                "FL-07",
                "Checkpoint effects require a managed recovery contract",
                node,
                module,
                incomplete=True,
            )
        if values.get("hard_timeout") is not None:
            timeout = values["hard_timeout"]
            if type(timeout) not in (int, float) or not 0 < timeout < float("inf"):
                self.fail(
                    "OP-07", "hard_timeout must be finite and positive", node, module
                )
            if values.get("execution") == "thread":
                self.fail(
                    "FL-08",
                    "Thread execution cannot enforce hard_timeout",
                    node,
                    module,
                )
        for port in sem["datasets"]:
            value = values.get(port, ())
            parts = value if isinstance(value, tuple) else (value,)
            for ds in parts:
                if not isinstance(ds, DatasetPort):
                    self.fail("FL-04", f"{port} requires a Dataset", node, module)
                inputs.append(ds)
        model_dependencies = set()
        if kind == "fit":
            recipe = values["recipe"]
            if not isinstance(recipe, DeclaredValue) or recipe.type != scalar("training_recipe"):
                self.fail("FL-06", "fit requires a declared TrainingRecipe", node, module)
            monitors = values["monitors"] if values["monitors"] is not None else {}
            if not isinstance(monitors, dict) or monitors.keys() != recipe.value["monitor_schemas"].keys():
                self.fail("FL-04", "monitor datasets must match declared recipe ports", node, module)
            for name, ds in sorted(monitors.items()):
                if not isinstance(ds, DatasetPort):
                    self.fail("FL-04", "monitor requires a Dataset: " + name, node, module)
                inputs.append(ds)
            parent = values["resume_from"]
            if isinstance(parent, DeclaredValue):
                if parent.type != scalar("model_selection") or len(parent.value["steps"]) != 1:
                    self.fail("FL-04", "resume_from requires a single model selection", node, module)
                model_dependencies.add(parent.value["fit_node"])
        elif kind == "predict":
            selection = values["self"]
            if not isinstance(selection, DeclaredValue) or selection.type != scalar("model_selection"):
                self.fail("FL-04", "predict requires a model selection", node, module)
            model_dependencies.add(selection.value["fit_node"])
        runtime_values.update(model_dependencies)
        if kind == 'prepend' and (len(inputs)!=2 or inputs[1].stream):
            self.fail('FL-08','Prepend prefix must be a finite synchronous Dataset without actors',node,module)
        stream = sem["execution"] == "stream" or any(ds.stream for ds in inputs)
        resource_declaration = None
        native_options = (values["self"].value["recipe"]["predictor"]["native_options"]
                          if kind == 'predict' else values.get("backend_options"))
        if native_options is not None:
            if sem['role'] != 'transform':
                self.fail('FL-08', 'Managed native resources currently require an operator', node, module, incomplete=True)
            try:
                from ..data.native_options import resource_only_options
                resource_declaration = resource_only_options(native_options,
                    family='batch_transform' if kind in {'map_batches', 'batch_map', 'predict'} else 'row_transform')
            except (ValueError, TypeError) as exc:
                self.fail('FL-08', str(exc), node, module, incomplete=True)
        schema = {}
        effects = list(sem["effects"])
        operator = None
        description = None
        callbacks, callable_facts = {}, []
        resources = {}
        if kind in {'read_checkpoint', 'run_stream'} and values.get('checkpoint') is not None:
            checkpoint = values['checkpoint']
            if not isinstance(checkpoint, RuntimeValue) or checkpoint.type['kind'] != 'checkpoint':
                self.fail('FL-07', 'Checkpoint must come from the native run-owned checkpoint action', node, module)
            resources['checkpoint'] = checkpoint.type
        if kind == 'read_checkpoint' and 'checkpoint' not in resources:
            self.fail('FL-07', 'Checkpoint read requires a declared checkpoint', node, module)
        if kind == 'open_checkpoint':
            from pathlib import PurePosixPath
            path = values['path']
            if type(path) is not str or PurePosixPath(path).is_absolute() or '..' in PurePosixPath(path).parts or PurePosixPath(path).parts[:1] != ('checkpoints',):
                self.fail('FL-07', 'Checkpoint path must be relative to checkpoints/ in the run', node, module)
            outputs = values['outputs']
            if type(outputs) is not dict or not outputs:
                self.fail('FL-09', 'Checkpoint requires declared outputs', node, module)
            for output in outputs.values():
                if type(output) is not dict or set(output) != {'uri', 'schema'}:
                    self.fail('FL-09', 'Checkpoint output requires uri and schema', node, module)
                if output['uri'] not in self.targets or isinstance(self.targets, dict) and self.targets[output['uri']] != {'mode': 'checkpoint', 'expected_version': None}:
                    self.fail('FL-09', 'Checkpoint target requires explicit checkpoint ownership permission', node, module)
                if output['uri'] in self.checkpoint_targets:
                    self.fail('FL-09', 'One target cannot be owned by two checkpoints', node, module)
                declared = output['schema']
                if not isinstance(declared, DeclaredValue) or declared.type != scalar('schema'):
                    self.fail('FL-06', 'Checkpoint schema must use the native schema declaration', node, module)
                self.checkpoint_targets.add(output['uri'])
        if kind == 'save_lance':
            from ..lance.schema import build_schema
            from ..data.flow_contract import arrow_fields
            declared = values['schema']
            if not isinstance(declared, DeclaredValue) or declared.type != scalar('schema'):
                self.fail('FL-06', 'Stream sink requires a native schema declaration', node, module)
            uri = values['uri']
            grant = self.targets.get(uri) if isinstance(self.targets, dict) else None
            legacy = uri in self.checkpoint_targets and values['mode'] == 'append'
            native = (grant is not None and values['mode'] == 'overwrite'
                      and (grant == {'mode': 'create', 'expected_version': None}
                           or grant.get('mode') == 'overwrite'
                           and type(grant.get('expected_version')) is int and grant['expected_version'] > 0))
            overlap = any(t['uri'] == uri for t in self.tables.values())
            if (uri in self.writes or not (legacy or native)
                    or overlap and (grant is None or grant.get('mode') != 'overwrite')):
                self.fail('FL-09', 'Stream sink requires one declared create or fixed-baseline overwrite target', node, module)
            resources['schema_fields'] = arrow_fields(build_schema(declared.value['schema_declaration']))
            self.writes.add(values['uri'])
        if kind == 'run_stream':
            if 'checkpoint' in resources:
                origin = values['checkpoint'].expression['runtime_value']['node']
                if origin in self.used_checkpoints:
                    self.fail('FL-07', 'One checkpoint belongs to one stream action per attempt', node, module)
                self.used_checkpoints.add(origin)
            pending, seen, sinks = [ds.node for ds in inputs], set(), {}
            while pending:
                child = pending.pop()
                if child in seen:
                    continue
                seen.add(child)
                current_node = self.node_index[child]
                if current_node['kind'] == 'save_lance':
                    p = current_node['normalized_parameters']
                    if p['stage'] in sinks:
                        self.fail('FL-09', 'Duplicate checkpoint stage in stream', node, module)
                    sinks[p['stage']] = {'uri': p['uri'], 'schema_identity': p['schema']['identity']}
                if current_node['role'] != 'bridge':
                    pending.extend(self.parent_index[child])
            expected = {k: {name: v[name] for name in ('uri', 'schema_identity')}
                        for k, v in resources.get('checkpoint', {}).get('outputs', {}).items()}
            if 'checkpoint' in resources and sinks != expected:
                self.fail('FL-09', 'Stream sinks must match the exact declared checkpoint stages and schemas', node, module)
            if 'checkpoint' not in resources and any(v['uri'] in self.checkpoint_targets for v in sinks.values()):
                self.fail('FL-09', 'Legacy checkpoint targets require their original checkpoint owner', node, module)
        if kind == 'from_items':
            items = values['items']
            if not isinstance(items, RuntimeValue) and (type(items) is not list or not 1 <= len(items) <= 128):
                self.fail('FL-06', 'Managed from_items requires 1..128 bounded summary rows', node, module)
            if isinstance(items, RuntimeValue) and any(self.node_index[origin]['kind'] != 'collect_summary' for origin in runtime_values):
                self.fail('FL-06', 'Runtime row lists require the bounded summary query', node, module)
            resources['value_types'] = {'items': self._value_type(items)}
        if kind == "read_table":
            uri, version = values.get("uri"), values.get("version")
            receipt_origin = None
            if isinstance(version, RuntimeValue):
                reference = version.expression.get('runtime_value', {})
                receipt_origin = self.node_index.get(reference.get('node'))
                if (set(version.expression) != {'runtime_value'} or reference.get('field') != 'committed_version'
                        or receipt_origin is None or receipt_origin['kind'] != 'write_table'
                        or receipt_origin['parameters']['uri'] != uri or runtime_values != {receipt_origin['id']}
                        or self._runtime_values({k: v for k, v in values.items() if k != 'version'})):
                    self.fail('FL-05', 'Derived table version must come from the matching native write receipt', node, module)
            elif runtime_values:
                self.fail('FL-05', 'Only the committed version may be a runtime read argument', node, module)
            table = next(
                (
                    t
                    for t in self.tables.values()
                    if t["uri"] == uri and t["version"] == version
                ),
                None,
            )
            if receipt_origin is not None:
                table = {'fields': receipt_origin['fields']}
            if receipt_origin is None and (type(version) is not int or not table):
                self.fail(
                    "FL-05",
                    "Input must match a bound table root and fixed version",
                    node,
                    module,
                )
            if values.get("projection") or values.get("storage_options"):
                self.fail(
                    "FL-06",
                    "Projection expressions/storage options are not covered",
                    node,
                    module,
                    incomplete=True,
                )
            from ..lance.model import LanceScanSpec

            try:
                LanceScanSpec(
                    **{
                        k: values[k]
                        for k in (
                            "uri",
                            "version",
                            "columns",
                            "filter",
                            "limit",
                            "batch_size",
                            "batch_readahead",
                            "fragment_readahead",
                        )
                            if k in values and not (k == 'version' and receipt_origin is not None)
                    }
                )
            except (ValueError, TypeError) as exc:
                self.fail("FL-03", str(exc), node, module)
            schema = copy.deepcopy(table["fields"])
            resources["fields"] = schema
            if values.get("columns"):
                try:
                    require_fields(schema, values["columns"])
                except ValueError as exc:
                    self.fail("FL-06", str(exc), node, module)
                schema = {k: schema[k] for k in values["columns"]}
            if any("unsupported" in json.dumps(f["type"]) for f in schema.values()):
                self.fail(
                    "FL-06",
                    "Selected fields contain unsupported port types; explicitly project supported fields",
                    node,
                    module,
                    incomplete=True,
                )
        else:
            schema = inputs[0].fields if inputs else {}
            returns = None
            if primary:
                definition = values[primary]
                construction = definition if isinstance(definition, Construction) else None
                if construction is not None:
                    definition = construction.definition
                    if values.get("fn_constructor_args") or values.get("fn_constructor_kwargs"):
                        self.fail("OP-02", "Actor instance already has constructor arguments", node, module)
                description = self.operators.describe(definition)
                if kind == 'reduce' and (description['class'] or description['async']):
                    self.fail('OP-02', 'Reducer must be a synchronous function', node, module)
                if not description["class"] and (
                    values.get("fn_constructor_args")
                    or values.get("fn_constructor_kwargs")
                ):
                    self.fail(
                        "OP-02",
                        "Constructor arguments require a callable class",
                        node,
                        module,
                    )
                lifecycle = description["class"] and any(
                    isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and n.name in {"astart", "astop", "aclose"}
                    for n in description["class"].body
                )
                streaming_options = any(
                    values.get(k) is not None
                    for k in (
                        "concurrency",
                        "queue_depth",
                        "execution",
                        "catch",
                        "label",
                        "callable_scope",
                    )
                )
                if lifecycle and not streaming_options:
                    self.fail(
                        "OP-07",
                        "Lifecycle hooks require the native streaming actor protocol",
                        node,
                        module,
                    )
                if description["async"] and sem["execution"] != "stream":
                    self.fail(
                        "OP-02",
                        "Synchronous node cannot accept an async callable",
                        node,
                        module,
                    )
                binding = values.get("inputs")
                fixed = values.get("fn_kwargs") or {}
                if binding is not None:
                    if values.get("fn_args"):
                        self.fail(
                            "OP-02",
                            "Field-bound positional static arguments are unsupported",
                            node,
                            module,
                        )
                    from ..data.plan import normalize_bound_inputs

                    try:
                        binding = normalize_bound_inputs(binding)
                        validate_field_binding(
                            description["parameters"],
                            binding,
                            fixed,
                            label=definition.node.name,
                        )
                    except (ValueError, TypeError) as exc:
                        self.fail("OP-02", str(exc), node, module)
                    actual = {}
                    for param, name in binding.items():
                        if name not in schema or not schema[name]["required"]:
                            self.fail(
                                "FL-06",
                                f"Input field {name!r} must exist for {param}",
                                node,
                                module,
                            )
                        actual[param] = schema[name]["type"]
                    actual.update({k: self._value_type(v) for k, v in fixed.items()})
                else:
                    row = {**scalar("record"), "fields": schema}
                    if kind == "batch_map":
                        row = {**scalar("list"), "item": row}
                    try:
                        actual = bind_parameters(
                            description["parameters"],
                            ([union_type(literal_type(values['initial']), description['returns'])] if kind == 'reduce' else []) + [
                                row,
                                *[
                                    literal_type(v)
                                    for v in (values.get("fn_args") or [])
                                ],
                            ],
                            {k: self._value_type(v) for k, v in fixed.items()},
                            defaults={
                                k: literal_type(v)
                                for k, v in description["defaults"].items()
                            },
                        )
                    except TypeError as exc:
                        self.fail("OP-02", str(exc), node, module)
                for p in description["parameters"]:
                    if p["name"] in actual and not compatible(
                        actual[p["name"]], p["type"]
                    ):
                        self.fail(
                            "FL-06",
                            f"Input type conflicts with {definition.node.name}.{p['name']}",
                            node,
                            module,
                        )
                self.operators.check(
                    description,
                    constructor_args=construction.args if construction else values.get("fn_constructor_args") or (),
                    constructor_kwargs=construction.kwargs if construction else values.get("fn_constructor_kwargs"),
                    native_resources=resource_declaration,
                    concurrency=values.get('concurrency') or 1,
                )
                returns = description["returns"]
                operator = {
                    "module": definition.module,
                    "name": definition.node.name,
                    "line": definition.node.lineno,
                    "end_line": definition.node.end_lineno,
                    "column": definition.node.col_offset,
                }
                if construction:
                    operator["construction"] = self._json_value(construction)
                if any(
                    values.get(k) is not None
                    for k in (
                        "concurrency",
                        "queue_depth",
                        "execution",
                        "catch",
                        "label",
                        "callable_scope",
                    )
                ):
                    stream = True
                    for key in ("concurrency", "queue_depth"):
                        if values.get(key) is not None and (
                            type(values[key]) is not int or values[key] < 1
                        ):
                            self.fail(
                                "OP-07",
                                f"{key} must be a positive integer",
                                node,
                                module,
                            )
                    if values.get("execution") not in (None, "inline", "thread"):
                        self.fail("FL-08", "Unsupported execution mode", node, module)
                    if values.get("label") is not None and (
                        type(values["label"]) is not str or not values["label"].strip()
                    ):
                        self.fail(
                            "OP-07", "label must be a nonempty string", node, module
                        )
                    if values.get("callable_scope") not in (None, "stage", "worker"):
                        self.fail("OP-07", "Invalid callable scope", node, module)
                    if (
                        values.get("callable_scope") == "worker"
                        and not description["class"]
                    ):
                        self.fail(
                            "OP-07",
                            "Worker scope requires a callable class",
                            node,
                            module,
                        )
                    catch = values.get("catch") or ()
                    if not isinstance(catch, tuple) or any(
                        not isinstance(v, Symbol)
                        or v.name
                        not in {
                            "builtins.ValueError",
                            "builtins.TypeError",
                            "builtins.RuntimeError",
                            "builtins.Exception",
                        }
                        for v in catch
                    ):
                        self.fail(
                            "OP-08",
                            "catch requires explicit supported Exception types",
                            node,
                            module,
                        )
                    if catch:
                        effects.append("drop_and_count_declared_errors")
                if description["async"] and values.get("execution") == "thread":
                    self.fail(
                        "FL-08",
                        "Thread execution cannot invoke an async callable",
                        node,
                        module,
                    )
            if kind == "limit" and (
                type(values["limit"]) is not int or values["limit"] < 0
            ):
                self.fail("FL-03", "limit must be a nonnegative integer", node, module)
            if kind == "write_table":
                uri = values["uri"]
                if uri not in self.targets:
                    self.fail(
                        "FL-09",
                        "Output must be a declared workspace-owned target",
                        node,
                        module,
                    )
                if "mode" not in kwargs:
                    self.fail("FL-09", "Write mode must be explicit", node, module)
                overlap = any(t['uri'] == uri for t in self.tables.values())
                if uri in self.writes or overlap and not (values['mode'] == 'overwrite' and type(values.get('expected_version')) is int):
                    self.fail(
                        "FL-09",
                        "Multiple writes or read/write overlap require a single declared fixed-baseline overwrite",
                        node,
                        module,
                    )
                if not values.get("return_receipt"):
                    self.fail(
                        "FL-09",
                        "Managed outputs must request a native receipt",
                        node,
                        module,
                    )
                if isinstance(self.targets, dict) and self.targets[uri] != {'mode': values['mode'], 'expected_version': values.get('expected_version')}:
                    self.fail('FL-09', 'Native write differs from its declared output mode/baseline', node, module)
                if values['mode'] not in {'create', 'overwrite'}:
                    self.fail('FL-09', 'Shared append/merge requires a separate managed sink contract', node, module)
                declared_schema = values.get('schema')
                if values.get("storage_options") or declared_schema is not None and not (
                        isinstance(declared_schema, DeclaredValue) and declared_schema.type == scalar('schema')):
                    self.fail(
                        "FL-09",
                        "Nonliteral sink schema/storage options are not covered",
                        node,
                        module,
                        incomplete=True,
                    )
                from ..lance.model import LanceWriteSpec
                from ..lance.schema import build_schema
                from ..data.flow_contract import arrow_fields

                try:
                    write_values = dict(values)
                    if declared_schema is not None:
                        write_values['schema'] = build_schema(declared_schema.value['schema_declaration'])
                        expected_fields = arrow_fields(write_values['schema'])
                        if set(schema) != set(expected_fields) or not compatible(
                                {**scalar('record'), 'fields': schema}, {**scalar('record'), 'fields': expected_fields}):
                            raise ValueError('Explicit write schema conflicts with the input fields or nullability')
                    spec = LanceWriteSpec(
                        **{
                            k: write_values[k]
                            for k in (
                                "uri",
                                "expected_version",
                                "mode",
                                "schema",
                                "on",
                                "update_columns",
                                "when_not_matched",
                            )
                            if k in values and values[k] is not None
                        }
                    )
                except (ValueError, TypeError) as exc:
                    self.fail("FL-09", str(exc), node, module)
                if spec.uri != uri:
                    self.fail(
                        "FL-09",
                        "Output resolves away from its declared location; bind the physical target explicitly",
                        node,
                        module,
                    )
                if (
                    values["mode"] != "create"
                    and values.get("expected_version") is None
                ):
                    self.fail(
                        "FL-09",
                        "Existing-output writes require a fixed expected_version",
                        node,
                        module,
                    )
                self.writes.add(uri)
            if kind == "materialize":
                stream = False
        try:
            for name, port in callable_ports.items():
                if port['role'] != 'predicate':
                    continue
                definition = values[name]
                if definition is None and port['optional']:
                    continue
                predicate = self.operators.describe(definition)
                if predicate['class'] or predicate['async'] or not compatible(predicate['returns'], scalar('bool')):
                    self.fail('OP-02', 'Predicate must be a synchronous function returning bool', node, module)
                actual = bind_parameters(predicate['parameters'], [{**scalar('record'), 'fields': inputs[0].fields}], {},
                                         defaults={k: literal_type(v) for k, v in predicate['defaults'].items()})
                if any(not compatible(actual[p['name']], p['type']) for p in predicate['parameters'] if p['name'] in actual):
                    self.fail('FL-06', 'Predicate input conflicts with declared row', node, module)
                self.operators.check(predicate)
                callbacks[name] = {'module': definition.module, 'name': definition.node.name, 'line': definition.node.lineno}
                callable_facts.append({'name': name, 'role': port['role'], 'returns': predicate['returns'], 'async': False})
            ordinary = {key: value for key, value in values.items()
                        if key not in sem["datasets"] and key not in callable_ports}
            normalized = handlers["normalize"](self._json_value(ordinary))
            references = handlers["resource_references"](normalized)
            if references:
                if type(references) is not list:
                    self.fail("FL-07", "Native resource declaration must be a finite list", node, module)
                self.resource_count += len(references)
                if self.resource_count > 256:
                    self.fail("FL-10", "Resource reference budget exceeded", node, module, incomplete=True)
                files = []
                for reference in references:
                    if reference.get('kind') == 'table':
                        if set(reference) != {'kind', 'parameter', 'value'}:
                            self.fail('FL-07', 'Invalid native table reference', node, module)
                        table = next((t for t in self.tables.values() if {k: t[k] for k in ('uri', 'version')} == reference['value']), None)
                        if table is None:
                            self.fail('FL-05', 'Resource table must match a bound fixed table version', node, module)
                        resources[reference['parameter']] = copy.deepcopy(table)
                    else:
                        files.append(reference)
                if files:
                    if self.resource_resolver is None:
                        self.fail("FL-07", "Declared resources require workspace binding", node, module, incomplete=True)
                    resources.update(self.resource_resolver(self.tree, files, self.resource_bindings))
            analysis = handlers["analyze"](
                normalized, inputs=[ds.fields for ds in inputs],
                callables=([{'name': primary, 'role': 'transform', "returns": description["returns"], "async": description["async"]}] if description else []) + callable_facts,
                resources=resources,
            )
            bounded_json(analysis, 1024 * 1024)
            if analysis.get('input_driver') == 'pull' and any(ds.stream for ds in inputs):
                self.fail('FL-08', 'Native input requires materialized synchronous input', node, module)
            if analysis.get('input_driver') == 'stream' and not any(ds.stream for ds in inputs):
                self.fail('FL-08', 'Native streaming action requires a streaming input', node, module)
            required = set(sem['effects']) & {'network', 'ledger', 'objects'}
            if required - {r['kind'] for r in analysis.get('resources', [])}:
                self.fail('FL-07', 'Native effects require explicit resource facts', node, module)
            schema = analysis["fields"]
            analysis = {key: value for key, value in analysis.items() if key != "fields"}
        except InspectionError:
            raise
        except (ValueError, TypeError, KeyError) as exc:
            self.fail("FL-06", str(exc), node, module)
        # Third-party computation uses the existing platform snapshot/extras
        # authority. Even an unused torch import executes module initialization,
        # so its dependency belongs to the original sealed environment closure.
        if any(isinstance(value, Symbol) and value.name.split('.')[0] == 'torch'
               for namespace in self.tree.modules.values() for value in namespace.values()):
            analysis['required_distributions'] = sorted(set(analysis.get('required_distributions', ())) | {'torch'})
        stream = stream or analysis['driver']=='stream'
        from ..execution.capabilities import check_composition, check_call

        capability_fact = {
            "kind": analysis.get('kind', "async_map" if kind == "map" and sem["execution"] == "stream" else kind),
            "driver": analysis["driver"],
            "bound": values.get("inputs") is not None,
            "native_options": native_options is not None,
            "stream_options": kind == "map" and sem["execution"] != "stream" and analysis["driver"] == "stream",
        }
        if resource_declaration is not None:
            capability_fact['resource_declaration'] = resource_declaration
        driver = 'stream' if analysis['driver'] == 'stream' or any(ds.stream for ds in inputs) else 'pull'
        capability = check_composition([], driver=driver)
        if sem["role"] not in {"source"} and kind not in {"join", "union", "reduce"}:
            if inputs:
                capability = self._segment_capability(inputs[0].node, driver)
            if capability['supported'] and sem['role'] == 'transform':
                capability = check_call(capability_fact, driver=driver)
        if not capability["supported"]:
            self.fail("FL-08", capability["reason"], node, module,cause='unsupported')
        if (
            len(self.nodes) >= self.tree.limits["nodes"]
            or len(self.edges) + len(self.value_edges) + len(inputs) + len(runtime_values) + len(self.actions) > self.tree.limits["edges"]
        ):
            self.fail("FL-10", "Graph budget exceeded", node, module, incomplete=True)
        identifier = hashlib.sha256(
            json.dumps(
                [module, node.lineno, node.col_offset, self.stack, len(self.nodes)],
                ensure_ascii=False,
            ).encode()
        ).hexdigest()[:16]
        result = 'value' if sem['role'] in {'query', 'value'} else "action" if sem["role"] == "action" else "dataset"
        self.nodes.append(
            {
                "id": identifier,
                "api": record["id"],
                "kind": kind,
                "role": sem["role"],
                "contract": {key: sem[key] for key in ("contract_id", "version", "owner")},
                "normalized_parameters": normalized,
                "analysis": analysis,
                "capability_fact": capability_fact,
                "capability": capability,
                "source": module.replace(".", "/") + ".py",
                "line": node.lineno,
                "column": node.col_offset,
                "end_line": node.end_lineno,
                "parameter_origins": self._origin_ports(parameter_origins),
                "parameters": self._json_value(values),
                "fields": schema,
                "operator": operator,
                "callbacks": callbacks,
                "source_resources": [{**reference, 'sha256': self.resource_bindings[reference['path']]['sha256']}
                                     for reference in references if reference.get('kind')=='config'
                                     and reference.get('path') in self.resource_bindings],
                "effects": effects,
                "execution": "stream" if stream else "lazy",
                "result": result,
                "runtime": "not_run",
                "schema_evidence": (
                    "declared/derived" if operator else "metadata/derived"
                ),
            }
        )
        self.node_index[identifier] = self.nodes[-1]
        self.parent_index[identifier] = [ds.node for ds in inputs]
        for origin in sorted(runtime_values):
            self.value_edges.append({'from': origin, 'to': identifier})
        if sem['role'] == 'transform' and kind not in {'join', 'union', 'reduce'}:
            self.capability_cache[(identifier, driver)] = capability
        for index, ds in enumerate(inputs):
            self.edges.append({"from": ds.node, "to": identifier, "port": index})
            self.consumed.add(ds.node)
        if sem["role"] in {"action", "bridge", "query"}:
            self.actions.append(identifier)
        if result == 'value':
            if kind == "fit":
                return DeclaredValue({"fit_node": identifier, "recipe": values["recipe"].value}, scalar("fit"))
            if sem['role'] == 'value':
                return DeclaredValue(analysis['value'], analysis['value_type'])
            return RuntimeValue({'runtime_value': {'node': identifier}}, analysis['value_type'])
        return (
            Action(identifier)
            if result == "action"
            else DatasetPort(identifier, schema, stream)
        )

    def _training_declaration(self, kind, values, node, module):
        """Bind source-level callable identities. No project code is imported or run."""
        from ..data.training import predictor_values, recipe_values, training_json
        key = "trainer" if kind == "training_recipe" else "factory"
        definition = values[key]
        if not isinstance(definition, Definition):
            self.fail("OP-02", "training callables must resolve to project source", node, module)
        if kind == "predictor_spec":
            if not isinstance(definition.node, ast.ClassDef):
                self.fail("OP-02", "predictor factory requires an actor class", node, module)
            methods = {m.name: m for m in definition.node.body if isinstance(m, ast.FunctionDef)}
            if "__init__" not in methods or "__call__" not in methods:
                self.fail("OP-02", "predictor needs a model constructor and batch call", node, module)
            bind_parameters(parameters(methods["__init__"], drop_self=True), [], dict(model=None, inputs={}, parameters={}))
            bind_parameters(parameters(methods["__call__"], drop_self=True), [None], {})
            normalized = predictor_values(values)
        else:
            if isinstance(definition.node, ast.ClassDef):
                methods = {m.name: m for m in definition.node.body if isinstance(m, ast.FunctionDef)}
                if "__call__" not in methods:
                    self.fail("OP-02", "training actors require a synchronous call", node, module)
                if "__init__" in methods:
                    bind_parameters(parameters(methods["__init__"], drop_self=True), [], {})
                bind_parameters(parameters(methods["__call__"], drop_self=True), [None, None], {})
            elif isinstance(definition.node, ast.FunctionDef):
                bind_parameters(parameters(definition.node), [None, None], {})
            else:
                self.fail("OP-02", "trainer must be a synchronous function or actor class", node, module)
            predictor = values["predictor"]
            if not isinstance(predictor, DeclaredValue) or predictor.type != scalar("predictor_spec"):
                self.fail("FL-06", "recipe requires an original PredictorSpec", node, module)
            normalized = recipe_values(values)
        value = {**self._json_value(values), **training_json(normalized)}
        bounded_json(value, 1024 * 1024)
        return DeclaredValue(value, scalar(kind))

    def _segment_capability(self, identity, driver):
        """At most one evaluation per node and driver, including shared paths."""
        from ..execution.capabilities import check_call, check_composition
        pending = []
        while (identity, driver) not in self.capability_cache:
            previous = self.node_index[identity]
            if previous['role'] in {'source', 'bridge'} or previous['kind'] in {'join', 'union', 'reduce'} or len(self.parent_index[identity]) != 1:
                self.capability_cache[(identity, driver)] = check_composition([], driver=driver)
                break
            pending.append(identity)
            identity = self.parent_index[identity][0]
        result = self.capability_cache[(identity, driver)]
        for identity in reversed(pending):
            if result['supported']:
                result = check_call(self.node_index[identity]['capability_fact'], driver=driver)
            self.capability_cache[(identity, driver)] = result
        return result

    def _has_callback(self, value):
        if isinstance(value, (Definition, Construction, API, Symbol)):
            return True
        if isinstance(value, (list, tuple)):
            return any(self._has_callback(v) for v in value)
        if isinstance(value, dict):
            return any(self._has_callback(v) for v in value.values())
        return False

    def _value_type(self, value):
        if isinstance(value, RuntimeValue):
            return value.type
        if type(value) is dict:
            return {**scalar('record'), 'fields': {key: field(self._value_type(child)) for key, child in value.items()}}
        if type(value) in (list, tuple):
            return {**scalar('list' if type(value) is list else 'tuple'),
                    'item': union_type(*(self._value_type(v) for v in value)) if value else scalar('never')}
        return literal_type(value)

    def _runtime_values(self, value):
        def origins(expression):
            if type(expression) is dict:
                if set(expression) == {'runtime_value'}:
                    return {expression['runtime_value']['node']}
                return set().union(*(origins(v) for v in expression.values()))
            if type(expression) is list:
                return set().union(*(origins(v) for v in expression))
            return set()
        if isinstance(value, RuntimeValue):
            return origins(value.expression)
        if isinstance(value, (dict, list, tuple)):
            return set().union(*(self._runtime_values(v) for v in (value.values() if isinstance(value, dict) else value)))
        return set()

    def _json_value(self, value):
        if isinstance(value, DeclaredValue):
            return value.value
        if isinstance(value, RuntimeValue):
            return value.expression
        if isinstance(value, Action):
            return {'action_node': value.node}
        if isinstance(value, Construction):
            return {"actor": value.definition.module + ":" + value.definition.node.name,
                    "construction_id": value.identity, "args": list(value.args),
                    "kwargs": value.kwargs, "state": value.state}
        if isinstance(value, DatasetPort):
            return {"dataset_node": value.node}
        if isinstance(value, Definition):
            return {"callable": value.module + ":" + value.node.name}
        if isinstance(value, Symbol):
            return {"symbol": value.name}
        if isinstance(value, dict):
            return {k: self._json_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._json_value(v) for v in value]
        return value
