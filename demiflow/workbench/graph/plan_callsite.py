"""Passive API provenance bound to the actual compiler's object/occurrence map.

This observer runs only in the disposable read-only preview worker. It never
wraps SDK methods, changes plans, evaluates arguments, or builds a second DAG.
"""
import ast
import hashlib
import inspect
from itertools import islice
from pathlib import Path
import sys
from types import FunctionType
import weakref

from demiflow.workbench.python_sources import read_source_bytes
from demiflow.workbench.projects import workspace as ws
from .plan_details import material, safe_json, unavailable


class PlanCallsites:
    """At most 1024 weak object locations; no retained frames or row payloads."""
    def __init__(self, root, sources):
        from demiflow.data.api import DataAPI
        from demiflow.data.dataset import Dataset, MaterializedDataset
        self.root, self.sources = Path(root), sources
        self.codes = {v.__code__: (owner.__name__ + '.' + k, inspect.signature(v))
                      for owner in (DataAPI, Dataset, MaterializedDataset)
                      for k, v in vars(owner).items()
                      if not k.startswith('_') and type(v) is FunctionType}
        self.dataset_types = (Dataset, MaterializedDataset)
        self.locations, self.files, self.nodes, self.active = {}, {}, {}, {}
        self.api_definitions = {}
        self.retained_bytes = 0
        self.compilation = None
        self.unavailable = None

    def __enter__(self):
        self.previous = sys.getprofile()
        if self.previous is None:
            sys.setprofile(self._observe)
        return self

    def __exit__(self, *exc):
        if self.previous is None:
            sys.setprofile(None)
        self.active.clear()

    def _location(self, frame):
        api = frame if frame.f_code in self.codes else None
        caller = frame.f_back
        for _ in range(64):
            if caller is None:
                return None
            if caller.f_code in self.codes:
                api = caller
            path = Path(caller.f_code.co_filename)
            if api is not None and path.is_absolute() and path.is_relative_to(self.root):
                relative = path.relative_to(self.root).as_posix()
                if relative in self.sources:
                    position = next(islice(caller.f_code.co_positions(), caller.f_lasti // 2, None), None)
                    if position is not None and all(type(n) is int for n in position):
                        if id(api) in self.active:
                            return self.active[id(api)]
                        name, signature = self.codes[api.f_code]
                        location = (relative, position, name, self._arguments(api, signature))
                        import json
                        return location if len(json.dumps(location, ensure_ascii=False).encode()) <= 32768 else None
                    return None
            caller = caller.f_back
        return None

    def _value(self, value, *, depth=0, budget=None):
        """Never invoke repr, descriptors, actors, iterators or row readers."""
        budget = [256] if budget is None else budget
        budget[0] -= 1
        if depth > 6 or budget[0] < 0:
            return {'unavailable': '超过入参展示范围'}
        if value is None or type(value) in (bool, int, float):
            # JSON cannot carry NaN/Inf; preserve their type without pretending null.
            if type(value) is float:
                import math
                if not math.isfinite(value):
                    return {'type': 'float', 'unavailable': '非有限数值'}
            return value
        if type(value) is str:
            return value if len(value) <= 2048 else {'type': 'str', 'characters': len(value), 'unavailable': '长文本请查看源码或 Prompt'}
        if type(value) in self.dataset_types:
            return {'type': 'Dataset', 'source': '上游数据流；Schema 见输入 / 输出'}
        # Arrow Schema is immutable type metadata, never a table/row iterator.
        import pyarrow as pa
        if type(value) is pa.Schema:
            if len(value) > 256:
                return {'type': 'Schema', 'unavailable': 'Schema 超过 256 字段展示范围'}
            return {'type': 'Schema', 'fields': [dict(name=f.name[:2048], type=str(f.type)[:1024], nullable=f.nullable) for f in value]}
        if type(value) is FunctionType:
            return {'type': 'function', 'reference': value.__module__ + '.' + value.__qualname__}
        if type(value) in (tuple, list):
            if len(value) > 64:
                return {'type': type(value).__name__, 'length': len(value), 'unavailable': '不展开数据载荷'}
            return [self._value(v, depth=depth + 1, budget=budget) for v in value]
        if type(value) is dict and len(value) <= 64 and all(type(k) is str for k in value):
            return {k: self._value(v, depth=depth + 1, budget=budget) for k, v in value.items()}
        return {'type': type(value).__name__, 'unavailable': '对象不求值；请查看调用表达式及专属材料'}

    def _arguments(self, frame, signature):
        arguments = []
        budget = [512]
        for name, parameter in signature.parameters.items():
            if len(arguments) >= 80:
                break
            if name not in frame.f_locals or (name == 'self' and self.codes[frame.f_code][0].startswith('DataAPI.')):
                continue
            value = self._value(frame.f_locals[name], budget=budget)
            value = safe_json({name: value})[name]
            arguments.append(dict(name=name, value=value, parameter_kind=parameter.kind.name,
                                  has_default=parameter.default is not inspect.Parameter.empty))
        return arguments

    def _remember(self, obj, location):
        if location is None or len(self.locations) >= 1024:
            return
        try:
            ref = weakref.ref(obj)
        except TypeError:
            return
        import json
        size = len(json.dumps(location, ensure_ascii=False).encode())
        if size > 32768 or self.retained_bytes + size > 2 * 1024**2:
            return
        old = self.locations.get(id(obj))
        self.retained_bytes += size - (old[2] if old else 0)
        self.locations[id(obj)] = (ref, location, size)

    def _observe(self, frame, event, returned):
        if frame.f_code in self.codes and event == 'call':
            # Freeze at API ENTRY, before implementations normalize/reassign
            # parameters. Keep no frame; nested SDK helpers reuse the same
            # business-facing call rather than exposing internal defaults.
            if len(self.active) < 64:
                location = self._location(frame)
                if location is not None:
                    self.active[id(frame)] = location
            return
        if event != 'return':
            return
        if frame.f_code not in self.codes:
            return
        location = self.active.pop(id(frame), None)
        if type(returned) not in self.dataset_types:
            return
        if location is None:
            return
        from demiflow.execution.host.inspection import dataset_declaration_links
        links = dataset_declaration_links(returned)
        if links is None:
            return
        source, operations = links
        receiver = frame.f_locals.get('self')
        if type(receiver) in self.dataset_types:
            previous = dataset_declaration_links(receiver)
            if previous is None:
                return
            old_source, old_ops = previous
        else:
            old_source, old_ops = None, ()
        if source is not old_source:
            self._remember(source, location)
        # Borrowed object identities identify only the new part of this call.
        old_ids = {id(op) for op in old_ops}
        for operation in operations:
            if id(operation) not in old_ids:
                self._remember(operation, location)

    def bind_compilation(self, provenance):
        """Consume the real compiler's final IDs, while the SDK action is active."""
        if self.compilation is not None:
            raise ValueError('Callsites already belong to a compilation')
        self.compilation = (provenance.action_id, provenance.dag_id)
        self.unavailable = provenance.unavailable
        if self.unavailable is not None:
            return
        for binding in provenance.bindings:
            saved = self.locations.get(binding.object_id)
            if saved is not None and saved[0]() is not None:
                self.nodes[binding.occurrence_id] = saved[1]
        # Public API entry already froze the business call location/arguments.
        # Nested SDK helpers reuse that location; never inspect compiler frames.
        terminal = next(reversed(self.active.values()), None)
        if terminal is not None:
            for occurrence in provenance.terminals:
                self.nodes[occurrence] = terminal

    def check_compilation(self, graph):
        if self.compilation != (graph.action_id, graph.dag_id):
            raise ValueError('Callsites do not belong to this compiled graph')

    def detail(self, occurrence):
        saved = self.nodes.get(occurrence)
        if saved is None:
            return unavailable(self.unavailable or '该节点未记录直接的公开 API 调用（可能由引擎展开）；不会推测源码。')
        relative, (line, end_line, column, end_column), api, arguments = saved
        if relative not in self.files:
            path = self.root / relative
            ws.no_symlinks(path)
            raw = read_source_bytes(path)
            expected = self.sources[relative]
            if hashlib.sha256(raw).hexdigest() != expected:
                return unavailable('调用源码在解析期间发生变化。')
            text = raw.decode('utf-8-sig')
            self.files[relative] = (text, ast.parse(text), expected)
        text, tree, expected = self.files[relative]
        def at_executed_position(n):
            if not isinstance(n, ast.Call):
                return False
            if (n.lineno, n.end_lineno, n.col_offset, n.end_col_offset) == (line, end_line, column, end_column):
                return True
            # CPython gives a continued fluent call the method-token span,
            # while AST Call starts at the entire receiver expression. Both
            # start (exact attribute token) AND end must agree; never nearest line.
            return (isinstance(n.func, ast.Attribute)
                    and (n.func.end_lineno, n.func.end_col_offset - len(n.func.attr.encode())) == (line, column)
                    and (n.end_lineno, n.end_col_offset) == (end_line, end_column))
        candidates = [n for n in ast.walk(tree) if at_executed_position(n)]
        if len(candidates) != 1:
            return unavailable('无法唯一定位这次 API 调用的源码表达式。')
        call = candidates[0]
        full = ast.get_source_segment(text, call)
        invocation = full
        if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Call):
            # The selected link of a fluent chain, with exact surrounding code
            # preserved separately. No synthetic receiver variable is invented.
            from types import SimpleNamespace
            start = call.func.end_col_offset - len(call.func.attr.encode()) - 1
            line_bytes = text.splitlines()[call.func.end_lineno - 1].encode()
            if start >= 0 and line_bytes[start:start + 1] == b'.':
                span = SimpleNamespace(lineno=call.func.end_lineno, col_offset=start,
                    end_lineno=call.end_lineno, end_col_offset=call.end_col_offset)
                invocation = ast.get_source_segment(text, span)
            # Unusual whitespace around the attribute keeps the complete exact
            # expression instead of fabricating a shortened call.

        expressions = {kw.arg: ast.get_source_segment(text, kw.value) for kw in call.keywords if kw.arg}
        positional = [p for p in arguments if p['name'] != 'self' and p['parameter_kind'] in ('POSITIONAL_ONLY', 'POSITIONAL_OR_KEYWORD')]
        for parameter, expression in zip(positional, call.args):
            expressions[parameter['name']] = ast.get_source_segment(text, expression)
        if isinstance(call.func, ast.Attribute):
            expressions['self'] = ast.get_source_segment(text, call.func.value)
        expansions = [ast.get_source_segment(text, kw.value) for kw in call.keywords if kw.arg is None]
        rows = [dict(p, expression=expressions.get(p['name']),
                     origin='explicit' if p['name'] in expressions else 'expanded_or_default' if expansions else 'default')
                for p in arguments]
        if api not in self.api_definitions:
            from .api_definition import current
            self.api_definitions[api] = current(api)
        return material(invocation, language='python', path=relative, sha256=expected,
                        start_line=line, end_line=end_line,
                        metadata=dict(api=api, definition=self.api_definitions[api], arguments=rows, expansions=expansions,
                                      context=full[:32768], context_truncated=len(full) > 32768,
                                      summary=' '.join(invocation.split())[:256]))


def pipeline_material(root, target, sources):
    """Snapshot the declared entry file, including code after the first action."""
    relative = target.relative_to(root).as_posix()
    ws.no_symlinks(target)
    raw = read_source_bytes(target)
    expected = sources[relative]
    if hashlib.sha256(raw).hexdigest() != expected:
        return unavailable('入口源码在解析期间发生变化。')
    text = raw.decode('utf-8-sig')
    return material(text, language='python', path=relative, sha256=expected,
                    start_line=1, end_line=len(text.splitlines()))
