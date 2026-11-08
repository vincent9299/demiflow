"""Bounded, opt-in benchmark controls; never imported by the platform package."""
import ast
from copy import deepcopy
import inspect

from demiflow import schema as authority


def lazy_python_validator():
    """Keep every original check; evaluate only the selected type predicate."""
    tree = ast.parse(inspect.getsource(authority._validate))
    class LazyTypes(ast.NodeTransformer):
        replaced = 0
        def visit_Assign(self, node):
            if (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == 'valid' and isinstance(node.value, ast.Subscript)
                    and isinstance(node.value.value, ast.Dict)):
                self.replaced += 1
                pairs = zip(node.value.value.keys, node.value.value.values)
                branch = None
                for key, value in reversed(list(pairs)):
                    branch = ast.If(test=ast.Compare(left=ast.Name(id='kind', ctx=ast.Load()),
                        ops=[ast.Eq()], comparators=[key]),
                        body=[ast.Assign(targets=node.targets, value=value)],
                        orelse=[] if branch is None else [branch])
                return ast.copy_location(branch, node)
            return self.generic_visit(node)
    change = LazyTypes(); tree = change.visit(tree)
    if change.replaced != 1: raise RuntimeError('Original validator changed; review the control')
    scope = vars(authority).copy()
    exec(compile(ast.fix_missing_locations(tree), '<lazy-python-schema-control>', 'exec'), scope)
    def validate(value, schema, *, label='value'):
        scope['_validate'](value, schema, '$', label)
    return validate


def bounded_snapshot(schema):
    """Bound the copy before allocating it, including strings and huge integers."""
    nodes, chars = 0, 0
    def visit(value, depth):
        nonlocal nodes, chars
        nodes += 1
        if depth > 40 or nodes > 8192: raise ValueError('Schema snapshot budget')
        if type(value) is dict:
            if len(value) > 512: raise ValueError('Schema mapping budget')
            for key, child in value.items():
                if type(key) is not str: raise ValueError('Non-JSON schema key')
                visit(key, depth + 1); visit(child, depth + 1)
        elif type(value) is list:
            if len(value) > 512: raise ValueError('Schema list budget')
            for child in value: visit(child, depth + 1)
        elif type(value) is str:
            chars += len(value)
            if len(value) > 65536 or chars > 262144: raise ValueError('Schema text budget')
        elif type(value) is int:
            if value.bit_length() > 4096: raise ValueError('Schema integer budget')
        elif type(value) not in (float, bool, type(None)): raise ValueError('Non-JSON schema')
    visit(schema, 0)
    return deepcopy(schema)


def python_predicate(schema):
    """Compiled Python control for the same supported subset and traversal cap."""
    kind = schema['type']
    props = {k: python_predicate(v) for k, v in schema.get('properties', {}).items()}
    keys = frozenset(props); required = frozenset(schema.get('required', ()))
    extra = schema.get('additionalProperties', False)
    item = python_predicate(schema['items']) if kind == 'array' else None
    low = schema.get('minItems' if kind == 'array' else 'minLength')
    high = schema.get('maxItems' if kind == 'array' else 'maxLength')
    sentinel = object(); constant = schema.get('const', sentinel)
    choices = tuple(schema['enum']) if 'enum' in schema else None
    bounds = tuple((k, schema[k]) for k in ('minimum','maximum','exclusiveMinimum','exclusiveMaximum') if k in schema)
    expected = {'object': dict, 'array': list, 'string': str, 'integer': int,
                'number': (int, float), 'boolean': bool, 'null': type(None)}[kind]
    def accepts(value, budget):
        if budget[0] == 0: return False
        budget[0] -= 1
        if (type(value) not in expected if kind == 'number' else type(value) is not expected): return False
        if kind == 'object':
            if len(value) > budget[0]: return False
            budget[0] -= len(value)
            if any(type(k) is not str for k in value): return False
            if not required.issubset(value) or (not extra and not keys.issuperset(value)): return False
            for key, child in props.items():
                if key in value and not child(value[key], budget): return False
        elif kind in ('array', 'string'):
            size = len(value)
            if (low is not None and size < low) or (high is not None and size > high): return False
            if kind == 'array':
                if size > budget[0]: return False
                for v in value:
                    if not item(v, budget): return False
        if constant is not sentinel and value != constant: return False
        if choices is not None and value not in choices: return False
        for key, bound in bounds:
            if key == 'minimum' and value < bound: return False
            if key == 'maximum' and value > bound: return False
            if key == 'exclusiveMinimum' and value <= bound: return False
            if key == 'exclusiveMaximum' and value >= bound: return False
        return True
    return accepts


class SnapshotValidator:
    """One immutable experiment snapshot; mutation forces authoritative fallback."""
    def __init__(self, schema, backend='rust'):
        self.snapshot = None
        self.predicate = None
        try:
            snapshot = bounded_snapshot(schema)
            from _demiflow_schema_probe import Predicate
            compiled = Predicate(snapshot)
        except (ValueError, TypeError, OverflowError):
            return
        self.snapshot = snapshot
        if backend == 'rust':
            self.predicate = compiled.accepts
        else:
            accepts = python_predicate(snapshot)
            self.predicate = lambda value: accepts(value, [65536])

    def __call__(self, value, schema, *, label='value'):
        if self.snapshot is not None and same_json_schema(schema, self.snapshot):
            if self.predicate(value): return
        authority.validate_instance(value, schema, label=label)


def same_json_schema(current, snapshot):
    # Do not trust equality supplied by a Mapping/str/numeric subclass after
    # mutation. Traversal is bounded by the already checked snapshot's shape.
    if type(current) is not type(snapshot): return False
    if type(snapshot) is dict:
        return (len(current) == len(snapshot) and all(type(k) is str for k in current)
                and all(k in current and same_json_schema(current[k], v) for k, v in snapshot.items()))
    if type(snapshot) is list:
        return len(current) == len(snapshot) and all(same_json_schema(a, b) for a, b in zip(current, snapshot))
    return current == snapshot


def variants(schema):
    return {'baseline': authority.validate_instance, 'python-lazy': lazy_python_validator(),
            'python-compiled': SnapshotValidator(schema, 'python'), 'rust': SnapshotValidator(schema)}
