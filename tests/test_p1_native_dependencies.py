"""Pin retained native construction/uses and the narrow SDK assembly imports.

The approved native expressions come from 568d55c, not the implementation under
review. This is a declaration boundary ratchet, not a Python security sandbox.
Changes to constructors, instance method references or execution entry imports
require an explicit reviewed update of the corresponding fixture expression.
"""
import ast
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/p1-native-uses.json'
NATIVE_METHODS = {
    'map_cached': ('CachedMap', 'CachedSyncMap'),
    'enqueue': ('QueueProducer',),
    'ack_queue': ('QueueAcknowledge',),
    'fetch': ('RawFetch',),
    'lookup_lance': ('LanceLookup',),
    'admit_rows': ('StreamAdmission',),
    'save_lance': ('StreamLanceSink',),
}


def _scoped(tree):
    """Yield each node with its exact lexical scope and parent links."""
    parents, scopes = {}, {}
    def walk(node, scope=()):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            scope = (*scope, node.name)
        scopes[node] = '.'.join(scope)
        for child in ast.iter_child_nodes(node):
            parents[child] = node
            walk(child, scope)
    walk(tree)
    return parents, scopes


def _expression(node):
    return ast.unparse(node)


def native_uses(source):
    tree = ast.parse(source)
    parents, scopes = _scoped(tree)
    methods = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'Dataset')
    result = {}
    all_classes = {name for names in NATIVE_METHODS.values() for name in names}
    # Class imports are guarded even for cache classes inside the SDK package.
    result['imports'] = sorted([
        [scopes[node], _expression(node)] for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and any(alias.name in all_classes for alias in node.names)
    ])
    for name, constructors in NATIVE_METHODS.items():
        method = next(node for node in methods.body if isinstance(node, ast.FunctionDef) and node.name == name)
        names = set(constructors)
        for node in ast.walk(method):
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name) and node.value.func.id in constructors):
                names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        statements = {}
        for node in ast.walk(method):
            if not isinstance(node, ast.Name) or node.id not in names:
                continue
            statement = node
            while not isinstance(statement, ast.stmt):
                statement = parents[statement]
            # Keep conditional location, e.g. synchronous cached construction.
            context, parent = [], parents[statement]
            child = statement
            while parent is not method:
                if isinstance(parent, ast.If):
                    branch = 'body' if child in parent.body else 'orelse'
                    context.append(['if', _expression(parent.test), branch])
                child, parent = parent, parents[parent]
            statements[statement] = [list(reversed(context)), _expression(statement)]
        result[name] = [value for node, value in sorted(statements.items(), key=lambda item: (item[0].lineno, item[0].col_offset))]
    return result


def _module(path, node):
    if not node.level:
        return node.module or ''
    package = path.with_suffix('').parts[:-1]
    base = package[:len(package) - node.level + 1]
    return '.'.join((*base, *((node.module or '').split('.') if node.module else ())))


def execution_entries(path, source):
    tree = ast.parse(source)
    parents, scopes = _scoped(tree)
    imports, bindings = [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = _module(path, node)
            for alias in node.names:
                target = module + '.' + alias.name
                if module == 'demiflow.execution' or module.startswith('demiflow.execution.'):
                    imports.append([scopes[node], module, alias.name, alias.asname])
                    bindings.append((scopes[node], alias.asname or alias.name))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'demiflow.execution' or alias.name.startswith('demiflow.execution.'):
                    imports.append([scopes[node], alias.name, '<module>', alias.asname])
                    bindings.append((scopes[node], alias.asname or alias.name.split('.')[0]))
    # Retained source/default-host constructors also constrain subsequent
    # instance uses, so a new source.claim()/executor.close() cannot hide
    # behind an already permitted class import.
    constructors = {(scope, name) for scope, name in bindings if name[:1].isupper()}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and (scopes[node], node.value.func.id) in constructors):
            bindings.extend((scopes[node], target.id) for target in node.targets if isinstance(target, ast.Name))
    uses = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Name):
            continue
        if not any(node.id == name and (not scope or scopes[node] == scope or scopes[node].startswith(scope + '.'))
                   for scope, name in bindings):
            continue
        value = node
        while isinstance(parents.get(value), ast.Attribute) and parents[value].value is value:
            value = parents[value]
        parent = parents.get(value)
        if isinstance(parent, ast.Call) and parent.func is value:
            value = parent
        uses.append([scopes[node], _expression(value)])
    return {'imports': sorted(imports, key=str), 'uses': sorted(uses, key=str)}


def assert_native_boundary(source, expected):
    assert native_uses(source) == expected, 'Retained native constructor/instance use changed'


def test_retained_native_classes_only_have_original_construction_and_uses():
    fixture = json.loads(FIXTURE.read_text())
    assert fixture['native_baseline'] == '568d55c90b696ad6b76fc34bb080b9f5aa2a408a'
    # The only approved expression change is the already-reviewed pure
    # preflight rename in save_lance; constructor/instance arguments are exact.
    assert fixture['approved_native_delta'] == 'save_lance: make_native_operation -> validate_native_operation'
    assert_native_boundary((ROOT / 'demiflow/data/dataset.py').read_text(), fixture['native_uses'])


def test_sdk_assembly_execution_entries_are_exact_and_scoped():
    fixture = json.loads(FIXTURE.read_text())
    for filename, expected in fixture['assembly_entries'].items():
        assert execution_entries(Path(filename), (ROOT / filename).read_text()) == expected, filename


@pytest.mark.parametrize('added', ['actor.restore_state({})', 'actor.close()', 'resume = actor.restore_state'])
def test_guard_rejects_added_native_runtime_calls_and_method_references(added):
    source = (ROOT / 'demiflow/data/dataset.py').read_text()
    expected = json.loads(FIXTURE.read_text())['native_uses']
    tree = ast.parse(source)
    method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'enqueue')
    method.body.insert(-1, ast.parse(added).body[0])
    with pytest.raises(AssertionError, match='constructor/instance use changed'):
        assert_native_boundary(ast.unparse(tree), expected)


def test_guard_rejects_changed_native_constructor_options():
    source = (ROOT / 'demiflow/data/dataset.py').read_text()
    expected = json.loads(FIXTURE.read_text())['native_uses']
    tree = ast.parse(source)
    constructor = next(node for node in ast.walk(tree) if isinstance(node, ast.Call)
                       and isinstance(node.func, ast.Name) and node.func.id == 'QueueProducer')
    next(keyword for keyword in constructor.keywords if keyword.arg == 'queue_depth').value = ast.Constant(999)
    with pytest.raises(AssertionError, match='constructor/instance use changed'):
        assert_native_boundary(ast.unparse(tree), expected)


def test_guard_rejects_an_action_import_in_a_reader():
    filename = 'demiflow/data/api.py'
    tree = ast.parse((ROOT / filename).read_text())
    method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'from_items')
    method.body.insert(1, ast.ImportFrom(module='execution.checked', names=[ast.alias(name='action')], level=2))
    expected = json.loads(FIXTURE.read_text())['assembly_entries'][filename]
    assert execution_entries(Path(filename), ast.unparse(tree)) != expected
