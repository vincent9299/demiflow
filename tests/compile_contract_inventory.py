"""Source-level inventory of the compile <-> runtime contract.

The facts about one operator kind live in many tables and branches across
``planning`` and ``execution``. This module reads them from source (nothing is
imported or executed) and assembles one view, so that the facts can be pinned
by a test and later moved into one registration without changing any of them.

It is an inventory, not a second table: every value is read from where the
engine keeps it today. ``python tests/compile_contract_inventory.py --write``
regenerates the pinned snapshot after a deliberate change.
"""
import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / 'demiflow'
SNAPSHOT = Path(__file__).with_name('fixtures') / 'compile-contract-inventory.json'

COMPILER = 'planning/dataflow_compile.py'
REGISTRY = 'planning/dataflow_registry.py'
KERNELS = 'execution/kernels/dataflow.py'
OPERATOR = 'execution/kernels/operator.py'
TERMINAL_BLOCKS = 'execution/outputs/terminal_blocks.py'
SEAL = 'execution/seal/plan.py'
KINDS = 'execution/contracts/dataflow_kinds.py'
API = 'execution/executors/dataflow_api.py'
SDK = 'data/dataset.py'
DECLARATION = 'planning/dataflow_declaration.py'
# An SDK entry preflights through the pure validator, which returns nothing. The node
# constructor is recognised too: an entry that called it and kept the result would be listed as used.
PREFLIGHT = ('validate_native_operation', 'make_native_operation')
NATIVE_DECLARATION = 'execution/operator_bindings/dataflow_model_factory.py'
PLAN_FACTS = 'execution/seal/plan_facts.py'
LEGACY_TRAITS = 'planning/traits.py'
# Files on the call chain of capture_graph(): the compile stage proper.
COMPILE_STAGE = (COMPILER, 'planning/dataflow_shared.py', 'planning/dataflow_graph_adapter.py',
                 REGISTRY, 'planning/dataflow_relational_regions.py')
# The one module that reads the compiler's inputs from its surroundings and hands them over explicitly.
INPUT_RESOLVER = 'execution/compiler_support/dataflow_compile_inputs.py'


def source(relative):
    return (PACKAGE / relative).read_text(encoding='utf-8')


class Constants:
    """Evaluate module-level literal tables without importing the module."""

    def __init__(self, relative, imports=None):
        self.relative = relative
        self.tree = ast.parse(source(relative))
        self.imports = imports or {}
        self.assignments = {}
        for node in self.tree.body:
            target, value = None, None
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target, value = node.targets[0], node.value
            elif isinstance(node, ast.AnnAssign):
                target, value = node.target, node.value
            if isinstance(target, ast.Name) and value is not None:
                self.assignments[target.id] = value

    def value(self, name):
        if name in self.assignments:
            return self.evaluate(self.assignments[name])
        if name in self.imports:
            return self.imports[name].value(name)
        if self.relative != KINDS and name in ('TYPED', 'NATIVE'):
            return kind_table().value(name)          # imported from the kind table
        raise KeyError(f'{self.relative}: {name} is not a module-level constant')

    def evaluate(self, node):
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return self.value(node.id)
        if isinstance(node, (ast.Tuple, ast.List)):
            return tuple(self.evaluate(item) for item in node.elts)
        if isinstance(node, ast.Set):
            return frozenset(self.evaluate(item) for item in node.elts)
        if isinstance(node, ast.Dict):
            return {self.evaluate(key): self.evaluate(item) for key, item in zip(node.keys, node.values)}
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return frozenset(self.evaluate(node.left)) | frozenset(self.evaluate(node.right))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.evaluate(node.left) + self.evaluate(node.right)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and len(node.args) == 1:
            if node.func.id == 'frozenset':
                return frozenset(self.evaluate(node.args[0]))
            if node.func.id == 'MappingProxyType':
                return self.evaluate(node.args[0])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            arguments = [self.evaluate(argument) for argument in node.args]
            if node.func.id == '_kind' and len(arguments) == 5:      # one row of the kind table
                return tuple(arguments)
            # Views of the kind table, evaluated from its rows exactly as the module does.
            if node.func.id == 'kinds_with' and len(arguments) == 1:
                return kind_table().with_role(arguments[0])
            if node.func.id == 'traits_of' and len(arguments) == 1:
                return kind_table().traits_of(arguments[0])
            if node.func.id == 'operator_rows' and not arguments:
                return kind_table().operator_rows()
        raise ValueError(f'{self.relative}: unsupported constant expression at line {node.lineno}')


class KindTable(Constants):
    """The rows of execution/contracts/dataflow_kinds.py, read from source."""

    def rows(self):
        return self.value('_RECORDS')          # (name, entry, roles, operator, traits) per kind

    def with_role(self, role):
        assert role in self.value('ROLES'), role
        return frozenset(name for name, _, roles, _, _ in self.rows() if role in roles)

    def traits_of(self, entry):
        return {name: traits for name, declared, _, _, traits in self.rows() if declared == entry}

    def operator_rows(self):
        return tuple((name, *operator) for name, _, _, operator, _ in self.rows() if operator is not None)


def kind_table():
    return KindTable(KINDS)


def _sorted(value):
    """Canonical JSON form: sets and tuples become sorted/ordered lists."""
    if isinstance(value, (set, frozenset)):
        return sorted(_sorted(item) for item in value)
    if isinstance(value, (tuple, list)):
        return [_sorted(item) for item in value]
    if isinstance(value, dict):
        return {key: _sorted(item) for key, item in sorted(value.items())}
    return value


def tables():
    registry, compiler = Constants(REGISTRY), Constants(COMPILER)
    kernels = Constants(KERNELS)
    kernel_sets = ('_UNARY_KINDS', '_BATCH_KINDS', '_MODEL_KINDS', '_RELATIONAL_KINDS',
                   '_ORDERING_KINDS', '_AGGREGATE_KINDS', '_STREAM_GROUP_KINDS', 'KERNEL_KINDS')
    operator = Constants(OPERATOR, imports={name: kernels for name in kernel_sets})
    result = {
        'registry.operators': {kind: {'compiler_symbol': compiler_symbol, 'kernel_symbol': kernel, 'inputs': inputs}
                               for kind, compiler_symbol, kernel, inputs in registry.value('_OPERATORS')},
        'registry.sources': {kind: {'reader_symbol': reader, 'cursor_fields': fields}
                             for kind, reader, fields in registry.value('_SOURCES')},
        'registry.terminals': {kind: {'adapter_symbol': adapter, 'result_kind': result_kind}
                               for kind, adapter, result_kind in registry.value('_TERMINALS_TABLE')},
        'compiler.traits': compiler.value('_TRAITS'),
        'compiler.native_traits': compiler.value('_NATIVE_TRAITS'),
        'compiler.finite_complete_kinds': compiler.value('_FINITE_COMPLETE_KINDS'),
        'compiler.ordering_kinds': compiler.value('_ORDERING_KINDS'),
        'runtime.task_kinds': kind_table().value('TASK_KINDS'),
        'runtime.batch_membership_kinds': kind_table().value('NATIVE_BATCH_KINDS'),
        'runtime.instance_kinds': operator.value('_INSTANCE_KINDS'),
        'runtime.batch_instance_kinds': operator.value('_BATCH_INSTANCE_KINDS'),
    }
    for name in kernel_sets:
        result['runtime.' + name.strip('_').lower()] = kernels.value(name)
    result['runtime.layout_preserving_kinds'] = kind_table().with_role('keeps_layout')
    return result


# Each runtime decision is an expression over the sets above, written at its use
# site. ``snippet`` must still be present there: when a use site changes, the
# decision recorded here has to be revisited, not silently kept.
PREDICATES = (
    ('admitted', KERNELS, 'if kind not in KERNEL_KINDS:',
     lambda t, kind: kind in t['runtime.kernel_kinds']),
    ('needs_instance', OPERATOR, 'if self._spec.kind in _INSTANCE_KINDS:',
     lambda t, kind: kind in t['runtime.instance_kinds']),
    ('row_call', OPERATOR, 'if kind in _UNARY_KINDS or (kind in _MODEL_KINDS and kind not in _BATCH_INSTANCE_KINDS):',
     lambda t, kind: kind in t['runtime.unary_kinds'] or (
         kind in t['runtime.model_kinds'] and kind not in t['runtime.batch_instance_kinds'])),
    ('row_call_uses_slot_instance', OPERATOR, 'instance=self._slot_instance(0) if kind in _INSTANCE_KINDS else self._instance,',
     lambda t, kind: kind in t['runtime.instance_kinds']),
    ('batch_call', OPERATOR, 'elif kind in _BATCH_INSTANCE_KINDS:',
     lambda t, kind: kind in t['runtime.batch_instance_kinds']),
    ('stream_group', OPERATOR, 'elif kind in _STREAM_GROUP_KINDS:',
     lambda t, kind: kind in t['runtime.stream_group_kinds']),
    ('ordering_finish', OPERATOR, 'elif kind in _ORDERING_KINDS:',
     lambda t, kind: kind in t['runtime.ordering_kinds']),
    ('aggregate_finish', OPERATOR, 'elif kind in _AGGREGATE_KINDS:',
     lambda t, kind: kind in t['runtime.aggregate_kinds']),
    ('native_batch_membership', OPERATOR, 'if self._spec.kind in KINDS:',
     lambda t, kind: kind in t['runtime.batch_membership_kinds']),
    ('layout_preserving', TERMINAL_BLOCKS, "preserving = kinds_with('keeps_layout')",
     lambda t, kind: kind in t['runtime.layout_preserving_kinds']),
    ('task_scoped', COMPILER, "if kernel.kind in TASK_KINDS and not kernel.configuration.get('streaming_map'):",
     lambda t, kind: kind in t['runtime.task_kinds']),
    ('input_finite_complete', COMPILER, 'finite = kind in _FINITE_COMPLETE_KINDS or name in {"left", "right", "prefix"}',
     lambda t, kind: kind in t['compiler.finite_complete_kinds']),
    ('input_global_ordering', COMPILER, 'ordering = "global" if kind in _ORDERING_KINDS else "none"',
     lambda t, kind: kind in t['compiler.ordering_kinds']),
    ('sealable_without_program_as_transform', SEAL, 'return node.kind != "source" and node.kernel.kind in table',
     lambda t, kind: kind in t['registry.operators']),
)
UNKNOWN_KINDS = ('NoSuchOp', 'no_such_native')


def universe(t):
    return sorted(set(t['registry.operators']) | set(t['registry.sources']) | set(t['registry.terminals'])
                  | set(t['runtime.kernel_kinds']) | set(UNKNOWN_KINDS))


def predicates(t):
    return {kind: {name: bool(rule(t, kind)) for name, _, _, rule in PREDICATES} for kind in universe(t)}


def missing_predicate_snippets():
    return sorted((name, relative) for name, relative, snippet, _ in PREDICATES
                  if snippet not in source(relative))


def differences(t):
    operators, admitted = set(t['registry.operators']), set(t['runtime.kernel_kinds'])
    traits = set(t['compiler.traits']) | set(t['compiler.native_traits'])
    overlap = defaultdict(list)
    for name in ('unary', 'batch', 'model', 'relational', 'ordering', 'aggregate', 'stream_group'):
        for kind in t['runtime.' + name + '_kinds']:
            overlap[kind].append(name)
    return {
        'registered_operators_without_traits': operators - traits,
        'traits_for_unregistered_operators': traits - operators,
        'registered_operators_not_admitted_by_runtime': operators - admitted,
        'runtime_kinds_not_registered_anywhere': admitted - operators - set(t['registry.sources']) - set(t['registry.terminals']),
        'runtime_kinds_registered_as_sources': admitted & set(t['registry.sources']),
        'runtime_kinds_registered_as_terminals': admitted & set(t['registry.terminals']),
        'runtime_kinds_in_several_sets': {kind: names for kind, names in overlap.items() if len(names) > 1},
        'ordering_kinds_runtime_minus_compiler': set(t['runtime.ordering_kinds']) - set(t['compiler.ordering_kinds']),
    }


def _calls(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and (getattr(node.func, 'id', None) == name
                                           or getattr(node.func, 'attr', None) == name):
            yield node


def entries():
    """SDK entry names: preflight declarations whose result is discarded."""
    tree = ast.parse(source(SDK))
    discarded, used = [], []
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef):
            continue
        for node in ast.walk(function):
            call = node.value if isinstance(node, (ast.Expr, ast.Assign, ast.Return)) else None
            if not (isinstance(call, ast.Call) and getattr(call.func, 'id', None) in PREFLIGHT):
                continue
            item = [function.name, call.args[0].value]
            (discarded if isinstance(node, ast.Expr) else used).append(item)
    # The registry check both entries share reads its aliases and exemptions from the kind table.
    declaration = ast.parse(source(DECLARATION))
    read = {node.id for function in ast.walk(declaration)
            if isinstance(function, ast.FunctionDef) and function.name == 'validate_native_kind'
            for node in ast.walk(function) if isinstance(node, ast.Name)}
    kinds = kind_table()
    aliases = dict(kinds.value('NATIVE_DECLARATION_ALIASES')) if 'NATIVE_DECLARATION_ALIASES' in read else {}
    exempt = sorted(kinds.value('NATIVE_DECLARATION_ONLY')) if 'NATIVE_DECLARATION_ONLY' in read else []
    return {'preflight_result_discarded': sorted(discarded), 'preflight_result_used': sorted(used),
            'aliases': aliases, 'names_exempt_from_registry': exempt}


_GENERIC = {'FunctionType', 'BuiltinFunctionType', 'MethodWrapperType', 'CallableSpec', 'LogicalOp',
            'Mapping', 'SourcePlan', 'LogicalPlan'}


def _imported_type_predicates(function, relative):
    """Resolve local is_* predicates only when the dispatcher calls the imported name."""
    nodes = list(ast.walk(function))
    calls = {n.func.id for n in nodes if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    for node in nodes:
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if node.level:
            target = Path(relative).parents[node.level - 1] / (node.module.replace('.', '/') + '.py')
        elif node.module.startswith('demiflow.'):
            target = Path(node.module.removeprefix('demiflow.').replace('.', '/') + '.py')
        else:
            continue
        for alias in node.names:
            if alias.name.startswith('is_') and (alias.asname or alias.name) in calls:
                yield target, alias.name


def _classes_tested(relative, functions=None, *, follow_predicates=False):
    found = set()
    for function in ast.walk(ast.parse(source(relative))):
        if not isinstance(function, ast.FunctionDef) or (functions and function.name not in functions):
            continue
        if follow_predicates:
            for target, predicate in _imported_type_predicates(function, relative):
                found.update(_classes_tested(target, {predicate}))
        for node in ast.walk(function):
            candidates = []
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'isinstance' and len(node.args) == 2:
                candidates.append(node.args[1])
            elif isinstance(node, ast.Compare) and isinstance(node.ops[0], (ast.Is, ast.IsNot, ast.In, ast.NotIn)):
                left = node.left
                if (isinstance(left, ast.Call) and getattr(left.func, 'id', None) == 'type') or isinstance(
                        node.ops[0], (ast.Is, ast.IsNot)):
                    candidates.extend(node.comparators)
            for candidate in candidates:
                for item in ast.walk(candidate):
                    name = item.id if isinstance(item, ast.Name) else item.attr if isinstance(item, ast.Attribute) else None
                    if name and name[:1].isupper() and not name.isupper() and name not in _GENERIC:
                        found.add(name)
    return found


def class_dispatch():
    return {
        'execution.native_model_declaration': _classes_tested(
            NATIVE_DECLARATION, {'native_model_declaration'}, follow_predicates=True),
        'execution.plan_facts': _classes_tested(
            PLAN_FACTS, {'operation_facts', 'operation_capability'}, follow_predicates=True),
        'planning.compiler': _classes_tested(COMPILER),
        'planning.legacy_traits': _classes_tested(LEGACY_TRAITS),
    }


def implicit_inputs(files=COMPILE_STAGE):
    """Ambient reads in the compile-stage files, as ``file::function: source line``.

    Not every function in these files runs during compilation; the enclosing
    function is recorded so the two can be told apart.
    """
    patterns = (r'_dataflow_executor', r'\bcurrent\(\)', r'os\.environ', r'ContextVar\(', r'\.journal\b',
                r'\.directory\b', r'_local_kernel', r'_block_size')
    found = []
    for relative in files:
        text = source(relative)
        lines = text.splitlines()
        owner = {}
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for number in range(node.lineno, node.end_lineno + 1):
                    # Innermost function wins: later (nested) definitions overwrite.
                    if number not in owner or owner[number][1] <= node.lineno:
                        owner[number] = (node.name, node.lineno)
        for number, line in enumerate(lines, 1):
            if any(re.search(pattern, line) for pattern in patterns):
                found.append(relative + '::' + owner.get(number, ('<module>', 0))[0] + ': ' + line.strip())
    return sorted(found)


def _is_configuration(node):
    return (isinstance(node, ast.Attribute) and node.attr == 'configuration') or (
        isinstance(node, ast.Name) and node.id == 'configuration')


def configuration_access():
    """Kernel configuration keys by the way source reaches them.

    literal: ``x.configuration.get('k')``, ``x.configuration['k']``, ``'k' in x.configuration``
    accessor: ``_cfg(spec, 'k')``
    constant: the same three forms with a NAME as key (the key's value lives elsewhere)
    written: subscript stores, ``configuration.update(k=...)`` and ``configuration={...}`` literals
    """
    literal, accessor, constant, written = (defaultdict(set) for _ in range(4))
    for package in ('planning', 'execution', 'operators'):
        for path in sorted((PACKAGE / package).rglob('*.py')):
            relative = path.relative_to(PACKAGE).as_posix()
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                key, store = None, False
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == 'get' and _is_configuration(node.func.value) and node.args):
                    key = node.args[0]
                elif isinstance(node, ast.Subscript) and _is_configuration(node.value):
                    key, store = node.slice, isinstance(node.ctx, ast.Store)
                elif (isinstance(node, ast.Compare) and len(node.ops) == 1 and isinstance(node.ops[0], (ast.In, ast.NotIn))
                      and _is_configuration(node.comparators[0])):
                    key = node.left
                elif isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_cfg' and len(node.args) >= 2:
                    if isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                        accessor[node.args[1].value].add(relative)
                    continue
                elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                      and node.func.attr == 'update' and _is_configuration(node.func.value)):
                    for keyword in node.keywords:
                        if keyword.arg:
                            written[keyword.arg].add(relative)
                    continue
                elif isinstance(node, ast.keyword) and node.arg == 'configuration' and isinstance(node.value, ast.Dict):
                    for item in node.value.keys:
                        if isinstance(item, ast.Constant) and isinstance(item.value, str):
                            written[item.value].add(relative)
                    continue
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    (written if store else literal)[key.value].add(relative)
                elif isinstance(key, ast.Name) and key.id.isupper():
                    constant[key.id].add(relative)
    return {name: {key: sorted(files) for key, files in sorted(table.items())}
            for name, table in (('literal', literal), ('accessor', accessor), ('constant', constant), ('written', written))}


def inventory():
    t = tables()
    return _sorted({
        'tables': t,
        'predicates': predicates(t),
        'differences': differences(t),
        'entries': entries(),
        'class_dispatch': class_dispatch(),
        'implicit_inputs': implicit_inputs(),
        'input_resolver': implicit_inputs((INPUT_RESOLVER,)),
        'configuration_access': configuration_access(),
    })


def render(value):
    return json.dumps(value, indent=1, sort_keys=True, ensure_ascii=False) + '\n'


if __name__ == '__main__':
    if '--write' in sys.argv:
        SNAPSHOT.parent.mkdir(exist_ok=True)
        SNAPSHOT.write_text(render(inventory()), encoding='utf-8')
        print('wrote', SNAPSHOT)
    else:
        sys.stdout.write(render(inventory()))
