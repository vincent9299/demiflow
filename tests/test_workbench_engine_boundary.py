"""Finite static regression checks for the approved host and independent domains.

This checks trusted platform dependencies, not arbitrary Python sandboxing. The
native-owned literal manifest drives both exports and documentation. Known
object origins flow through aliases, constructors, context managers and material
collections; unresolved reflection on engine objects never becomes permission.
"""
import ast
import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
WORKBENCH = REPO / 'demiflow/workbench'
HOST = REPO / 'demiflow/execution/host'
SURFACE = ast.literal_eval(ast.parse((HOST / '_surface.py').read_text()).body[1].value)


def _module(path):
    parts = Path(path).with_suffix('').parts
    return '.'.join(parts[:-1] if parts[-1] == '__init__' else parts)


def _imports(tree, path):
    current = _module(path)
    package = current if str(path).endswith('/__init__.py') else current.rpartition('.')[0]
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ''
            if node.level:
                module = importlib.util.resolve_name('.' * node.level + module, package)
            for item in node.names:
                yield node, module, item.name, item.asname or item.name
        elif isinstance(node, ast.Import):
            for item in node.names:
                yield node, item.name, '<module>', item.asname or item.name.split('.')[0]


def _import_errors(tree, path):
    errors = []
    for node, module, name, alias in _imports(tree, path):
        if not (module == 'demiflow' or module.startswith('demiflow.')):
            continue
        if module.startswith(('demiflow.workbench.', 'demiflow.forge.')) or module in ('demiflow.workbench', 'demiflow.forge'):
            continue
        side = module.removeprefix('demiflow.execution.host.')
        if side in SURFACE['exports'] and name in SURFACE['exports'][side]:
            continue
        allowed = SURFACE['independent_imports'].get(module, {}).get(name, [])
        if str(path) in allowed:
            continue
        errors.append((node.lineno, 'unapproved import', module, name))
    return errors


class Uses(ast.NodeVisitor):
    """Small origin interpreter, deliberately scoped to approved platform types."""
    def __init__(self, tree, path):
        self.path, self.function = str(path), '<module>'
        self.env = {alias: SURFACE['reexport_origins'].get(
                        module if name == '<module>' else module + '.' + name,
                        module if name == '<module>' else module + '.' + name)
                    for _, module, name, alias in _imports(tree, path)}
        self.parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        self.errors = []
        for function in ast.walk(tree):
            if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.env.setdefault(function.name, _module(path) + '.' + function.name)
                results = [self.origin(n.value) for n in ast.walk(function)
                           if isinstance(n, ast.Return) and n.value is not None]
                if results and len(set(results)) == 1 and results[0] in SURFACE['objects']:
                    self.env[function.name] = 'factory:' + results[0]

    def problem(self, node, message):
        self.errors.append((node.lineno, message))

    def origin(self, node):
        if isinstance(node, ast.Name):
            return self.env.get(node.id, 'builtins.' + node.id if node.id in ('getattr', 'vars', '__import__', 'object') else None)
        if isinstance(node, ast.Attribute):
            base = self.origin(node.value)
            if base:
                key = base + '.' + node.attr
                key = SURFACE['reexport_origins'].get(key, key)
                return SURFACE['result_origins'].get(key, key)
        if isinstance(node, ast.Subscript):
            base = self.origin(node.value)
            if base and base.startswith(('map:', 'seq:')):
                return base[4:]
            if base in SURFACE['record_fields'] and isinstance(node.slice, ast.Constant):
                return SURFACE['record_fields'][base].get(node.slice.value)
        if isinstance(node, ast.Tuple):
            return 'tuple:' + ','.join(self.origin(item) or 'unknown' for item in node.elts)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == 'enter_context' and node.args \
                    and self.origin(node.func.value) == 'stdlib:ExitStack':
                return self.origin(node.args[0])
            target = self.origin(node.func)
            if target and target.endswith('.get') and target[:-4] in SURFACE['record_fields']:
                if node.args and isinstance(node.args[0], ast.Constant):
                    return SURFACE['record_fields'][target[:-4]].get(node.args[0].value)
            if isinstance(node.func, ast.Name) and node.func.id == 'dict' and len(node.args) == 1:
                original = self.origin(node.args[0])
                if original in SURFACE['record_fields']:
                    return original
            if target in SURFACE['result_origins']:
                return SURFACE['result_origins'][target]
            if target in SURFACE['objects']:
                return target
            if target and target.startswith('factory:'):
                return target[8:]
            if isinstance(node.func, ast.Name) and node.func.id == 'enumerate' and node.args:
                value = self.origin(node.args[0])
                if value and value.startswith('seq:'):
                    return 'items:' + value[4:]
            if target and target.startswith('map:'):
                if target.endswith('.values'):
                    return 'seq:' + target[4:-7]
                if target.endswith('.items'):
                    return 'items:' + target[4:-6]
                if target.endswith('.get'):
                    return target[4:-4]
            if isinstance(node.func, ast.Name) and node.func.id == 'safe_json' and node.args:
                return self.origin(node.args[0])
        return None

    def _recording_use(self, node):
        rule = SURFACE['opaque_forwarding'].get(self.origin(node))
        if rule is None:
            return
        parent = self.parents.get(node)
        if isinstance(parent, (ast.Assign, ast.AnnAssign)) and parent.value is node:
            return  # A local alias retains its opaque origin.
        if isinstance(parent, ast.Call) and self.origin(parent.func) == rule['call'] \
                and len(parent.args) > rule['argument'] and parent.args[rule['argument']] is node:
            return
        self.problem(node, 'native recording value may only be forwarded unchanged to action_event')

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self._recording_use(node)

    def bind(self, target, value):
        if isinstance(target, ast.Name):
            value = value or SURFACE['parameter_origins'].get(self.path + ':' + self.function, {}).get(target.id)
            if value:
                self.env[target.id] = value
            else:
                self.env.pop(target.id, None)
        elif isinstance(target, (ast.Tuple, ast.List)) and value:
            if value.startswith('items:') and len(target.elts) == 2:
                self.bind(target.elts[1], value[6:])
            elif value.startswith('tuple:'):
                for item, kind in zip(target.elts, value[6:].split(',')):
                    self.bind(item, None if kind == 'unknown' else kind)

    def visit_Assign(self, node):
        self.visit(node.value)
        value = self.origin(node.value)
        for target in node.targets:
            self.bind(target, value)

    def visit_AnnAssign(self, node):
        if node.value is not None:
            self.visit(node.value)
            self.bind(node.target, self.origin(node.value))

    def visit_FunctionDef(self, node):
        previous, name = self.env.copy(), self.function
        self.function = node.name
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        for argument in arguments:
            self.env.pop(argument.arg, None)
        self.env.update(SURFACE['parameter_origins'].get(self.path + ':' + node.name, {}))
        for statement in node.body:
            self.visit(statement)
        self.env, self.function = previous, name

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_With(self, node):
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars:
                self.bind(item.optional_vars, self.origin(item.context_expr))
        for statement in node.body:
            self.visit(statement)

    def visit_For(self, node):
        self.visit(node.iter)
        value = self.origin(node.iter)
        if value and value.startswith('opaque:'):
            self.problem(node, 'opaque value iterated')
        self.bind(node.target, value[4:] if value and value.startswith('seq:') else value)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_Attribute(self, node):
        base = self.origin(node.value)
        module_members = SURFACE['independent_module_members'].get(base)
        if module_members is not None and node.attr not in module_members.get(self.path, []):
            self.problem(node, 'unapproved independent module member: ' + node.attr)
        base = SURFACE['result_origins'].get(base, base)
        if base in SURFACE['record_fields'] and node.attr != 'get':
            self.problem(node, 'native projection requires a named field: ' + node.attr)
        if base in SURFACE['objects'] and node.attr not in SURFACE['objects'][base]:
            self.problem(node, 'unapproved ' + base + '.' + node.attr)
        if base and base.startswith('opaque:'):
            self.problem(node, 'opaque display value inspected: ' + node.attr)
        self.generic_visit(node)

    def visit_Subscript(self, node):
        self._recording_use(node)
        base = self.origin(node.value)
        if base in SURFACE['record_fields'] and (not isinstance(node.slice, ast.Constant)
                or node.slice.value not in SURFACE['record_fields'][base]):
            self.problem(node, 'unapproved native projection field')
        if base and base.startswith('opaque:'):
            self.problem(node, 'opaque display value indexed')
        self.generic_visit(node)

    def visit_Call(self, node):
        self._recording_use(node)
        name = self.origin(node.func)
        allowed_keywords = SURFACE['constructor_keywords'].get(name)
        if allowed_keywords is not None:
            for keyword in node.keywords:
                if keyword.arg not in allowed_keywords:
                    self.problem(keyword, 'unapproved native constructor keyword: ' + str(keyword.arg))
        if name and name.endswith('.get') and name[:-4] in SURFACE['record_fields']:
            if not node.args or not isinstance(node.args[0], ast.Constant) \
                    or node.args[0].value not in SURFACE['record_fields'][name[:-4]]:
                self.problem(node, 'unapproved native projection field')
        direct = node.func.id if isinstance(node.func, ast.Name) else None
        reflective = direct in ('getattr', 'vars') or name in ('builtins.getattr', 'builtins.vars', 'builtins.object.__getattribute__')
        if reflective and node.args:
            base = self.origin(node.args[0])
            base = SURFACE['result_origins'].get(base, base)
            if base in SURFACE['objects'] or (base and (base.startswith('opaque:') or _host_interface(base))):
                self.problem(node, 'reflective native access requires explicit contract')
        if name in ('importlib.import_module', 'builtins.__import__') or direct == '__import__':
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                module = node.args[0].value
                if module.startswith('demiflow.'):
                    self.problem(node, 'dynamic platform import bypasses named surface')
            elif self.path + ':' + self.function not in SURFACE['reflection_sites']:
                self.problem(node, 'unclassified dynamic import')
        self.generic_visit(node)


def _member_errors(tree, path):
    scanner = Uses(tree, path)
    scanner.visit(tree)
    return scanner.errors


def test_workbench_imports_are_classified_by_exact_surface_and_consumer():
    errors = []
    for path in sorted(WORKBENCH.rglob('*.py')):
        relative = str(path.relative_to(REPO))
        tree = ast.parse(path.read_text(), filename=relative)
        errors.extend((relative, *error) for error in _import_errors(tree, relative))
    assert not errors, errors


def test_approved_objects_do_not_leak_members_or_opaque_configuration():
    errors = []
    for path in sorted(WORKBENCH.rglob('*.py')):
        relative = str(path.relative_to(REPO))
        tree = ast.parse(path.read_text(), filename=relative)
        errors.extend((relative, *error) for error in _member_errors(tree, relative))
    assert not errors, errors


def _host_interface(target):
    return target == 'demiflow.execution.host' or target.startswith('demiflow.execution.host.')


def test_host_interface_uses_complete_package_name():
    assert _host_interface('demiflow.execution.host')
    assert _host_interface('demiflow.execution.host.inspection')
    assert not _host_interface('demiflow.execution.hosts.ray_worker_host')


def test_native_core_never_imports_host_or_product():
    errors = []
    for package in ('execution', 'planning', 'data', 'pipeline'):
        for path in (REPO / 'demiflow' / package).rglob('*.py'):
            if path.is_relative_to(HOST):
                continue
            relative = str(path.relative_to(REPO))
            for node, module, name, _ in _imports(ast.parse(path.read_text()), relative):
                target = module + '.' + name
                if target.startswith(('demiflow.workbench', 'demiflow.forge')) or _host_interface(target):
                    errors.append((relative, node.lineno, target))
    assert not errors, errors


def test_surface_document_is_generated_from_runtime_literal():
    assert json.loads((REPO / 'docs/workbench-engine-surface-v3.json').read_text()) == SURFACE
    for side, exports in SURFACE['exports'].items():
        assert all(not name.startswith('_') for name in exports)
        tree = ast.parse((HOST / (side + '.py')).read_text())
        assert {n.name for n in tree.body if isinstance(n, ast.FunctionDef)} == {'__getattr__', '__dir__'}
        assert not any(isinstance(n, (ast.ClassDef, ast.AsyncFunctionDef)) for n in ast.walk(tree))
        allowed_calls = {'tuple', 'getattr', 'import_module', 'globals', 'AttributeError', 'sorted', 'set'}
        assert all(isinstance(n.func, ast.Name) and n.func.id in allowed_calls
                   for n in ast.walk(tree) if isinstance(n, ast.Call))


@pytest.mark.parametrize('code', [
    'from demiflow.execution.journal.run_journal import RunJournal as J',
    'import demiflow.execution.journal.run_journal as j',
    'from demiflow.execution.host.control import *',
])
def test_import_negative_alias_fixtures(code):
    assert _import_errors(ast.parse(code), 'demiflow/workbench/fixture.py')


@pytest.mark.parametrize('code', [
    'from demiflow.execution.host.control import RunJournal as J\nr = J("x")\nother = r\nother.db',
    'from demiflow.execution.host.inspection import read_run as read\nwith read("x") as r:\n    alias=r\n    alias.tables',
    'from demiflow.execution.host.control import RunJournal\nj=RunJournal("x")\ngetattr(j, "db")',
    'from demiflow.execution.host.inspection import compiled_node_materials\nm=compiled_node_materials(c,bounds=b)\nitem=m["x"]\ncfg=item.configuration.engine_configuration\ncfg.get("query")',
    'import importlib\nimportlib.import_module("demiflow.execution.journal.run_journal")',
    'from demiflow.execution.host.inspection import capture_pipeline\nr=capture_pipeline(f,{})\nn=r.compiled.graph.nodes[0]\nn.kernel.configuration',
    'from demiflow.execution.host.control import RunJournal as J\nj=J("x")\npeek=getattr\npeek(j, "db")',
])
def test_member_negative_alias_and_return_fixtures(code):
    assert _member_errors(ast.parse(code), 'demiflow/workbench/fixture.py')


def test_host_private_state_and_independent_borrow_are_not_native_leaks():
    code = '''import os
from demiflow.execution.datafusion import DataFusionSession
class Own:
    def stop(self):
        self._plan = None
        os._exit(1)
with DataFusionSession(resource_directory='x') as session:
    with session.borrow_resources():
        pass
'''
    assert not _member_errors(ast.parse(code), 'demiflow/workbench/analysis/read_worker.py')


def test_indices_are_lazy_and_reexport_original_objects():
    import os
    import subprocess
    import sys
    code = """import sys
import demiflow.execution
before = set(sys.modules)
from demiflow.execution.host import inspection, control
from demiflow.execution.host._surface import SURFACE
owners = {module for table in SURFACE['exports'].values() for module, name in table.values()}
assert not ((set(sys.modules) - before) & owners)
from importlib import import_module
for side in (inspection, control):
    for name in side.__all__:
        module, member = SURFACE['exports'][side.__name__.rsplit('.',1)[1]][name]
        assert getattr(side, name) is getattr(import_module(module), member)
        assert getattr(side, name) is side.__dict__[name]
    try:
        side.not_a_contract
    except AttributeError:
        pass
    else:
        raise AssertionError('Unknown export accepted')
"""
    subprocess.run([sys.executable, '-c', code], cwd=REPO,
                   env={**os.environ, 'PYTHONPATH': str(REPO), 'PYTHONDONTWRITEBYTECODE': '1'},
                   check=True, capture_output=True, text=True, timeout=30)


def test_native_storage_layout_is_not_reinterpreted_in_workbench():
    forbidden = {'run.sqlite', 'run.sqlite-wal', 'run.sqlite-journal', '_demiflow_commit',
                 'runtime_sources', 'runtime_actions', 'runtime_commits'}
    errors = []
    for path in WORKBENCH.rglob('*.py'):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in forbidden:
                errors.append((str(path.relative_to(REPO)), node.lineno, node.value))
    assert not errors, errors


def test_every_index_export_has_an_actual_workbench_consumer():
    used = {side: set() for side in SURFACE['exports']}
    for path in WORKBENCH.rglob('*.py'):
        for _, module, name, _ in _imports(ast.parse(path.read_text()), str(path.relative_to(REPO))):
            side = module.removeprefix('demiflow.execution.host.')
            if side in used:
                used[side].add(name)
    assert used == {side: set(exports) for side, exports in SURFACE['exports'].items()}


@pytest.mark.parametrize('path,function,statement', [
    ('demiflow/workbench/runs/requests.py', '_ready', 'journal.claim_execution({}, "unexpected")'),
    ('demiflow/workbench/runs/native_stages.py', 'admit_prepared', 'journal.claim_execution({}, "unexpected")'),
])
def test_real_journal_consumers_cannot_hide_private_calls(path, function, statement):
    tree = ast.parse((REPO / path).read_text())
    target = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == function)
    # Insert before the real final operation/try, preserving live assignments
    # and avoiding an unreachable statement after the native adapter's return.
    target.body.insert(len(target.body) - 1, ast.parse(statement).body[0])
    assert any('RunJournal.claim_execution' in str(error) for error in _member_errors(tree, path))


@pytest.mark.parametrize('prefix', [
    'from demiflow.execution.host.control import RunJournal\n',
    'from demiflow.workbench.runs.manager import RunJournal\n',
])
@pytest.mark.parametrize('arguments', ['target, _maintenance_deadline=1', 'target, **options'])
def test_private_constructor_options_cannot_bypass_reexport_contract(prefix, arguments):
    tree = ast.parse(prefix + 'run = RunJournal(' + arguments + ')')
    assert any('constructor keyword' in str(error) for error in _member_errors(tree, 'demiflow/workbench/runs/requests.py'))


def test_real_registration_rejects_decoding_native_recording():
    path = 'demiflow/workbench/runs/manager.py'
    tree = ast.parse((REPO / path).read_text())
    class InsertDecode(ast.NodeTransformer):
        def visit_Assign(self, node):
            if any(isinstance(target, ast.Name) and target.id == 'recording' for target in node.targets):
                return [node, ast.parse('recording["value"]["intent"]').body[0]]
            return node
    tree = InsertDecode().visit(tree)
    assert any('opaque' in str(error) or 'native recording value' in str(error)
               for error in _member_errors(tree, path))


@pytest.mark.parametrize('producer', ['verify_write_receipt', 'verify_published_output', '_verified_output'])
@pytest.mark.parametrize('use', ['raw["intent"]', 'raw.get("intent")', 'dict(raw)', 'str(raw)', 'another_writer(raw)'])
def test_publication_recording_is_not_another_host_protocol(producer, use):
    prefix = ('from demiflow.execution.host.inspection import ' + producer + '\n'
              if producer != '_verified_output' else 'def _verified_output(*args, **kwargs):\n    pass\n')
    code = prefix + 'verified = ' + producer + '(target, action)\nrecording = verified.get("recording")\nraw = recording.get("value")\n' + use
    assert _member_errors(ast.parse(code), 'demiflow/workbench/runs/manager.py')


def test_native_recording_can_only_be_aliased_and_forwarded_unchanged():
    code = '''from demiflow.workbench.runs.manager import RunJournal
from demiflow.execution.host.inspection import verify_write_receipt
run = RunJournal(target, attempt=1)
verified = verify_write_receipt(target, action, expected_uri=uri)
recording = verified['recording']
if recording is not None:
    payload = recording['value']
    emit = run.action_event
    emit(action, recording['kind'], payload)
'''
    assert not _member_errors(ast.parse(code), 'demiflow/workbench/runs/manager.py')


def test_approved_reexport_matches_its_real_import():
    for reexport, original in SURFACE['reexport_origins'].items():
        module, name = reexport.rsplit('.', 1)
        path = module.replace('.', '/') + '.py'
        imported = {(owner + '.' + symbol, alias) for _, owner, symbol, alias in
                    _imports(ast.parse((REPO / path).read_text()), path)}
        assert (original, name) in imported
