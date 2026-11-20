"""P12 domain boundaries: real source graph, inert imports and explicit owners.

This is a finite static dependency guard, not arbitrary Python sandboxing.
Application modules may compose project and run domains; seven project core
modules may not acquire the application/worker/run domains, even indirectly.
"""
import ast
from collections import deque
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROJECTS = 'demiflow.workbench.projects'
CORE = {'workspace', 'checks', 'discovery', 'definitions', 'documents', 'io', 'index'}
APPLICATION = {'registry', 'watch', 'flow_arguments'}
FORBIDDEN = {PROJECTS + '.' + name for name in APPLICATION} | {
    'demiflow.workbench.worker', 'demiflow.workbench.api',
    'demiflow.workbench.project_operations', 'demiflow.workbench.directory_operations'}


def import_targets(source, module, *, package=False):
    package_name = module if package else module.rpartition('.')[0]
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            yield from (item.name for item in node.names)
        elif isinstance(node, ast.ImportFrom):
            origin = (importlib.util.resolve_name('.' * node.level + (node.module or ''), package_name)
                      if node.level else node.module or '')
            yield origin
            yield from (origin + '.' + item.name for item in node.names)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == '__import__':
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                yield node.args[0].value


def forbidden_path(graph, start, forbidden):
    pending, seen = deque([[start]]), set()
    while pending:
        path = pending.popleft(); current = path[-1]
        if current in seen: continue
        seen.add(current)
        if forbidden(current): return path
        pending.extend(path + [target] for target in sorted(graph.get(current, ())))
    return None


@pytest.fixture(scope='module')
def graph():
    paths = {'.'.join(path.relative_to(ROOT).with_suffix('').parts).removesuffix('.__init__'): path
             for path in (ROOT / 'demiflow').rglob('*.py')}
    return {module: set(import_targets(path.read_text(), module, package=path.name == '__init__.py')) & paths.keys()
            for module, path in paths.items()}


def test_project_directory_has_seven_core_three_application_and_inert_initializer():
    files = {path.stem for path in (ROOT / 'demiflow/workbench/projects').glob('*.py')}
    assert files == CORE | APPLICATION | {'__init__'}
    tree = ast.parse((ROOT / 'demiflow/workbench/projects/__init__.py').read_text())
    assert all(isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
               and isinstance(node.value.value, str) for node in tree.body)


@pytest.mark.parametrize('name', sorted(CORE))
def test_core_has_no_transitive_run_worker_or_application_dependency(graph, name):
    path = forbidden_path(graph, PROJECTS + '.' + name,
        lambda module: module in FORBIDDEN or module.startswith('demiflow.workbench.runs'))
    assert path is None, ' -> '.join(path or [])


def test_index_cannot_reach_domain_source_or_platform_readers(graph):
    path = forbidden_path(graph, PROJECTS + '.index', lambda module:
        module == PROJECTS + '.workspace' or module.startswith('demiflow.workbench.runs')
        or module.startswith('demiflow.execution.host'))
    assert path is None, ' -> '.join(path or [])


@pytest.mark.parametrize('module', ['table_bindings', 'pipeline_policy'])
def test_neutral_modules_do_not_depend_on_domain_owners(graph, module):
    path = forbidden_path(graph, 'demiflow.workbench.' + module, lambda target:
        target.startswith((PROJECTS, 'demiflow.workbench.runs')) or target in FORBIDDEN)
    assert path is None, ' -> '.join(path or [])


def test_guard_detects_function_local_relative_and_indirect_imports():
    origin = PROJECTS + '.workspace'
    targets = set(import_targets('def read():\n    from . import discovery\n', origin))
    assert PROJECTS + '.discovery' in targets
    graph = {origin: targets, PROJECTS + '.discovery': {'bridge'},
             'bridge': {'demiflow.workbench.runs.manager'}}
    assert forbidden_path(graph, origin, lambda target: target.startswith('demiflow.workbench.runs')) == [
        origin, PROJECTS + '.discovery', 'bridge', 'demiflow.workbench.runs.manager']


def test_cold_import_of_all_project_core_modules_does_not_load_domains():
    code = ('import importlib,sys; '
            + '; '.join('importlib.import_module(' + repr(PROJECTS + '.' + name) + ')' for name in sorted(CORE))
            + '; assert not any(n.startswith("demiflow.workbench.runs") or n in '
            + repr(FORBIDDEN) + ' for n in sys.modules), sorted(n for n in sys.modules if n.startswith("demiflow.workbench"))')
    result = subprocess.run([sys.executable, '-P', '-B', '-c', code], cwd=ROOT,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
