"""Every place the platform starts a child process says how that child relates to an execution unit.

A unit's capacity is released on the proof that its lock has no holder. That proof covers
exactly the children that inherit the lock, so a new spawn site must be classified here:

  unit      the child inherits the unit lock and counts as the unit until it exits;
  outside   the caller runs outside any execution unit (host, driver or a standalone tool);
  service   a shared service with a lifecycle of its own, deliberately not tied to one unit;
  operator  an operator's own child of bounded work; it does not hold the unit lock, so a
            killed worker's capacity can return while such a child finishes.

Source-only: nothing is imported or executed.
"""
import ast
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / 'demiflow'
HOSTS = {'workbench', 'forge', '_vendor'}

SITES = {
    'resources/process_unit.py': ('unit', 1), 'resources/process_group.py': ('unit', 2),
    'execution/hosts/datafusion.py': ('unit', 1), 'execution/hosts/isolation.py': ('unit', 1),
    'execution/hosts/isolated_pool.py': ('unit', 1),
    'execution/hosts/local.py': ('outside', 1), 'execution/hosts/python_environment.py': ('outside', 1),
    'execution/hosts/worker_bootstrap.py': ('outside', 1), 'execution/hosts/managed_devices.py': ('outside', 1),
    'execution/hosts/processes.py': ('outside', 1), 'collect/supervisor.py': ('outside', 1),
    'collect/fleet.py': ('outside', 1),
    'services/manage.py': ('service', 1), 'services/http.py': ('service', 1),
    'services/preparation_io.py': ('operator', 1), 'services/model_worker.py': ('operator', 1),
    'operator_llm/codex_exec.py': ('operator', 1), 'operator_llm/codex_agent.py': ('operator', 1),
    'collect/exec_curl.py': ('operator', 1), 'collect/image_decode.py': ('operator', 1),
    'collect/image_resize.py': ('operator', 1), 'collect/native_search/runtime.py': ('operator', 1),
    'collect/pdf_text.py': ('operator', 1), 'collect/document_library.py': ('operator', 1),
}
SPAWNERS = {'Popen', 'run', 'check_output', 'check_call', 'call', 'create_subprocess_exec',
            'create_subprocess_shell', 'ProcessPoolExecutor', 'fork', 'posix_spawn'}
MODULES = {'subprocess', 'asyncio', 'os', 'multiprocessing'}


def _spawns(tree):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, 'id', None)
        owner = target.value.id if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) else None
        if name == 'ProcessPoolExecutor' or (name in SPAWNERS and owner in MODULES and not (
                owner in ('os', 'asyncio') and name in ('run', 'call'))):
            yield node


def test_every_platform_child_process_is_classified_and_unit_children_inherit_the_lock():
    found = {}
    for path in sorted(PACKAGE.rglob('*.py')):
        relative = path.relative_to(PACKAGE)
        if relative.parts[0] in HOSTS:
            continue
        calls = list(_spawns(ast.parse(path.read_text())))
        if calls:
            found[relative.as_posix()] = calls
    assert {name: len(calls) for name, calls in found.items()} == {name: count for name, (_, count) in SITES.items()}
    for name, calls in found.items():
        if SITES[name][0] == 'unit':
            assert all(any(keyword.arg == 'pass_fds' for keyword in call.keywords) for call in calls), name
