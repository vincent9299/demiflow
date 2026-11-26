"""Installed symlink libraries remain environment-owned; business code stays pinned."""
from dataclasses import asdict
import importlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import types

import pytest

from demiflow import data
from demiflow.execution.seal import business_sources
from demiflow.execution.contracts.dataflow import ArtifactRef
from runtime_v3_program_probe import program_store, stage_callable
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext, RunAction


def _module(name, path):
    module = types.ModuleType(name)
    module.__file__ = str(path)
    return module


def _installation(monkeypatch, path):
    original = sysconfig.get_path
    monkeypatch.setattr(sysconfig, 'get_path', lambda name, *a, **k:
                        str(path) if name in ('purelib', 'platlib') else original(name, *a, **k))


def test_installation_package_symlink_is_not_business(tmp_path, monkeypatch):
    site = tmp_path/'python-installation'
    actual = tmp_path/'wheel-files'/'library'
    site.mkdir(); actual.mkdir(parents=True)
    (actual/'__init__.py').write_text('VALUE = 1\n')
    (site/'library').symlink_to(actual, target_is_directory=True)
    _installation(monkeypatch, site)
    module = _module('library', site/'library'/'__init__.py')
    assert Path(module.__file__).resolve() == actual/'__init__.py'
    assert business_sources._classify_dependency_module(module) is None


@pytest.mark.parametrize('kind', ['same_name', 'fake_site_packages', 'business_symlink', 'editable', 'dotdot'])
def test_business_paths_do_not_acquire_installation_exemption(tmp_path, monkeypatch, kind):
    site = tmp_path/'installation'
    site.mkdir()
    source = tmp_path/'business'/'site-packages'/'numpy.py'
    source.parent.mkdir(parents=True)
    source.write_text('VALUE = 1\n')
    _installation(monkeypatch, site)
    filename = source
    if kind == 'business_symlink':
        filename = tmp_path/'business-link.py'
        filename.symlink_to(source)
    elif kind == 'editable':
        # A PEP 660 editable import resolves __file__ to the business source;
        # metadata/.pth registration is not a permission to ignore that source.
        dist = site/'owned-business-1.dist-info'
        dist.mkdir()
        (dist/'direct_url.json').write_text(json.dumps({'url': source.parent.as_uri(), 'dir_info': {'editable': True}}))
        (site/'owned-business.pth').write_text(str(source.parent)+'\n')
        monkeypatch.syspath_prepend(str(source.parent))
    elif kind == 'dotdot':
        filename = site/'..'/'business'/'site-packages'/'numpy.py'
    module = _module('numpy', filename)
    assert business_sources._classify_dependency_module(module) == source.resolve()


def test_source_and_module_path_must_agree_on_installation(tmp_path, monkeypatch):
    import inspect
    site = tmp_path/'installation'; site.mkdir()
    source = tmp_path/'real-business.py'; source.write_text('VALUE = 1\n')
    installed = site/'library.py'; installed.symlink_to(source)
    module = _module('library', installed)
    _installation(monkeypatch, site)
    original = inspect.getsourcefile
    monkeypatch.setattr(inspect, 'getsourcefile', lambda value: str(source) if value is module else original(value))
    assert business_sources._classify_dependency_module(module) == source


@pytest.mark.parametrize('name', ['numpy', 'pandas', 'pyarrow', 'pytest'])
def test_real_installed_third_party_is_environment_owned(name):
    assert business_sources._classify_dependency_module(importlib.import_module(name)) is None


def _load(path, name, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('library', ['numpy', 'pandas'])
def test_actual_pin_archive_and_fresh_worker_helper_drift(tmp_path, monkeypatch, library):
    helper_name = 'owned_pin_helper'
    rule_name = 'owned_pin_rule'
    helper = tmp_path/(helper_name+'.py')
    helper.write_text('def delta():\n    return 1\n')
    rule = tmp_path/(rule_name+'.py')
    rule.write_text(f'import {library}\nimport {helper_name}\n\ndef transform(row):\n    return {{"x": row["x"] + {helper_name}.delta()}}\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    _load(helper, helper_name, monkeypatch)
    module = _load(rule, rule_name, monkeypatch)
    plan = data.from_items([{'x': 1}]).map(module.transform)._plan
    pin = business_sources.plan_code_pin(plan)
    assert set(pin['dependencies']) == {helper_name, rule_name}
    with RunContext.start(tmp_path/'run') as run:
        declaration = {'kind': 'chain-materialize', 'source': {'source_id': 'fixture'}}
        run.journal.bind_execution_action(run.fence, 'main-action', declaration)
        action = RunAction(run, 'main-action', declaration)
        with program_store(action) as (store, bounds):
            ref, manifest = stage_callable(plan.operations[0].callable, store=store, bounds=bounds)
            sources = {name: ArtifactRef(**item) for name, item in manifest['business_sources'].items()}
            assert set(sources) == {helper_name, rule_name}

            def archived():
                return {name: b''.join(store.read(item, max_bytes=item.size_bytes)) for name, item in sources.items()}

            original = archived()
            assert original == {helper_name: helper.read_bytes(), rule_name: rule.read_bytes()}
            params = {'root': str(run.directory / '.dataflow-store'), 'ref': asdict(ref),
                      'bounds': asdict(bounds), 'code_identity': manifest['code_identity']}
            script = """
import json, pathlib, sys
from demiflow.execution.contracts.dataflow import ArtifactRef, Bounds
from demiflow.execution.seal.plan import verify_dataflow_program
from demiflow.execution.storage.objects import RunArtifactStore
params = json.load(sys.stdin)
ref = ArtifactRef(**params['ref'])
bounds = Bounds(**params['bounds'])
store = RunArtifactStore(root=pathlib.Path(params['root']), store_id=ref.store_id,
                         journal=None, readonly=True, bounds=bounds)
try:
    program = verify_dataflow_program(ref, expected_pin=params['code_identity'], store=store, bounds=bounds)
    print(json.dumps(program.value.target({'x': 2})))
finally:
    store.close()
"""
            env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(tmp_path), str(Path.cwd()))),
                       PYTHONDONTWRITEBYTECODE='1')

            def worker():
                return subprocess.run([sys.executable, '-B', '-c', script], input=json.dumps(params),
                                      capture_output=True, text=True, env=env, timeout=30)

            unchanged = worker()
            assert unchanged.returncode == 0, unchanged.stderr
            assert json.loads(unchanged.stdout) == {'x': 3}
            helper.write_text('def delta():\n    return 9999\n')
            changed = worker()
            assert changed.returncode != 0 and 'Loaded program code differs' in changed.stderr
            assert archived() == original
        run.journal.complete_execution_action(run.fence, 'main-action', {'kind': 'test-complete'})


def _captured_plan(module):
    def transform(row):
        return module.transform(row)
    return data.from_items([{'x': 1}]).map(transform)._plan


def test_module_count_limit_is_independent_of_source_size(tmp_path):
    assert business_sources._DEP_MAX_MODULES == 64
    modules = []
    for index in range(65):
        path = tmp_path/f'business_{index}.py'; path.write_text('VALUE = 1\n')
        modules.append(_module(f'owned_budget_{index}', path))
    for current, next_module in zip(modules, modules[1:]):
        current.next = next_module
    with pytest.raises(RecoveryContractError, match='manifest exceeds budget'):
        business_sources.plan_code_pin(_captured_plan(modules[0]))
    path = tmp_path/'wide_business.py'
    with path.open('wb') as stream:
        for _ in range(129):
            stream.write(b'#' + b'x'*65534 + b'\n')
    pin = business_sources.plan_code_pin(_captured_plan(_module('owned_wide_business', path)))
    assert pin['dependencies']['owned_wide_business']['bytes'] == path.stat().st_size


def test_ast_relative_constant_and_lazy_imports_are_bundled(tmp_path, monkeypatch):
    """Constant-only relative imports (`from ._schemas import TAG`) and function-body lazy
    relative imports (`from . import _lazy`) must be captured into business_dependency_modules
    so isolated workers can import the operator module without ModuleNotFoundError.
    """
    pkg = tmp_path / 'ast_dep_pkg'
    ops = pkg / 'operators'
    ops.mkdir(parents=True)
    (pkg / '__init__.py').write_text('', encoding='utf-8')
    (ops / '__init__.py').write_text('', encoding='utf-8')
    (ops / '_schemas.py').write_text('TAG = "v1"\n', encoding='utf-8')
    (ops / '_lazy.py').write_text('def suffix():\n    return "-ok"\n', encoding='utf-8')
    (ops / 'worker_op.py').write_text(
        'from ._schemas import TAG\n'
        'def transform(row):\n'
        '    from . import _lazy\n'
        '    return {"x": f"{row[\'x\']}:{TAG}{_lazy.suffix()}"}\n',
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    import importlib
    mod = importlib.import_module('ast_dep_pkg.operators.worker_op')
    try:
        plan = data.from_items([{'x': 1}]).map(mod.transform)._plan
        deps = business_sources.business_dependency_modules(plan)
        assert 'ast_dep_pkg.operators.worker_op' in deps
        assert 'ast_dep_pkg.operators._schemas' in deps
        assert 'ast_dep_pkg.operators._lazy' in deps
    finally:
        for name in list(sys.modules):
            if name == 'ast_dep_pkg' or name.startswith('ast_dep_pkg.'):
                sys.modules.pop(name, None)


@pytest.fixture
def lazy_absolute_graph(tmp_path, monkeypatch):
    """A business module whose row function imports a sibling module inside its body.

    Returns the module names and the imported entry module. The helper is left
    unloaded; both this process and a managed worker can import it by name.
    """
    import uuid
    tag = uuid.uuid4().hex[:8]
    names = {key: f'lazyabs_{tag}_{key}' for key in ('entry', 'top', 'helper')}
    folder = tmp_path / 'business'
    folder.mkdir()
    (folder / (names['top'] + '.py')).write_text('TAG = "v1"\n')
    (folder / (names['helper'] + '.py')).write_text('def scale(value):\n    return value * 10\n')
    (folder / (names['entry'] + '.py')).write_text(
        f'from {names["top"]} import TAG\n'
        'def transform(row):\n'
        f'    import {names["helper"]} as helper\n'
        '    return {"x": helper.scale(row["x"]), "tag": TAG}\n')
    monkeypatch.syspath_prepend(str(folder))
    monkeypatch.setenv('PYTHONPATH', str(folder) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    importlib.invalidate_caches()
    try:
        yield names, importlib.import_module(names['entry']), folder
    finally:
        for name in names.values():
            sys.modules.pop(name, None)


def test_lazy_project_import_is_pinned_independently_of_preload(lazy_absolute_graph):
    """Whether the sealing process happens to have loaded a module that a row function
    imports inside its body must not change the pin: a fresh worker has not loaded it."""
    names, entry, _ = lazy_absolute_graph
    plan = data.from_items([{'x': 1}]).map(entry.transform)._plan
    unloaded = business_sources.plan_code_pin(plan)
    assert set(unloaded['dependencies']) == set(names.values())
    assert names['helper'] not in sys.modules
    importlib.import_module(names['helper'])
    assert business_sources.plan_code_pin(plan) == unloaded


def test_cold_package_import_pins_initializers_and_transitive_modules(lazy_absolute_graph):
    names, entry, folder = lazy_absolute_graph
    package = names['helper'] + '_package'
    marker = folder / 'imported'
    pkg = folder / package
    pkg.mkdir()
    (pkg / '__init__.py').write_text(f'from pathlib import Path\nPath({str(marker)!r}).touch()\n')
    (pkg / 'ops.py').write_text(f'from {package}.settings import SCALE\n')
    (pkg / 'settings.py').write_text('SCALE = 10\n')
    (folder / (names['helper'] + '.py')).write_text(
        f'def scale(value):\n    from {package} import ops\n    return value * ops.SCALE\n')
    plan = data.from_items([{'x': 1}]).map(entry.transform)._plan
    pin = business_sources.plan_code_pin(plan)
    assert set(pin['dependencies']) == {*names.values(), package, package + '.ops', package + '.settings'}
    assert not marker.exists()
    (pkg / 'settings.py').write_text('SCALE = 99\n')
    assert business_sources.plan_code_pin(plan) != pin


def test_conflicting_import_root_does_not_silently_change_the_row_function(
        lazy_absolute_graph, tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    names, entry, _ = lazy_absolute_graph
    shadow = tmp_path / 'other'
    shadow.mkdir()
    (shadow / (names['helper'] + '.py')).write_text('def scale(value):\n    return value * 99\n')
    monkeypatch.syspath_prepend(str(shadow))
    plan = data.from_items([{'x': 1}]).map(entry.transform)._plan
    with pytest.raises(DataflowError, match='Ambiguous business import'):
        business_sources.plan_code_pin(plan)


def test_seal_that_counted_a_loaded_lazy_import_still_authenticates(lazy_absolute_graph):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.seal.plan import _code_identity_from_value, _matches_original_program
    names, entry, folder = lazy_absolute_graph
    importlib.import_module(names['helper'])
    spec = CallableSpec.create(entry.transform)
    earlier = _code_identity_from_value('callable', spec, discovery=business_sources.LOADED_IMPORTS)
    eager = _code_identity_from_value('callable', spec, discovery=business_sources.EAGER_IMPORTS)
    current = _code_identity_from_value('callable', spec)
    assert names['helper'] in earlier['dependencies'] and names['helper'] not in eager['dependencies']
    assert _matches_original_program('callable', spec, current)
    assert _matches_original_program('callable', spec, earlier)
    assert _matches_original_program('callable', spec, eager)
    sys.modules.pop(names['helper'])
    assert _matches_original_program('callable', spec, earlier)
    # The earlier seal pinned the helper it lists: editing it is still refused.
    (folder / (names['helper'] + '.py')).write_text('def scale(value):\n    return value * 99\n')
    assert not _matches_original_program('callable', spec, earlier)
    assert not _matches_original_program('callable', spec, current)
    assert _matches_original_program('callable', spec, eager)
    # Editing the sealed module itself is refused under either discovery.
    (folder / (names['entry'] + '.py')).write_text((folder / (names['entry'] + '.py')).read_text() + '# edited\n')
    assert not _matches_original_program('callable', spec, current)


@pytest.mark.parametrize('preloaded', [False, True])
def test_process_worker_runs_a_row_function_with_a_lazy_absolute_import(lazy_absolute_graph, tmp_path, preloaded):
    names, entry, _ = lazy_absolute_graph
    if preloaded:
        importlib.import_module(names['helper'])    # the driver uses the helper too
    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=1, worker_mode='process'):
        rows = data.from_items([{'x': 1}, {'x': 2}]).map(entry.transform).take_all()
    assert sorted(row['x'] for row in rows) == [10, 20] and {row['tag'] for row in rows} == {'v1'}
