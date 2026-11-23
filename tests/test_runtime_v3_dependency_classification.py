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
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext, RunAction


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
