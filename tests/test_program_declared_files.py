import importlib
import sys

import pytest

from demiflow.data.plan import CallableSpec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_code import business_sources, source_identity


@pytest.fixture
def recipe(tmp_path, monkeypatch):
    parent = tmp_path / 'pinned_training_project'
    parent.mkdir()
    (parent / '__init__.py').write_text('def entry(request, context): return None\n')
    (parent / 'worker.py').write_text('def run(): return 0\n')
    (parent / 'prompt.yaml').write_text('hello')
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module('pinned_training_project')
    yield module.entry, parent
    sys.modules.pop(module.__name__, None)


def sources(entry):
    return source_identity(business_sources('callable', CallableSpec.create(entry)))


def test_explicit_runtime_module_and_data_change_program_identity(recipe):
    entry, parent = recipe
    original = sources(entry)
    entry.__demiflow_package_files__ = {'pinned_training_project': ('worker.py', 'prompt.yaml')}
    before = sources(entry)
    assert before['pinned_training_project.worker']['bytes'] > 0
    assert before['pinned_training_project:prompt.yaml']['bytes'] == 5
    (parent / 'prompt.yaml').write_text('world')
    assert sources(entry) != before
    (parent / 'worker.py').unlink()
    with pytest.raises(DataflowError):
        sources(entry)
    del entry.__demiflow_package_files__
    assert sources(entry) == original


@pytest.mark.parametrize('filename', ['../elsewhere', '/etc/hosts', 'a//b', 'linked.py', 'dir/file', 'a:b'])
def test_package_files_refuse_unowned_paths(recipe, tmp_path, filename):
    entry, parent = recipe
    (parent / 'linked.py').symlink_to(parent / 'worker.py')
    (parent / 'dir').symlink_to(tmp_path, target_is_directory=True)
    entry.__demiflow_package_files__ = {'pinned_training_project': (filename,)}
    with pytest.raises(DataflowError):
        sources(entry)


def test_large_package_member_stages_and_mounts(recipe, tmp_path, monkeypatch):
    from demiflow.execution.dataflow_code import mount_sources, stage_sources
    from test_runtime_v3_d import FakeStore
    entry, parent = recipe
    member = parent / 'extra'
    with member.open('wb') as stream:
        stream.truncate(8 * 1024**2 + 1)
    entry.__demiflow_package_files__ = {'pinned_training_project': ('extra',)}
    files = business_sources('callable', CallableSpec.create(entry))
    pin = source_identity(files)
    name = 'pinned_training_project:extra'
    assert pin[name]['bytes'] == member.stat().st_size
    store = FakeStore(tmp_path / 'store')
    refs = stage_sources(files, store=store)
    monkeypatch.setattr(sys, 'path', sys.path.copy())
    cache = tmp_path / 'cache'
    mount_sources(refs, pin, store=store, cache=cache)
    cached, = cache.rglob('extra')
    assert cached.stat().st_size == member.stat().st_size
    with cached.open('r+b') as stream:
        stream.write(b'X')
    with pytest.raises(DataflowError, match='Cached business code differs'):
        mount_sources(refs, pin, store=store, cache=cache)
    with member.open('r+b') as stream:
        stream.write(b'Y')
    with pytest.raises(DataflowError, match='changed during sealing'):
        stage_sources(files, store=store)


def test_inherited_package_root_is_classified_by_the_active_interpreter(tmp_path, monkeypatch):
    import site
    import types
    from demiflow.execution.chain import _classify_dependency_module
    installed = tmp_path / 'base' / 'site-packages'
    source = installed / 'example_wheel.py'
    installed.mkdir(parents=True)
    source.write_text('version = 1')
    module = types.ModuleType('example_wheel')
    module.__file__ = str(source)
    monkeypatch.syspath_prepend(str(installed))
    assert _classify_dependency_module(module) == source
    monkeypatch.setattr(site, 'getsitepackages', lambda: [str(installed)])
    assert _classify_dependency_module(module) is None
