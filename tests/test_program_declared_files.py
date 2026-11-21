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


def test_package_files_share_the_existing_source_byte_budget(recipe, monkeypatch):
    from demiflow.execution import dataflow_code
    entry, parent = recipe
    (parent / 'extra').write_bytes(b'x' * 4097)
    monkeypatch.setattr(dataflow_code, 'MAX_SOURCE_BYTES', 4096)
    entry.__demiflow_package_files__ = {'pinned_training_project': ('extra',)}
    with pytest.raises(DataflowError, match='source budget'):
        sources(entry)
