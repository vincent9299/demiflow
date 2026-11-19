"""A managed run keeps its original allocator through tempdirs and cleanup."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path

import pytest

from demiflow.execution.host.control import local_capacity, local_capacity_domain
from demiflow.workbench.runs import native_resources
from demiflow.workbench.runs.manager import worker_environment
from demiflow.workbench.runs.operations import OPERATIONS, worker_arguments
from demiflow.forge.schema import ForgeError


def test_capacity_selection_is_context_local_and_does_not_rewrite_deployment(tmp_path, monkeypatch):
    deployment = str(tmp_path / 'deployment')
    original = str(tmp_path / 'original')
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', deployment)
    with local_capacity_domain(original):
        assert str(local_capacity()[0]) == original
        assert os.environ['DEMIFLOW_RESOURCE_ROOT'] == deployment
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(lambda: str(local_capacity()[0])).result() == deployment
    assert str(local_capacity()[0]) == deployment
    with pytest.raises(ValueError):
        with local_capacity_domain('relative/path'):
            pass


def test_worker_domain_survives_attempt_tempdir_and_service_environment_changes(tmp_path, monkeypatch):
    original = str(tmp_path / 'original')
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', original)
    captured = native_resources.capacity_domain()
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'later-deployment'))
    first = worker_environment(tmp_path / 'run', capacity_domain=captured)
    second = worker_environment(tmp_path / 'run', capacity_domain=captured)
    second['TMPDIR'] = str(tmp_path / 'run/work/attempt-2')
    assert first['DEMIFLOW_RESOURCE_ROOT'] == second['DEMIFLOW_RESOURCE_ROOT'] == original
    assert first['TMPDIR'] != second['TMPDIR']


def test_capacity_domain_is_host_internal_not_a_client_run_parameter(tmp_path):
    arguments = dict(entry='flows/example.py:run', params={}, capacity_domain=str(tmp_path))
    with pytest.raises(ForgeError, match='unknown run arguments'):
        OPERATIONS['prepare'].validate(arguments)
    assert worker_arguments('run_prepare', arguments)['capacity_domain'] == str(tmp_path)


def test_deployment_environment_is_explicit_project_scoped_and_names_only(tmp_path, monkeypatch):
    import json
    root = tmp_path / 'project'
    root.mkdir()
    declaration = tmp_path / 'host-env.json'
    declaration.write_text(json.dumps(dict(format='demiflow_managed_environment_v1',
        workspaces={str(root): ['MODEL_API_KEY', 'TOOL_SETTINGS']})))
    monkeypatch.setenv('DEMIFLOW_WORKBENCH_RUN_ENVIRONMENT', str(declaration))
    monkeypatch.setenv('MODEL_API_KEY', 'private-test-value')
    assert native_resources.trusted_environment(workspace=root) == ['MODEL_API_KEY', 'TOOL_SETTINGS']
    assert native_resources.trusted_environment(workspace=tmp_path / 'other') == []
    with pytest.raises(ForgeError):
        OPERATIONS['prepare'].validate(dict(entry='x.py:run', params={}, environment_names=['MODEL_API_KEY']))
    for names in (['HOME'], ['PYTHONPATH'], ['TMPDIR'], ['DEMIFLOW_RESOURCE_ROOT'], [{}]):
        with pytest.raises(ForgeError):
            native_resources.environment_names(names)


def test_deployment_executable_path_is_sealed_and_not_client_controlled(tmp_path, monkeypatch):
    import json
    import subprocess
    root = tmp_path / 'project'
    root.mkdir()
    tools = tmp_path / 'tools'
    tools.mkdir()
    binary = tools / 'fixture-tool'
    binary.write_text('#!/bin/sh\nprintf deployment-tool')
    binary.chmod(0o700)
    declaration = tmp_path / 'host-env.json'
    declaration.write_text(json.dumps(dict(format='demiflow_managed_environment_v1',
        workspaces={str(root): []}, executable_dirs={str(root): [str(tools)]})))
    monkeypatch.setenv('DEMIFLOW_WORKBENCH_RUN_ENVIRONMENT', str(declaration))
    settings = native_resources.trusted_process_environment(workspace=root)
    assert settings == dict(environment_names=[], executable_dirs=[str(tools)])
    assert native_resources.trusted_process_environment(workspace=tmp_path / 'other')['executable_dirs'] == []
    with pytest.raises(ForgeError):
        OPERATIONS['prepare'].validate(dict(entry='x.py:run', params={}, executable_dirs=[str(tools)]))
    with pytest.raises(ForgeError):
        native_resources.executable_directories([str(root)], workspace=root)
    environment = worker_environment(tmp_path / 'run', executable_dirs=settings['executable_dirs'])
    result = subprocess.run(['fixture-tool'], env=environment, capture_output=True, text=True, check=True)
    assert result.stdout == 'deployment-tool'
