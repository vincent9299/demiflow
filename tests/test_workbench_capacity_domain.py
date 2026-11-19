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
