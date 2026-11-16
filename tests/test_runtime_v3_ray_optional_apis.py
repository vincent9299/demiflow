"""Real Ray coverage for the native search and Wiki optional dependencies.

Use the original API-matrix cases, including their output, bound, execution,
and native-ACK assertions. Missing dependencies fail these tests; they are not
silently converted into skipped or counted coverage.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="module")
def ray_executor():
    import ray
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import assert_tree_binding

    assert_tree_binding()
    for key in ("DEMIFLOW_RESOURCE_ROOT", "DEMIFLOW_RESOURCE_HOME", "DEMIFLOW_FORGE_DOMAIN"):
        assert os.environ.get(key, "").startswith("/tmp/demiflow-"), key
    assert not ray.is_initialized(), "this fixture only owns its private cluster"
    ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False,
             object_store_memory=100 * 1024**2,
             _temp_dir=tempfile.mkdtemp(prefix="demiflow-api-ray-", dir="/tmp"))
    executor = RayDatasetExecutor()
    try:
        with _use_executor(executor):
            yield
    finally:
        try:
            executor.close()
        finally:
            ray.shutdown()
    assert not ray.is_initialized()
    assert_tree_binding()


@pytest.mark.parametrize("case_name", [
    "native-search-http", "wiki-native", "wiki-character-bound",
])
def test_original_optional_api_case_on_real_ray(ray_executor, tmp_path, case_name):
    from runtime_v3_api_scenarios import _cases, _run_case

    assert str(tmp_path).startswith("/tmp/demiflow-")
    source = tmp_path / "sources"
    source.mkdir()
    cases = [case for case in _cases(source) if case.name == case_name]
    assert len(cases) == 1
    outcome = _run_case(SimpleNamespace(root=tmp_path), cases[0], 0, source)
    (tmp_path / "case-evidence.json").write_text(
        json.dumps(outcome, indent=2, ensure_ascii=False, default=str)
    )
    assert outcome["status"] == "passed", outcome
    assert not outcome["ray_data_import_attempts"]
    if cases[0].kind == "positive":
        assert outcome["execute_action_calls"]
        assert outcome["native_commits"]
        assert all(commit["status"] == "acked" for commit in outcome["native_commits"])
