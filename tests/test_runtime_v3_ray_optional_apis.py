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
    ray = pytest.importorskip("ray", reason="requires a real Ray Core environment")
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


def test_real_ray_source_identity_and_bounded_overflow(ray_executor, tmp_path):
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext, current_run
    from demiflow.execution.run_journal import RunJournal

    def identity_rows():
        from demiflow.execution.pipeline.run_context import current_run, current_action
        run = current_run()
        assert not hasattr(run, 'journal') and current_action() is None
        yield {'directory': run.ref.directory, 'run_id': run.ref.run_id, 'pid': os.getpid()}

    with RunContext.start(tmp_path / 'identity') as run:
        result = data.from_iter(identity_rows).take_all()
        assert len(result) == 1 and result[0]['directory'] == run.ref.directory
        assert result[0]['run_id'] == run.ref.run_id and result[0]['pid'] != os.getpid()
        assert current_run() is run
    assert current_run() is None

    evidence = tmp_path / 'source-reads.jsonl'
    def rows():
        try:
            for index in range(5):
                with evidence.open('a') as stream:
                    stream.write(json.dumps({'index': index, 'pid': os.getpid()}) + '\n')
                yield {'index': index}
        finally:
            evidence.with_suffix('.closed').write_text(current_run().ref.run_id)

    with pytest.raises(ValueError, match='more than the take_all limit'):
        with RunContext.start(tmp_path / 'overflow'):
            data.from_iter(rows).take_all(limit=2)
    observed = [json.loads(line) for line in evidence.read_text().splitlines()]
    assert [item['index'] for item in observed] == [0, 1, 2]
    assert all(item['pid'] != os.getpid() for item in observed)
    journal = RunJournal(tmp_path / 'overflow')
    assert evidence.with_suffix('.closed').read_text() == journal.recovery_identity()['run_id']
    assert journal.execution_action('action-1')['result'] is None
    assert current_run() is None
