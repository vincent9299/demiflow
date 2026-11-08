import pytest

from demiflow.forge.schema import ForgeError
from demiflow.forge.store import locked
from demiflow.forge.verify import verify_project
from forge_support import create_project


def test_verify_passes_and_does_not_inherit_secrets(tmp_path, monkeypatch):
    project = create_project(tmp_path)
    (project / "tests" / "test_pipeline.py").write_text(
        "import os\n\n"
        "def test_environment_is_closed():\n"
        "    assert 'FORGE_TEST_SECRET' not in os.environ\n"
        "    assert 'PYTEST_ADDOPTS' not in os.environ\n"
    )
    monkeypatch.setenv("FORGE_TEST_SECRET", "do-not-forward")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--invalid-option")
    report = verify_project(project)
    assert report["passed"], report
    assert report["test"]["returncode"] == 0
    from pathlib import Path
    import hashlib
    log = report["test"]["logs"]["stdout"]
    assert log["sha256"] == hashlib.sha256(Path(log["path"]).read_bytes()).hexdigest()


def test_verify_fails_on_test_failure_timeout_and_output_flood(tmp_path):
    project = create_project(tmp_path, timeout=1)
    (project / "tests" / "test_pipeline.py").write_text("import time\ntime.sleep(30)\n")
    report = verify_project(project)
    assert not report["passed"]
    assert report["test"]["failure"] == "timeout"
    project = create_project(tmp_path / "flood")
    (project / "tests" / "test_pipeline.py").write_text("print('x' * 1100000)\nraise RuntimeError('release captured output')\n")
    report = verify_project(project)
    assert not report["passed"]
    assert report["test"]["failure"] == "output_limit"
    assert report["test"]["observed_bytes"]["stdout"] <= 1100000 + 65536


def test_verify_rejects_source_mutation_and_concurrent_writer(tmp_path):
    project = create_project(tmp_path)
    (project / "tests" / "test_pipeline.py").write_text(
        "from pathlib import Path\n"
        "Path('pipeline.py').write_text('changed during tests')\n"
    )
    report = verify_project(project)
    assert not report["passed"]
    assert report["source_changed_during_tests"]
    assert report["changes"]["changed"] == ["pipeline.py"]
    project = create_project(tmp_path / "busy")
    with locked(project):
        with pytest.raises(ForgeError, match="busy"):
            verify_project(project)
