"""The actual cluster dies with its owner; the existing fixture FD gate stays."""
import json
from pathlib import Path
import tempfile
import pytest


def test_ray_matrix_child_releases_process_owned_fds(tmp_path):
    pytest.importorskip('ray')
    from runtime_v3_support import RuntimeV3Fixture
    from runtime_v3_ray_matrix_process import run_ray_api_matrix
    root = Path(tempfile.mkdtemp(prefix='demiflow-ray-matrix-process-', dir='/tmp'))
    fixture = RuntimeV3Fixture(root, provider='ray_core', bounds={'max_rows': 1000})
    try:
        report = run_ray_api_matrix(fixture, 'models')
        for path in fixture.evidence_dir.iterdir():
            if path.is_file():
                (tmp_path / path.name).write_bytes(path.read_bytes())
        assert not report['failed']
        assert report['cases'] and all(case['status']=='passed' for case in report['cases'])
        # This focused test is not the 98 API gate and makes no such claim.
        assert report['not_run'] and not report['complete']
        process = report['process_boundary']
        assert process['normal_exit'] and process['returncode'] == 0
        assert len(process['parent_fds_after']) <= len(process['parent_fds_before']) + 8
        child = json.loads((tmp_path/'ray-api-child.json').read_text())
        assert not child['changed_inputs'] and not child['ray_initialized_after']
        assert len(child['cases']) == len(report['cases'])
        assert all(c['execute_action_calls'] for c in report['cases'] if c['kind']=='positive')
    finally:
        fixture.close()
