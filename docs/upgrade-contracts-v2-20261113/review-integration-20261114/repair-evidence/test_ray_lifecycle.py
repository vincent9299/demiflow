"""Run L016's existing process/cleanup proofs with our bounded owned cluster."""
import pytest

from test_gpt_ray_integration import ray, ray_cluster
from test_upgrade_v2_unit_L016 import (
    test_real_ray_success_issues_the_drained_terminal,
    test_real_ray_worker_failure_issues_the_abandon_terminal,
    test_real_ray_terminal_converges_to_closed_and_confirmed,
    test_real_ray_actor_death_terminal_and_resume_leaves_no_orphan,
    test_real_ray_failure_before_any_progress_leaves_no_residual_actor,
)


@pytest.fixture
def ray_local(ray_cluster):
    return ray
