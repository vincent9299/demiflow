"""Observation pages and revisions: engine reader vs workbench copy.

Existing scenarios collected again with every host read shadowed by the engine
reader (see ``boundary_shadow``). Temporary: delete once the workbench reads
through ``execution.run_reader`` / ``execution.sealed_plan_facts``.
"""
from boundary_shadow import compared, shadow  # noqa: F401
from test_forge_workbench import (  # noqa: F401
    demo, isolated, private_runtime_domain, request, server, service,
    test_observation_cursor_rejects_same_run_reference_in_another_project,
    test_observation_pages_are_occurrence_and_attempt_bound,
    test_observation_revision_detects_add_update_rollback_and_cleanup)


def test_zz_engine_readers_were_compared(compared):
    made = compared()
    print('engine comparisons:', dict(made))
    assert made['observations'] >= 2 and made['observation_rows'] >= 1 and made['observation_revision'] >= 2, made
