"""Runtime snapshot, control events and state: engine reader vs workbench copy.

Existing scenarios collected again with every host read shadowed by the engine
reader (see ``boundary_shadow``). Temporary: delete once the workbench reads
through ``execution.run_reader`` / ``execution.sealed_plan_facts``.
"""
from boundary_shadow import compared, shadow  # noqa: F401
from test_forge_runtime_management import (  # noqa: F401
    base, root,
    test_rm04_repeated_cancel_preserves_first_request,
    test_rm18_large_action_values_never_enter_runtime_snapshot,
    test_rm18_snapshot_revision_includes_usage_and_attempt_cursor)
from test_architecture_boundaries import (  # noqa: F401
    test_run_context_is_project_read_only_and_does_not_reconcile)


def test_zz_engine_readers_were_compared(compared):
    made = compared()
    print('engine comparisons:', dict(made))
    assert (made['snapshot'] >= 1 and made['events'] >= 1 and made['state'] >= 2
            and made['context_facts'] >= 1), made
