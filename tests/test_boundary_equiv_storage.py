"""Storage snapshot: engine reader vs workbench copy.

Existing scenarios collected again with every host read shadowed by the engine
reader (see ``boundary_shadow``). Temporary: delete once the workbench reads
through ``execution.run_reader`` / ``execution.sealed_plan_facts``.
"""
from boundary_shadow import compared, shadow  # noqa: F401
from test_forge_history_admission import *  # noqa: F401,F403

# This scenario injects a one-shot fault into the first ``sqlite3.connect``. The
# host call consumes it, so a second (engine) call cannot see the same fault.
# The engine's own behaviour under that fault is proven in ``test_run_reader``.
del test_journal_replaced_before_sql_open_cannot_be_admitted  # noqa: F821


def test_zz_engine_readers_were_compared(compared):
    made = compared()
    print('engine comparisons:', dict(made))
    assert made['storage_snapshot'] >= 5, made
