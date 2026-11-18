"""Real v3 run: observation scope and v3 sealed plan view, engine vs workbench.

Existing scenarios collected again with every host read shadowed by the engine
reader (see ``boundary_shadow``). Temporary: delete once the workbench reads
through ``execution.run_reader`` / ``execution.sealed_plan_facts``.
"""
from boundary_shadow import compared, shadow  # noqa: F401
from test_ui_d19_observation_scope import *  # noqa: F401,F403


def test_zz_engine_readers_were_compared(compared):
    made = compared()
    print('engine comparisons:', dict(made))
    assert made['observations'] >= 1 and made['state'] >= 1, made
