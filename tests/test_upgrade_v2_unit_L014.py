"""Current worker admission must bind all original occurrences, in order."""
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.seal.plan import verify_action_programs
from test_runtime_v3_real_seams import prepared


@pytest.mark.parametrize('damage', ['reorder', 'omit_middle', 'insert', 'foreign'])
def test_three_stage_plan_rejects_changed_program_membership(tmp_path, damage):
    dataset = data.range(3).map(lambda row: row).map(lambda row: dict(row)).map(lambda row: row['id'])
    with prepared(tmp_path, dataset) as (execution, services):
        original = execution._sealed
        members = original.programs
        assert len(members) >= 3

        def verify(value):
            return verify_action_programs(services.action, execution._graph, execution._physical, value,
                                          store=services.store, bounds=services.bounds)

        verify(original)
        if damage == 'reorder':
            altered = (members[1], members[0], *members[2:])
        elif damage == 'omit_middle':
            altered = (members[0], *members[2:])
        elif damage == 'insert':
            altered = (*members, members[0])
        else:
            from demiflow.execution.recovery_contract import identity
            altered = (members[0], replace(members[1], digest=identity('other-occurrence')), *members[2:])
        with pytest.raises(DataflowError):
            verify(replace(original, programs=altered))
        assert not services.action.context.journal.execution_commits(services.action.action_id)
