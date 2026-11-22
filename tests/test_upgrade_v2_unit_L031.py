"""The original interpreter authenticates path, stage and program data before exposing a v2 layout."""
import copy

import pytest

from demiflow.execution.plan_interpreter import PlanContractError, interpret_physical_plan
from demiflow.execution.sealed_plans import load_execution_bindings, load_sealed_plan
from test_runtime_v3_sealed_version_selection import write_published_native_seal


@pytest.mark.parametrize(('field','bad'), [
    ('input_path', [-1]), ('input_path', [None]), ('input_path', 'source'),
    ('source_identity_ref', None), ('program_id', ''),
    ('plan_ref', {}), ('sealed_operators', {}), ('operator_bindings', []),
])
def test_corrupt_archived_program_binding_refused(tmp_path, legacy_seal_writer, field, bad):
    root, action = write_published_native_seal(legacy_seal_writer, tmp_path / 'run', shape='union')
    sealed = load_sealed_plan(root, action, version='v2')
    original = load_execution_bindings(root, action, sealed_plan=sealed)
    def read(bundle):
        return interpret_physical_plan(sealed['logical_dag'], sealed['physical_plan'], bindings=bundle,
                                       manifest_ref=sealed['facts']['artifact_manifest_ref'])
    assert len(read(original).executions) == 1
    bad_bundle = copy.deepcopy(original)
    program = next(p for p in bad_bundle['document']['executions'][0]['programs'] if p['operator_bindings'])
    program[field] = bad
    with pytest.raises(PlanContractError):
        read(bad_bundle)
    assert load_execution_bindings(root, action, sealed_plan=sealed) == original
