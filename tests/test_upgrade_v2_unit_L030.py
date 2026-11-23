"""Each actual native v2 subprogram retains the original source, order and code pin."""
import pytest

from demiflow.execution.recovery_contract import identity
from demiflow.execution.seal.sealed_plans import load_execution_bindings, load_sealed_plan
from test_runtime_v3_sealed_version_selection import write_published_native_seal


@pytest.mark.parametrize('shape', ['root', 'union', 'join'])
def test_published_bindings_preserve_nested_paths_and_program_pins(tmp_path, legacy_seal_writer, shape):
    root, action_id = write_published_native_seal(legacy_seal_writer, tmp_path / 'run', shape=shape)
    sealed = load_sealed_plan(root, action_id, version='v2')
    bindings = load_execution_bindings(root, action_id, sealed_plan=sealed)
    programs = bindings['document']['executions'][0]['programs']
    paths = {tuple(program['input_path']) for program in programs}
    expected = {()}
    if shape == 'union':
        expected.update({('source', 'inputs', 0), ('source', 'inputs', 1)})
    if shape == 'join':
        expected.update({('source', 'left'), ('source', 'right')})
    assert paths == expected
    assert len(paths) == len(programs)
    for program in programs:
        assert program['program_id'] == identity([action_id, program['input_path']])
        assert len(program['plan_ref']['code_pin_digest']) == 64
        assert len(program['source_identity_ref']) == 64
        assert [item['program_index'] for item in program['operator_bindings']] == list(
            range(len(program['sealed_operators'])))
