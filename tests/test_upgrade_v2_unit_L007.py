"""Interpret the native driver layouts actually written by the published v2 engine."""
import pytest

from demiflow.execution.seal.plan import load_native_execution
from demiflow.execution.seal.sealed_plans import load_execution_bindings, load_sealed_plan
from demiflow.execution.run_context import RunContext
from test_runtime_v3_sealed_version_selection import write_published_native_seal


@pytest.mark.parametrize(("kind", "sink"), [("materialize", "output"), ("write_lance", "lance")])
def test_original_native_driver_owns_its_stages_and_sink(tmp_path, legacy_seal_writer, kind, sink):
    root, action = write_published_native_seal(legacy_seal_writer, tmp_path / 'run', kind=kind)
    sealed = load_sealed_plan(root, action, version='v2')
    doc = load_execution_bindings(root, action, sealed_plan=sealed)['document']
    layout, placement = load_native_execution(RunContext(root), action,
                                             execution_id=doc['executions'][0]['execution_id'])
    assert placement.placement == 'driver' and placement.backend == 'datafusion'
    assert placement.recovery == 'final_commit'
    assert not layout.workers and not sealed['physical_plan']['worker_groups']
    assert all(placement.stage_limits[sid]['inflight_rows'] > 0 for sid in placement.stage_ids)
    assert sink in {s for terminal in sealed['logical_dag']['terminals'] for s in terminal['sink_ids']}
