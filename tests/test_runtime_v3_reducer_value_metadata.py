"""Sink metadata uses real bounded artifacts for table and value collections."""
from dataclasses import replace
import json

import pytest

from demiflow.execution.contracts.dataflow import ArtifactRef, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.staging import DriverArtifactPort
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.contracts import record_codec as frames
from demiflow.execution.outputs import sinks
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.resources.contracts import ResourceProfile


def test_bounded_sink_table_consistency_drift_and_mixed_values(tmp_path):
    from runtime_v3_regression_inventory import assert_private
    from test_runtime_v3_c import bounds
    from test_runtime_v3_d import (ACTION, SINK_ID, make_graph, make_physical,
        make_kernel, make_record, runtime_services, driver_services)
    from demiflow.execution.seal.plan import compute_physical_digest
    assert_private()
    # This new case admits the actual frame workspace. D009/D010 retain their
    # original 1 KiB grants and are not changed to reach the metadata branch.
    budget = bounds(max_nodes=16, max_edges=16, max_partitions=8, max_depth=16,
        max_control_bytes=8192, max_row_bytes=4096, max_batch_bytes=8192,
        max_resident_bytes=65536, max_state_bytes=65536, max_spill_bytes=1024**2,
        max_pending_events=16, max_open_files=16, max_output_rows=16)
    with RunContext.start(tmp_path / 'run') as run:
        declaration = {'kind': 'dataflow', 'source': {'source_id': 'sink-metadata'}}
        run.journal.bind_execution_action(run.fence, ACTION, declaration)
        action = RunAction(run, ACTION, declaration)
        store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id='test',
            journal=DriverArtifactPort(action, bounds=budget), bounds=budget)
        try:
            partitions = ('p0', 'p1', 'p2', 'p3', 'p4', 'p-empty', 'p-unknown')
            physical = make_physical(ACTION, make_graph(), store, partitions=partitions)
            profile = ResourceProfile(requests={'cpu_millis': 100, 'memory_bytes': 65536})
            physical = replace(physical, bounds=budget,
                stages=tuple(replace(stage, profile=profile) for stage in physical.stages))
            physical = replace(physical, plan_id=compute_physical_digest(physical))
            runtime = runtime_services(action, physical, store, partition_ids=partitions)
            driver = driver_services(action, physical, store)

            def stage(partition, values, *, external=False):
                from demiflow.execution.contracts.record_codec import _encode_payload, _decode_payload
                spec = make_kernel(configuration={'sink_id': SINK_ID, 'partition_id': partition})
                def records():
                    for i, value in enumerate(values):
                        record = make_record(i, value, budget)
                        if external:
                            payload = _encode_payload(value, max_bytes=budget.max_row_bytes)
                            ref = store.put(iter((payload,)), max_bytes=budget.max_row_bytes,
                                media_type='application/octet-stream')
                            record = replace(record, inline=None, payload_ref=ref)
                        yield record
                ref = sinks.prepare_sink_partition(records(), spec=spec, services=runtime)
                manifest = json.loads(b''.join(store.read(ref, max_bytes=budget.max_control_bytes)))
                raw = b''.join(store.read(ArtifactRef(**manifest['data_ref']), max_bytes=budget.max_batch_bytes))
                actual = []
                for line in raw.splitlines(keepends=True):
                    document, fields = frames.decode_frame(line, budget)
                    record = frames.decode_document(document, record_id=fields['record_id'],
                        sequence=fields['sequence'], bounds=budget)
                    value = thaw_record_value(record.inline)
                    if record.payload_ref is not None:
                        value = _decode_payload(b''.join(store.read(record.payload_ref,
                            max_bytes=budget.max_row_bytes)), max_bytes=budget.max_row_bytes)
                    actual.append(value)
                assert actual == values
                assert manifest['rows'] == len(values)
                return ref, manifest

            first, first_manifest = stage('p0', [{'x': 1}])
            same, _ = stage('p1', [{'x': 2}])
            empty, _ = stage('p-empty', [])
            assert first_manifest['columns'] == ['x']
            consistent = sinks.combine_prepared_sinks((first, same, empty), sink_id=SINK_ID, store=driver.store, bounds=driver.bounds)
            assert consistent['columns'] == ['x'] and consistent['total_rows'] == 2

            different, _ = stage('p2', [{'y': 3}])
            unknown, unknown_manifest = stage('p-unknown', [{'x': 4}], external=True)
            assert unknown_manifest['columns'] is None
            assert 'non_mapping_values' not in unknown_manifest
            for refs in ((first, different), (different, first),
                         (first, different, unknown), (unknown, different, first)):
                with pytest.raises(DataflowError, match='SOURCE_CHANGED.*schema drifted'):
                    sinks.combine_prepared_sinks(refs, sink_id=SINK_ID, store=driver.store, bounds=driver.bounds)

            value, value_manifest = stage('p3', [['a', 'b']])
            assert value_manifest['columns'] is None
            assert value_manifest['non_mapping_values'] is True
            for refs in ((first, different, value), (value, different, first)):
                mixed = sinks.combine_prepared_sinks(refs, sink_id=SINK_ID, store=driver.store, bounds=driver.bounds)
                assert mixed['columns'] is None and 'schema_ref' not in mixed
                assert mixed['total_rows'] == 3
            mixed_ref, mixed_manifest = stage('p4', [{'x': 1}, ['a', 'b'], {'y': 2}])
            assert mixed_manifest['columns'] is None
            assert mixed_manifest['non_mapping_values'] is True
            combined = sinks.combine_prepared_sinks((first, mixed_ref), sink_id=SINK_ID, store=driver.store, bounds=driver.bounds)
            assert combined['columns'] is None and 'schema_ref' not in combined
            assert combined['total_rows'] == 4
            from test_runtime_v3_small_terminal_metadata import owner
            def restored(ref):
                runner = owner(store, ref)
                runner._terminal_spec = make_kernel(configuration={'sink_id': SINK_ID})
                runner.e._restore_image.contributions[0].sinks = {SINK_ID: ref}
                return runner
            # Read the original, authenticated manifests through the real
            # unfinished-terminal restoration path, including old absent flags.
            for ref, flag in ((first, False), (unknown, False), (mixed_ref, True)):
                runner = restored(ref)
                runner._terminal_non_mapping_values = not flag
                try:
                    runner._restore_terminal()
                    assert runner._restored_terminal
                    assert runner._terminal_non_mapping_values is flag
                    if flag:
                        assert runner._terminal_columns is None
                finally:
                    runner._terminal_blocks.close()
            for malformed in (None, 0, 1, 'true', []):
                raw = json.dumps({**unknown_manifest, 'non_mapping_values': malformed}).encode()
                invalid = store.put(iter((raw,)), max_bytes=budget.max_control_bytes,
                    media_type=sinks.MANIFEST_MEDIA)
                with pytest.raises(DataflowError, match='CORRUPT_ARTIFACT.*non_mapping_values'):
                    sinks.combine_prepared_sinks((first, different, invalid), sink_id=SINK_ID, store=driver.store, bounds=driver.bounds)
                runner = restored(invalid)
                try:
                    with pytest.raises(DataflowError, match='CORRUPT_ARTIFACT.*non_mapping_values'):
                        runner._restore_terminal()
                    assert not runner._restored_terminal
                finally:
                    runner._terminal_blocks.close()
        finally:
            store.close()
        run.journal.complete_execution_action(run.fence, ACTION, {'kind': 'test-complete'})
