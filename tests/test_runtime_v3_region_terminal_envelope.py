"""The original full-value reducer counterexample, through the native region."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.record_codec import _encode_payload
from _native_relational_fixtures import native_region_evidence, oracle_join


@pytest.mark.parametrize('row_bytes,key_bytes', [(4096, 400), (16384, 6144)])
def test_region_legal_fold_payload_survives_terminal_envelope(tmp_path, row_bytes, key_bytes):
    # Keep the original r5 data and full tuple output. A compact checksum alone
    # cannot prove that a legal wide result survives staging and public reading.
    keys = ['a' * key_bytes, 'b' * key_bytes, None]
    left = [{'k': keys[i % 3], 'v': i} for i in range(24)]
    right = [{'k': key, 'r': j} for key in keys for j in range(2)]
    last = [{'k': key, 'z': j} for key in keys for j in range(2)]
    groups = {}
    for row in oracle_join(oracle_join(left, right, how='left'), last, how='left'):
        groups.setdefault(row['k'], []).append((row['v'], row.get('r'), row.get('z')))
    expected = [{'k': key, 'values': groups[key]}
                for key in [*sorted(k for k in groups if k is not None), None]]
    assert all(len(_encode_payload(row, max_bytes=row_bytes)) <= row_bytes for row in expected)
    limits = {'memory_bytes': 256 * row_bytes, 'max_row_bytes': row_bytes,
              'max_batch_bytes': 2 * row_bytes, 'max_state_bytes': 32768, 'checkpoint_rows': 64}
    with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=2) as local:
        actual = data.from_items(left).join(data.from_items(right), on='k', how='left').join(
            data.from_items(last), on='k', how='left').reduce_by_key('k', lambda state, row: {
                'k': row['k'], 'values': (state or {}).get('values', []) + [
                    (row['v'], row.get('r'), row.get('z'))]}).take_all()
        assert actual == expected
        _, physical, counts = native_region_evidence(local)
        assert len(counts) == 1
        assert counts[0]['sort_runs'] > 2
        assert counts[0]['partition_builds'] == 3
        assert counts[0]['intermediate_key_rebuilds'] == counts[0]['intermediate_payload_bytes'] == 0
        region = next(stage for stage in physical.stages if stage.stage_id.startswith('relational-v1-'))
        assert region.resident_bytes < physical.bounds.max_resident_bytes
        commits = run.journal.execution_commits('action-1')
        assert commits and all(item['status'] == 'acked' for item in commits)
