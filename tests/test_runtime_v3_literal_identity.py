"""Large literal payloads retain old identity bytes without becoming metadata."""
import hashlib
import json
import tracemalloc

import pytest

from demiflow import data
from demiflow.data.sources import ItemsSource
from demiflow.execution.seal.program_identity import literal_source_digest
from demiflow.execution.contracts.recovery_contract import document, identity, RecoveryContractError
from demiflow.execution.seal.program_identity import describe, source_identity
from demiflow.execution.pipeline.run_context import RunContext


@pytest.mark.parametrize('count', [0, 1, 1024, 1025, 2500])
def test_literal_digest_preserves_old_collection_and_source_bytes(count):
    rows = tuple({'i': i, 'payload': '\\"\n中文🙂', 'zero': -0.0, 'raw': b'raw'}
                 for i in range(count))
    for value in (rows, (rows,), ItemsSource(rows)):
        assert literal_source_digest(value) == identity(describe(value))


def test_large_literal_hash_is_incremental_and_metadata_limit_is_unchanged():
    value = (({'image': 'data:image/png;base64,' + 'YWJj' * 400000},),)
    expected = hashlib.sha256(json.dumps(describe(value), sort_keys=True,
        ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()).hexdigest()
    with pytest.raises(RecoveryContractError, match='byte budget'):
        document(describe(value))
    tracemalloc.start()
    try:
        actual = literal_source_digest(value)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert actual == expected
    assert peak < 512 * 1024  # No full JSON/UTF-8 copy of the 1.6 MB string.
    changed = (({'image': value[0][0]['image'][:-1] + 'K'},),)
    assert literal_source_digest(changed) != actual


def test_large_row_inside_historical_collection_projection():
    rows = [{'image': 'x' * 1600000}] + [{'i': i} for i in range(1024)]
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(describe(row), sort_keys=True, ensure_ascii=False,
                                 separators=(',', ':')).encode())
        digest.update(b'\n')
    expected = identity([{'items': len(rows), 'sha256': digest.hexdigest()}])
    assert literal_source_digest((rows,)) == expected


def test_literal_identity_still_rejects_content_and_structural_overflow():
    with pytest.raises(RecoveryContractError, match='content byte budget'):
        literal_source_digest({'x': 'y' * 80}, max_bytes=64)
    cycle = []
    cycle.append(cycle)
    with pytest.raises(RecoveryContractError, match='structural budget'):
        literal_source_digest(cycle)
    with pytest.raises(RecoveryContractError, match='Nonfinite'):
        literal_source_digest([float('inf')])


def test_real_large_literal_dataset_commits_and_reuses_original_result(tmp_path):
    payload = 'data:image/png;base64,' + 'YWJj' * 400000
    with RunContext.start(tmp_path / 'run', limits={
            'memory_bytes': 256 * 1024**2, 'max_row_bytes': 4 * 1024**2,
            'max_batch_bytes': 4 * 1024**2}), data.local_execution(workers=1):
        ds = data.from_items([{'image': payload}])
        before = source_identity(ds._source)
        result = ds.map(lambda row: {'bytes': len(row['image'])}).materialize()
        try:
            assert result.take_all() == [{'bytes': len(payload)}]
            assert result.take_all() == [{'bytes': len(payload)}]
            assert source_identity(ds._source) == before
        finally:
            result.release()
