"""Native callbacks retain one run and release it in its original context."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow.data.api import DataAPI
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext, current_run, native_rows
from demiflow.execution.run_journal import RunJournal


def on_thread(fn):
    def invoke():
        try:
            return fn()
        finally:
            assert current_run() is None  # No scope leaked into the native caller.
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(invoke).result(timeout=30)


class Reader:
    def __init__(self):
        self.closed_under = None

    @native_rows
    def rows(self, fail=False):
        try:
            yield current_run().ref
            if fail:
                raise RuntimeError('source-stopped')
            yield current_run().ref
        finally:
            self.closed_under = current_run().ref


def test_existing_run_is_retained_through_native_pull_and_close(tmp_path):
    reader = Reader()
    with RunContext.start(tmp_path / 'run') as run:
        stream = reader.rows()
        assert on_thread(lambda: next(stream)) == run.ref
        on_thread(stream.close)
        assert reader.closed_under == run.ref
        assert current_run() is run and run.active
    assert RunJournal(tmp_path / 'run').execution_status()['status'] == 'succeeded'


def test_unscoped_pull_owns_one_context_across_thread_switches():
    reader = Reader()
    stream = reader.rows()
    reference = on_thread(lambda: next(stream))
    assert on_thread(lambda: next(stream)) == reference
    with pytest.raises(StopIteration):
        on_thread(lambda: next(stream))
    assert reader.closed_under == reference and current_run() is None
    assert RunJournal(reference.directory).execution_status()['status'] == 'succeeded'


def test_failed_pull_releases_owner_without_masking_original_error():
    reader = Reader()
    stream = reader.rows(fail=True)
    reference = on_thread(lambda: next(stream))
    with pytest.raises(RuntimeError, match='source-stopped'):
        on_thread(lambda: next(stream))
    assert reader.closed_under == reference and current_run() is None
    assert RunJournal(reference.directory).execution_status()['status'] == 'failed'
    with RunContext.resume(reference):
        pass  # Ownership lease was released by the failed iterator.


def test_native_arrow_pack_uses_parent_run_for_every_callback(tmp_path):
    seen = []
    def records():
        for key in range(6):
            seen.append(current_run().ref)
            yield {'k': key, 'payload': b'x' * (2 * 1024**2)}
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    source = DataAPI(executor)
    try:
        with RunContext.start(tmp_path / 'run') as run:
            rows = (source.from_iter(records)
                    .join(source.from_items([{'k': i} for i in range(6)]), on='k')
                    .map(lambda row: {'k': row['k']}).take_all())
            assert sorted(rows, key=lambda r: r['k']) == [{'k': i} for i in range(6)]
            assert seen == [run.ref] * 6
            assert executor._run_ref == run.ref
    finally:
        executor.close()


def test_wide_native_row_rejects_without_commit_and_accepts_explicit_budget(tmp_path):
    from demiflow.execution import RecoveryContractError
    from test_dataset_large_payload import test_indivisible_payload_over_eight_mib_survives_join_and_sink
    # The old frozen candidate incorrectly used the 8 MiB batching target as
    # a default row ceiling. Keep its evidence; exercise a real explicit cap here.
    with pytest.raises(RuntimeContractError, match='byte budget'):
        with RunContext.start(tmp_path / 'rejected-run', limits={
                'max_row_bytes': 8 * 1024**2, 'max_batch_bytes': 8 * 1024**2}):
            test_indivisible_payload_over_eight_mib_survives_join_and_sink(tmp_path)
    assert not (tmp_path / 'wide.lance').exists()
    with RunContext.start(tmp_path / 'admitted-run', limits={
            'max_row_bytes': 16 * 1024**2, 'max_batch_bytes': 32 * 1024**2}):
        test_indivisible_payload_over_eight_mib_survives_join_and_sink(tmp_path)
