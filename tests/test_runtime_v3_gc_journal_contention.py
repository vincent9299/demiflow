"""Optional published-object GC cannot turn supervisor accounting into fencing."""
import threading

from demiflow.execution.dataflow_gc import _exclusive
from demiflow.execution.run_journal import RunJournal, ExecutionConflict
from test_runtime_v3_artifact_accounting import store_fixture


def test_supervisor_usage_waits_for_original_gc_lease(tmp_path, monkeypatch):
    import fcntl
    import os
    with store_fixture(tmp_path) as (run, store, port):
        store.close()
        entered, release, attempted, finished = (threading.Event() for _ in range(4))
        failures = []
        original_flock = fcntl.flock

        def observed_flock(fd, operation):
            if (threading.current_thread().name == 'supervisor-metadata-test'
                    and operation & fcntl.LOCK_SH):
                attempted.set()
            return original_flock(fd, operation)

        monkeypatch.setattr(fcntl, 'flock', observed_flock)

        def collector():
            try:
                with _exclusive(run.journal, run.fence):
                    entered.set()
                    assert release.wait(3)
            except BaseException as exc:
                failures.append(exc)

        def supervisor():
            try:
                # A separate supervisor owns its own journal connection, as in
                # the actual workbench process; it must retain attempt fencing.
                RunJournal(run.directory, attempt=run.attempt).record_usage(
                    elapsed_ms=1000, output_bytes=12)
            except BaseException as exc:
                failures.append(exc)
            finally:
                finished.set()

        before = len(os.listdir('/proc/self/fd'))
        gc = threading.Thread(target=collector)
        observer = threading.Thread(target=supervisor, name='supervisor-metadata-test')
        gc.start()
        try:
            assert entered.wait(2)
            observer.start()
            assert attempted.wait(2)
            assert not finished.wait(.05), repr(failures)
        finally:
            release.set()
            gc.join(3)
            if observer.ident is not None:
                observer.join(3)
        assert not gc.is_alive() and not observer.is_alive()
        assert not failures
        assert run.journal.execution_status()['usage'] == {'elapsed_ms':1000, 'output_bytes':12}
        assert len(os.listdir('/proc/self/fd')) == before


def test_waiting_metadata_still_rejects_another_attempt(tmp_path):
    import pytest
    with store_fixture(tmp_path) as (run, store, port):
        store.close()
        original = run.journal.execution_status()
        with pytest.raises(ExecutionConflict, match='Stale execution attempt'):
            RunJournal(run.directory, attempt=run.attempt + 1).record_usage(
                elapsed_ms=1000, output_bytes=12)
        assert run.journal.execution_status() == original


def test_contended_shared_wait_is_bounded_and_leaves_no_descriptor(tmp_path):
    import os
    import time
    import pytest
    from demiflow.execution.dataflow_object_lease import shared_access
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with store_fixture(tmp_path) as (run, store, port):
        store.close()
        entered, release = threading.Event(), threading.Event()
        failures = []

        def collector():
            try:
                with _exclusive(run.journal, run.fence):
                    entered.set()
                    assert release.wait(3)
            except BaseException as exc:
                failures.append(exc)

        gc = threading.Thread(target=collector)
        gc.start()
        try:
            assert entered.wait(2)
            before = len(os.listdir('/proc/self/fd'))
            started = time.monotonic()
            with pytest.raises(DataflowError, match='CAPACITY_PENDING'):
                with shared_access(store.root, wait_ms=30):
                    raise AssertionError('entered a protected collector interval')
            assert .025 <= time.monotonic() - started < 1
            assert len(os.listdir('/proc/self/fd')) == before
        finally:
            release.set()
            gc.join(3)
        assert not gc.is_alive() and not failures


def test_changed_protocol_during_wait_is_still_fenced(tmp_path, monkeypatch):
    import fcntl
    import json
    from demiflow.execution.dataflow_object_lease import HEADER, shared_access
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with store_fixture(tmp_path) as (run, store, port):
        store.close()
        entered, release, attempted = (threading.Event() for _ in range(3))
        failures, outcome = [], []
        header = store.root / HEADER
        original = header.read_bytes()
        original_flock = fcntl.flock

        def observed_flock(fd, operation):
            try:
                return original_flock(fd, operation)
            except BlockingIOError:
                if threading.current_thread().name == 'waiting-journal-test':
                    attempted.set()
                raise

        monkeypatch.setattr(fcntl, 'flock', observed_flock)

        def collector():
            try:
                with _exclusive(run.journal, run.fence):
                    entered.set()
                    assert release.wait(3)
            except BaseException as exc:
                failures.append(exc)

        def waiter():
            try:
                with shared_access(store.root, wait_ms=500):
                    outcome.append('incorrectly admitted')
            except BaseException as exc:
                outcome.append(exc)

        gc = threading.Thread(target=collector)
        waiting = threading.Thread(target=waiter, name='waiting-journal-test')
        gc.start()
        try:
            assert entered.wait(2)
            waiting.start()
            assert attempted.wait(2)
            identity = json.loads(original)
            # Tamper the original lock inode in either version, retaining the
            # original wait, FENCED, and no-admission assertions below.
            inode_index = 1 if len(identity) == 2 else 3
            identity[inode_index] += 1
            header.write_text(json.dumps(identity))
            release.set()
            gc.join(3)
            waiting.join(3)
            assert not gc.is_alive() and not waiting.is_alive() and not failures
            assert len(outcome) == 1 and isinstance(outcome[0], DataflowError)
            assert 'FENCED' in str(outcome[0]) and 'changed while waiting' in str(outcome[0])
        finally:
            release.set()
            gc.join(3)
            if waiting.ident is not None:
                waiting.join(3)
            header.write_bytes(original)
