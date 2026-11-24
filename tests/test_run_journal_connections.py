"""Connection reuse must preserve transaction, ownership and resource boundaries."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sqlite3
import threading

import pytest

from demiflow.errors import ExecutionConflict
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal


@pytest.mark.parametrize('failed', [False, True])
def test_owner_connections_are_bounded_and_closed_on_exit(tmp_path, monkeypatch, failed):
    opened = []
    class Tracked(sqlite3.Connection):
        closed = False
        def close(self):
            self.closed = True
            return super().close()
    original = sqlite3.connect
    def connect(*args, **kwargs):
        db = original(*args, **kwargs, factory=Tracked)
        opened.append(db)
        return db
    monkeypatch.setattr(sqlite3, 'connect', connect)
    scope = RunContext.start(tmp_path)
    try:
        with scope as run:
            for i in range(64):
                run.check_stop()
                run.journal.reserve_budget(run.fence, 'requests', str(i), 1, 64)
            assert sum(not db.closed for db in opened) == 2
            if failed:
                raise RuntimeError('operator failed')
    except RuntimeError as exc:
        assert failed and str(exc) == 'operator failed'
    assert all(db.closed for db in opened)
    assert RunJournal(tmp_path).budget_consumed('requests') == 64
    assert RunJournal(tmp_path).execution_status()['status'] == ('failed' if failed else 'succeeded')


def test_native_threads_share_exact_budget_and_observe_external_cancellation(tmp_path):
    with pytest.raises(RecoveryContractError, match='cancellation'):
        with RunContext.start(tmp_path) as run:
            def reserve(i):
                run.journal.reserve_budget(run.fence, 'requests', str(i % 40), 1, 40)
                return run.journal.budget_consumed('requests')
            with ThreadPoolExecutor(max_workers=4) as pool:
                assert max(pool.map(reserve, range(160))) == 40
                assert run.journal.budget_consumed('requests') == 40
                run.journal.execution_view()  # Must release this read snapshot.
                assert RunContext.cancel(run.ref)
                # Reusing a read connection must see the next committed state.
                pool.submit(run.check_stop).result(timeout=5)
    assert RunJournal(tmp_path).execution_status()['status'] == 'cancelled'


def test_uncommitted_writer_state_is_never_observed_and_rollback_is_reusable(tmp_path):
    with RunContext.start(tmp_path) as run:
        before = run.journal.execution_status()
        with pytest.raises(OSError, match='injected storage failure'):
            with run.journal.execution_guard(run.fence) as db:
                state = dict(before, status='stopping')
                db.execute('UPDATE execution_state SET document=?', (json.dumps(state),))
                assert run.journal.execution_status() == before
                raise OSError('injected storage failure')
        run.check_stop()
        run.journal.reserve_budget(run.fence, 'requests', 'after-rollback', 1, 1)
    assert RunJournal(tmp_path).budget_consumed('requests') == 1


def test_native_sink_thread_can_poll_while_publisher_holds_fence(tmp_path):
    with RunContext.start(tmp_path) as run:
        with ThreadPoolExecutor(max_workers=1) as pool:
            with run.journal.execution_guard(run.fence):
                # Lance consumes Arrow batches on another thread and waits for
                # that callback before returning from the fenced publication.
                assert pool.submit(run.journal.execution_status).result(timeout=2)['status'] == 'running'


def test_cached_attempt_cannot_mutate_after_independent_owner_transfer(tmp_path):
    control = RunJournal(tmp_path)
    control.initialize({'fixed': True}, attempts=True)
    control.transition({None}, 'running')
    worker = RunJournal(tmp_path, attempt=1)
    with worker.connection_scope():
        worker.execution_view()
        worker.record_usage(elapsed_ms=1, output_bytes=0, complete=True)
        control.transition({'running'}, 'interrupted')
        state = control.execution_status()
        control.begin_attempt(expected_attempt=1, expected_revision=state['revision'])
        with pytest.raises(ValueError, match='Stale'):
            worker.record_usage(elapsed_ms=2, output_bytes=0)
        with pytest.raises(ValueError, match='Stale'):
            worker.execution_status()
    assert control.execution_status()['attempt'] == 2
    assert control.execution_status()['usage']['elapsed_ms'] == 1


def test_sqlite_full_rolls_back_partial_budget_and_reopens_connection(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({'fixed': True}, attempts=True)
    with journal.connection_scope():
        with pytest.raises(sqlite3.OperationalError, match='full'):
            with journal._transaction() as db:
                pages = db.execute('PRAGMA page_count').fetchone()[0]
                db.execute('PRAGMA max_page_count=' + str(pages))
                db.execute("INSERT INTO runtime_budgets VALUES ('partial',1,2)")
                db.execute('INSERT INTO runtime_sources VALUES (?,?)', ('huge', 'x' * 1024**2))
        assert journal.budget_consumed('partial') == 0
        assert journal.source_binding('huge') is None
        assert journal.transition({None}, 'prepared')
    assert journal.execution_status()['status'] == 'prepared'


@pytest.mark.parametrize('write', [False, True])
def test_connection_initialization_failure_closes_handle(tmp_path, monkeypatch, write):
    journal = RunJournal(tmp_path)
    journal.initialize({'fixed': True})
    opened = []
    class Failing(sqlite3.Connection):
        closed = False
        def execute(self, sql, *args):
            if 'PRAGMA cache_size' in sql:
                raise sqlite3.OperationalError('injected initialization failure')
            return super().execute(sql, *args)
        def close(self):
            self.closed = True
            return super().close()
    original = sqlite3.connect
    def connect(*args, **kwargs):
        db = original(*args, **kwargs, factory=Failing)
        opened.append(db)
        return db
    monkeypatch.setattr(sqlite3, 'connect', connect)
    with journal.connection_scope(), pytest.raises(sqlite3.OperationalError, match='initialization'):
        with journal._connection(write=write):
            pytest.fail('uninitialized connection admitted')
    assert opened and all(db.closed for db in opened)


def test_forked_owner_is_rejected_before_inherited_lock_acquisition(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({'fixed': True}, attempts=True)
    held, release = threading.Event(), threading.Event()
    with journal.connection_scope():
        def hold():
            with journal._connection_lock, journal._reader_lock:
                held.set()
                release.wait(5)
        thread = threading.Thread(target=hold)
        thread.start()
        assert held.wait(2)
        pid = os.fork()
        if pid == 0:
            try:
                import signal
                signal.alarm(3)
                try:
                    journal.execution_status()
                except ExecutionConflict:
                    # A fresh object can still inspect without claiming the run.
                    assert RunJournal(tmp_path).execution_status()['attempt'] == 1
                    os._exit(0)
                os._exit(2)
            except BaseException:
                os._exit(3)
        release.set()
        thread.join(3)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
