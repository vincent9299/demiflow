"""Explicit managed host bindings keep the existing run and cleanup owner."""
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.pipeline.checked import CheckedExecution, current as current_checked
from demiflow.execution.compiler_support.dataflow_compile_inputs import executor_defaults
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.pipeline.run_context import RunContext, current_run
from demiflow.execution.run_journal import RunJournal


@pytest.mark.parametrize('failure', [None, RuntimeError('cancelled by host')])
def test_host_configuration_reuses_journal_and_closes_on_exit(tmp_path, monkeypatch, failure):
    journal = RunJournal(tmp_path / 'run')
    journal.initialize({'run_id': 'host-fixture', 'root': str(tmp_path), 'entry': 'fixture:run',
        'digest': 'b' * 64, 'report_digest': 'a' * 64, 'parameter_path': None,
        'outputs': [], 'limits': {}, 'created_at': 0}, attempts=True)
    journal.transition({None}, 'running')
    closed = []
    close = LocalDatasetExecutor.close
    monkeypatch.setattr(LocalDatasetExecutor, 'close', lambda self: (closed.append(self), close(self))[-1])
    context = CheckedExecution(nodes=[], edges=[], actions=[], operators=[], source_root=tmp_path,
        limits={}, record=lambda *args: None, check_stop=lambda: None, report_digest='a' * 64)

    def run():
        with RunContext.attach(journal) as owner, data.local_execution(
                workers=1, worker_mode='thread', resource_root=tmp_path / 'source',
                diagnostics_root=tmp_path / 'diagnostics', block_size=7, batch_rows=11,
                planning={'limits': {'max_parallelism': 1}}) as session, context.bind_context():
            executor = data.range(1)._executor
            assert owner.journal.path == journal.path and current_run() is owner
            assert owner.attempt == journal.execution_status()['attempt'] == 1
            assert session.run_ref == owner.ref and current_checked() is context
            assert context.report_digest == 'a' * 64
            assert executor.resource_root == tmp_path / 'source'
            assert executor.diagnostics_root == tmp_path / 'diagnostics'
            assert executor_defaults(executor).block_size == 7
            assert executor_defaults(executor).batch_rows == 11
            assert executor._planning_policy.max_parallelism == 1
            assert not executor.resource_root.exists() and not executor.diagnostics_root.exists()
            if failure is not None:
                raise failure

    if failure is None:
        run()
    else:
        with pytest.raises(RuntimeError, match='cancelled by host'):
            run()
    assert len(closed) == 1 and closed[0]._local_kernel_closed
    assert current_run() is None and current_checked() is None
    assert journal.path.exists()
    assert list(tmp_path.rglob(journal.path.name)) == [journal.path]


def test_ordinary_block_and_host_defaults(tmp_path):
    with RunContext.start(tmp_path / 'ordinary') as owner, data.local_execution(workers=1):
        executor = data.range(1)._executor
        assert executor.resource_root == Path.cwd()
        assert executor.diagnostics_root is None
        assert executor_defaults(executor).block_size == 256
        assert executor_defaults(executor).batch_rows == 8192


@pytest.mark.parametrize('value', [0, -1, True, '4'])
def test_invalid_block_size_fails_before_execution_scope(tmp_path, monkeypatch, value):
    def forbidden(*args, **kwargs):
        raise AssertionError('invalid host configuration entered a run')
    monkeypatch.setattr(RunContext, '__enter__', forbidden)
    with pytest.raises(ValueError, match='block_size'):
        with data.local_execution(block_size=value):
            pytest.fail('invalid configuration entered the body')


def test_planning_ceiling_is_not_lost_by_host_options(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('over-budget host configuration entered a run')
    monkeypatch.setattr(RunContext, '__enter__', forbidden)
    with pytest.raises(ValueError, match='max_parallelism'):
        with data.local_execution(workers=2, planning={'limits': {'max_parallelism': 1}}, resource_root=tmp_path):
            pytest.fail('over-budget configuration entered the body')
