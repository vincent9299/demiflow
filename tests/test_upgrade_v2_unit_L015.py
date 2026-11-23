"""L015's sealed-placement/authority obligations on the actual unified chain.

Each test binds one v3 request to the run it owns, then checks original seals,
actual assignments and allocations.
"""
import json
import os
from pathlib import Path

import pytest

from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.seal.plan import load_dataflow
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext
from test_runtime_v3_chain_compatibility import _entry


def _materialize(dataset):
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    request = make_action_request(dataset._executor, dataset._source, dataset._plan, kind='materialize', arguments={})
    return run_dataset_action(request)


def _identity(row):
    return row


def _fail(row):
    raise ValueError('owned chain failure')


def _mark(row, *, path):
    with Path(path).open('a') as stream:
        stream.write(str(row['id']) + '\n')
    return row


@pytest.fixture(scope='module')
def actual_chain(tmp_path_factory):
    root = tmp_path_factory.mktemp('chain-contract')
    assignments, executions, commits = [], [], []
    original_assignment = DataflowExecution._assignment
    from demiflow.execution.commit import CommitCoordinator
    original_commit = CommitCoordinator.commit
    def assign(execution, *args, **kwargs):
        value = original_assignment(execution, *args, **kwargs)
        assignments.append(value)
        if execution not in executions:
            executions.append(execution)
        return value
    def commit(owner, *args, **kwargs):
        fact = original_commit(owner, *args, **kwargs)
        commits.append((os.getpid(), fact['commit_id']))
        return fact
    with pytest.MonkeyPatch.context() as patch:
        for key, suffix in [('DEMIFLOW_RESOURCE_ROOT', 'pool'), ('DEMIFLOW_RESOURCE_HOME', 'home'),
                            ('DEMIFLOW_FORGE_DOMAIN', 'domain')]:
            patch.setenv(key, str(root / suffix))
        patch.setattr(DataflowExecution, '_assignment', assign)
        patch.setattr(CommitCoordinator, 'commit', commit)
        with RunContext.start(root / 'run', limits={'memory_bytes': 32 * 2**20,
                'max_row_bytes': 4096, 'max_batch_bytes': 16384}) as run, _entry(root) as api:
            dataset = api.from_items([{'id': 1}, {'id': 2}]).map(_identity)
            with run.action(plan_identity('materialize', dataset._source, dataset._plan)) as action:
                result = _materialize(dataset)
                try:
                    assert list(result.rows()) == [{'id': 1}, {'id': 2}]
                    assert len(executions) == 1 and assignments
                    execution = executions[0]
                    graph, physical, sealed = load_dataflow(run.directory, action.action_id,
                        store=execution._store, bounds=execution._bounds)
                    with run.journal._connection() as db:
                        allocations = {key: json.loads(spec) for key, spec in
                            db.execute('SELECT allocation_id,spec FROM resource_allocations')}
                    facts = dict(run=run, action=action, execution=execution, graph=graph,
                        physical=physical, sealed=sealed, assignments=assignments,
                        allocations=allocations, commits=commits)
                    action.complete({'kind': 'materialized', 'rows': result.row_count})
                finally:
                    result.release()
        # The fact checks are read-only. Do not keep a live module-scoped run
        # around tests that must create their own failure/recovery action.
        yield facts


def test_layout_is_interpreted_from_the_sealed_plan(actual_chain):
    facts = actual_chain
    assert facts['execution']._physical == facts['physical']
    assert facts['execution']._graph == facts['graph']
    assert all(a.sealed == facts['sealed'] for a in facts['assignments'])


def test_worker_profile_comes_from_the_layout(actual_chain):
    for assignment in actual_chain['assignments']:
        allocation = actual_chain['allocations'][assignment.lease.allocation_id]
        assert len(assignment.stages) == 1
        assert allocation['request']['profile'] == assignment.stages[0].profile.to_dict()
        assert allocation['effective']['memory_bytes'] >= assignment.stages[0].profile.requests['memory_bytes']


def test_stages_and_partitions_come_from_the_placement(actual_chain):
    stages = {stage.stage_id: stage for stage in actual_chain['physical'].stages}
    for assignment in actual_chain['assignments']:
        assert assignment.stages == (stages[assignment.lease.stage_id],)
        assert set(assignment.lease.partition_ids) <= set(assignment.stages[0].partitions)


def test_program_binding_comes_from_the_sealed_bindings(actual_chain):
    execution = actual_chain['execution']
    assert execution._sealed == actual_chain['sealed']
    marker = actual_chain['run'].journal.source_binding(actual_chain['action'].action_id + '-dataflow-sealed')
    assert marker['physical_ref']['digest'] == actual_chain['sealed'].physical_ref.digest
    for allocation in actual_chain['allocations'].values():
        assert allocation['request']['launch']['artifact_refs']['physical']['digest'] == marker['physical_ref']['digest']


def test_assignment_channels_are_validated_against_the_layout(actual_chain):
    for assignment in actual_chain['assignments']:
        partitions = set(assignment.lease.partition_ids)
        edges = actual_chain['physical'].exchanges
        assert assignment.inputs == tuple(e for e in edges if partitions.intersection(e.consumer_partitions)
                                         and not partitions.intersection(e.producer_partitions))
        assert assignment.outputs == tuple(e for e in edges if partitions.intersection(e.producer_partitions)
                                          and not partitions.intersection(e.consumer_partitions))


def test_driver_holds_the_only_commit_authority(actual_chain):
    facts = actual_chain
    original = facts['run'].journal.execution_commits(facts['action'].action_id)
    assert original and all(item['status'] == 'acked' for item in original)
    assert {key for _, key in facts['commits']} == {item['commit_id'] for item in original}
    assert {pid for pid, _ in facts['commits']} == {os.getpid()}


def test_failure_terminal_releases_the_resource_scope(tmp_path):
    run = None
    with pytest.raises(ValueError, match='owned chain failure'):
        with RunContext.start(tmp_path / 'run') as run, _entry(tmp_path) as api:
            ds = api.from_items([{'id': 1}]).map(_fail)
            with run.action(plan_identity('materialize', ds._source, ds._plan)):
                _materialize(ds)
    with run.journal._connection() as db:
        cleanup = db.execute('SELECT cleanup FROM resource_allocations').fetchall()
    assert cleanup and all(value == ('CONFIRMED',) for value in cleanup)
    assert not run.journal.execution_commits('action-1')


def test_restore_point_comes_from_the_terminal_journal(tmp_path):
    class Interrupted(Exception):
        pass
    marker = tmp_path / 'calls'
    reference, first_plan, original_commits = None, None, None
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, _entry(tmp_path) as api:
            ds = api.from_items([{'id': 1}, {'id': 2}]).map(_mark, fn_kwargs={'path': str(marker)})
            with run.action(plan_identity('materialize', ds._source, ds._plan)) as action:
                result = _materialize(ds)
                result.release()
                reference = run.ref
                first_plan = run.journal.source_binding('action-1-dataflow-sealed')
                original_commits = run.journal.execution_commits('action-1')
                assert original_commits and all(item['status'] == 'acked' for item in original_commits)
                raise Interrupted()
    original_calls = marker.read_bytes()
    with RunContext.resume(reference) as run, _entry(tmp_path) as api:
        ds = api.from_items([{'id': 1}, {'id': 2}]).map(_mark, fn_kwargs={'path': str(marker)})
        with run.action(plan_identity('materialize', ds._source, ds._plan)) as action:
            result = _materialize(ds)
            try:
                assert list(result.rows()) == [{'id': 1}, {'id': 2}]
                assert marker.read_bytes() == original_calls
                assert run.journal.source_binding('action-1-dataflow-sealed') == first_plan
                assert run.journal.execution_commits('action-1') == original_commits
                action.complete({'kind': 'materialized', 'rows': 2})
            finally:
                result.release()
