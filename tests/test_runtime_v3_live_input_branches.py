"""Shared live inputs keep source ownership and honest attempt-only recovery."""
import threading
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
from demiflow.execution.run_context import RunContext
from _worker_event_fixtures import emit, events


class StopAfterCut(Exception):
    pass


def _live_source(path, count):
    lock = threading.Lock()
    def source():
        for n in range(count):
            with lock:
                emit(path, 'source', n=n)
            yield {'n': n}
    return source


def test_live_iterator_final_result_is_readable_without_registry_or_source_replay(tmp_path, monkeypatch):
    source = _live_source(str(tmp_path/'calls'), 3)
    plans = []
    original = DataflowExecution.prepare
    def prepare(execution):
        result = original(execution)
        plans.append((execution._graph, execution._physical))
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', prepare)
    def build():
        return data.from_iter(source).map(lambda row: row, execution='thread')
    with pytest.raises(StopAfterCut):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2):
            ref = run.ref
            assert build().take_all() == [{'n': n} for n in range(3)]
            commits = run.journal.execution_commits('action-1')
            assert commits and all(c['status'] == 'acked' for c in commits)
            raise StopAfterCut()
    graph, physical = plans[0]
    assert physical.recovery == 'attempt_only'
    assert all(s.replay == 'attempt_only' for s in physical.sources)
    for stage in physical.stages:
        assert not (set(stage.occurrences) & set(graph.sources) and len(stage.occurrences) > 1)
    first = events(tmp_path/'calls')
    assert [item['n'] for item in first] == [0, 1, 2]
    with RunContext.resume(ref) as run, data.local_execution(workers=2):
        assert build().take_all() == [{'n': n} for n in range(3)]
        assert run.journal.execution_commits('action-1') == commits
    assert events(tmp_path/'calls') == first


def test_live_iterator_partial_cut_cannot_resume_a_new_attempt(tmp_path, monkeypatch):
    source = _live_source(str(tmp_path/'calls'), 10)
    original = DataflowCheckpointCoordinator.commit
    images = []
    def stop_after_commit(coordinator, cut_id, *, sinks):
        image = original(coordinator, cut_id, sinks=sinks)
        images.append(image)
        assert not image.cut.final
        raise StopAfterCut()
    def build():
        return data.from_iter(source).map(lambda row: row, execution='thread')
    with pytest.raises(StopAfterCut):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 1}) as run, data.local_execution(workers=2):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop_after_commit)
                build().take_all()
    assert images and not images[0].cut.final
    first = events(tmp_path/'calls')
    assert 0 < len(first) < 10
    with pytest.raises(DataflowError, match='attempt|live|registry'):
        with RunContext.resume(ref), data.local_execution(workers=2):
            build().take_all()
    assert events(tmp_path/'calls') == first


def test_process_scope_refuses_live_iterator_before_read(tmp_path):
    source = _live_source(str(tmp_path/'calls'), 2)
    with pytest.raises(DataflowError, match='serializable declaration'):
        with data.local_execution(workers=2, worker_mode='process'):
            data.from_iter(source).map(lambda row: row, execution='thread').take_all()
    assert events(tmp_path/'calls') == []
