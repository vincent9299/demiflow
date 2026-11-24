"""Late native EOI must receive a real final ACK before completed-result replay."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.hosts.endpoints import _EndpointBase
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.prompt import RunPromptJournal


@pytest.mark.parametrize('stop_when_complete', [False, True])
def test_late_native_eoi_obtains_final_ack_and_replays_live_result(
        tmp_path, monkeypatch, stop_when_complete):
    release_http = threading.Event()
    response_confirmed = threading.Event()
    publication_released = threading.Event()
    requests, images, starts, results = [], [], [], []
    current = None
    first_cut_id = None
    native_completed_during_publication = False
    terminal_stopped = False

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            assert release_http.wait(15), 'native request never reached prepared publication'
            raw = json.dumps({'data': [{'index': 0, 'embedding': [1., 0., 0.]}],
                              'usage': {'prompt_tokens': 1}}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    model = EmbeddingModel('fixture', 'v1', 3, f'http://127.0.0.1:{server.server_port}/v1')

    def pipeline():
        return (data.from_items([{'text': 'one'}]).map_embeddings(
            model=model, inputs={'text': 'text'}, batch_size=8, concurrency=2, max_requests=1)
            .map(lambda row: row, execution='thread'))

    begin = DataflowCheckpointCoordinator.begin
    finish_publication = CommitCoordinator._finish
    pump = DataflowExecution.pump
    commit = DataflowCheckpointCoordinator.commit
    set_result = DataflowExecution._set_result
    start = DataflowExecution.start
    response = RunPromptJournal.response

    def begin_cut(coordinator, **kwargs):
        nonlocal first_cut_id
        cut = begin(coordinator, **kwargs)
        if first_cut_id is None:
            assert not cut.final
            assert current._durable_residency is None
            assert len(current._phases) == 1
            assert set(current._endpoints) - current._done
            first_cut_id = cut.cut_id
        return cut

    def delayed_publication(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1 and fact['status'] == 'prepared':
            assert current._cut.cut_id == first_cut_id
            # Every original cut contribution exists before publication can
            # overlap business. Only this already-prepared sink publication
            # waits; the driver and worker control threads keep pumping.
            assert set(current._cut.participants) <= current._contributions.keys()
            release_http.set()
            assert publication_released.wait(15), 'native EOI did not arrive during publication'
        return finish_publication(coordinator, fact, sinks, acknowledge)

    def confirmed_response(journal, *args, **kwargs):
        result = response(journal, *args, **kwargs)
        response_confirmed.set()
        return result

    def timed_pump(execution, **kwargs):
        nonlocal current, native_completed_during_publication, terminal_stopped
        current = execution
        result = pump(execution, **kwargs)
        if (not native_completed_during_publication and first_cut_id is not None
                and execution._publication_future is not None
                and execution._publication_resumed
                and set(execution._endpoints) <= execution._done):
            assert response_confirmed.is_set()
            assert execution._cut.cut_id == first_cut_id and not execution._cut.final
            assert not execution._publication_future.done()
            native_completed_during_publication = True
            if stop_when_complete:
                execution.satisfy_terminal()
                terminal_stopped = True
            publication_released.set()
        return result

    def record_commit(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        assert coordinator._action.context.journal.execution_commit(image.commit_id)['status'] == 'acked'
        images.append(image)
        return image

    def record_result(execution, image):
        results.append(image)
        return set_result(execution, image)

    def record_start(execution):
        starts.append(execution)
        return start(execution)

    class InterruptedAfterCompletion(Exception):
        pass

    expected = [{'text': 'one', 'embedding': [1., 0., 0.]}]
    try:
        with monkeypatch.context() as patch:
            patch.setattr(DataflowCheckpointCoordinator, 'begin', begin_cut)
            patch.setattr(CommitCoordinator, '_finish', delayed_publication)
            patch.setattr(DataflowExecution, 'pump', timed_pump)
            patch.setattr(RunPromptJournal, 'response', confirmed_response)
            patch.setattr(DataflowCheckpointCoordinator, 'commit', record_commit)
            patch.setattr(DataflowExecution, '_set_result', record_result)
            patch.setattr(DataflowExecution, 'start', record_start)
            with pytest.raises(InterruptedAfterCompletion):
                with RunContext.start(tmp_path / 'run', limits={'timeout_s': 25,
                        'checkpoint_rows': 64, 'checkpoint_interval_ms': 100,
                        'max_requests': 1}) as run, data.local_execution(workers=4, worker_mode='thread'):
                    ref = run.ref
                    assert pipeline().take_all() == expected
                    commits = run.journal.execution_commits('action-1')
                    assert len(commits) == 2 and all(c['status'] == 'acked' for c in commits)
                    assert [image.cut.final for image in images] == [False, True]
                    assert images[0].cut.cut_id == first_cut_id
                    assert images[0].commit_id != images[1].commit_id
                    assert all(image.cut.final for image in results)
                    assert starts[0]._terminal_result.completion == 'source_exhausted'
                    assert native_completed_during_publication
                    assert terminal_stopped == stop_when_complete
                    raise InterruptedAfterCompletion()
            # Real registry and workers have been retired. Replay must read the
            # original final receipt without reconstructing the live callable.
            with RunContext.resume(ref) as resumed, data.local_execution(workers=4, worker_mode='thread'):
                assert pipeline().take_all() == expected
                assert resumed.journal.execution_commits('action-1') == commits
                assert len(starts) == 2 and starts[1]._endpoints == {}
                assert starts[1]._restore_image.commit_id == images[-1].commit_id
                with resumed.journal._connection() as db:
                    assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
        assert len(requests) == 1 and requests[0]['input'] == ['one']
    finally:
        release_http.set()
        publication_released.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()


@pytest.mark.parametrize('stop_when_complete', [False, True])
def test_parked_complete_input_replays_without_new_worker_or_commit(
        tmp_path, monkeypatch, stop_when_complete):
    from demiflow.execution.hosts.thread import ThreadHostEndpoint

    original_send = ThreadHostEndpoint.send_business
    original_begin = DataflowCheckpointCoordinator.begin
    original_cut = _EndpointBase.request_cut
    original_pump = DataflowExecution.pump
    original_commit = DataflowCheckpointCoordinator.commit
    original_start = DataflowExecution.start
    current = None
    selected_cut = None
    held_eoi, held_cut, images, executions = [], [], [], []

    def delay_eoi(endpoint, command):
        terminal = current is not None and any(
            set(stage.occurrences) & set(current._graph.terminals)
            for stage in endpoint._assignment.stages)
        if command.get('kind') == 'edge_eoi' and terminal and selected_cut is None:
            held_eoi.append((endpoint, dict(command)))
        else:
            original_send(endpoint, command)

    def begin(coordinator, **kwargs):
        nonlocal selected_cut
        cut = original_begin(coordinator, **kwargs)
        if held_eoi:
            assert current._durable_residency is not None and not cut.final
            assert current._checkpoint.inputs_complete()
            assert set(current._endpoints) - current._done
            selected_cut = cut.cut_id
            for endpoint, command in held_eoi:
                original_send(endpoint, command)
            held_eoi.clear()
        return cut

    def request_cut(endpoint, cut):
        if cut.cut_id == selected_cut:
            held_cut.append((endpoint, cut))
        else:
            original_cut(endpoint, cut)

    def pump(execution, **kwargs):
        nonlocal current
        current = execution
        status = original_pump(execution, **kwargs)
        if held_cut and set(execution._endpoints) <= execution._done:
            assert execution._durable_residency.final(True)
            assert execution._checkpoint.inputs_complete()
            if stop_when_complete:
                execution.satisfy_terminal()
            for endpoint, cut in held_cut:
                original_cut(endpoint, cut)
            held_cut.clear()
        return status

    def commit(coordinator, *args, **kwargs):
        image = original_commit(coordinator, *args, **kwargs)
        assert coordinator._action.context.journal.execution_commit(image.commit_id)['status'] == 'acked'
        images.append(image)
        return image

    def start(execution):
        executions.append(execution)
        return original_start(execution)

    class InterruptedAfterCompletion(Exception):
        pass

    expected = [{'n': 0}]
    with monkeypatch.context() as patch:
        patch.setattr(ThreadHostEndpoint, 'send_business', delay_eoi)
        patch.setattr(DataflowCheckpointCoordinator, 'begin', begin)
        patch.setattr(_EndpointBase, 'request_cut', request_cut)
        patch.setattr(DataflowExecution, 'pump', pump)
        patch.setattr(DataflowCheckpointCoordinator, 'commit', commit)
        patch.setattr(DataflowExecution, 'start', start)
        with pytest.raises(InterruptedAfterCompletion):
            with RunContext.start(tmp_path / 'run', limits={
                    'memory_bytes': 40 * 1024, 'max_batch_bytes': 16 * 1024,
                    'max_row_bytes': 8 * 1024, 'max_state_bytes': 128 * 1024,
                    'timeout_s': 30, 'checkpoint_rows': 2,
                    'checkpoint_interval_ms': 60000}) as run, data.local_execution(worker_mode='thread'):
                ref = run.ref
                assert data.from_items(expected).take_all() == expected
                commits = run.journal.execution_commits('action-1')
                assert len(commits) == 3 and all(c['status'] == 'acked' for c in commits)
                assert [image.cut.final for image in images] == [False, False, True]
                assert images[1].cut.cut_id == selected_cut
                assert executions[0]._observed_worker_count == 2
                assert executions[0]._terminal_result.completion == 'source_exhausted'
                assert not held_eoi and not held_cut
                raise InterruptedAfterCompletion()
        with RunContext.resume(ref) as resumed, data.local_execution(worker_mode='thread'):
            assert data.from_items(expected).take_all() == expected
            assert resumed.journal.execution_commits('action-1') == commits
            assert len(executions) == 2 and executions[1]._endpoints == {}
            assert executions[1]._observed_worker_count == 0
            assert executions[1]._restore_image.commit_id == images[-1].commit_id


def test_parked_incomplete_take_keeps_original_prefix_receipt(tmp_path, monkeypatch):
    original = DataflowExecution._set_result
    completed = []

    def record_result(execution, image):
        completed.append((execution, image))
        return original(execution, image)

    monkeypatch.setattr(DataflowExecution, '_set_result', record_result)
    with RunContext.start(tmp_path / 'run', limits={
            'memory_bytes': 40 * 1024, 'max_batch_bytes': 16 * 1024,
            'max_row_bytes': 8 * 1024, 'max_state_bytes': 128 * 1024,
            'timeout_s': 30, 'checkpoint_rows': 2,
            'checkpoint_interval_ms': 60000}) as run, data.local_execution(worker_mode='thread'):
        assert data.range(10000).take(2) == [{'id': 0}, {'id': 1}]
        assert len(completed) == 1
        execution, image = completed[0]
        assert execution._durable_residency is not None
        assert execution._stopping and not execution._checkpoint.inputs_complete()
        assert not image.cut.final
        assert execution._terminal_result.completion == 'terminal_satisfied'
        assert run.journal.execution_commit(image.commit_id)['status'] == 'acked'
        cursors = [cursor for contribution in image.contributions for cursor in contribution.sources]
        assert cursors and all(not cursor.end_of_input and cursor.next_sequence <= 2 for cursor in cursors)
