"""Late native EOI must receive a real final ACK before completed-result replay."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
from demiflow.execution.dataflow_hosts import _EndpointBase
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.stream_recovery import RunPromptJournal


@pytest.mark.parametrize('stop_when_complete', [False, True])
def test_late_native_eoi_obtains_final_ack_and_replays_live_result(
        tmp_path, monkeypatch, stop_when_complete):
    release_http = threading.Event()
    response_confirmed = threading.Event()
    requests, held, images, starts, results = [], [], [], [], []
    current = None
    first_cut_id = None
    native_dispatched = False
    terminal_stopped = False

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            assert release_http.wait(15), 'native request never reached an original nonfinal cut'
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
    request_cut = _EndpointBase.request_cut
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
            release_http.set()
        return cut

    def delay_cut(endpoint, cut):
        if cut.cut_id == first_cut_id:
            held.append((endpoint, cut))
        else:
            request_cut(endpoint, cut)

    def confirmed_response(journal, *args, **kwargs):
        result = response(journal, *args, **kwargs)
        response_confirmed.set()
        return result

    def timed_pump(execution, **kwargs):
        nonlocal current, native_dispatched, terminal_stopped
        current = execution
        result = pump(execution, **kwargs)
        if not held or not response_confirmed.is_set():
            return result
        terminal_stages = {stage.stage_id for stage in execution._physical.stages
                           if set(stage.occurrences) & set(execution._graph.terminals)}
        if not native_dispatched:
            # Read actual completed callback output without consuming it. The
            # runner stays paused until its original cut command is delivered.
            native_jobs = []
            for endpoint, _ in held:
                host = endpoint._connector._host(endpoint._allocation_id)
                for kernel in host._executor._kernels.values():
                    if kernel._spec.kind == 'embedding' and kernel._calls is not None:
                        native_jobs.extend(tuple(kernel._calls.jobs))
            if not native_jobs or any(output.empty() for _, output, _, _ in native_jobs):
                return result
            native_dispatched = True
        for item in tuple(held):
            endpoint, cut = item
            is_terminal = endpoint._assignment.lease.stage_id in terminal_stages
            if is_terminal:
                if not set(execution._endpoints) <= execution._done:
                    continue
                if stop_when_complete and not terminal_stopped:
                    execution.satisfy_terminal()
                    terminal_stopped = True
            # Only delay genuine control delivery; never reorder worker events,
            # synthesize EOI/contributions, or change the original cut identity.
            held.remove(item)
            request_cut(endpoint, cut)
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
            patch.setattr(_EndpointBase, 'request_cut', delay_cut)
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
                    assert not held and native_dispatched
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
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()
