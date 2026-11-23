"""Native aligned barriers: deterministic overlap and immutable local cuts."""
import asyncio
import json
import threading
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.storage.state_artifacts import StateArtifacts

_OVERLAP = None


class NestedCounter:
    def __init__(self):
        self.state = {'seen': []}

    async def __call__(self, row):
        self.state['seen'].append(row['id'])
        if row['id'] == 8 and _OVERLAP is not None:
            _OVERLAP.set()
        await asyncio.sleep(0)
        return row

    def snapshot_state(self):
        return self.state

    def restore_state(self, value):
        self.state = value


def test_upstream_continues_before_commit_and_snapshot_is_detached(tmp_path, monkeypatch):
    import time
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.dataflow_sinks import _artifact_from_json
    from demiflow.execution.contracts import state_codec

    marker = tmp_path / 'upstream-8'

    class ObservedCounter(NestedCounter):
        async def __call__(self, row):
            result = await super().__call__(row)
            if row['id'] == 8:
                marker.write_text('original worker reached input 8')
            return result

    original = CommitCoordinator._finish
    recorded = []
    executions = []
    original_start = DataflowExecution._start_publication

    def capture(execution):
        executions[:] = [execution]
        return original_start(execution)

    def slow_commit(coordinator, fact, sinks, acknowledge):
        if fact['intent']['sinks'] == ['materialize'] and fact['sequence'] == 1:
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.005)
            assert marker.exists(), 'upstream remained blocked on global publication'
            cut = StateArtifacts(coordinator.run).read(fact['prepared']['state']['dataflow-cut-v3'])
            execution = executions[0]
            states = []
            for contribution in cut['contributions']:
                for ref in contribution['operator_states'].values():
                    ref = _artifact_from_json(ref)
                    if ref.media_type == 'application/x-demiflow-kernel-image-v3':
                        state = state_codec.loads(b''.join(execution._store.read(ref,
                            max_bytes=execution._bounds.max_state_bytes)),
                            max_bytes=execution._bounds.max_state_bytes)
                        if state.get('state') is not None:
                            states.append(state['state'])
            assert states == [{'mode': 'hooks', 'value': {'seen': [0, 1, 2, 3]}}]
            from demiflow.execution.dataflow_replay import PublishedRecordReader
            prepared = fact['prepared']['sinks']['materialize']
            publication = json.loads(b''.join(execution._store.read(_artifact_from_json(prepared['ref']),
                max_bytes=execution._bounds.max_control_bytes)))
            reader = PublishedRecordReader(publication, store=execution._store, bounds=execution._bounds)
            rows = []
            from demiflow.execution.contracts.record_codec import _decode_record_document
            try:
                while True:
                    raw, record_id, sequence, _ = reader.pop()
                    rows.append(dict(_decode_record_document(raw, record_id=record_id, sequence=sequence, max_bytes=execution._bounds.max_row_bytes).inline))
            except StopIteration:
                pass
            finally:
                reader.close()
            assert rows == [{'id': i} for i in range(4)]
            recorded.append(cut)
        return original(coordinator, fact, sinks, acknowledge)

    monkeypatch.setattr(DataflowExecution, '_start_publication', capture)
    monkeypatch.setattr(CommitCoordinator, '_finish', slow_commit)
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000}):
        result = data.from_items([{'id': i} for i in range(12)]).map_async(ObservedCounter()).materialize().take_all()
    assert result == [{'id': i} for i in range(12)]
    assert len(recorded) == 1


async def identity(row):
    await asyncio.sleep(0)
    return row


async def batch_identity(rows):
    return [{**row, 'group': [r['id'] for r in rows]} for row in rows]


def test_fixed_batch_spans_barriers_without_checkpoint_flush(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_interval_ms': 60000}) as run:
        result = data.from_items([{'id': i} for i in range(11)]).batch_map(
            batch_identity, max_batch=3, concurrency=1).materialize().take_all()
    assert result == [{'id': i, 'group': list(range(i//3*3, min(11, i//3*3+3)))} for i in range(11)]
    facts = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert 2 <= len(facts) <= 6
    assert facts[0]['receipts']['materialize']['data']['rows'] == 0
    # The original scalar now sequences global cuts; the original journal's
    # source vector, not that scalar, proves the precise two-row frontier.
    saved = StateArtifacts(run).read(facts[0]['prepared']['state']['dataflow-cut-v3'])
    assert saved['dataflow.sources.v3'][0]['offset']['index'] == 2


class SharedCounter:
    def __init__(self):
        self.count = 0

    async def __call__(self, row):
        self.count += 1
        await asyncio.sleep(0)
        return row

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, value):
        self.count = value['count']


def test_shared_actor_uses_one_snapshot_at_last_owner(tmp_path, monkeypatch):
    from test_dataflow_shared_actor import capture_images
    traces = capture_images(monkeypatch)
    actor = SharedCounter()
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000}):
        result = data.from_items([{'id': i} for i in range(12)]).map_async(actor, concurrency=3).map_async(
            identity, concurrency=2).map_async(actor, concurrency=2).materialize().take_all()
    assert len(traces) >= 2
    for trace in traces:
        hooks = [(o, state) for o, state in trace['states'] if state['mode'] == 'hooks']
        aliases = [(o, state) for o, state in trace['states'] if state['mode'] == 'shared_owner']
        assert len(hooks) == len(aliases) == 1
        assert aliases[0][1]['owner'] == hooks[0][0]
        assert hooks[0][1]['value']['count'] == sum(trace['outputs'][o] for o, _ in hooks + aliases)
    assert next(s['value']['count'] for _, s in traces[-1]['states'] if s['mode'] == 'hooks') == 24
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]


def test_one_input_1201_objects_crosses_bounded_manifest_pages(tmp_path):
    from demiflow.execution.object_sink import ObjectRowsSink
    from demiflow.execution.recovery_contract import SourcePosition
    from demiflow.execution.storage.artifact_manifest import manifest_pages, entries
    from demiflow.objects import ObjectRef
    with RunContext.start(tmp_path/'run') as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'one-to-1201'}}) as action:
            sink = ObjectRowsSink(action, max_object_bytes=128, max_objects=64)
            def expanded():
                for i in range(1201):
                    yield {'id': i, 'image': sink.objects.stage(str(i).encode())}
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, expanded())},
                position=SourcePosition('single-source-occurrence', 1))
            descriptor = fact['prepared']['sinks']['output']['objects']['objects']
            pages = list(manifest_pages(run.directory, descriptor))
            assert len(pages) == 19
            assert max(len(page) for _, page in pages) == 64
            assert sum(len(page) for _, page in pages) == 1201
            assert len(json.dumps(fact['prepared'])) < 4096
            result = list(sink.rows.iter_rows(fact['prepared']['sinks']['output']['rows']))
            action.complete({'rows': len(result)})
    assert len(result) == 1201
    assert ObjectRef(**result[-1]['image']).read(max_bytes=128) == b'1200'
    assert len(list(entries(run, descriptor))) == 1201  # committed pages survive cleanup


def test_block_pages_preserve_output_order_and_recovery(tmp_path):
    from demiflow.execution.outputs.block_sink import BlockSink
    from demiflow.execution.recovery_contract import SourcePosition
    with RunContext.start(tmp_path/'run', limits={'max_row_bytes': 1024, 'max_batch_bytes': 1024}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'blocks'}}) as action:
            sink = BlockSink(action)
            sink.block_bytes = 64
            rows = [{'id': i, 'payload': bytes(64)} for i in range(1201)]
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, iter(rows))},
                position=SourcePosition('blocks-fixture', 1201))
            prepared = fact['prepared']['sinks']['output']
            assert type(prepared['blocks']) is dict
            assert list(sink.iter_rows(prepared)) == rows
            action.complete({'rows': len(rows)})
    assert list(sink.iter_rows(prepared)) == rows


def test_private_temporary_registration_is_released_but_references_are_kept(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError, SourcePosition
    from demiflow.execution.outputs.block_sink import BlockSink
    with RunContext.start(tmp_path/'run') as run:
        path = run.directory/'scratch'/'private'
        path.parent.mkdir()
        with run.temporary_artifacts() as allocate:
            allocate(path)
            path.write_bytes(b'x')
            path.unlink()
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0] == 0
        with run.action({'kind': 'materialize', 'source': {'fixture': 'reference'}}) as action:
            sink = BlockSink(action)
            ref = run.directory/'scratch'/'protected'
            run.allocate_artifact(ref)
            ref.write_bytes(b'x')
            fact = CommitCoordinator(action).commit(1, outputs={'output': (sink, iter([]))},
                position=SourcePosition('source', 0), states={'owner': {'path': 'scratch/protected'}})
            ref.unlink()
            with pytest.raises(RecoveryContractError, match='still references'):
                run.release_temporaries([ref])
            action.complete({'rows': 0})


def test_snapshot_failure_stops_without_confirming_output(tmp_path):
    class Broken(NestedCounter):
        def snapshot_state(self):
            if self.state['seen']:
                raise RuntimeError('snapshot failed')
            return self.state
    with pytest.raises(RuntimeError, match='snapshot failed'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}):
            data.from_items([{'id': i} for i in range(6)]).map_async(Broken()).materialize()
    assert RunJournal(tmp_path/'run').execution_commits('action-1') == []


class ObservedCounter:
    """Observe the real worker, rather than its unmodified driver-side clone."""
    def __init__(self, path):
        self.path, self.count = path, 0

    async def __call__(self, row):
        self.count += 1
        Path(self.path).write_text(str(self.count))
        return row

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, state):
        self.count = state['count']


def test_direct_lance_prepares_before_source_finishes_and_publishes_once(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.outputs.lance_sink import LanceSink
    from demiflow.execution.dataflow_sinks import _DataflowArtifactSink
    from demiflow.execution.contracts.dataflow import ArtifactRef
    progress = tmp_path/'actual-worker-count'
    original = _DataflowArtifactSink.prepare
    native_commit = LanceSink.commit
    observed, publications = [], []
    def prepare(sink, manifest, context):
        prepared = original(sink, manifest, context)
        if not observed:
            # The unified terminal stages immutable data before the input is
            # exhausted. Native Lance encoding/publication happens at final
            # commit; the early durable prefix is the actual bounded handoff.
            seen = int(progress.read_text())
            assert 0 < seen < 1024
            assert 0 < prepared['rows'] <= seen
            assert sink._store.inspect(ArtifactRef(**prepared['ref']))
            observed.append(seen)
        return prepared
    def publish(sink, prepared, context):
        result = native_commit(sink, prepared, context)
        publications.append(context.commit_id)
        return result
    monkeypatch.setattr(_DataflowArtifactSink, 'prepare', prepare)
    monkeypatch.setattr(LanceSink, 'commit', publish)
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 64, 'checkpoint_interval_ms': 60000}):
        data.from_items([{'id': i} for i in range(1024)]).map_async(ObservedCounter(str(progress))).write_lance(
            str(tmp_path/'out.lance'), mode='overwrite')
    assert observed and int(progress.read_text()) == 1024
    dataset = lance.dataset(str(tmp_path/'out.lance'))
    assert dataset.version == 1 and dataset.count_rows() == 1024
    assert dataset.to_table().to_pylist() == [{'id': i} for i in range(1024)]
    facts = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert 2 <= len(facts) <= 17 and all(f['status'] == 'acked' for f in facts)
    # An exact multiple may need its own final EOF cut; it cannot publish twice.
    receipts = [f['receipts']['write_lance']['data'] for f in facts]
    assert all('native_receipt' not in value for value in receipts[:-1])
    assert receipts[-1]['native_receipt']['data']['committed_version'] == 1
    assert publications == [facts[-1]['commit_id']]


def test_temporary_release_rejects_live_files_and_stale_fence(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.run_journal import ExecutionConflict
    with RunContext.start(tmp_path/'run') as run:
        path = run.directory/'scratch'/'live'
        path.parent.mkdir()
        run.allocate_artifact(path)
        path.write_bytes(b'live')
        with pytest.raises(RecoveryContractError, match='not removed'):
            run.release_temporaries([path])
        with pytest.raises(ExecutionConflict, match='Stale'):
            run.journal.release_temporaries('obsolete-owner', ['scratch/live'])
        assert path.read_bytes() == b'live'
        path.unlink()
        run.release_temporaries([path])


def test_enospc_does_not_confirm_checkpoint_or_return_partial_result(tmp_path, monkeypatch):
    import errno
    import demiflow.execution.dataflow_sinks as sinks
    original = sinks.atomic_bytes
    def fail(path, raw):
        if '/dataflow-sinks/materialize/.receipts/' in str(path):
            raise OSError(errno.ENOSPC, 'controlled full disk')
        return original(path, raw)
    monkeypatch.setattr(sinks, 'atomic_bytes', fail)
    with pytest.raises(OSError, match='full disk'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}):
            data.from_items([{'id': i} for i in range(8)]).map_async(identity).materialize()
    journal = RunJournal(tmp_path/'run')
    facts = journal.execution_commits('action-1')
    # Immutable prepared input survives for explicit recovery, but no sink
    # publication, receipt, ACK or successful result may escape the failed I/O.
    assert len(facts) == 1 and facts[0]['status'] == 'prepared'
    assert facts[0]['receipts'] == {}
    assert journal.execution_action('action-1')['result'] is None
    assert not list((tmp_path/'run'/'materializations'/'dataflow-sinks'/'materialize'/'.receipts').glob('*.json'))


def test_checkpoint_deadline_fails_and_drains_publication_thread(tmp_path, monkeypatch):
    import time
    from demiflow.execution.dataflow_sinks import _DataflowArtifactSink
    original = _DataflowArtifactSink.commit
    completed = []
    def slow(sink, prepared, context):
        if sink._sink_id == 'materialize':
            time.sleep(1.2)
        receipt = original(sink, prepared, context)
        completed.append(context.commit_id)
        return receipt
    monkeypatch.setattr(_DataflowArtifactSink, 'commit', slow)
    with pytest.raises(TimeoutError):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2, 'checkpoint_timeout_s': 1}):
            data.from_items([{'id': i} for i in range(8)]).map_async(identity).materialize()
    # A started native write is drained before ownership/cleanup is released.
    journal = RunJournal(tmp_path/'run')
    assert completed, 'the actual native publication must have drained'
    assert all(journal.execution_commit(key)['status'] == 'acked' for key in completed)
    assert journal.execution_action('action-1')['result'] is None
    assert journal.execution_status()['status'] == 'failed'
    assert not [t for t in threading.enumerate() if t.name.startswith('demiflow-terminal-input')]


def test_publication_fence_does_not_hold_control_db_during_io(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    with RunContext.start(tmp_path/'run') as run:
        configuration = run.journal.recovery_identity()
        with ThreadPoolExecutor(max_workers=2) as pool:
            entered = threading.Event()
            def replacement():
                entered.set()
                run.journal.claim_execution(configuration, 'replacement')
            with run.journal.publication_guard(run.fence):
                changed = pool.submit(replacement)
                assert entered.wait(1)
                # A budget writer can proceed while a publisher owns its fence.
                assert pool.submit(run.reserve, 'probe', 1).result(timeout=1) == 1
                assert not changed.done()
            changed.result(timeout=2)
        from demiflow.execution.run_journal import ExecutionConflict
        with pytest.raises(ExecutionConflict, match='Stale'):
            with run.journal.publication_guard(run.fence):
                pytest.fail('old publisher admitted')
        run.journal.claim_execution(configuration, run.fence)


def test_unified_checkpoint_deadline_includes_alignment_before_publication(tmp_path, monkeypatch):
    published = []
    original = CommitCoordinator._finish
    def finish(coordinator, *args, **kwargs):
        published.append(True)
        return original(coordinator, *args, **kwargs)
    monkeypatch.setattr(CommitCoordinator, '_finish', finish)
    async def slow(row):
        await asyncio.sleep(1.6)
        return row
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 1,
                'checkpoint_timeout_s': 1, 'checkpoint_interval_ms': 60000}), data.local_execution(workers=2):
            data.from_items([{'id': i} for i in range(3)]).map_async(slow, concurrency=1).materialize()
    assert not published, 'timeout must include waiting for the first aligned cut'
    assert RunJournal(tmp_path/'run').execution_action('action-1')['result'] is None


class NativeSlowFixture:
    """Bounded loopback model; only real PromptActor owns native capability."""
    def __init__(self):
        self.first_cut = threading.Event()
        self.seven = threading.Event()
        self.slow_started = threading.Event()
        self.zero_saved = threading.Event()
        self.reply_release = threading.Event()
        self.requests, self.errors = [], []
        self.lock = threading.Lock()

    def __enter__(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.connection.settimeout(5)
                length = int(self.headers.get('Content-Length', 0))
                if not 0 < length <= 8192:
                    self.send_error(413)
                    return
                request = json.loads(self.rfile.read(length))
                item = int(request['messages'][-1]['content'])
                with fixture.lock:
                    fixture.requests.append(item)
                if item == 0:
                    fixture.slow_started.set()
                    ready = fixture.seven.wait(20)
                else:
                    # The first real checkpoint must capture requests before
                    # their outputs; release only after its actual native ACK.
                    ready = fixture.first_cut.wait(20)
                    if item == 7:
                        fixture.seven.set()
                if not ready:
                    with fixture.lock:
                        fixture.errors.append(('dependency timeout', item))
                    self.send_error(504)
                    return
                body = json.dumps({'choices': [{'message': {'content': json.dumps({'answer': item})}}],
                                   'usage': {'prompt_tokens': 1, 'completion_tokens': 1}}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.02), daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.first_cut.set(); self.seven.set(); self.reply_release.set()
        self.server.shutdown(); self.server.server_close()
        self.thread.join(timeout=3)
        assert not self.thread.is_alive()

    def config(self):
        from demiflow.operator_llm import parse_prompt_pack
        return parse_prompt_pack(json.dumps({'schema_version': 'demiflow_prompt_pack_v2',
            'prompts': {'answer': {'version': 'native-slow-v1',
                'model': {'name': 'loopback', 'transport': 'openai_compatible',
                    'base_url': f'http://127.0.0.1:{self.server.server_port}/v1',
                    'api_key_env': 'DEMIFLOW_NATIVE_SLOW_TEST_KEY'},
                'schema_retries': 0, 'template': '{{ item }}',
                'response_schema': {'type': 'object', 'additionalProperties': False,
                    'required': ['answer'], 'properties': {'answer': {'type': 'integer'}}}}}}))


def test_native_inflight_cut_allows_free_slots_past_slow_call(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts import state_codec
    from demiflow.execution.kernels.operator import IMAGE_MEDIA_TYPE
    from demiflow.operator_llm.stream_recovery import RunPromptJournal
    monkeypatch.setenv('DEMIFLOW_NATIVE_SLOW_TEST_KEY', 'fixture-only')
    original_commit = DataflowCheckpointCoordinator.commit
    original_pump, original_start = DataflowExecution.pump, DataflowExecution.start
    original_response = RunPromptJournal.response
    observed, interrupted, restored = [], [], []
    class Interrupted(Exception):
        pass

    with NativeSlowFixture() as fixture:
        config = fixture.config()
        def pipeline():
            return data.from_items([{'id': i} for i in range(12)]).map_prompt_async(
                'answer', config=config, inputs={'item': 'id'}, output='answer',
                concurrency=3, max_requests=12,
                request_policy={'initial_concurrency': 3, 'max_concurrency': 3},
                options={'trust_env': False, 'timeout_s': 25, 'max_request_bytes': 8192,
                         'max_response_bytes': 8192, 'max_event_bytes': 8192})

        def pending_ids(image, store, bounds):
            ids = []
            for contribution in image.contributions:
                for ref in contribution.operator_states.values():
                    if ref.media_type != IMAGE_MEDIA_TYPE:
                        continue
                    raw = b''.join(store.read(ref, max_bytes=bounds.max_state_bytes))
                    state = state_codec.loads(raw, max_bytes=bounds.max_state_bytes)
                    if state.get('kind') == 'prompt':
                        ids.extend(call['input'].inline['id'] for call in state['inflight_calls'])
            return ids

        def committed(coordinator, *args, **kwargs):
            image = original_commit(coordinator, *args, **kwargs)
            if coordinator._action.action_id == 'action-1' and not observed:
                assert not image.cut.final
                pending = pending_ids(image, coordinator._store, coordinator._physical.bounds)
                assert 0 in pending, 'first cut drained or lost the real slow invocation'
                fact = coordinator._action.context.journal.execution_commit(image.commit_id)
                assert fact['status'] == 'acked'
                assert sum(value['data']['rows'] for value in fact['receipts'].values()) < 2
                assert fixture.slow_started.wait(5), 'slow HTTP request was never dispatched'
                assert not fixture.seven.is_set()
                observed.append(image.commit_id)
                fixture.first_cut.set()
            return image

        def persisted_response(journal, request, *args, **kwargs):
            result = original_response(journal, request, *args, **kwargs)
            if int(request['payload']['messages'][-1]['content']) == 0 and not interrupted:
                # Delay only delivery of a real, already durable journal reply.
                # This exposes a pending invocation with a known response, the
                # recoverable crash window; no result or receipt is invented.
                fixture.zero_saved.set()
                assert fixture.reply_release.wait(25), 'driver did not reach recovery boundary'
            return result

        def pump(execution, **kwargs):
            result = original_pump(execution, **kwargs)
            if (not interrupted and fixture.zero_saved.is_set()
                    and execution._services.action.action_id == 'action-1'):
                journal = execution._services.action.context.journal
                with journal._connection() as db:
                    complete = db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0]
                image = execution._restore_image
                if (complete == 12 and image is not None and not image.cut.final
                        and execution._publication_future is None):
                    # Fault after a settled ACK. An already-started publisher is
                    # correctly drained during close; crashing while it runs can
                    # advance the durable cut beyond this stale in-memory image.
                    assert 0 in pending_ids(image, execution._store, execution._bounds)
                    assert fixture.seven.is_set() and sorted(fixture.requests) == list(range(12))
                    interrupted.append(image.commit_id)
                    fixture.reply_release.set()
                    raise Interrupted('after original pending cut and durable model responses')
            return result

        def start(execution):
            result = original_start(execution)
            if (execution._services.action.action_id == 'action-1'
                    and execution._services.action.context.resuming):
                restored.append(execution._restore_image.commit_id)
            return result

        monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', committed)
        monkeypatch.setattr(RunPromptJournal, 'response', persisted_response)
        monkeypatch.setattr(DataflowExecution, 'pump', pump)
        monkeypatch.setattr(DataflowExecution, 'start', start)
        try:
            with pytest.raises(Interrupted, match='original pending cut'):
                with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2,
                        'checkpoint_interval_ms': 60000, 'max_requests': 12, 'timeout_s': 60}) as run, data.local_execution(workers=4, worker_mode='thread'):
                    reference = run.ref
                    pipeline().materialize().take_all()
            assert observed and len(interrupted) == 1
            before = run.journal.execution_commit(interrupted[0])
            with RunContext.resume(reference) as resumed, data.local_execution(workers=4, worker_mode='thread'):
                result = pipeline().materialize().take_all()
                assert resumed.journal.execution_commit(interrupted[0]) == before
                assert resumed.journal.budget_consumed('requests') == 12
                with resumed.journal._connection() as db:
                    assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 12
                    assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 12
            assert restored == interrupted
            assert sorted(result, key=lambda row: row['id']) == [{'id': i, 'answer': i} for i in range(12)]
            assert sorted(fixture.requests) == list(range(12)) and not fixture.errors
        finally:
            fixture.reply_release.set()



class RecoveryEmitter:
    def __init__(self):
        self.pending = [{'id': 101}, {'id': 102}]

    def snapshot_state(self):
        return {'pending': self.pending}

    def restore_state(self, state):
        self.pending = state['pending']

    def recovery_rows(self):
        rows, self.pending = self.pending, []
        return rows

    async def __call__(self, row):
        return row


def test_recovery_rows_are_in_final_checkpoint_and_not_republished(tmp_path, monkeypatch):
    import demiflow.execution.commit as protocol
    stopped = []
    def stop(name, commit_id):
        fact = RunJournal(tmp_path/'run').execution_commit(commit_id)
        if name != 'after_acked' or fact['intent']['sinks'] != ['materialize']:
            return
        carrier = StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
        if carrier['cut']['final']:
            stopped.append(commit_id)
            raise RuntimeError('stop after recovered outputs')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='stop after recovered outputs'):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            data.from_items([]).map_async(RecoveryEmitter()).materialize()
    assert len(stopped) == 1
    before = RunJournal(tmp_path/'run').execution_commits('action-1')
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    with RunContext.resume(reference):
        rows = data.from_items([]).map_async(RecoveryEmitter()).materialize().take_all()
    assert rows == [{'id': 101}, {'id': 102}]
    assert RunJournal(tmp_path/'run').execution_commits('action-1') == before


class WideState:
    def __init__(self, size=600):
        self.payload = 'x' * size

    def snapshot_state(self):
        return {'payload': self.payload}

    def restore_state(self, state):
        self.payload = state['payload']

    async def __call__(self, row):
        return row


def test_aggregate_state_admission_applies_across_alignment_groups(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    # The terminal's own bounded metadata needs a legal minimum. Each actor's
    # image fits; the pair exceeds the original whole-cut state allowance.
    limits = {'max_state_bytes': 64 * 1024, 'checkpoint_rows': 1}
    with RunContext.start(tmp_path/'single', limits=limits), data.local_execution(workers=4):
        assert data.from_items([{'id': 1}]).map_async(WideState(40000)).materialize().take_all() == [{'id': 1}]
    with pytest.raises(DataflowError, match='Aggregate checkpoint state'):
        with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(workers=4):
            data.from_items([{'id': 1}]).map_async(WideState(40000)).map_async(WideState(40000)).materialize()
    assert not RunJournal(tmp_path/'run').execution_commits('action-1')


def test_checksum_proof_detects_replacement_and_same_size_corruption(tmp_path):
    import hashlib
    from demiflow.execution.storage.artifact_proof import ArtifactProofs
    from demiflow.execution.recovery_contract import RecoveryContractError
    path = tmp_path/'blob'
    path.write_bytes(b'valid bytes')
    proof = ArtifactProofs(capacity=2)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    proof.verify(path, 11, sha)
    import time
    time.sleep(2.05)
    proof.verify(path, 11, sha)
    assert proof.hits == 1 and proof.hashed_bytes == 11
    replacement = tmp_path/'replacement'
    replacement.write_bytes(b'valid bytes')
    replacement.replace(path)
    proof.verify(path, 11, sha)
    assert proof.hashed_bytes == 22
    path.write_bytes(b'wrong bytes')
    with pytest.raises(RecoveryContractError, match='checksum differs'):
        proof.verify(path, 11, sha)


def test_late_writer_keeps_run_lease_and_handles_until_it_exits(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError
    release, entered, saw_stop = threading.Event(), threading.Event(), threading.Event()
    run = RunContext.start(tmp_path/'run')
    def writer():
        entered.set()
        assert release.wait(5)
        try:
            run.check_stop()
        except RecoveryContractError:
            saw_stop.set()
    worker = threading.Thread(target=writer, name='native-drain-fixture')
    try:
        with pytest.raises(RecoveryContractError, match='isolated while draining'):
            with run:
                reference = run.ref
                worker.start()
                assert entered.wait(1)
                run.quarantine_worker(worker)
        assert run.active and run._lease is not None
        with pytest.raises(BlockingIOError):
            with RunContext.resume(reference):
                pass
    finally:
        release.set()
        worker.join(5)
        run._drain_finalizer.join(5)
    assert saw_stop.is_set() and not run.active and run._lease is None
    assert not worker.is_alive() and not run._drain_finalizer.is_alive()
    with RunContext.resume(reference):
        pass


def test_temporary_hot_path_does_not_scan_tenfold_history(tmp_path, monkeypatch):
    from demiflow.execution.outputs.block_sink import storage_usage
    results = []
    for history in (1000, 10000):
        with RunContext.start(tmp_path/str(history)) as run:
            with run.journal.execution_guard(run.fence) as db:
                db.executemany('INSERT INTO runtime_artifacts(path,protected) VALUES (?,1)',
                    [('state/fixture-' + str(i),) for i in range(history)])
            instructions = [0]
            def tick():
                instructions[0] += 100
                return 0
            with run.journal._connection(write=True) as db:
                db.set_progress_handler(tick, 100)
            with run.journal._connection() as db:
                db.set_progress_handler(tick, 100)
            with monkeypatch.context() as patch:
                def no_scan(*args, **kwargs):
                    raise AssertionError('hot path scanned a directory')
                patch.setattr(Path, 'iterdir', no_scan)
                for i in range(64):
                    path = run.directory/'scratch'/str(i)
                    run.allocate_artifact(path, size=32)
                    assert storage_usage(run) == 64
                    run.release_temporaries([path])
                    assert storage_usage(run) == 0
            with run.journal._connection(write=True) as db:
                db.set_progress_handler(None, 0)
            with run.journal._connection() as db:
                db.set_progress_handler(None, 0)
                assert db.execute('SELECT items FROM runtime_storage').fetchone()[0] == history
            results.append(instructions[0])
    assert results[1] < results[0] * 1.5, results
