"""Recovery through the ordinary Dataset executor, without Forge or a model."""
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RunRef, RecoveryContractError


class Counter:
    def __init__(self):
        self.n = 0

    async def __call__(self, row):
        self.n += 1
        trace = os.environ.get("FOUNDATION_TRACE")
        if trace:
            with open(trace, "a") as stream:
                stream.write(str(self.n) + "\n")
        if os.environ.get("FOUNDATION_CRASH") == "during_batch" and self.n == 603:
            import psutil
            Path(os.environ['FOUNDATION_FAULT']).write_text(json.dumps({
                'window': 'during_batch', 'n': self.n, 'pid': os.getpid(),
                'created': psutil.Process().create_time()}))
            os._exit(37)
        if row["id"] % 5 == 0:
            return None
        return [{"id": row["id"], "n": self.n, "child": child} for child in range(2)]

    def snapshot_state(self):
        return {"n": self.n}

    def restore_state(self, state):
        self.n = state["n"]


class CutWindowInterrupted(RuntimeError):
    pass


def run_graph(root, window=None):
    from contextlib import ExitStack
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from runtime_v3_residency_scenarios import _cut_facts
    root = Path(root)
    directory = root / "run"
    prior = RunJournal(directory).recovery_identity()
    scope = (RunContext.resume(RunRef(str(directory), prior["run_id"]))
             if prior else RunContext.start(directory))
    recover = DataflowCheckpointCoordinator.recover
    def observe_recovery(coordinator, **kwargs):
        image = recover(coordinator, **kwargs)
        if image is not None and coordinator._action.action_id == 'action-1':
            (root/'recovered-image.json').write_text(json.dumps(_cut_facts(coordinator, image)))
        return image
    def interrupt(name, commit_id):
        fact = RunJournal(directory).execution_commit(commit_id)
        if name == window and fact['intent']['sinks'] == ['materialize'] and fact['sequence'] == 2:
            (root/'fault.json').write_text(json.dumps({'window': name, 'pid': os.getpid(), 'fact': fact}))
            # The v3 driver owns publication and must unwind its real workers.
            # during_batch below separately kills an actual managed worker.
            raise CutWindowInterrupted(window)
    with ExitStack() as stack:
        stack.enter_context(patch.object(DataflowCheckpointCoordinator, 'recover', observe_recovery))
        if window and window != 'during_batch':
            stack.enter_context(patch('demiflow.execution.checkpoint.commit.fault_point', interrupt))
        run = stack.enter_context(scope)
        rows = [{"id": i} for i in range(1093)] + [{"id": 1}] * 7
        materialized = data.from_items(rows).map_async(Counter(), concurrency=1).materialize()
        result = materialized.take_all()
        return result, run.ref


@pytest.mark.parametrize('window', [
    pytest.param('during_batch', id='during_batch-588'),
    pytest.param('after_prepared', id='after_prepared-76'),
    pytest.param('after_sink_commit_before_receipt', id='after_sink_commit_before_receipt-76'),
    pytest.param('after_checkpoint_committed', id='after_checkpoint_committed-76'),
])
def test_real_actor_state_outputs_and_occurrences_resume_together(tmp_path, monkeypatch, window):
    repo = Path(__file__).resolve().parents[1]
    trace = tmp_path / "calls"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(repo), str(repo / "tests")]),
        "FOUNDATION_TRACE": str(trace), "FOUNDATION_CRASH": window,
        'FOUNDATION_FAULT': str(tmp_path/'fault.json'),
        "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}
    with subprocess.Popen([sys.executable, "-B", "-c",
        "import sys; from test_execution_recovery_stream import run_graph; run_graph(sys.argv[1], sys.argv[2])",
        str(tmp_path), window], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
        stdout, stderr = child.communicate(timeout=45)
    assert child.returncode == 1, stdout + stderr
    fault = json.loads((tmp_path/'fault.json').read_text())
    assert fault['window'] == window
    if window == 'during_batch':
        import psutil
        assert fault['pid'] != child.pid and fault['n'] == 603
        assert not psutil.pid_exists(fault['pid']) or psutil.Process(fault['pid']).create_time() != fault['created']
        assert 'UNKNOWN_EFFECT' in stderr
    else:
        assert fault['pid'] == child.pid and 'CutWindowInterrupted' in stderr
        assert fault['fact']['intent']['sinks'] == ['materialize']
    journal = RunJournal(tmp_path/'run')
    before = journal.execution_commits('action-1')
    assert before and any(f['status'] == 'acked' for f in before)
    previous = len(trace.read_text().splitlines())
    monkeypatch.setenv("FOUNDATION_TRACE", str(trace))
    monkeypatch.delenv('FOUNDATION_CRASH', raising=False)
    result, reference = run_graph(tmp_path)
    image = json.loads((tmp_path/'recovered-image.json').read_text())
    states = [s for s in image['kernel_images'] if s['state'] and s['state']['mode'] == 'hooks']
    assert len(states) == 1
    restored = states[0]['state']['value']['n']
    assert 0 < restored < 1100 and image['sources']
    assert all(c['next_sequence'] >= restored for c in image['sources'])
    calls = [int(n) for n in trace.read_text().splitlines()]
    assert calls[:previous] == list(range(1, previous+1))
    # Original ACKed actor state, not the retired 512-row block size, defines
    # the replay boundary. Calls after that frontier may have run without ACK.
    assert calls[previous:] == list(range(restored+1, 1101))
    assert all(calls.count(n) == 1 for n in range(1, restored+1))
    expected = [dict(id=i, n=i+1, child=c) for i in range(1093) if i % 5 for c in range(2)]
    expected += [dict(id=1, n=n, child=c) for n in range(1094, 1101) for c in range(2)]
    assert result == expected
    journal = RunJournal(reference.directory)
    assert journal.execution_status()["attempt"] == 2
    import sqlite3
    with sqlite3.connect(journal.path) as db:
        actions = [key for key, raw in db.execute("SELECT action_id,declaration FROM runtime_actions")
                   if json.loads(raw)["kind"] == "materialize"]
    assert len(actions) == 1
    facts = journal.execution_commits(actions[0])
    assert [f['sequence'] for f in facts] == list(range(1, len(facts)+1))
    assert all(c['status'] == 'acked' for c in facts)
    by_id = {f['commit_id']: f for f in facts}
    for fact in before:
        after = by_id[fact['commit_id']]
        if fact['status'] == 'acked':
            assert after == fact
        else:
            assert after['intent'] == fact['intent'] and after['prepared'] == fact['prepared']
    assert image['commit_id'] in by_id
    assert not list(Path(reference.directory).rglob("checkpoint.json"))


def test_actor_without_state_contract_fails_before_first_call(tmp_path, monkeypatch):
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.scheduler import native_limits as action_limits
    calls = tmp_path/'calls'
    class UnsafeCounter:
        def __init__(self):
            self.n = 0
            self.opaque_resource = object()  # Plain counters now use auto_json.
        async def __call__(self, row):
            with calls.open('a') as stream:
                stream.write(json.dumps(row)+'\n')
            self.n += 1
            return row
    resolve = action_limits.resolve_runtime_policy
    def checkpoint(options, limits, **kwargs):
        return resolve({**options, 'required_recovery': 'checkpoint'}, limits, **kwargs)
    monkeypatch.setattr(action_limits, 'resolve_runtime_policy', checkpoint)
    with pytest.raises(RecoveryContractError, match="snapshot_state"):
        with RunContext.start(tmp_path / "run") as run:
            data.range(10).map_async(UnsafeCounter).materialize()
    assert not calls.exists()
    assert not run.journal.execution_commits('action-1')
    assert run.journal.source_binding('action-1-state-recovery-restriction') is None


@pytest.mark.parametrize('resource', ['lock', 'task'])
def test_checkpoint_factory_rejection_closes_started_resources_once(tmp_path, monkeypatch, resource):
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.scheduler import native_limits as action_limits
    events = tmp_path/'lifecycle'
    class ResourceActor:
        def __init__(self):
            import threading
            self.resource = threading.Lock() if resource == 'lock' else None
            self.event('created')
        def event(self, name):
            with events.open('a') as stream:
                stream.write(name+'\n')
        async def astart(self):
            import asyncio
            self.event('started')
            if resource == 'task':
                self.resource = asyncio.create_task(asyncio.sleep(3600))
        async def __call__(self, row):
            self.event('call')
            return row
        async def astop(self):
            import asyncio
            self.event('stopped')
            if resource == 'task':
                self.resource.cancel()
                try:
                    await self.resource
                except asyncio.CancelledError:
                    pass
                self.event('task-drained')
        async def aclose(self):
            if resource == 'task':
                assert self.resource.done()
            self.event('closed')
    resolve = action_limits.resolve_runtime_policy
    def checkpoint(options, limits, **kwargs):
        return resolve({**options, 'required_recovery': 'checkpoint'}, limits, **kwargs)
    monkeypatch.setattr(action_limits, 'resolve_runtime_policy', checkpoint)
    with pytest.raises(RecoveryContractError, match='required_recovery=checkpoint'):
        with RunContext.start(tmp_path/'run') as run:
            data.from_items([{'id': 1}]).map_async(ResourceActor).materialize()
    assert events.read_text().splitlines() == ['created', 'started', 'stopped'] + (
        ['task-drained'] if resource == 'task' else []) + ['closed']
    assert not run.journal.execution_commits('action-1')


@pytest.mark.parametrize('hooks', [False, True])
def test_checkpoint_accepts_plain_state_and_explicit_hooks(tmp_path, monkeypatch, hooks):
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.scheduler import native_limits as action_limits
    class PlainCounter:
        def __init__(self):
            self.n = 0
        async def __call__(self, row):
            self.n += 1
            return {**row, 'n': self.n}
    class HookCounter(PlainCounter):
        def snapshot_state(self):
            return {'n': self.n}
        def restore_state(self, state):
            self.n = state['n']
    resolve = action_limits.resolve_runtime_policy
    def checkpoint(options, limits, **kwargs):
        return resolve({**options, 'required_recovery': 'checkpoint'}, limits, **kwargs)
    monkeypatch.setattr(action_limits, 'resolve_runtime_policy', checkpoint)
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}) as run:
        rows = data.from_items([{'id': i} for i in range(4)]).map_async(
            HookCounter if hooks else PlainCounter).materialize().take_all()
        assert rows == [{'id': i, 'n': i+1} for i in range(4)]
        assert run.journal.source_binding('action-1-state-recovery-restriction') is None
        assert all(f['status'] == 'acked' for f in run.journal.execution_commits('action-1'))


def test_opaque_actor_default_attempt_only_is_recorded_and_cannot_resume(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    calls = tmp_path/'opaque-calls'
    class UnsafeCounter:
        def __init__(self):
            self.n, self.opaque_resource = 0, object()
        async def __call__(self, row):
            self.n += 1
            with calls.open('a') as stream:
                stream.write(json.dumps({'id': row['id'], 'n': self.n})+'\n')
            return {**row, 'n': self.n}
    def dataset():
        return data.from_items([{'id': i} for i in range(10)]).map_async(UnsafeCounter)
    class StoppedAfterAck(Exception):
        pass
    commit = DataflowCheckpointCoordinator.commit
    cuts = []
    def stop(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        assert not image.cut.final
        cuts.append(image.commit_id)
        raise StoppedAfterAck()
    with pytest.raises(StoppedAfterAck):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2,
                'checkpoint_interval_ms': 60000}) as run:
            reference = run.ref
            with monkeypatch.context() as scoped:
                scoped.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                dataset().materialize()
    assert cuts and calls.exists()
    previous = calls.read_bytes()
    restriction = run.journal.source_binding('action-1-state-recovery-restriction')
    assert restriction['recovery'] == 'attempt_only'
    assert 'snapshot_state/restore_state' in restriction['reason']
    before = run.journal.execution_commits('action-1')
    assert before and all(f['status'] == 'acked' for f in before)
    with pytest.raises(RecoveryContractError, match='Cannot resume attempt-only actor state'):
        with RunContext.resume(reference):
            dataset().materialize()
    assert calls.read_bytes() == previous
    assert run.journal.execution_commits('action-1') == before


def test_resume_keeps_sealed_checkpoint_requirement_over_current_default(tmp_path, monkeypatch):
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.scheduler import native_limits as action_limits
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    calls = tmp_path/'calls'
    class RuntimeResourceCounter:
        def __init__(self):
            self.n = 0
            if os.environ.get('DEMIFLOW_TEST_OPAQUE_ON_RESUME') == '1':
                self.resource = object()
        async def __call__(self, row):
            self.n += 1
            with calls.open('a') as stream:
                stream.write(str(self.n)+'\n')
            return {**row, 'n': self.n}
    def dataset():
        return data.from_items([{'id': i} for i in range(10)]).map_async(RuntimeResourceCounter)
    resolve = action_limits.resolve_runtime_policy
    def checkpoint(options, limits, **kwargs):
        return resolve({**options, 'required_recovery': 'checkpoint'}, limits, **kwargs)
    class AfterAck(Exception):
        pass
    commit = DataflowCheckpointCoordinator.commit
    def stop(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        assert not image.cut.final
        raise AfterAck()
    monkeypatch.delenv('DEMIFLOW_TEST_OPAQUE_ON_RESUME', raising=False)
    with pytest.raises(AfterAck):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}) as run:
            reference = run.ref
            with monkeypatch.context() as scoped:
                scoped.setattr(action_limits, 'resolve_runtime_policy', checkpoint)
                scoped.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                dataset().materialize()
    physical = load_sealed_plan(reference.directory, 'action-1')['physical']
    assert physical.policy.required_recovery == 'checkpoint'
    before_calls = calls.read_bytes()
    before_commits = run.journal.execution_commits('action-1')
    assert before_commits and all(f['status'] == 'acked' for f in before_commits)
    monkeypatch.setenv('DEMIFLOW_TEST_OPAQUE_ON_RESUME', '1')
    # The current resolver is back to its weaker default. Only the actual
    # historical sealed policy may govern the newly constructed actor.
    with pytest.raises(RecoveryContractError, match='required_recovery=checkpoint'):
        with RunContext.resume(reference):
            dataset().materialize()
    assert calls.read_bytes() == before_calls
    assert run.journal.execution_commits('action-1') == before_commits
    assert run.journal.source_binding('action-1-state-recovery-restriction') is None
    assert load_sealed_plan(reference.directory, 'action-1')['physical'] == physical


def test_fixed_source_is_reused_after_source_capture_commit(tmp_path):
    def source():
        path = tmp_path / "mutable.json"
        yield from json.loads(path.read_text())
    path = tmp_path / "mutable.json"
    path.write_text('[{"id":1},{"id":2}]')
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path / "run") as run:
            reference = run.ref
            result = data.from_iter(source).map_async(Counter()).materialize().take_all()
            raise RuntimeError("later action failed")
    path.write_text('[{"id":999}]')
    with RunContext.resume(reference):
        assert data.from_iter(source).map_async(Counter()).materialize().take_all() == result
