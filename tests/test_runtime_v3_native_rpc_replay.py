"""Native reserve delivery retries use the real original journal and budget."""
from contextlib import contextmanager
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import json
import time

import pytest

from demiflow.execution.contracts.dataflow import Bounds, WorkerEvent, WorkerLease
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_authority import DriverArtifactPort, DriverJournalService, _ref_to_json
from demiflow.execution.dataflow_store import RunArtifactStore
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_context import RunContext, RunAction
from demiflow.operator_llm.journal import RequestRecord, UncertainPromptCall


class IntentionalInterruption(Exception):
    pass


@contextmanager
def interrupted_run(location, *, resume=False):
    # These counterexamples intentionally leave an unresolved native request;
    # closing the owning run must record failure, never fabricated completion.
    try:
        scope = RunContext.resume(location) if resume else RunContext.start(location, limits={'max_requests': 10})
        with scope as run:
            yield run
            raise IntentionalInterruption()
    except IntentionalInterruption:
        pass


@contextmanager
def service(run, *, pending=4):
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
    bounds = replace(bounds, max_pending_events=pending)
    run.journal.bind_execution_action(run.fence, 'action-1', {'kind': 'native-replay'})
    action = RunAction(run, 'action-1', {'kind': 'native-replay'})
    port = DriverArtifactPort(action, bounds=bounds)
    store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id='native-store', journal=port, bounds=bounds)
    authority = DriverJournalService(action, store=store, bounds=bounds)
    authority.bind_recovery_policy(policy)
    grant = store.put(iter([b'grant']), max_bytes=5, media_type='text/plain')
    lease = WorkerLease(allocation_id='worker-1', boot_id='boot-1', stage_id='stage-1', partition_ids=('p0',),
                        host_id='host-1', endpoint_ref='ep-1', grant_ref=grant,
                        expires_at_ms=int(time.time()*1000)+60000)
    authority.register_worker(lease, ('node-1',))
    try:
        yield authority, store, lease
    finally:
        authority.close(deadline_ms=1000)


def event(run, store, seed='a'):
    request = RequestRecord({'protocol': 'openai_compatible', 'model_contract': {'name': 'fixture'}, 'body': {'input': seed}})
    raw = json.dumps(request).encode()
    ref = store.put(iter([raw]), max_bytes=len(raw), media_type='application/json')
    operation_id = identity(['rpc', seed])
    payload = {'operation_id': operation_id, 'operation': 'reserve', 'action_id': 'action-1', 'node_id': 'node-1',
               'request_key': request.request_key, 'request_ref': _ref_to_json(ref), 'value_ref': None,
               'reservation': [identity(['budget', seed]), {'requests': [1, 10]}], 'options': {}}
    return WorkerEvent(schema='demiflow.dataflow_event.v3', kind='native_request', operation_id=operation_id,
                       run_id=run.ref.run_id, attempt=run.attempt, fence=run.fence,
                       allocation_id='worker-1', boot_id='boot-1', plan_revision=0, scope_id='stage-1',
                       controller_epoch=0, payload=payload, payload_ref=None)


def charged(run):
    with run.journal._connection() as db:
        return (db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0],
                db.execute('SELECT count(*) FROM calls').fetchone()[0])


def altered(original, **changes):
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    payload = _thaw_json(original.payload)
    payload.update(changes)
    return replace(original, payload=payload, operation_id=payload['operation_id'])


def test_native_reserve_retry_is_same_receipt_and_single_charge(tmp_path):
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        request = altered(event(run, store), options={'marker': True, 'sign': -0.0})
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, retry = list(pool.map(authority.dispatch, (request, request)))
        assert first == retry and first.status == 'ok' and first.payload['value'] is True
        assert charged(run) == (1, 1)
        changed = authority.dispatch(altered(request, options={'other': True}))
        assert changed.status == 'error' and changed.error_code == 'INVALID_CONTRACT'
        for options in ({'marker': 1, 'sign': -0.0}, {'marker': True, 'sign': 0.0}):
            assert authority.dispatch(altered(request, options=options)).error_code == 'INVALID_CONTRACT'
        assert charged(run) == (1, 1)
        different_id = altered(request, operation_id=identity(['different-rpc']))
        with pytest.raises(UncertainPromptCall, match='active writer'):
            authority.dispatch(different_id)
        assert different_id.operation_id not in authority._native_reserve_replies
        assert charged(run) == (1, 1)
        authority.register_worker(replace(lease, expires_at_ms=1), ('node-1',))
        assert authority.dispatch(request).error_code == 'FENCED'
        authority.register_worker(lease, ('node-1',))
        authority.close(deadline_ms=1000)
        with pytest.raises(DataflowError, match='FENCED'):
            authority.dispatch(request)
        assert not authority._native_reserve_replies


def test_native_reserve_reply_window_is_bounded_and_eviction_fails_closed(tmp_path):
    with interrupted_run(tmp_path/'run') as run, service(run, pending=1) as (authority, store, _):
        first = event(run, store, 'a')
        second = event(run, store, 'b')
        assert authority.dispatch(first).status == authority.dispatch(second).status == 'ok'
        assert len(authority._native_reserve_replies) == 1
        with pytest.raises(UncertainPromptCall, match='active writer'):
            authority.dispatch(first)
        assert charged(run) == (2, 2)


def test_new_driver_fence_never_reuses_old_reserve_lease(tmp_path):
    with interrupted_run(tmp_path/'run') as run:
        ref = run.ref
        with service(run) as (authority, store, _):
            old = event(run, store)
            assert authority.dispatch(old).status == 'ok'
            assert charged(run) == (1, 1)
    with interrupted_run(ref, resume=True) as resumed, service(resumed) as (replacement, _, _):
        with pytest.raises(DataflowError, match='FENCED'):
            replacement.dispatch(old)
        from demiflow.execution.contracts.dataflow_codec import _thaw_json
        current = replace(old, attempt=resumed.attempt, fence=resumed.fence, payload=_thaw_json(old.payload))
        with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
            replacement.dispatch(current)
        assert not replacement._native_reserve_replies
        assert charged(resumed) == (1, 1)
