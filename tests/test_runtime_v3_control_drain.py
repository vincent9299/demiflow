"""Original async service replies remain deliverable during K3 worker cleanup."""
from dataclasses import replace
import select
import threading
import time
from concurrent.futures import Future
import pytest
from types import SimpleNamespace

from demiflow.execution.dataflow_hosts import LocalHostEndpoint
from demiflow.execution.contracts.dataflow_codec import _thaw_json
from test_runtime_v3_native_rpc_replay import interrupted_run, service, event, charged


@pytest.mark.parametrize('transport',['direct','thread'])
def test_late_reply_drain_preserves_other_worker_boot_and_original_notify(tmp_path,monkeypatch,transport):
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority,store,lease):
        other=replace(lease,allocation_id='worker-2',boot_id='boot-2')
        authority.register_worker(other,('node-1',))
        first=event(run,store,'own')
        second=event(run,store,'other')
        second=replace(second,allocation_id='worker-2',boot_id='boot-2',payload=_thaw_json(second.payload))
        entered,release=threading.Event(),threading.Event()
        dispatch=authority.dispatch
        def delayed(request):
            if request==first:
                entered.set()
                assert release.wait(5)
            return dispatch(request)
        monkeypatch.setattr(authority,'dispatch',delayed)
        assert authority.submit(first) and entered.wait(2)
        assert authority.submit(second)
        def await_replies(count):
            deadline=time.monotonic()+3
            while time.monotonic()<deadline:
                with authority._lock:
                    if len(authority._completed)==count:return
                time.sleep(.005)
            raise AssertionError('Original service reply did not finish')
        try:
            await_replies(1)
            with authority._lock:original=tuple(authority._completed)
            assert authority.poll_worker_replies(max_replies=4,allocation_id=lease.allocation_id,boot_id=lease.boot_id)==()
            release.set()
            await_replies(2)
            assert authority.poll_worker_replies(max_replies=4,allocation_id=lease.allocation_id,boot_id='stale-boot')==()
            assert select.select([authority.reply_waitable()],[],[],0)[0]
            from demiflow.execution.hosts.thread import ThreadHostEndpoint
            host=ThreadHostEndpoint if transport=='thread' else LocalHostEndpoint
            endpoint=host.__new__(host)
            endpoint._allocation_id,endpoint._boot_id=lease.allocation_id,lease.boot_id
            endpoint._services=SimpleNamespace(journal_service=authority,bounds=authority._bounds,
                policy=SimpleNamespace(control_timeout_ms=1000))
            delivered=[];endpoint.deliver_reply=delivered.append
            if transport=='thread':
                completion=Future()
                endpoint._connector=SimpleNamespace(units={lease.allocation_id:SimpleNamespace(
                    submit=lambda *args:completion)},pull=lambda *a,**kw:())
                endpoint._k3_envelope=lambda kind,payload:{}
                endpoint._accept_final_observations=lambda result:None
                def deliver(reply):
                    delivered.append(reply)
                    completion.set_result({'receipt':{'status':'ok'}})
                endpoint.deliver_reply=deliver
                assert endpoint._send_k3('Cancel',{'reason':'cooperative:run_cancelled'})=={'status':'ok'}
                assert endpoint._cleanup_completed
            else:
                endpoint._deliver_pending_journal_replies()
            assert len(delivered)==1 and delivered[0].operation_id==first.operation_id
            assert delivered[0].status=='ok'
            with authority._lock:
                assert tuple(authority._completed)==original
                assert second.operation_id in authority._pending_payloads
                assert first.operation_id not in authority._pending_payloads
            assert select.select([authority.reply_waitable()],[],[],0)[0]
            remaining=authority.poll_replies(max_replies=4)
            assert remaining==original and remaining[0].allocation_id=='worker-2'
            assert not select.select([authority.reply_waitable()],[],[],0)[0]
            endpoint._deliver_pending_journal_replies()
            assert len(delivered)==1 and charged(run)==(2,2)
        finally:release.set()


@pytest.mark.parametrize('reason',['cooperative:operator_stop_file','cooperative:run_cancelled'])
def test_early_stopped_reserve_returns_original_error_reply_and_releases_waiter(tmp_path,reason):
    from concurrent.futures import ThreadPoolExecutor
    from demiflow.execution.dataflow_hosts import ControlMailbox
    from demiflow.execution.contracts.dataflow_failures import restore_failure
    from demiflow.execution.request_limits import ServiceStopped
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority,store,lease):
        admitted=event(run,store,'admitted')
        assert authority.dispatch(admitted).status=='ok'
        authority.stop_model_admission(reason)
        stopped=event(run,store,'stopped')
        mailbox=ControlMailbox(bounds=authority._bounds,check_stop=lambda:None)
        endpoint=LocalHostEndpoint.__new__(LocalHostEndpoint)
        endpoint._allocation_id,endpoint._boot_id=lease.allocation_id,lease.boot_id
        endpoint._assignment=stopped
        endpoint._services=SimpleNamespace(journal_service=authority,bounds=authority._bounds)
        endpoint.deliver_reply=mailbox.deliver
        with ThreadPoolExecutor(1) as pool:
            waiter=pool.submit(mailbox.request,stopped,deadline_ms=2000)
            try:
                until=time.monotonic()+1
                events=()
                while not events and time.monotonic()<until:
                    events=mailbox.drain(max_events=1)
                    if not events:time.sleep(.001)
                assert events==(stopped,)
                endpoint._early_event(stopped)
                reply=waiter.result(1)
                assert reply.status=='error' and reply.operation_id==stopped.operation_id
                error=restore_failure(dict(reply.payload))
                assert type(error) is ServiceStopped and str(error)==reason.split(':',1)[1]
                assert charged(run)==(1,1)
                assert not mailbox._pending
            finally:
                mailbox.cancel_pending()
                mailbox.close()
