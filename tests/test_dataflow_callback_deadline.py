"""Only fully drained explicit callback deadlines may become business misses."""
import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.async_instance import AsyncInstance, CallbackDeadlineExceeded


def services(timeout_ms=2000):
    return SimpleNamespace(bounds=SimpleNamespace(timeout_ms=timeout_ms), check_stop=lambda: None)


def test_explicit_callback_deadline_drains_child_and_finally():
    seen=[]
    async def child():
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(.01)
            seen.append('child')
    async def call(row):
        asyncio.create_task(child())
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(.01)
            seen.append('call')
    instance=AsyncInstance(call,services(),timeout=.01)
    try:
        with pytest.raises(CallbackDeadlineExceeded):
            instance(None)
        assert sorted(seen)==['call','child']
    finally:
        instance.close()


@pytest.mark.parametrize('returns', [False,True])
def test_callback_cancel_translation_preserves_actual_outcome(returns):
    async def call(row):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if returns:
                return row
            raise TimeoutError('business cancellation translation')
    instance=AsyncInstance(call,services(),timeout=.01)
    try:
        if returns:
            assert instance(7)==7
        else:
            with pytest.raises(TimeoutError,match='business cancellation translation'):
                instance(7)
    finally:
        instance.close()


def test_callback_deadline_drains_only_its_shared_owner_invocation():
    from demiflow.execution.dataflow_shared import SharedActor
    class Actor:
        async def __call__(self,row):
            await asyncio.sleep(.12 if row=='long' else 10)
            return row
    owner=Actor(); group=SharedActor({'owner':'a','members':['a','b'],'state_owner':'b'},owner)
    first=group.acquire('a',owner,services(),timeout=.01,await_calls=True,lifecycle=None)
    second=group.acquire('b',owner,services(),timeout=.5,await_calls=True,lifecycle=None)
    try:
        with ThreadPoolExecutor(2) as pool:
            long=pool.submit(second,'long')
            with pytest.raises(CallbackDeadlineExceeded):
                first('short')
            assert long.result(timeout=1)=='long'
    finally:
        first.close(); second.close()


def test_callback_deadline_waits_for_actual_to_thread_completion():
    done=threading.Event()
    def blocking():
        time.sleep(.06)
        done.set()
    async def call(row):
        await asyncio.to_thread(blocking)
    instance=AsyncInstance(call,services(),timeout=.01)
    try:
        with pytest.raises(CallbackDeadlineExceeded):
            instance(None)
        assert done.is_set()
    finally:
        instance.close()


def test_undrained_background_work_is_fatal_general_deadline():
    release=threading.Event(); entered=threading.Event()
    def blocking():
        entered.set(); release.wait(2)
    async def call(row):
        await asyncio.to_thread(blocking)
    instance=AsyncInstance(call,services(60),timeout=.01)
    try:
        with pytest.raises(DataflowError) as failure:
            instance(None)
        assert entered.is_set()
        assert type(failure.value) is DataflowError
        assert failure.value.code=='DEADLINE_EXCEEDED'
    finally:
        release.set()
        instance.close()


def test_hard_timeout_does_not_change_actor_lifecycle_deadline():
    class Actor:
        async def astart(self):
            await asyncio.sleep(.03)
        async def __call__(self,row):
            return row
    instance=AsyncInstance(Actor(),services(),timeout=.005)
    try:
        instance.start()
        assert instance(1)==1
    finally:
        instance.close()


class ObservedDeadline:
    def __init__(self, path):
        self.path=str(path)
    def snapshot_state(self):
        return {}
    def restore_state(self, value):
        pass
    async def __call__(self, rows):
        try:
            await asyncio.Event().wait()
        finally:
            with open(self.path,'a') as stream:
                stream.write('cancelled\n')


@pytest.mark.parametrize('kind', ['row','batch'])
@pytest.mark.parametrize('caught', [False,True])
def test_real_stream_deadline_preserves_catch_statistics_and_dead_rows(tmp_path, kind, caught):
    from demiflow import data
    actor=ObservedDeadline(tmp_path/'cancelled')
    source=data.from_items([{'id':i} for i in range(3)])
    options=dict(concurrency=1,hard_timeout=.01,label='probe',catch=(TimeoutError,) if caught else ())
    ds=(source.batch_map(actor,max_batch=2,**options) if kind=='batch'
        else source.map_async(actor,**options))
    stats=ds.run_stream()
    calls=2 if kind=='batch' else 3
    reason='TimeoutError' if caught else 'hard_timeout'
    assert stats.emitted==0 and stats.miss=={'probe:'+reason:calls}
    assert (tmp_path/'cancelled').read_text().splitlines()==['cancelled']*calls
    if kind=='batch':
        assert [v['reason'] for v in stats.dead_batches]==[reason]*calls
        assert [v['rows'] for v in stats.dead_batches]==[[{'id':0},{'id':1}],[{'id':2}]]
    else:
        assert stats.dead_batches==[]
