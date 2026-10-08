import asyncio
import time

from demiflow.execution.request_limits import RequestGate


def test_unpaced_preparations_overlap_without_exceeding_capacity():
    async def run():
        gate=RequestGate(2,interval_s=0)
        both_prepared=asyncio.Event(); finish=asyncio.Event(); prepared=[]
        async def request(index):
            async with gate.reserve() as permit:
                prepared.append(index)
                if len(prepared)==2:both_prepared.set()
                await both_prepared.wait()
                async with permit:
                    await finish.wait()
        tasks=[asyncio.create_task(request(i)) for i in range(3)]
        await asyncio.wait_for(both_prepared.wait(),1)
        await asyncio.sleep(0)
        assert len(prepared)==2 and gate.active==2 and gate.peak==2
        assert not gate.pace.locked()
        finish.set();await asyncio.wait_for(asyncio.gather(*tasks),1)
        assert gate.admitted==3 and gate.active==0 and gate.peak==2
    asyncio.run(run())


def test_unused_reservation_releases_capacity_without_counting_http():
    async def run():
        gate=RequestGate(1,interval_s=.01)
        async with gate.reserve():
            assert gate.admitted==0 and gate.active==0
        async with gate.enter():
            assert gate.active==1 and gate.admitted==1
        assert gate.active==0 and not gate.pace.locked()
    asyncio.run(run())


def test_pacing_tracks_actual_start_after_variable_preparation():
    async def run():
        gate=RequestGate(3,interval_s=.045)
        starts=[]
        async def request(delay):
            async with gate.reserve() as permit:
                await asyncio.sleep(delay)
                async with permit:
                    starts.append(time.monotonic())
                    await asyncio.sleep(.07)
        await asyncio.wait_for(asyncio.gather(request(.075),request(0),request(.01)),1)
        assert len(starts)==3 and all(b-a>=.04 for a,b in zip(starts,starts[1:]))
        assert gate.peak==2 and gate.admitted==3 and gate.active==0
    asyncio.run(run())


def test_cancelling_paced_reservation_releases_start_lock():
    async def run():
        gate=RequestGate(1,interval_s=.1)
        async with gate.enter():pass
        async def request():
            async with gate.reserve():raise AssertionError('Should still be waiting')
        task=asyncio.create_task(request())
        await asyncio.sleep(.015)
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        assert gate.admitted==1 and gate.active==0 and not gate.pace.locked()
        async def next_request():
            async with gate.enter():pass
        await asyncio.wait_for(next_request(),.3)
        assert gate.admitted==2
    asyncio.run(run())
