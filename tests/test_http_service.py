import asyncio
import sys
import socket
import json
from pathlib import Path
import pytest
from test_model_service import managed, assert_reaped
from demiflow import data
from demiflow.services import ManagedHTTPService, ModelServiceError
from demiflow.services.manage import start_service, stop_service, status_service


def declaration(managed, **options):
    prior, _, _, _, port, _ = managed
    return ManagedHTTPService([sys.executable,str(prior.root/'server.py')],
        base_url=f'http://127.0.0.1:{port}/v1',root=prior.root,expected_model='fixture',
        startup_timeout_s=5,shutdown_timeout_s=.2,poll_interval_s=.01,**options)


def test_generic_service_native_prompt_cache_and_cleanup(managed):
    spec=declaration(managed)
    _,_,processes,_,_,pack=managed
    assert not processes
    options={'sqlite_journal':{'path':str(spec.root/'calls.sqlite')}}
    for _ in range(2):
        result=data.from_items([{'x':i} for i in range(8)]).map_prompt_async(
            'test',config=pack,inputs={'value':'x'},output='result',concurrency=4,
            service=spec,options=options).materialize().take_all()
        assert [row['result'] for row in result]==['ok']*8
    assert len(processes)==1
    assert_reaped(processes)


def test_generic_custom_actor_scope_and_gpu_environment(managed):
    spec=declaration(managed,gpus=[2,3],env={'OFFLOAD_MODE':'sequential'})
    async def use():
        async with spec.bind() as owner:
            await asyncio.gather(*(owner.ensure_ready() for _ in range(8)))
            environment=Path(f'/proc/{owner._process.pid}/environ').read_bytes().split(b'\0')
            assert b'CUDA_VISIBLE_DEVICES=2,3' in environment
            assert b'OFFLOAD_MODE=sequential' in environment
            # The same explicit scope can cover multiple custom nodes/runs.
            await owner.ensure_ready()
    asyncio.run(use())
    assert_reaped(managed[2])


def test_generic_service_refuses_occupied_port_without_killing_owner(managed):
    spec=declaration(managed)
    async def use():
        async with spec.bind() as owner:
            other=spec.bind()
            with pytest.raises(ModelServiceError,match='occupied'):
                await other.ensure_ready()
            await other.aclose()
            await owner.ensure_ready()
    asyncio.run(use())
    assert len(managed[2])==1
    assert_reaped(managed[2])


def test_generic_service_cancelled_startup_cleans_process(managed):
    spec=declaration(managed)
    spec.command=(sys.executable,'-c','import time;time.sleep(30)')
    async def use():
        owner=spec.bind()
        pending=asyncio.create_task(owner.ensure_ready())
        while owner._process is None:await asyncio.sleep(.01)
        pending.cancel()
        await owner.aclose()
        await asyncio.gather(pending,return_exceptions=True)
    asyncio.run(use())
    assert_reaped(managed[2])


def test_cancelled_context_entry_cleans_without_explicit_close(managed):
    spec = declaration(managed)
    spec.command = (sys.executable, '-c', 'import time;time.sleep(30)')
    async def use():
        owner = spec.bind()
        async def enter():
            async with owner:
                pytest.fail('Sleeping process cannot become ready')
        pending = asyncio.create_task(enter())
        while owner._process is None:
            await asyncio.sleep(.01)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert_reaped(managed[2])
    asyncio.run(use())


def test_persistent_supervisor_survives_clients_and_stops_owned_child(managed):
    spec=declaration(managed)
    config={'command':list(spec.command),'base_url':spec.base_url,'expected_model':'fixture',
            'startup_timeout_s':5,'shutdown_timeout_s':.2,'poll_interval_s':.01}
    record=None
    try:
        record=start_service(spec.root,'fixture',config)
        assert record['state']=='ready' and record['alive']
        assert status_service(spec.root,'fixture',probe=True)['healthy']
        assert status_service(spec.root,'fixture')['child_pid']==record['child_pid']
        with pytest.raises(RuntimeError,match='already alive'):
            start_service(spec.root,'fixture',config)
        # The supervisor exposes the endpoint without binding its lifecycle to
        # a pipeline client. Starting/stopping clients does not replace its PID.
        assert status_service(spec.root,'fixture')['manager_pid']==record['manager_pid']
    finally:
        stop_service(spec.root,'fixture',timeout_s=5)
    assert not status_service(spec.root,'fixture')['alive']
    assert record is not None
    assert not Path(f'/proc/{record["child_pid"]}').exists()


def test_generic_service_config_validation(tmp_path):
    for options in ({'command':'shell text'}, {'gpus':[0,0]}, {'startup_timeout_s':0},
                    {'env':{'CUDA_VISIBLE_DEVICES':'0'}}, {'ready_path':'//remote/health'}):
        kwargs={'command':[sys.executable,'-V'],'base_url':'http://127.0.0.1:18461/v1','root':tmp_path,**options}
        with pytest.raises(ValueError):ManagedHTTPService(**kwargs)
    assert not list(tmp_path.iterdir())
