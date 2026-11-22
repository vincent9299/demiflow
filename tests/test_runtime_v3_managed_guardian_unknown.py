"""Actual cleanup helper IO failures retain the original unit and capacity."""
import json
import os
import sys
import time
import psutil
import pytest

from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.process_unit import ManagedProcessUnit, birth_start
from demiflow.resources.process_observation import unit_stopped
from runtime_v3_managed_http_scenarios import managed_fixture, assert_reaped


def test_cleanup_helper_unknown_keeps_original_fd_and_charge_until_observation_recovers(tmp_path, monkeypatch, request):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode='detached')
    marker, observed = tmp_path / 'hold-observer-error', tmp_path / 'unknown-seen'
    marker.touch()
    declaration = {'parent': os.getpid(), 'birth': birth_start(os.getpid()),
        'command': list(fixture['service'].command), 'shutdown_timeout_s': .5}
    script = '''import json,sys
from pathlib import Path
from demiflow.resources import process_group as child
from demiflow.resources import process_observation as observation
original=observation._read
def failed_proc_read(path,*args,**kwargs):
    ledger,marker,seen=map(Path,sys.argv[2:5])
    if marker.exists() and ledger.exists() and any(json.loads(line)['event']=='descendant' for line in ledger.read_text().splitlines()):
        seen.touch()
        raise OSError('injected-original-proc-observation-error')
    return original(path,*args,**kwargs)
observation._read=failed_proc_read
child.main(sys.argv[1])
'''
    unit = ManagedProcessUnit(tmp_path / 'units')
    pool = LocalCapacityPool(tmp_path / 'pool', capacity={'cpu_millis': 1000, 'memory_bytes': 256*1024**2})
    pool.reserve('guardian', 'b'*64, {'cpu_millis': 1000, 'memory_bytes': 256*1024**2}, 10000)
    spawned = unit.spawn('service', 'guardian', 'b'*64,
        [sys.executable, '-B', '-c', script, json.dumps(declaration), str(fixture['events']), str(marker), str(observed)],
        log_path=tmp_path / 'guardian.log', extra_env={'CUDA_VISIBLE_DEVICES': ''})
    pool.bind_identity('guardian', spawned['identity'])
    process = spawned['process']
    try:
        deadline = time.monotonic() + 10
        while not observed.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert observed.exists()
        time.sleep(.4)  # Persistent failures cover multiple bounded retry ticks.
        assert process.poll() is None
        assert not unit_stopped(unit.directory('service', 'guardian') / 'unit.lock', spawned['identity']['unit'])
        snapshot = pool.snapshot()
        assert snapshot['charged']['memory_bytes'] == 256*1024**2
        assert snapshot['active'][0]['state'] == 'BOUND'
        assert not (unit.directory('service', 'guardian') / 'cleanup.json').exists()
        marker.unlink()
        assert process.wait(timeout=10) != 0  # Original observer error is not a successful service result.
        assert_reaped(fixture, requests=0)
        evidence = unit.stop('service', 'guardian', terminate_deadline_ms=1000, process=process)
        assert evidence['confirmed']
        pool.release('guardian', evidence)
        assert not pool.snapshot()['active']
        (tmp_path / 'unknown-proof.json').write_text(json.dumps({'charged_while_unknown': snapshot,
            'original_exit_evidence': evidence, 'released': pool.snapshot()}, indent=2))
    finally:
        marker.unlink(missing_ok=True)
        if process.poll() is None:
            process.wait(timeout=10)


def test_worker_owner_and_outer_unit_do_not_kill_unknown_guardian(tmp_path, monkeypatch, request):
    """Real owner close timeout + original outer worker stop keep unknown charged."""
    from demiflow.execution.dataflow_managed_http import freeze_managed_http
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode='detached')
    marker, observed = tmp_path / 'hold', tmp_path / 'unknown'
    ready, failed = tmp_path / 'owner-ready.json', tmp_path / 'owner-close.json'
    guardian_script = '''import json,sys
from pathlib import Path
from demiflow.resources import process_group as child
from demiflow.resources import process_observation as observation
original=observation._read
def failed_read(path,*args,**kwargs):
    if Path(sys.argv[2]).exists():
        Path(sys.argv[3]).touch()
        raise OSError('injected-cleanup-helper-unknown')
    return original(path,*args,**kwargs)
observation._read=failed_read
child.main(sys.argv[1])
'''
    worker_script = '''import asyncio,json,os,subprocess,sys,time
from pathlib import Path
from demiflow.execution.dataflow_managed_http import restore_worker_managed_http
from demiflow.resources.process_observation import inherited_unit,UNIT_ENV
from demiflow.resources.process_unit import birth_start
service=restore_worker_managed_http(json.loads(sys.argv[1]))
owner=service.bind()
def spawn(env):
    unit=inherited_unit()
    launch=dict(parent=os.getpid(),birth=birth_start(os.getpid()),command=list(service.command),shutdown_timeout_s=.5)
    return subprocess.Popen([sys.executable,'-B','-c',sys.argv[2],json.dumps(launch),sys.argv[3],sys.argv[4]],
        cwd=service.root,env={**env,UNIT_ENV:str(unit[0])},pass_fds=(*unit,*owner._resource_fds),stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True)
owner._spawn_process=spawn
async def run():
    await owner.ensure_ready()
    Path(sys.argv[5]).write_text(json.dumps(dict(guardian=owner._process.pid,birth=birth_start(owner._process.pid))))
    deadline=time.monotonic()+20
    while not Path(sys.argv[4]).exists() and time.monotonic()<deadline: await asyncio.sleep(.01)
    try: await owner.aclose()
    except Exception as error:
        Path(sys.argv[6]).write_text(json.dumps(dict(error=str(error),guardian_alive=owner._process.poll() is None)))
    else: Path(sys.argv[6]).write_text(json.dumps(dict(error=None)))
    while True: await asyncio.sleep(.1)
asyncio.run(run())
'''
    unit = ManagedProcessUnit(tmp_path / 'units')
    pool = LocalCapacityPool(tmp_path / 'pool', capacity={'cpu_millis': 1000, 'memory_bytes': 256*1024**2})
    pool.reserve('worker', 'c'*64, {'cpu_millis': 1000, 'memory_bytes': 256*1024**2}, 10000)
    spawned = unit.spawn('service', 'worker', 'c'*64,
        [sys.executable, '-B', '-c', worker_script, json.dumps(freeze_managed_http(fixture['service'])),
         guardian_script, str(marker), str(observed), str(ready), str(failed)], log_path=tmp_path / 'worker.log')
    pool.bind_identity('worker', spawned['identity'])
    process = spawned['process']
    def await_file(path):
        deadline = time.monotonic() + 12
        while not path.exists() and time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / 'worker.log').read_text()
            time.sleep(.01)
        assert path.exists(), (tmp_path / 'worker.log').read_text()
    def assert_port_ownership(*, locked):
        import fcntl
        import socket
        from urllib.parse import urlsplit
        from pathlib import Path
        name = f'port_{urlsplit(fixture["service"].base_url).port}.lock'
        paths = [fixture['service'].resource_directory / name,
                 Path(os.environ['DEMIFLOW_RESOURCE_HOME']) / socket.gethostname() / name]
        for path in paths:
            with path.open('a') as stream:
                if locked:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return [str(p) for p in paths]
    try:
        await_file(ready)
        guardian = json.loads(ready.read_text())
        marker.touch(); await_file(observed); await_file(failed)
        close = json.loads(failed.read_text())
        assert close['guardian_alive'] and 'unconfirmed' in close['error']
        # The worker has no SIGTERM handler. Original unit stop kills that
        # worker, while the separate guardian retains unit FD and owned tree.
        unknown = unit.stop('service', 'worker', terminate_deadline_ms=500, process=process)
        assert not unknown['confirmed']
        assert birth_start(guardian['guardian']) == guardian['birth']
        assert not unit_stopped(unit.directory('service', 'worker') / 'unit.lock', spawned['identity']['unit'])
        with pytest.raises(Exception, match='capacity stays charged'):
            pool.release('worker', unknown)
        assert pool.snapshot()['charged']['memory_bytes'] == 256*1024**2
        port_locks = assert_port_ownership(locked=True)
        marker.unlink()
        psutil.Process(guardian['guardian']).wait(timeout=10)
        assert_reaped(fixture, requests=0)
        evidence = unit.stop('service', 'worker', terminate_deadline_ms=1000, process=process)
        assert evidence['confirmed']
        assert_port_ownership(locked=False)
        pool.release('worker', evidence)
        assert not pool.snapshot()['active']
        (tmp_path / 'outer-unit-proof.json').write_text(json.dumps({'close': close, 'unknown': unknown,
            'final_exit': evidence, 'released': pool.snapshot(), 'original_port_locks': port_locks,
            'port_locks_held_after_worker_exit': True, 'port_locks_released_after_tree_exit': True}, indent=2))
    finally:
        marker.unlink(missing_ok=True)
        if process.poll() is None:
            unit.stop('service', 'worker', terminate_deadline_ms=2000, process=process)
