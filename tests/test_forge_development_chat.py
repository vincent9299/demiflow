"""Development invariants with small resource budgets and protocol fixtures."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.service import Service
from demiflow.workbench.conversations.store import Store
from demiflow.forge.bridge.transport import decode
from demiflow.forge.schema import ForgeError


def configuration(tmp_path):
    root=tmp_path/'project';root.mkdir()
    (root/'AGENTS.md').write_text('Preserve notebook outputs. Use existing tests.')
    (root/'code.py').write_text('# initial dirty bytes\n')
    state=tmp_path/'state'
    fixture=Path(__file__).with_name('devchat_fake_provider.py')
    executable=tmp_path/'fake-codex'
    executable.write_text('#!'+sys.executable+'\nimport runpy\nrunpy.run_path('+repr(str(fixture))+',run_name="__main__")\n')
    executable.chmod(0o700)
    config=dict(format='demiflow_development_deployment_v1',mode='trusted_local',state_root=str(state),
        projects={'wfixture':dict(root=str(root),kind='platform',phase='implementation',writes=['code.py'],watch=['code.py'],sources=['AGENTS.md'],
            non_goals=['production'],authorization='Explicit fixture implementation')},
        providers={'codex':dict(executable=str(executable),version='0.164.0',model='fixture',effort='xhigh')},
        host_budget=dict(memory_bytes=8*1024**3,run_memory_bytes=0,other_memory_bytes=1024**3,
                         disk_bytes=4*1024**3,run_disk_bytes=0,other_disk_bytes=256*1024**2,device_path=str(tmp_path)),
        limits=dict(turn_seconds=20,input_seconds=10,output_bytes=256*1024,events_per_turn=100))
    path=tmp_path/'deployment.json';path.write_text(json.dumps(config))
    return root,path,config


def wait_turn(service, tid, terminal=True):
    until=time.monotonic()+15
    while time.monotonic()<until:
        value=service.store.turn(tid)
        if (value['cleanup']=='complete') if terminal else value['state']=='waiting_input': return value
        time.sleep(.05)
    raise AssertionError(service.store.turn(tid))


def test_defaults_off_and_partition_scope(tmp_path, monkeypatch):
    monkeypatch.setattr('demiflow.workbench.conversations.policy.execution_facts',lambda _:dict(
        available=False,filesystem_enforcement='unverified',reason='fixture unavailable'))
    root,path,value=configuration(tmp_path)
    value['mode']='restricted';deployment=Deployment(value)
    assert not deployment.capabilities('wfixture',root)['start']
    assert deployment.capabilities('wfixture',root)['filesystem_enforcement']=='unverified'
    value['host_budget']['memory_bytes']=1
    with pytest.raises(ForgeError,match='partition'): Deployment({**value,'projects':{'wfixture':{k:v for k,v in value['projects']['wfixture'].items() if k!='identity'}}})


def test_service_turn_idempotency_history_and_resume(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    try:
        session=service.create('wfixture',root,'create-1','codex')
        sid=session['id']
        assert service.create('wfixture',root,'create-1','codex')['id']==sid
        assert not service.workers
        turn=service.start('wfixture',root,sid,'send-1',0,'A small normal task')
        duplicate=service.start('wfixture',root,sid,'send-1',0,'A small normal task')
        assert duplicate['id']==turn['id']
        with pytest.raises(ForgeError,match='different text'):
            service.start('wfixture',root,sid,'send-1',0,'different')
        done=wait_turn(service,turn['id']);assert done['state']=='completed',done
        assert done['delivery']=='acknowledged' and done['dispatch']=='acknowledged'
        page=service.store.events('wfixture',sid)
        assert [e['sequence'] for e in page['events']]==list(range(1,len(page['events'])+1))
        assert any(e['type']=='message.completed' for e in page['events'])
        assert not service.store.events('wfixture',sid,page['next_sequence'])['events']
        next_turn=service.start('wfixture',root,sid,'send-2',1,'Continue')
        assert wait_turn(service,next_turn['id'])['state']=='completed'
        assert (root/'code.py').read_text()=='# initial dirty bytes\n'
    finally: service.close()


def test_input_identity_expiry_and_single_answer(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    try:
        sid=service.create('wfixture',root,'c','codex')['id']
        tid=service.start('wfixture',root,sid,'s',0,'input')['id']
        wait_turn(service,tid,False)
        pending=service.store.pending(tid)[0]
        with pytest.raises(ForgeError): service.input('wfixture',root,sid,tid,pending['id'],'wrong',{'q1':'yes'})
        with pytest.raises(ForgeError): service.input('wfixture',root,sid,tid,pending['id'],pending['digest'],{})
        service.input('wfixture',root,sid,tid,pending['id'],pending['digest'],{'q1':'yes'})
        with pytest.raises(ForgeError): service.input('wfixture',root,sid,tid,pending['id'],pending['digest'],{'q1':'no'})
        assert wait_turn(service,tid)['state']=='completed'
    finally: service.close()


def test_cancel_detached_child_does_not_kill_sentinel(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    sentinel=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
    try:
        sid=service.create('wfixture',root,'c','codex')['id']
        tid=service.start('wfixture',root,sid,'s',0,'hang')['id']
        until=time.monotonic()+10
        while service.store.turn(tid)['state']!='running' and time.monotonic()<until: time.sleep(.05)
        time.sleep(.2)
        process=json.loads(service.store.turn(tid)['process'])['supervisor']['pid']
        from demiflow.resources.process_observation import descendants
        children=set(descendants(process));assert len(children)>=2
        stop=service.cancel('wfixture',root,sid,tid,'stop')
        assert service.cancel('wfixture',root,sid,tid,'stop-again')['stop_at']==stop['stop_at']
        result=wait_turn(service,tid)
        assert result['state']=='cancelled' and result['cleanup']=='complete'
        assert sentinel.poll() is None
        assert not any(Path('/proc',str(p)).exists() for p in children)
    finally:
        service.close();sentinel.terminate();sentinel.wait()


def test_flood_and_connection_loss_leave_truthful_results(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    try:
        sid=service.create('wfixture',root,'c','codex')['id']
        tid=service.start('wfixture',root,sid,'s',0,'flood')['id']
        result=wait_turn(service,tid)
        assert result['state']=='failed',result
        assert result['bytes']<400*1024
        tid=service.start('wfixture',root,sid,'s2',1,'disconnect')['id']
        result=wait_turn(service,tid);assert result['state']=='interrupted',result
        assert result['dispatch']=='acknowledged'
    finally: service.close()


def test_json_depth_duplicate_and_capacity(tmp_path):
    for raw in [b'{"x":1,"x":2}',b'{"x":'+b'['*65+b'0'+b']'*65+b'}']:
        with pytest.raises(ValueError): decode(raw)
    limits=Limits(sessions=1)
    store=Store(tmp_path/'private',limits,initialize=True)
    root=tmp_path/'root';root.mkdir()
    store.create('w',root_identity(root),'codex','c','policy')
    with pytest.raises(ForgeError,match='count'):store.create('w',root_identity(root),'codex','c2','policy')


def test_source_bytes_notebook_symlink_and_context_overflow(tmp_path):
    from demiflow.workbench.conversations.common import snapshot
    from demiflow.forge.bridge.agent_context import assemble
    root,path,value=configuration(tmp_path)
    notebook=root/'large.ipynb';notebook.write_text(json.dumps({'cells':[{'outputs':[{'text':'x'*200000}]}]}))
    before=notebook.read_bytes();first=snapshot(root,['large.ipynb'])
    assert first['large.ipynb']['bytes']==len(before) and notebook.read_bytes()==before
    with pytest.raises(ForgeError,match='byte budget'):snapshot(root,['large.ipynb'],max_bytes=1024)
    (root/'link').symlink_to(notebook)
    with pytest.raises(ForgeError,match='links'):snapshot(root,['link'])
    with pytest.raises(ForgeError,match='context'):
        assemble(value['projects']['wfixture'],'requirement',Limits(context_bytes=10))


def test_restart_unknown_and_root_replacement_preserve_lease(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    sid=service.create('wfixture',root,'c','codex')['id']
    pending,_=service.store.submit('wfixture',sid,'s',0,'not dispatched',root_identity(root),service.policy_hash)
    # Old records had no durable pre-spawn stage. Missing process metadata must
    # still fail closed; new admitted-stage recovery is tested separately.
    service.store.update(pending['id'], launch_phase='unknown')
    service.close();service=Service(path)
    try:
        recovered=service.recover('wfixture',root,sid,1)
        assert recovered['recovery']['status']=='ownership_unknown'
        assert recovered['turn']['dispatch']=='not_sent'
        with pytest.raises(ForgeError,match='writer'):
            service.start('wfixture',root,sid,'s2',1,'continue')
        moved=root.with_name('old');root.rename(moved);root.mkdir()
        with pytest.raises(ForgeError,match='identity'):
            service.create('wfixture',root,'c2','codex')
    finally:service.close()


def test_shared_json_primitive_preserves_row_client_budget_exception():
    from demiflow.operator_llm.codex_agent import _encode, _strict_json
    from demiflow.agent_transport import encode, strict_json
    from demiflow.operator_llm.errors import PromptBudgetExceededError
    payload={'text':'\\"中文','items':[1,None,True]}
    assert _encode(payload,1024)==encode(payload,1024)
    assert _strict_json(encode(payload,1024))==strict_json(encode(payload,1024))
    with pytest.raises(PromptBudgetExceededError):_encode(payload,1)
    with pytest.raises(ValueError):encode(payload,1)


def test_overlap_and_two_project_total_capacity(tmp_path):
    root,path,value=configuration(tmp_path)
    other=tmp_path/'other';other.mkdir();(other/'AGENTS.md').write_text('other');(other/'code.py').write_text('# other')
    value['projects']['other']=dict(value['projects']['wfixture'],root=str(other))
    value['projects']['alias']=dict(value['projects']['wfixture'])
    path.write_text(json.dumps(value));service=Service(path)
    try:
        a=service.create('wfixture',root,'a','codex')['id'];b=service.create('other',other,'b','codex')['id'];alias=service.create('alias',root,'c','codex')['id']
        t=service.start('wfixture',root,a,'s',0,'hang')
        with pytest.raises(ForgeError,match='writer'):service.start('alias',root,alias,'s',0,'normal')
        other_turn=service.start('other',other,b,'s',0,'hang')
        assert len(service.workers)==2
        service.cancel('wfixture',root,a,t['id'],'stop')
        assert wait_turn(service,t['id'])['cleanup']=='complete'
        assert service.store.turn(other_turn['id'])['cleanup']!='complete'
        service.cancel('other',other,b,other_turn['id'],'stop')
        assert wait_turn(service,other_turn['id'])['cleanup']=='complete'
    finally:service.close()


def test_disk_admission_preserves_source_and_cleanup_reservation(tmp_path,monkeypatch):
    from types import SimpleNamespace
    root,path,_=configuration(tmp_path);service=Service(path)
    try:
        sid=service.create('wfixture',root,'c','codex')['id']
        monkeypatch.setattr(os,'statvfs',lambda _:SimpleNamespace(f_bavail=0,f_frsize=4096))
        with pytest.raises(ForgeError,match='free space'):service.start('wfixture',root,sid,'s',0,'normal')
        assert not service.workers and service.store.show('wfixture',sid)['turn'] is None
        assert (service.root/'control.reserve').stat().st_size==service.deployment.limits.control_reserve
        assert (root/'code.py').read_text()=='# initial dirty bytes\n'
    finally:service.close()


def test_bounded_diff_and_wide_file_hash_preservation(tmp_path):
    from demiflow.forge.source_snapshot import capture
    from demiflow.forge.source_snapshot import differences
    root=tmp_path/'project';root.mkdir()
    (root/'source.py').write_text('value = 1\n')
    (root/'wide.json').write_text('x'*20000)
    before=capture(root,['source.py','wide.json'])
    assert before['wide.json']['preview_unavailable']=='preview_byte_budget'
    (root/'source.py').write_text('value = 2\n');(root/'wide.json').write_text('y'*20000)
    diff=list(differences(before,capture(root,['source.py','wide.json']),['source.py']))
    assert '+value = 2' in diff[0]['diff']
    assert not diff[1]['within_scope'] and diff[1]['diff_unavailable']
    assert diff[1]['before']['sha256']!=diff[1]['after']['sha256']


def test_parent_exit_drains_only_owned_development_unit(tmp_path):
    root,path,_=configuration(tmp_path)
    marker=tmp_path/'started.json'
    script=tmp_path/'service_parent.py'
    script.write_text('''import json,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[3])
from demiflow.workbench.conversations.service import Service
service=Service(sys.argv[1]);root=Path(service.deployment.projects['wfixture']['root'])
sid=service.create('wfixture',root,'c','codex')['id']
tid=service.start('wfixture',root,sid,'s',0,'hang')['id']
while service.store.turn(tid)['state']!='running':time.sleep(.05)
time.sleep(.2)
Path(sys.argv[2]).write_text(json.dumps({'sid':sid,'tid':tid}))
time.sleep(60)
''')
    parent=subprocess.Popen([sys.executable,'-B',str(script),str(path),str(marker),str(Path(__file__).resolve().parents[1])])
    sentinel=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
    try:
        deadline=time.monotonic()+10
        while not marker.exists() and time.monotonic()<deadline:time.sleep(.05)
        ids=json.loads(marker.read_text());store=Store(tmp_path/'state',Limits())
        identity=json.loads(store.turn(ids['tid'])['process'])['supervisor']
        from demiflow.resources.process_observation import descendants
        children=set(descendants(identity['pid']))
        parent.kill();parent.wait()
        deadline=time.monotonic()+10
        while store.turn(ids['tid'])['cleanup']!='complete' and time.monotonic()<deadline:time.sleep(.05)
        result=store.turn(ids['tid']);assert result['cleanup']=='complete',result
        assert sentinel.poll() is None
        assert not any(Path('/proc',str(pid)).exists() for pid in children)
    finally:
        if parent.poll() is None:parent.kill();parent.wait()
        sentinel.terminate();sentinel.wait()


def test_acp_close_failure_still_drains_detached_descendants(tmp_path):
    root, path, value = configuration(tmp_path)
    fixture = Path(__file__).with_name('devchat_fake_acp.py')
    executable = tmp_path / 'fake-acp'
    executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
    executable.chmod(0o700)
    value['providers'] = {'deepseek': dict(executable=str(executable), version='0.2.1-rc.2', model='fixture')}
    path.write_text(json.dumps(value))
    service = Service(path)
    original = service.capabilities
    def fixture_capabilities(*args):
        caps = original(*args)
        caps['providers'][0]['available'] = True
        return caps
    service.capabilities = fixture_capabilities
    sentinel = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'])
    try:
        sid = service.create('wfixture', root, 'acp', 'deepseek')['id']
        tid = service.start('wfixture', root, sid, 'close-failure', 0, 'Complete then reject close')['id']
        done = wait_turn(service, tid)
        assert done['cleanup'] == 'complete', done
        assert done['state'] == 'completed', done
        assert sentinel.poll() is None
        assert service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
    finally:
        service.close()
        sentinel.terminate(); sentinel.wait(timeout=5)


def test_persistent_metadata_and_input_answers_are_charged(tmp_path):
    root, path, value = configuration(tmp_path)
    service = Service(path)
    try:
        sid = service.create('wfixture', root, 'metadata', 'codex')['id']
        tid = service.start('wfixture', root, sid, 'first', 0, 'input')['id']
        wait_turn(service, tid, False)
        pending = service.store.pending(tid)[0]
        service.input('wfixture', root, sid, tid, pending['id'], pending['digest'], {'q1': 'a' * 8192})
        wait_turn(service, tid)
        store = service.store
        with store.connect() as db:
            t = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            p = dict(db.execute('SELECT * FROM pending WHERE turn=?', (tid,)).fetchone())
            expected = sum(len((t[k] or '').encode()) for k in ('text', 'context', 'baseline', 'process', 'result'))
            expected += len(p['request'].encode()) + len(p['answer'].encode())
        assert store.session(sid)['metadata_bytes'] == expected
        store.update(tid, context='x' * 800000)
        assert store.session(sid)['metadata_bytes'] == expected - len(t['context'].encode()) + 800000
        # Event bodies alone fit; retained metadata plus the next full turn
        # reservation must fail admission without removing existing history.
        from dataclasses import replace
        reserve = store.turn(tid)['reserve']
        store.limits = replace(store.limits, session_bytes=reserve + store.session(sid)['bytes'] + 1000)
        with pytest.raises(ForgeError, match='history byte'):
            store.submit('wfixture', sid, 'second', 1, 'next', root_identity(root), service.policy_hash)
        assert store.turn(tid)['context'] == 'x' * 800000
    finally:
        service.close()


def test_sqlite_full_uses_reserve_and_keeps_committed_history(tmp_path):
    limits = Limits(database_bytes=128 * 1024, control_reserve=4096)
    store = Store(tmp_path / 'private-full', limits, initialize=True)
    root = tmp_path / 'root-full'; root.mkdir()
    sid = store.create('w', root_identity(root), 'codex', 'c', 'p')['id']
    turn, _ = store.submit('w', sid, 's', 0, 'retained requirement', root_identity(root), 'p')
    with pytest.raises(ForgeError, match='database unavailable'):
        store.update(turn['id'], context='x' * 800000)
    assert (store.root / 'storage-pressure').exists()
    assert not (store.root / 'control.reserve').exists()
    assert store.turn(turn['id'])['text'] == 'retained requirement'
    assert store.turn(turn['id'])['context'] is None
    assert store.cancel('w', sid, turn['id'])['state'] == 'stopping'


def test_close_uses_liveness_pipe_when_cancel_storage_fails(tmp_path, monkeypatch):
    root, path, value = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'close-storage', 'codex')['id']
    tid = service.start('wfixture', root, sid, 'first', 0, 'hang')['id']
    until = time.monotonic() + 10
    while service.store.turn(tid)['state'] != 'running' and time.monotonic() < until:
        time.sleep(.05)
    monkeypatch.setattr(service.store, 'cancel', lambda *a: (_ for _ in ()).throw(OSError('injected control write failure')))
    service.close()
    done = service.store.turn(tid)
    assert done['state'] == 'cancelled' and done['cleanup'] == 'complete', done


def test_close_releases_service_lock_after_cleanup_read_failure(tmp_path, monkeypatch):
    root, path, value = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'close-read', 'codex')['id']
    tid = service.start('wfixture', root, sid, 'first', 0, 'hang')['id']
    until = time.monotonic() + 10
    while service.store.turn(tid)['state'] != 'running' and time.monotonic() < until:
        time.sleep(.05)
    monkeypatch.setattr(service.store, 'turn', lambda *a: (_ for _ in ()).throw(OSError('injected control read failure')))
    with pytest.raises(ForgeError, match='final cleanup state'):
        service.close()
    # A new owner can read the worker's independently committed drain receipt.
    replacement = Service(path)
    try:
        assert replacement.store.turn(tid)['cleanup'] == 'complete'
        assert replacement.recover('wfixture', root, sid, 1)['recovery']['can_continue']
    finally:
        replacement.close()


def test_private_disk_counts_allocated_blocks_and_policy_history_is_bounded(tmp_path):
    from demiflow.workbench.conversations.common import directory_bytes
    private = tmp_path / 'private-blocks'; private.mkdir()
    for i in range(10): (private / str(i)).write_bytes(b'x')
    expected = sum(max(p.stat().st_size, p.stat().st_blocks * 512) for p in [private, *private.iterdir()])
    assert directory_bytes(private) == expected and expected > 10
    root, path, value = configuration(tmp_path)
    state = Path(value['state_root']); state.mkdir(mode=0o700)
    for i in range(32): (state / (str(i) + '.json')).write_text('{}')
    with pytest.raises(ForgeError, match='policy history is full'):
        Service(path)
    # Failed initialization must release its service lock too.
    import fcntl
    fd = os.open(state / 'service.lock', os.O_RDWR)
    try: fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally: os.close(fd)


@pytest.mark.parametrize('removed', ['root', 'directory', 'file'])
def test_private_state_sampling_tolerates_owned_cleanup_without_losing_retained_bytes(tmp_path, monkeypatch, removed):
    """Review profile cleanup can overlap the supervisor's disk observation."""
    from contextlib import contextmanager
    import shutil
    from demiflow.workbench.conversations import common
    home = tmp_path / 'profile'; home.mkdir()
    retained = home / 'retained'; retained.write_bytes(b'x' * 65536)
    disappearing = home / 'storages'; disappearing.mkdir()
    (disappearing / 'entry').write_bytes(b'ephemeral')
    original = common.os.scandir
    if removed == 'root':
        shutil.rmtree(home)
        assert common.directory_bytes(home) == 0
        return

    @contextmanager
    def scan(path):
        with original(path) as rows:
            def entries():
                for row in rows:
                    if removed == 'file' and Path(row.path) == disappearing / 'entry':
                        Path(row.path).unlink()
                    yield row
            yield entries()
    # Preserve the real call's eager error behavior for a vanished directory.
    def eager_scan(path):
        if removed == 'directory' and Path(path) == disappearing:
            (disappearing / 'entry').unlink()
            disappearing.rmdir()
            return original(path)
        return scan(path)
    monkeypatch.setattr(common.os, 'scandir', eager_scan)
    assert common.directory_bytes(home) >= retained.stat().st_size
    assert retained.read_bytes() == b'x' * 65536


def test_private_directory_bytes_respects_caller_deadline_past_two_seconds(tmp_path, monkeypatch):
    from demiflow.workbench.conversations import common
    home = tmp_path / 'session-a'
    home.mkdir()
    (home / 'f1').write_bytes(b'abc')
    (home / 'f2').write_bytes(b'def')
    # Simulate a slow cold filesystem scan that takes 3.5s (> old 2s clamp, < 10s budget).
    ticks = iter([100.0, 100.0, 103.5, 103.5, 103.5, 103.5, 103.5, 103.5])
    monkeypatch.setattr(common.time, 'monotonic', lambda: next(ticks, 103.5))
    size = common.directory_bytes(home, deadline=115.0)
    assert size >= 6
