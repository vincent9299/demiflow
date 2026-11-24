"""Versioned shared-filesystem lease identity; optional two-physical-host proof."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.gc import collect_published_orphans
from demiflow.execution.storage.lease import (
    HEADER, NAME, ObjectLease, exclusive_access, held_exclusive, journal_identity,
    protocol_identity, record_protocol, shared_access,
)
from demiflow.execution.storage.objects import RunArtifactStore
from test_runtime_v3_artifact_accounting import consumed, store_fixture
from test_runtime_v3_artifact_recovery import _open, _store
from test_runtime_v3_real_seams import PreparationOnly


def _new(root, *, version=2):
    root.mkdir()
    lease = ObjectLease(root, create=True, exclusive=True, protocol_version=version)
    try:
        with held_exclusive(lease):
            record_protocol(root, lease.identity)
        return lease.identity
    finally:
        lease.close()


def test_shared_identity_is_original_journal_fact_and_gc_still_requires_exclusion(tmp_path):
    with store_fixture(tmp_path) as (run, store, port):
        identity = protocol_identity(store.root)
        original = run.journal.dataflow_gc_protocol()
        assert identity[0] == 2 and len(identity) == 5
        assert identity == journal_identity(original)
        assert identity[1] == os.statvfs(store.root).f_fsid
        assert identity[2] == store.root.stat().st_ino
        assert identity[3] == (store.root / NAME).stat().st_ino
        assert original[:3] == ((store.root / NAME).stat().st_dev, identity[3], 1)
        ref = store.put(iter([b'physical immutable data']), max_bytes=1024, media_type='application/octet-stream')
        assert b''.join(store.read(ref, max_bytes=ref.size_bytes)) == b'physical immutable data'
        assert collect_published_orphans(port._action, bounds=port._bounds)['blocked']
        store.close()
        result = collect_published_orphans(port._action, bounds=port._bounds)
        assert result == {'removed': 1, 'bytes': ref.size_bytes, 'blocked': None}
        assert consumed(run) == 0
        assert run.journal.dataflow_gc_protocol() == original


@pytest.mark.parametrize('mutation', ['lock_replace', 'symlink', 'copy', 'wrong_run', 'nonce', 'lock_nonce', 'unknown_version'])
def test_shared_identity_rejects_replacement_copy_and_wrong_domain(tmp_path, mutation):
    root = tmp_path / 'original'
    identity = _new(root)
    target = root
    if mutation == 'lock_replace':
        (root / NAME).unlink()
        (root / NAME).touch()
    elif mutation == 'symlink':
        original = tmp_path / 'other-lock'
        (root / NAME).rename(original)
        (root / NAME).symlink_to(original)
    elif mutation == 'copy':
        target = tmp_path / 'copy'
        shutil.copytree(root, target)
    elif mutation == 'wrong_run':
        target = tmp_path / 'other-run'
        _new(target)
    elif mutation == 'lock_nonce':
        (root / NAME).write_text('0' * 32)
    else:
        document = list(identity)
        if mutation == 'nonce':
            document[-1] = '0' * 32
        else:
            document[0] = 999
        (root / HEADER).write_text(json.dumps(document))
    with pytest.raises(DataflowError, match='FENCED|symlink'):
        with shared_access(target, identity, require_protocol=True):
            pytest.fail('a replaced/copied/different lease was admitted')


def test_live_original_fd_does_not_authorize_a_replacement_lock(tmp_path):
    root = tmp_path / 'root'
    identity = _new(root)
    lease = ObjectLease(root, expected=identity, exclusive=True)
    try:
        (root / NAME).unlink()
        (root / NAME).touch()
        with pytest.raises(DataflowError, match='inode changed'):
            lease.check()
        with pytest.raises(DataflowError, match='inode changed|original journal'):
            ObjectLease(root, expected=identity)
    finally:
        lease.close()


def test_v1_original_header_and_row_are_never_implicitly_upgraded(tmp_path):
    with pytest.raises(PreparationOnly):
        with _open(tmp_path / 'legacy-run') as run:
            root = run.directory / '.dataflow-store'
            identity = _new(root, version=1)
            with exclusive_access(root, identity):
                assert run.journal.bind_dataflow_gc_protocol(run.fence, identity, fresh=True)
            old_header = (root / HEADER).read_bytes()
            old_row = run.journal.dataflow_gc_protocol()
            assert old_row == (*identity, 1)
            store, port, _ = _store(run, ceiling=65536)
            try:
                ref = store.put(iter([b'original-v1']), max_bytes=128, media_type='application/octet-stream')
                assert b''.join(store.read(ref, max_bytes=ref.size_bytes)) == b'original-v1'
                assert protocol_identity(root) == identity
                assert (root / HEADER).read_bytes() == old_header
                assert run.journal.dataflow_gc_protocol() == old_row
                with pytest.raises(DataflowError, match='original'):
                    ObjectLease(root, expected=(2, os.statvfs(root).f_fsid,
                        root.stat().st_ino, identity[1], 'f' * 32))
            finally:
                store.close()
            raise PreparationOnly()


def test_readonly_old_database_without_new_column_keeps_v1_contract(tmp_path):
    import sqlite3
    from demiflow.execution.journal.run_journal import RunJournal
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE runtime_dataflow_gc_protocol '
                   '(singleton INTEGER, device INTEGER, inode INTEGER, eligible INTEGER)')
        db.execute('INSERT INTO runtime_dataflow_gc_protocol VALUES (1, 196, 99, 1)')
        assert RunJournal._dataflow_gc_protocol_row(db) == (196, 99, 1)


_REMOTE = r'''
import json,os,socket,sys
from pathlib import Path
from demiflow.execution.contracts.dataflow import ArtifactRef,Bounds
from demiflow.execution.storage.lease import ObjectLease,shared_access,NAME
from demiflow.execution.storage.objects import RunArtifactStore
cfg=json.loads(sys.stdin.readline());root=Path(cfg['root']);identity=tuple(cfg['identity'])
answer={'host':socket.gethostname(),'device':(root/NAME).stat().st_dev,
        'inode':(root/NAME).stat().st_ino,'fsid':os.statvfs(root).f_fsid}
if cfg['mode']=='blocked':
    try:
        with shared_access(root,identity,require_protocol=True):
            answer['blocked']=False
    except Exception as exc:
        answer.update(blocked=True,error=type(exc).__name__+': '+str(exc))
    print(json.dumps(answer),flush=True)
elif cfg['mode']=='hold':
    lease=ObjectLease(root,expected=identity)
    try:
        answer['held']=True;print(json.dumps(answer),flush=True)
        sys.stdin.readline()
    finally:
        lease.close()
else:
    ref=ArtifactRef(**cfg['ref'])
    store=RunArtifactStore(root=root,store_id=ref.store_id,journal=None,
        bounds=Bounds(**cfg['bounds']),readonly=True)
    try:answer['data']=b''.join(store.read(ref,max_bytes=ref.size_bytes)).decode()
    finally:store.close()
    print(json.dumps(answer),flush=True)
'''


def _remote_command():
    root = str(Path(__file__).resolve().parents[1])
    invocation = ['env', 'PYTHONPATH=' + root + ':' + root + '/tests',
                  sys.executable, '-B', '-u', '-c', _REMOTE]
    return ['ssh', '-F', '/yzp/zhaozy/yangzepeng/0905/.devkeys/config',
            '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'gpu32520', shlex.join(invocation)]


def test_two_physical_hosts_share_original_lease_read_and_gc_exclusion(tmp_path):
    directory = os.environ.get('DEMIFLOW_CROSS_HOST_LEASE_ROOT')
    if not directory:
        pytest.skip('explicit authorized shared scratch directory required')
    scratch = Path(tempfile.mkdtemp(prefix='demiflow-shared-lease-', dir=directory))
    observations = []
    # Every path below was created by this test. Never read or clean old runs.
    with store_fixture(scratch) as (run, store, port):
        ref = store.put(iter([b'actual-two-host-artifact']), max_bytes=1024,
                        media_type='application/octet-stream')
        identity = journal_identity(run.journal.dataflow_gc_protocol())
        cfg = {'root': str(store.root), 'identity': identity, 'ref': asdict(ref),
               'bounds': asdict(port._bounds), 'mode': 'read'}
        def remote(mode):
            answer = subprocess.run(_remote_command(), input=json.dumps({**cfg, 'mode': mode})+'\n',
                text=True, capture_output=True, timeout=30)
            assert answer.returncode == 0, answer.stderr
            result = json.loads(answer.stdout)
            observations.append(result)
            return result
        read = remote('read')
        assert read['data'] == 'actual-two-host-artifact'
        assert read['host'] != socket.gethostname()
        assert read['device'] != (store.root / NAME).stat().st_dev
        assert (read['fsid'], read['inode']) == (identity[1], identity[3])
        store.close()
        with exclusive_access(store.root, identity):
            blocked = remote('blocked')
            assert blocked['blocked'] and 'reconciled' in blocked['error']
        process = subprocess.Popen(_remote_command(), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            process.stdin.write(json.dumps({**cfg, 'mode': 'hold'})+'\n');process.stdin.flush()
            import select
            assert select.select([process.stdout], [], [], 30)[0], 'remote shared lease did not open'
            held = json.loads(process.stdout.readline());observations.append(held)
            assert held['held']
            before = consumed(run)
            result = collect_published_orphans(port._action, bounds=port._bounds)
            assert result['blocked'] and result['removed'] == 0 and consumed(run) == before
        finally:
            if process.poll() is None:
                process.stdin.write('release\n');process.stdin.flush()
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0, stderr
        assert collect_published_orphans(port._action, bounds=port._bounds) == {
            'removed': 1, 'bytes': ref.size_bytes, 'blocked': None}
        assert consumed(run) == 0
    legacy_root = scratch / 'original-v1-protocol'
    legacy_identity = _new(legacy_root, version=1)
    legacy_header = (legacy_root / HEADER).read_bytes()
    cfg.update(root=str(legacy_root), identity=legacy_identity)
    legacy_remote = remote('blocked')
    assert legacy_remote['blocked'] and 'original journal' in legacy_remote['error']
    assert (legacy_root / HEADER).read_bytes() == legacy_header
    assert protocol_identity(legacy_root) == legacy_identity
    (tmp_path / 'two-host-observations.json').write_text(json.dumps({
        'created_scratch': str(scratch), 'local_host': socket.gethostname(),
        'identity': identity, 'remote': observations}, indent=2))
