"""Real native reads + a deterministic real-DirEntry deletion interleaving.

The deletion hook controls timing, not worker data, PID, source or replies.
The separate Unix callback fault is injected after real peer/registry checks;
it proves error transport semantics, not a live production media replay.
"""
from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.projects import registry as console
from demiflow.workbench import reader as workspace_reader
from demiflow.workbench.reader import Reader
from demiflow.workbench.analysis.authority import Authority
from demiflow.workbench.analysis.ipc import Listener
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.web_service import WebService
from demiflow.forge.schema import ForgeError


@pytest.fixture
def fixed_table(tmp_path):
    root=tmp_path/'fixed.lance'
    lance.write_dataset(pa.table({'value':[1,2,3,4]}),str(root))
    return root


def read_request(root):
    return dict(operation='describe',root=str(root),arguments=dict(version=1))


def response_rows(value):
    assert value['ok'],value
    return value['value']['row_count']


def delete_listed_fixture_entries(monkeypatch,root,removed):
    original=os.scandir
    class Entries:
        def __init__(self,iterator):self.iterator=iterator
        def __enter__(self):self.iterator.__enter__();return self
        def __exit__(self,*args):return self.iterator.__exit__(*args)
        def close(self):return self.iterator.close()
        def __iter__(self):return self
        def __next__(self):
            entry=next(self.iterator)
            if entry.name.startswith('.tmp-native-fixture-'):
                Path(entry.path).unlink()
                removed.append(entry.name)
            return entry
    def scan(path):
        iterator=original(path)
        if isinstance(path,(str,bytes,os.PathLike)) and os.fsdecode(path)==str(root):
            return Entries(iterator)
        return iterator
    monkeypatch.setattr(workspace_reader.os,'scandir',scan)


def test_disappearing_native_temp_entry_keeps_same_read(fixed_table,monkeypatch):
    reader=Reader()
    before=len(os.listdir('/proc/self/fd'))
    try:
        assert response_rows(reader.call(read_request(fixed_table)))==4
        original=reader.identity
        scratch=Path(reader.scratch.name)
        (scratch/'.tmp-native-fixture-same-shape').write_bytes(b'temporary')
        removed=[]
        with monkeypatch.context() as patch:
            delete_listed_fixture_entries(patch,scratch,removed)
            assert response_rows(reader.call(read_request(fixed_table)))==4
        assert removed==['.tmp-native-fixture-same-shape']
        assert reader.identity==original and process_identity(original['pid'])==original
        assert response_rows(reader.call(read_request(fixed_table)))==4
        assert reader.identity==original
    finally:
        reader.close()
    assert process_identity(original['pid'])!=original
    assert len(os.listdir('/proc/self/fd'))<=before+1


def test_disappearing_entries_still_charge_entry_budget(fixed_table,monkeypatch):
    reader=Reader()
    try:
        assert response_rows(reader.call(read_request(fixed_table)))==4
        original=reader.identity;scratch=Path(reader.scratch.name)
        for index in range(257):(scratch/f'.tmp-native-fixture-{index:03}').touch()
        removed=[]
        with monkeypatch.context() as patch:
            delete_listed_fixture_entries(patch,scratch,removed)
            with pytest.raises(ForgeError) as exc:reader.call(read_request(fixed_table))
        assert exc.value.code=='resource_limit' and reader.process is None
        assert removed and process_identity(original['pid'])!=original
        assert response_rows(reader.call(read_request(fixed_table)))==4
    finally:reader.close()


def test_missing_scratch_root_is_not_ignored(fixed_table):
    reader=Reader()
    try:
        assert response_rows(reader.call(read_request(fixed_table)))==4
        original=reader.identity
        shutil.rmtree(reader.scratch.name)
        with pytest.raises(FileNotFoundError):reader.call(read_request(fixed_table))
        assert reader.process is None and process_identity(original['pid'])!=original
        assert response_rows(reader.call(read_request(fixed_table)))==4
        assert reader.identity!=original
    finally:reader.close()


@contextmanager
def callback(tmp_path):
    key='w'+'d'*32
    project=tmp_path/'project';project.mkdir()
    (project/'workspace.json').write_text(json.dumps(dict(id=key)))
    flags={name:True for name in console.PERMISSIONS|console.CAPABILITIES}
    inventory=dict(projects={key:dict(root=str(project),permissions=flags)})
    service=WebService.__new__(WebService)
    service.identity=process_identity(os.getpid());service.principal='private-test-owner'
    service.config=dict(helper_uid=os.geteuid(),helper_identity=service.identity)
    service.server=SimpleNamespace(inventory=lambda:inventory,
        effective_permissions=lambda item:item['permissions'],native_reader=None)
    service.closed=threading.Event();service.slots=threading.BoundedSemaphore(1)
    service.sources=threading.BoundedSemaphore(1);service.authority_controls=threading.BoundedSemaphore(1)
    # AF_UNIX has a fixed pathname limit; batch evidence paths are intentionally
    # descriptive and can exceed it. Own a short private socket directory.
    with tempfile.TemporaryDirectory(prefix='demiflow-auth-',dir='/tmp') as socket_root:
        service.listener=Listener(Path(socket_root)/'auth.sock',public=True)
        service.thread=threading.Thread(target=service._serve_authority,daemon=True)
        service.thread.start()
        authority=Authority.__new__(Authority);authority.project=key;authority.principal=service.principal
        authority.peer=Peer(os.getpid(),os.geteuid(),os.getegid(),service.identity)
        authority.deployment=SimpleNamespace(value=dict(site=dict(web_auth_socket_path=str(service.listener.path))))
        try:yield service,authority,key
        finally:
            service.closed.set();service.listener.close();service.thread.join(2)
            assert not service.thread.is_alive()


def drive(exchange):
    deadline=time.monotonic()+2
    while time.monotonic()<deadline:
        if exchange.advance():return exchange.value
        time.sleep(.001)
    pytest.fail('Local authority response did not complete within private bound')


def test_callback_io_error_is_structured_after_real_peer_and_registry_checks(tmp_path,caplog):
    with callback(tmp_path) as (service,authority,key):
        actual=service._authority;calls=[]
        def source_io_fault(request):
            actual(request)  # real original peer/principal + current registry check
            calls.append(request['challenge'])
            raise FileNotFoundError(errno.ENOENT,'private disappearing source entry','/private-owned/.tmp-native')
        service._authority=source_io_fault
        exchange=authority.begin('project-context',{})
        with pytest.raises(ForgeError) as exc:drive(exchange)
        assert exc.value.code=='operation_failed'
        assert 'I/O' in str(exc.value) and 'unconfirmed' in str(exc.value)
        assert '/private-owned/' not in str(exc.value)
        assert exchange.channel.closed and calls==[exchange.challenge]
        assert service.authority_controls.acquire(timeout=1);service.authority_controls.release()
        assert 'operation I/O failed' in caplog.text
        # No reconnect/replay for failed exchange. This is a separate explicit request.
        service._authority=actual
        assert authority.check(key,authority.principal,'analysis_execute') is True
        with pytest.raises(ForgeError,match='closed'):exchange.advance()
        assert calls==[exchange.challenge]


def test_callback_denial_preserves_original_error_code(tmp_path):
    with callback(tmp_path) as (service,authority,key):
        authority.principal='another-owner'
        exchange=authority.begin('project-context',{})
        with pytest.raises(ForgeError) as exc:drive(exchange)
        assert exc.value.code=='operation_rejected' and 'principal' in str(exc.value)
        assert exchange.channel.closed


