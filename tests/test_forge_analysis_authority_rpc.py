"""Ordinary local Unix exchanges; no production namespace/controller SLA proof."""
from contextlib import contextmanager
import os
import socket
import time
from types import SimpleNamespace

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis.authority_rpc import Exchange
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Peer
from demiflow.forge.schema import ForgeError


@contextmanager
def exchange(tmp_path):
    path=tmp_path/"a.sock"
    listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    listener.bind(str(path));listener.listen(2);listener.settimeout(1)
    authority=SimpleNamespace(peer=Peer(os.getpid(),os.geteuid(),os.getegid(),process_identity(os.getpid())),
        project="w"+"a"*32,principal="owner",deployment=SimpleNamespace(value=dict(site=dict(web_auth_socket_path=str(path)))))
    rpc=Exchange(authority,"export-source",dict(export_id="i"+"a"*32))
    remote=None
    try:
        # No accepted empty request may consume a Web worker before the
        # selector sends its complete bounded control frame.
        listener.setblocking(False)
        with pytest.raises(BlockingIOError):listener.accept()
        assert not rpc.advance() and not rpc.channel.outgoing
        listener.setblocking(True);listener.settimeout(1)
        remote,_=listener.accept();remote.settimeout(1)
        frames=Frames(remote);request=frames.json()
        yield rpc,frames,request
    finally:
        rpc.close()
        if remote is not None:remote.close()
        listener.close()
        path.unlink()


def finish(rpc):
    for _ in range(4):
        if rpc.advance():return rpc.value
    pytest.fail("complete local response did not advance")


def test_slow_source_reply_keeps_each_poll_nonblocking_and_can_be_cancelled(tmp_path):
    with exchange(tmp_path) as (rpc,remote,request):
        assert request["operation"]=="export-source"
        start=time.monotonic()
        for _ in range(100):assert rpc.advance() is False
        assert time.monotonic()-start<1
        rpc.close()
        with pytest.raises(ForgeError,match="closed"):rpc.advance()
        # No reconnect/replay after cancellation, even with a late reply.
        assert remote.connection.recv(1)==b""


def test_exact_challenge_response_and_explicit_deadline(tmp_path):
    with exchange(tmp_path) as (rpc,remote,request):
        remote.send("json",dict(challenge=request["challenge"],value=dict(exported=True),error=None,code=None))
        assert finish(rpc)==dict(exported=True) and rpc.channel.closed
    with exchange(tmp_path) as (rpc,remote,request):
        rpc.deadline=time.monotonic()-1
        with pytest.raises(ForgeError,match="timed out"):rpc.advance()
        assert rpc.channel.closed


def test_wrong_request_and_remote_denial_close_without_retry(tmp_path):
    with exchange(tmp_path) as (rpc,remote,request):
        remote.send("json",dict(challenge="wrong",value=True,error=None,code=None))
        with pytest.raises(ForgeError,match="another process/request"):finish(rpc)
    with exchange(tmp_path) as (rpc,remote,request):
        remote.send("json",dict(challenge=request["challenge"],value=None,error="revoked",code="operation_rejected"))
        with pytest.raises(ForgeError,match="revoked"):finish(rpc)
        assert rpc.channel.closed
