"""Real selector/socket readiness; no RPC parsing or runtime substitution."""
import selectors
import socket
from types import SimpleNamespace

import pytest

from demiflow.workbench.analysis.controller import Controller
from demiflow.workbench.analysis.ipc import Channel
from demiflow.forge.schema import ForgeError


def control():
    value=Controller.__new__(Controller);value.poll=selectors.DefaultSelector()
    value.connections={};value.sessions={}
    return value


def test_pending_authority_reply_wakes_original_selector_without_consuming_bytes():
    value=control();local,remote=socket.socketpair();channel=Channel(local)
    exchange=SimpleNamespace(channel=channel,connected=True,done=False)
    value.connections[1]=dict(metadata_task=SimpleNamespace(pending=('original',exchange)))
    try:
        remote.sendall(b'x')
        events=value._select()
        assert len(events)==1 and events[0][0].data==('authority-wake',local.fileno())
        assert local.recv(1)==b'x' # Original RPC remains the sole parser.
        assert len(value.poll.get_map())==0 and not channel.closed
        exchange.done=True
        assert value._authority_waitables()=={}
    finally:channel.close();remote.close();value.poll.close()


def test_authority_waitables_are_deduplicated_and_retain_existing_listener_registration():
    value=control();local,remote=socket.socketpair();channel=Channel(local)
    exchange=SimpleNamespace(channel=channel,connected=False,done=False)
    task=SimpleNamespace(pending=('original',exchange))
    value.connections={1:dict(metadata_task=task),2:dict(metadata_task=task)}
    try:
        assert len(value._authority_waitables())==1
        value.poll.register(local,selectors.EVENT_WRITE,('original-listener',local.fileno()))
        events=value._select()
        assert events[0][0].data[0]=='original-listener'
        assert len(value.poll.get_map())==1
    finally:channel.close();remote.close();value.poll.close()


def test_excess_original_waitables_refused_before_any_selector_registration():
    value=control();owned=[]
    try:
        for index in range(17):
            local,remote=socket.socketpair();channel=Channel(local);owned.append((channel,remote))
            exchange=SimpleNamespace(channel=channel,connected=True,done=False)
            value.connections[index]=dict(metadata_task=SimpleNamespace(pending=('original',exchange)))
        with pytest.raises(ForgeError) as failure:value._select()
        assert failure.value.code=='resource_limit' and len(value.poll.get_map())==0
    finally:
        for channel,remote in owned:channel.close();remote.close()
        value.poll.close()
