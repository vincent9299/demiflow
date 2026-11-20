"""The MCP client cannot choose a task, epoch, command or control-store path."""
import json
import socket
import time

import pytest

from demiflow.workbench.conversations.common import root_identity, wire
from demiflow.workbench.conversations.context import BridgeServer
from demiflow.workbench.conversations.inputs import InputAccess
from demiflow.workbench.conversations.mcp_bridge import call
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.store import Store


def test_control_channel_binds_token_turn_epoch_and_active_lifetime(tmp_path):
    root = tmp_path / 'project'; root.mkdir()
    store = Store(tmp_path / 'state', Limits(control_reserve=4096), initialize=True)
    session = store.create('w', root_identity(root), 'codex', 'c', 'p')
    turn, _ = store.submit('w', session['id'], 's', 0, 'Real request', root_identity(root), 'p')
    access = InputAccess(store, turn['id'], dict(root=root), tmp_path, denied_paths=(store.root,), runtime_paths=(), deadline=time.time()+20)
    channel = BridgeServer(access)
    try:
        endpoint = str(channel.path)
        assert not channel.path.is_relative_to(store.root)
        answer = call(endpoint, channel.token, turn['id'], turn['epoch'], 'tools/list', {})
        assert answer['tools'][0]['name'] == 'read_input'
        for token, tid, epoch in [('bad', turn['id'], turn['epoch']), (channel.token, 'other', turn['epoch']), (channel.token, turn['id'], 'stale')]:
            with pytest.raises(ValueError, match='does not match'):
                call(endpoint, token, tid, epoch, 'tools/list', {})
        with pytest.raises(ValueError, match='Unsupported'):
            call(endpoint, channel.token, turn['id'], turn['epoch'], 'control/read-file', {'path': str(store.path)})
        store.update(turn['id'], state='completed', cleanup='complete')
        with pytest.raises(ValueError, match='inactive'):
            access.invoke({})
        with pytest.raises(ValueError, match='inactive'):
            call(endpoint, channel.token, turn['id'], turn['epoch'], 'tools/list', {})
    finally:
        channel.close()
    assert not channel.directory.exists()


def test_control_channel_rejects_wide_frame_without_reading_payload(tmp_path):
    root = tmp_path/'project'; root.mkdir()
    store = Store(tmp_path/'state', Limits(control_reserve=4096), initialize=True)
    session = store.create('w', root_identity(root), 'codex', 'c', 'p')
    turn, _ = store.submit('w', session['id'], 's', 0, 'Real request', root_identity(root), 'p')
    access = InputAccess(store, turn['id'], dict(root=root), tmp_path, denied_paths=(store.root,), runtime_paths=(), deadline=time.time()+20)
    channel = BridgeServer(access)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(3); connection.connect(str(channel.path))
            connection.sendall(b'x'*65537)
            response = connection.recv(4096)
            assert b'frame exceeds budget' in response
    finally:
        channel.close()
