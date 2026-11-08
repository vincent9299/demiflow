"""One bounded, nonblocking authority exchange; never reconnect or replay.

The controller advances an exchange once per selector turn. A slow source
worker must not turn a 25-second RPC deadline into a 25-second controller wait.
This transports small JSON only and retains the original Web process fence.
"""
from __future__ import annotations

import errno
import socket
import time
import uuid

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.ipc import Peer


OPERATIONS = frozenset({"whoami", "permission", "project-context", "resolve-source", "verify-source",
    "export-source", "release-source", "source-query", "media-sources", "export-media"})


def response(authority, challenge, reply):
    c.fields(reply, {"challenge", "value", "error", "code"}, label="Web authority response")
    if reply["challenge"] != challenge or not authority.peer.live():
        c.reject("Web authority response belongs to another process/request", code="conflict")
    if reply["error"] is not None:
        c.text(reply["error"], "authority error", 1024)
        code = reply["code"]
        c.reject(reply["error"], code=code if type(code) is str and code in ForgeError.CODES else "operation_rejected")
    return reply["value"]


class Exchange:
    def __init__(self, authority, operation, payload):
        c.one_of(operation, OPERATIONS, "Web authority operation")
        c.encoded(payload)
        if not authority.peer.live():
            c.reject("Original Web authority is unavailable", code="operation_rejected")
        self.authority = authority
        self.challenge = "c" + uuid.uuid4().hex
        self.deadline = time.monotonic() + (2 if operation in {"whoami", "permission"} else 25)
        self.done, self.value = False, None
        self.connected = self.connecting = False
        self.channel = None
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM | socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
        try:
            self.channel = Channel(connection)
            self.channel.queue("json", dict(version=1, challenge=self.challenge, operation=operation,
                project=authority.project, web_parent=authority.peer.identity,
                principal=authority.principal, payload=payload))
        except BaseException:
            connection.close()
            raise

    def advance(self):
        """At most one bounded send and receive; False leaves the RPC pending."""
        if self.done:
            return True
        if self.channel is None or self.channel.closed:
            c.reject("Original authority exchange is closed", code="operation_rejected")
        try:
            if time.monotonic() >= self.deadline or not self.authority.peer.live():
                c.reject("Live Web authority timed out or exited; no replay", code="operation_rejected")
            connection = self.channel.connection
            if not self.connecting:
                # Do not occupy a Web callback worker before this selector
                # turn can send the request. Another live owner may need its
                # synchronous permission check between metadata turns.
                status=connection.connect_ex(self.authority.deployment.value['site']['web_auth_socket_path'])
                if status not in {0,errno.EINPROGRESS,errno.EALREADY,errno.EINTR}:
                    raise OSError(status,'authority connection was not admitted')
                self.connecting=True
            if not self.connected:
                error = connection.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if error:
                    raise OSError(error, "authority connection failed")
                try:
                    connection.getpeername()
                except OSError as exc:
                    if exc.errno in {errno.ENOTCONN, errno.EINPROGRESS}:
                        return False
                    raise
                peer = Peer.capture(connection, allowed_uids={self.authority.peer.uid})
                if peer.identity != self.authority.peer.identity:
                    c.reject("Authority socket is not the original Web process", code="conflict")
                self.connected = True
            if self.channel.outgoing:
                # One bounded control frame, one nonblocking send. Sending
                # its five-byte header alone would occupy a blocking Web
                # worker until the controller's next turn, which can deadlock
                # a simultaneous synchronous grant check. At most two iovecs,
                # 65536 JSON bytes plus the fixed header; no frame copies.
                views=list(self.channel.outgoing)
                if len(views)>2 or self.channel.outgoing_bytes>65541:
                    c.reject('Authority request frame exceeds its fixed bound',code='resource_limit')
                try:sent=connection.sendmsg(views)
                except BlockingIOError:return False
                if sent<=0:raise EOFError('Authority stopped consuming the original request')
                self.channel.outgoing_bytes-=sent;self.channel.progress=time.monotonic()
                while sent and self.channel.outgoing:
                    view=self.channel.outgoing[0]
                    if sent>=len(view):sent-=len(view);self.channel.outgoing.popleft()
                    else:self.channel.outgoing[0]=view[sent:];sent=0
                if self.channel.outgoing:
                    return False
            frame = self.channel.receive()
            if frame is None:
                return False
            kind, reply = frame
            if kind != "json":
                c.reject("Authority response must be bounded JSON")
            self.value = response(self.authority, self.challenge, reply)
            self.done = True
            self.close()
            return True
        except (OSError,EOFError):
            self.close()
            c.reject('Original Web authority transport ended; no reconnect or replay',code='operation_rejected')
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.channel is not None:
            self.channel.close()
