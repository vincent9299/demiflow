"""Live Web authority callback for the external controller.

The fixed Unix socket must belong to the SAME boot/start/PID as the Web peer
which opened the control request/session. A public caller never selects a
principal, filesystem root, grant callback or source-verification function.
The callback rereads current project membership/permissions on every check.
"""
from __future__ import annotations

from contextlib import closing
import socket
import uuid

from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.trusted_binding import admitted
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.records import Access
from demiflow.workbench.analysis.authority_rpc import Exchange
from demiflow.workbench.analysis.authority_rpc import OPERATIONS
from demiflow.workbench.analysis.authority_rpc import response


class Authority:
    def __init__(self, deployment, peer, project):
        if not admitted(deployment) or not isinstance(peer, Peer):
            c.reject("Analysis authority requires an admitted deployment and captured Web peer")
        project_identity(project)
        if peer.uid not in deployment.value["site"]["allowed_web_uids"] or not peer.live():
            c.reject("Original Web authority is unavailable", code="operation_rejected")
        self.deployment, self.peer, self.project = deployment, peer, project
        self.principal = None
        value = self.call("whoami", {})
        c.fields(value, {"principal"}, label="server-owned analysis principal")
        self.principal = c.identity(value["principal"], "server-owned principal")

    def call(self, operation, payload):
        c.one_of(operation, OPERATIONS, "Web authority operation")
        c.encoded(payload)
        if not self.peer.live():
            c.reject("Original Web parent has exited; no authority fallback", code="operation_rejected")
        challenge = "c" + uuid.uuid4().hex
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(2 if operation in {"whoami","permission"} else 25)
        try:
            with closing(connection):
                connection.connect(self.deployment.value["site"]["web_auth_socket_path"])
                server = Peer.capture(connection, allowed_uids={self.peer.uid})
                if server.identity != self.peer.identity:
                    c.reject("Authority socket is not the original Web process", code="conflict")
                frames = Frames(connection)
                frames.send("json", dict(version=1, challenge=challenge, operation=operation, project=self.project,
                    web_parent=self.peer.identity, principal=self.principal, payload=payload))
                reply = frames.json()
                return response(self, challenge, reply)
        except OSError:
            c.reject("Live Web authority is unavailable; operation remains unconfirmed", code="operation_rejected")

    def begin(self, operation, payload):
        return Exchange(self, operation, payload)

    def check(self, project, principal, permission):
        if project != self.project or principal != self.principal:
            return False
        c.one_of(permission, {"read", "analysis_execute", "analysis_write", "case_annotate"}, "analysis permission")
        value = self.call("permission", dict(permission=permission))
        return value == {"allowed": True}

    def access(self):
        return Access(self.project, self.principal, self.check)

    def resolve_source(self, locator):
        # A locator identifies an existing registered/recognized fixed table,
        # never a caller URI or source manifest. The Web resolver owns proof.
        return self.call("resolve-source", {"locator": locator})

    def verify_source(self, source, target=None, *, locator):
        value = self.call("verify-source", dict(locator=locator, source=source, target=target))
        return value == {"verified": True}
