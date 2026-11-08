"""Single active generation's independent stop fence, no second run ledger.

The controller publishes this small immutable owner before acknowledging a
session. The watchdog owns the live stop bit and fences every later job watch.
It can stop the original cgroups while the controller is blocked in file IO.
Only current Web authority may request a stop. It cannot choose a PID/path or
claim cleanup; SQL receipts and actual cleanup remain controller-owned.
Watchdog death must kill the controller/jobs through the admitted supervisor.
"""
from __future__ import annotations

import os
from pathlib import Path

from demiflow.workbench.process import process_identity
from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.authority import Authority
from demiflow.workbench.analysis.authority_rpc import Exchange
from demiflow.workbench.analysis.deployment import _read


NAME = "priority-owner.json"
FORMAT = "demiflow_analysis_priority_owner_v1"
FIELDS = frozenset({"format", "project", "principal", "session_id", "generation", "controller", "web_parent"})


def validate(value):
    c.fields(value, FIELDS, label="original generation control owner")
    if value["format"] != FORMAT:
        c.reject("Priority owner format differs; no adoption", code="conflict")
    project_identity(value["project"])
    for field in ("principal", "session_id", "generation"):
        c.identity(value[field], field)
    for field in ("controller", "web_parent"):
        item = value[field]
        c.fields(item, {"pid", "boot", "start"}, label="original " + field)
        c.integer(item["pid"], "parent PID", 2**31-1, 1)
        c.text(item["boot"], "boot identity", 64); c.text(item["start"], "start identity", 32)
    return value


def load(root):
    return validate(c.decode(_read(Path(root)/NAME)))


def publish(root, authority, session_id, generation):
    value = validate(dict(format=FORMAT, project=authority.project, principal=authority.principal,
        session_id=session_id, generation=generation, controller=process_identity(os.getpid()),
        web_parent=authority.peer.identity))
    path = no_symlinks(Path(root)/NAME)
    if path.exists():
        if load(root) != value:
            c.reject("Original priority owner needs proved cleanup before replacement", code="busy")
        return c.digest(value)
    atomic_write_json(path, value)
    return c.digest(value)


class StopRequest:
    """Two nonblocking live checks, then an original-generation stop only."""
    def __init__(self, deployment, peer, owner, payload):
        c.fields(payload, {"request_id", "session_id", "generation"}, label="independent generation stop")
        c.identity(payload["request_id"])
        if payload["session_id"] != owner["session_id"] or payload["generation"] != owner["generation"]:
            c.reject("Stop belongs to another generation", code="conflict")
        if not peer.live() or peer.identity != owner["web_parent"]:
            c.reject("Only the original live Web parent may stop this generation", code="operation_rejected")
        self.owner, self.payload = owner, payload
        self.authority = Authority.__new__(Authority)
        self.authority.deployment, self.authority.peer, self.authority.project, self.authority.principal = (
            deployment, peer, owner["project"], None)
        self.phase = "whoami"
        self.exchange = Exchange(self.authority, "whoami", {})

    def advance(self):
        if not self.exchange.advance():
            return False
        value = self.exchange.value
        if self.phase == "whoami":
            if value != dict(principal=self.owner["principal"]):
                c.reject("Stop principal differs from the original owner", code="operation_rejected")
            self.authority.principal = self.owner["principal"]
            self.phase = "permission"
            self.exchange = Exchange(self.authority, "permission", dict(permission="analysis_execute"))
            return False
        if value != dict(allowed=True):
            c.reject("Current analysis stop permission is absent", code="operation_rejected")
        return True

    def close(self):
        self.exchange.close()
