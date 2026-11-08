"""Test-only real Controller.serve/SQLite/Unix loop; runtime/watchdog are fakes.

No Deployment.load, OCI, cgroup or Jupyter execution is authorized or performed.
This fixture deliberately uses one test UID and never enables a real service.
"""
import json
import os
from pathlib import Path
import signal
import sys

from demiflow.workbench.analysis.artifacts import Artifacts
from demiflow.workbench.analysis.broker import Broker
from demiflow.workbench.analysis.controller import Controller
from demiflow.workbench.analysis.database import Database
from demiflow.workbench.analysis.database import FileSizeGuard
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.ipc import Listener
from demiflow.workbench.analysis.metadata import Metadata
from demiflow.workbench.analysis.operations import Operations
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.sources import Sources
import selectors
import time
from types import SimpleNamespace


class Runtime:
    mode='oci'
    def __init__(self, root):
        self.root=private_directory(root);self.leases={};self.created=0
    def create(self, **kwargs):
        self.created+=1
        raise AssertionError("test must cancel before any runtime admission")
    def owner(self,operation,digest,**kwargs):
        return dict(owner_digest=digest,bound=operation!='clear-owner',stop_requested=False)
    def close(self):pass


class Watchdog:
    def status(self):return True
    def owner(self, operation, digest, **kwargs):
        return dict(owner_digest=digest,bound=operation!="clear-owner",stop_requested=False)


def main():
    state=private_directory(Path(sys.argv[1]))
    transfer=private_directory(state/"transfer")
    outbox=transfer/"web-outbox";outbox.mkdir();outbox.chmod(0o2770)
    paths=SimpleNamespace(state=state,helper_gid=os.getegid())
    # Constructing this test object is not a deployment/resource admission.
    deployment=Deployment(dict(site=dict(allowed_web_uids=[os.geteuid()],helper_gid=os.getegid(),
        host_transfer_root=str(outbox),web_auth_socket_path=sys.argv[3]),
        owner_release=dict(helper_sha256="a"*64),profile_sha256="b"*64),paths,"production")
    service=Controller.__new__(Controller)
    service._recovery=None
    service.deployment=deployment;service.poll=selectors.DefaultSelector()
    service.listeners,service.connections,service.sessions={},{},{}
    service.database=Database(state/"database",guard=FileSizeGuard().install())
    service.records=Records(service.database)
    service.artifacts,service.operations=Artifacts(service.records),Operations(service.records)
    service.sources=Sources(service.records,state)
    service.metadata=Metadata(service.records,service.sources)
    service.runtime=Runtime(state/"runtime");service.watchdog=Watchdog()
    service.broker=Broker(service);service.last_watchdog_check=time.monotonic()
    service.public=Listener(sys.argv[2]);service._listener(service.public,"control",None,None)
    service.running=True
    signal.signal(signal.SIGTERM,lambda *_:setattr(service,"running",False))
    print(json.dumps(dict(ready=True)),flush=True)
    try:
        service.serve()
    finally:
        with service.database.locked() as db:
            proof=dict(runtime_create_calls=service.runtime.created,
                operations=[dict(row) for row in db.execute("SELECT state,cleanup_proven FROM analysis_operations ORDER BY id")],
                ready_objects=db.execute("SELECT count(*) FROM analysis_objects WHERE state='ready'").fetchone()[0],
                unresolved_objects=db.execute("SELECT count(*) FROM analysis_objects WHERE state!='cleanup_complete'").fetchone()[0],
                session_inputs=db.execute("SELECT count(*) FROM analysis_session_inputs").fetchone()[0])
        service.close()
        Path(sys.argv[4]).write_text(json.dumps(proof))


if __name__=="__main__":main()
