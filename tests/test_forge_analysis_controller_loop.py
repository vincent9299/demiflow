"""Real selector/metadata loop with delayed Unix authority and fake runtime.

This proves local control progress, not browser latency, real OCI stopping,
cross-UID credentials, cgroup memory or persistent-volume performance.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.ipc import Channel
from test_forge_analysis_records import document, source_manifest

ROOT=Path(__file__).resolve().parents[1]
PROJECT="w"+"1"*32


class Web:
    def __init__(self, root):
        self.path=root/"a.sock";self.helper=root/"h.sock"
        self.listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        self.listener.bind(str(self.path));self.listener.listen(8);self.listener.setblocking(False)
        self.channels=[];self.held=[];self.exports=[]
        self.source=c.source_snapshot(source_manifest())
        self.locator=dict(kind="workspace_table",alias="table",binding=self.source["binding"],columns=["input","output"])

    def step(self):
        while True:
            try:connection,_=self.listener.accept()
            except BlockingIOError:break
            self.channels.append(Channel(connection))
        for channel in list(self.channels):
            try:
                if channel.outgoing:channel.flush()
                if channel in self.held:continue
                frame=channel.receive()
                if frame is None:continue
                kind,request=frame;assert kind=="json"
                operation=request["operation"]
                if operation=="export-source":
                    self.exports.append(request);self.held.append(channel);continue
                if operation=="whoami":value=dict(principal="owner")
                elif operation=="permission":value=dict(allowed=True)
                elif operation=="resolve-source":value=dict(source=self.source,locator=self.locator)
                elif operation=="verify-source":value=dict(verified=True)
                else:raise AssertionError(operation)
                channel.queue("json",dict(challenge=request["challenge"],value=value,error=None,code=None))
            except (EOFError,OSError):
                channel.close();self.channels.remove(channel)

    def call(self,operation,payload=None,*,timeout=10):
        connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.connect(str(self.helper))
        channel=Channel(connection)
        channel.queue("json",dict(version=1,project=None if operation=="status" else PROJECT,
            operation=operation,payload={} if payload is None else payload))
        deadline=time.monotonic()+timeout
        try:
            while time.monotonic()<deadline:
                channel.flush();self.step()
                frame=channel.receive()
                if frame is not None:
                    assert frame[0]=="json";return frame[1]
                time.sleep(.005)
            raise AssertionError("local controller did not respond: "+operation)
        finally:channel.close()

    def until(self,condition,seconds=10):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            self.step()
            if condition():return
            time.sleep(.005)
        raise AssertionError("controlled delayed source condition timed out")

    def close(self):
        for channel in self.channels:channel.close()
        self.listener.close()


@contextmanager
def loop(tmp_path):
    web=Web(tmp_path);proof=tmp_path/"proof.json"
    process=subprocess.Popen([sys.executable,"-B","-u",str(Path(__file__).with_name("_forge_analysis_loop_controller.py")),
        str(tmp_path/"state"),str(web.helper),str(web.path),str(proof)],cwd=ROOT,
        env=dict(os.environ,PYTHONPATH=str(ROOT),PYTHONDONTWRITEBYTECODE="1"),
        stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        from selectors import DefaultSelector,EVENT_READ
        with DefaultSelector() as poll:
            poll.register(process.stdout,EVENT_READ);assert poll.select(5)
        assert json.loads(process.stdout.readline())==dict(ready=True)
        yield web
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:process.wait(5)
            except subprocess.TimeoutExpired:process.kill();process.wait(2)
        web.close()
        error=process.stderr.read().decode()
        process.stdout.close();process.stderr.close()
        assert process.returncode==0,error


def test_status_and_cancel_progress_while_source_export_reply_is_withheld(tmp_path,record_property):
    with loop(tmp_path) as web:
        created=web.call("notebook-create",dict(request_id="notebook",document=document()))
        registered=web.call("source-register",dict(locator=web.locator));assert registered==web.source
        session=web.call("session-start",dict(request_id="session",notebook_id=created["id"],revision=1,
            sources=dict(table=registered["digest"])))
        web.until(lambda:bool(web.exports))
        start=time.monotonic()
        assert web.call("status")["active"] is True
        status_seconds=time.monotonic()-start
        start=time.monotonic()
        stopped=web.call("session-control",dict(request_id="stop",session_id=session["id"],
            expected_generation=session["generation"],action="stop"))
        cancel_seconds=time.monotonic()-start
        record_property("local_status_seconds",status_seconds)
        record_property("local_cancel_ack_seconds",cancel_seconds)
        record_property("runtime_and_cgroups","fake; real Controller.serve and Unix authority loop")
        assert stopped["state"]=="stop_requested" and not stopped["cleanup_proven"]
        assert status_seconds<2 and cancel_seconds<4
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            current=web.call("session",dict(session_id=session["id"]))
            if current["state"]=="stopped":break
        assert current["state"]=="stopped" and current["cleanup_proven"]
        assert len(web.exports)==1
        # A late reply on the original cancelled exchange cannot bind inputs,
        # run a materializer or replay the export with another request ID.
        held=web.held[0]
        try:
            held.queue("json",dict(challenge=web.exports[0]["challenge"],value=None,error="late source error",code="operation_failed"))
            held.flush();held.flush()
        except (OSError,EOFError):pass
        assert web.call("session",dict(session_id=session["id"]))["state"]=="stopped"
    proof=json.loads((tmp_path/"proof.json").read_bytes())
    assert proof==dict(runtime_create_calls=0,operations=[dict(state="unknown",cleanup_proven=1)],
        ready_objects=0,unresolved_objects=0,session_inputs=0)
