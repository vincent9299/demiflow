"""Web-side bounded JSON/opaque-range transport to the admitted helper.

Uses one connection and one server-generated read identity, with no replay.
It notices HTTP cancellation while waiting and closes/stops that media read.
No Jupyter, image, codec or original-document decoding occurs in this process.
"""
from contextlib import closing
from dataclasses import dataclass
import select
import socket
import time
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.web_service import credentials


@dataclass(frozen=True)
class OutputRange:
    artifact: dict
    metadata: dict
    data: bytes


def _failure(value):
    if type(value) is dict and "error" in value:
        c.fields(value,{"error","code"},{"reason"},label="owned output failure")
        from demiflow.forge.schema import ForgeError
        c.text(value["error"],"owned output error",1024)
        if "reason" in value:
            from demiflow.workbench.analysis.media import MediaError
            raise MediaError(value["error"],reason=c.one_of(value["reason"],{"media_unavailable","range_not_satisfiable"},"media failure reason"))
        c.reject(value["error"],code=value["code"] if value["code"] in ForgeError.CODES else "operation_failed")


def stop(service,record):
    try:record["connection"].shutdown(socket.SHUT_RDWR)
    except OSError:pass
    identifier=record.get("read_request_id")
    if identifier is None:return
    if service.config.get('format')=='demiflow_analysis_trusted_web_config_v1':
        request_id='u'+uuid.uuid4().hex
        value=service.call(record['project'],'read-cancel',
            dict(request_id=request_id,read_request_id=identifier),_timeout=7)
        if (value.get('format')!='demiflow_analysis_managed_read_stop_v1'
                or value.get('read_request_id')!=identifier or value.get('request_id')!=request_id
                or value.get('accepted') is not True or value.get('execution_mode')!='trusted_local'
                or value.get('cleanup_proven') is not False):
            c.reject('Managed read stop acknowledgement differs',code='conflict')
        return
    connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.settimeout(7)
    with closing(connection):
        connection.connect(service.config["priority_socket"]);credentials(connection,service.config["helper_uid"])
        frames=Frames(connection);request_id="u"+uuid.uuid4().hex
        frames.send("json",dict(version=1,project=record["project"],operation="stop-read",payload=dict(request_id=request_id,read_request_id=identifier)))
        value=frames.json();_failure(value)
        if (value.get("format")!="demiflow_analysis_priority_read_stop_v1" or value.get("read_request_id")!=identifier
                or value.get("request_id")!=request_id or value.get("accepted") is not True or value.get("cleanup_proven") is not False):
            c.reject("Independent read stop acknowledgement differs",code="conflict")


def exchange(service,project,operation,payload,*,consumer_closed=None,cancellation_tag=None):
    if service.closed.is_set():c.reject("Output adapter is closed",code="operation_rejected")
    from demiflow.workbench.projects.registry import identity
    identity(project);c.one_of(operation,{"output","output-original","output-media","case-page","artifact-media","media-capture"},"owned output read")
    c.encoded(payload)
    connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.settimeout(2)
    channel=None;record=dict(connection=connection,project=project,tag=cancellation_tag,read_request_id=None)
    try:
        connection.connect(service.config["helper_socket"])
        options={'expected_identity':service.config['helper_identity']} if 'helper_identity' in service.config else {}
        credentials(connection,service.config["helper_uid"],**options)
        channel=Channel(connection,binary=True)
        with service.output_lock:
            if len(service.output_connections)>=2:c.reject("Output read transport capacity is busy",code="busy")
            service.output_connections[id(connection)]=record
        channel.queue("json",dict(version=1,project=project,operation=operation,payload=payload))
        admitted=False;header=None;blob=None;deadline=time.monotonic()+65
        while time.monotonic()<deadline:
            if service.closed.is_set() or consumer_closed is not None and consumer_closed():
                try:stop(service,record)
                except (OSError,ValueError):pass
                c.reject("Original HTTP output read was cancelled",code="operation_rejected")
            readable,writable,_=select.select([connection],[connection] if channel.outgoing else [],[],.05)
            if writable:channel.flush()
            if not readable:continue
            frame=channel.receive()
            if frame is None:continue
            kind,value=frame
            if kind=="json":
                _failure(value)
                if not admitted:
                    if operation=='media-capture' and value.get('format')!='demiflow_output_read_admission_v1':
                        if value.get('status') not in {'ready','pending','unavailable'} or value.get('request_id')!=payload['request_id']:c.reject('Capture result differs from its original request',code='conflict')
                        c.encoded(value,maximum=16*1024);return value
                    if operation=="case-page" and value.get("format")!="demiflow_output_read_admission_v1":
                        c.encoded(value,maximum=56*1024)
                        if value.get("kind")!="cases" or value.get("source_snapshot")!=payload["digest"]:c.reject("Case projection belongs to another fixed source",code="conflict")
                        return value
                    c.fields(value,{"format","accepted","read_request_id"},label="owned read admission")
                    if value["format"]!="demiflow_output_read_admission_v1" or value["accepted"] is not True:c.reject("Output read admission differs",code="conflict")
                    record["read_request_id"]=c.identity(value["read_request_id"]);admitted=True;continue
                if operation=='media-capture':
                    if value.get('status') not in {'ready','pending','unavailable'} or value.get('request_id')!=payload['request_id']:c.reject('Capture result differs from its original request',code='conflict')
                    c.encoded(value,maximum=16*1024);return value
                if operation=="case-page":
                    c.encoded(value,maximum=56*1024)
                    if value.get("kind")!="cases" or value.get("source_snapshot")!=payload["digest"]:c.reject("Private Case projection belongs to another fixed source",code="conflict")
                    return value
                if operation=="output":
                    c.encoded(value,maximum=56*1024)
                    if value.get("version")!="output_v1" or value.get("artifact",{}).get("id")!=payload["expected_output_id"] or value.get("provenance",{}).get("execution_id")!=payload["execution_id"]:
                        c.reject("Output projection belongs to another request",code="conflict")
                    return value
                if header is not None:c.reject("Output range repeated its header")
                c.fields(value,{"format","artifact","metadata"},label="owned output range")
                if value["format"]!="demiflow_output_range_v1" or value["artifact"].get("id")!=(payload["reference"]["artifact_id"] if operation=="artifact-media" else payload["expected_output_id"]) or value["artifact"].get("digest")!=(payload["reference"]["expected_binding"] if operation=="artifact-media" else payload["expected_binding"]):
                    c.reject("Output range belongs to another artifact/binding",code="conflict")
                header=value
            elif kind=="blob":
                if header is None or blob is not None:c.reject("Unexpected output range bytes")
                expected=header["metadata"]["end"]-header["metadata"]["start"]+1
                if len(value)!=expected or len(value)>1024*1024:c.reject("Opaque output range byte count differs",code="conflict")
                blob=value
            elif kind=="end":
                if header is None or blob is None:c.reject("Output range ended without confirmed bytes")
                return OutputRange(header["artifact"],header["metadata"],blob)
            else:c.reject("Unexpected output transport frame")
        try:stop(service,record)
        except (OSError,ValueError):pass
        c.reject("Original output read deadline ended; no automatic replay",code="operation_failed")
    except OSError:
        c.reject("Original output read connection ended; no automatic replay",code="operation_failed")
    finally:
        with service.output_lock:service.output_connections.pop(id(connection),None)
        if channel is not None:channel.close()
        else:connection.close()
