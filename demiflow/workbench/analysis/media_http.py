"""One scoped native-player route, backed by real admitted output reads.

POST preparation/renewal/revocation still requires the existing console token.
Only GET/HEAD on this exact route accepts its HttpOnly playback cookie. The
ticket cannot authenticate any other platform operation or select a new object.
"""
import re
import ssl
from urllib.parse import urlsplit

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.http import slot
from demiflow.workbench.analysis.media_tickets import path
from demiflow.workbench.analysis.output_transport import OutputRange

REFERENCE={"execution_id","expected_output_id","expected_binding","output_index","mime","expected_sha256"}


def endpoint(handler,*,playback=False):
    url=urlsplit(handler.path)
    match=re.fullmatch(r"/api/workspaces/([wl][0-9a-f]{32})/analysis-media",url.path)
    if match is None:return None
    if playback:
        binding=re.fullmatch(r"binding=([a-f0-9]{64})",url.query)
        if binding is None:c.reject("Playback URL requires its exact non-credential reference binding")
        return match[1],binding[1]
    if url.query:c.reject("Playback preparation accepts no query or credential")
    return match[1]


def reference(value):
    from demiflow.workbench.analysis.registered_media import REFERENCE as CASE_REFERENCE
    from demiflow.workbench.analysis.registered_media import reference as case_reference
    if type(value) is dict and set(value)==CASE_REFERENCE:return case_reference(value)
    c.fields(value,REFERENCE,label="fixed output media reference")
    c.identity(value["execution_id"]);c.identity(value["expected_output_id"])
    c.sha(value["expected_binding"]);c.sha(value["expected_sha256"])
    c.integer(value["output_index"],"media output index",255)
    c.one_of(value["mime"],{"image/png","image/jpeg","image/gif","image/webp","audio/wav","audio/mpeg","audio/ogg","audio/webm","video/mp4","video/webm","video/ogg"},"media MIME")
    c.encoded(value,maximum=8192)
    return value


def _service(handler):
    service=getattr(handler.server,"analysis",None)
    if service is None:c.reject("Admitted media reader is not connected",code="operation_rejected")
    c.identity(service.principal,"server media principal")
    return service


def _current(handler,service,project,ref,descriptor):
    # Current registration is re-read on EACH request and before byte return;
    # cookie possession never substitutes for it or for the helper's check.
    from demiflow.workbench.projects.registry import resolve_workspace
    resolve_workspace(handler.server.inventory(),project)
    return service.media_current(project,ref,descriptor)


def post(handler):
    project=endpoint(handler)
    if project is None:return False
    handler._authorize()
    payload=handler._body("analysis-media");action=c.one_of(payload["action"],{"prepare","renew","revoke"},"media playback action")
    c.fields(payload,{"action","reference"},label="playback action")
    ref=reference(payload["reference"])
    service=_service(handler);tickets=handler.server.media_tickets
    cookie=handler.headers.get("Cookie");secure=isinstance(handler.connection,ssl.SSLSocket)
    if action=="revoke":
        # Cleanup is allowed after project removal. No data/grant is acquired.
        old=tickets.peek(project,service.principal,cookie)
        if old is None or old.reference!=ref:
            handler._send(dict(revoked=False));return True
        header=tickets.revoke(project,service.principal,cookie,secure=secure)
        if old is not None:service.cancel_output(old.key)
        handler._send(dict(revoked=True),headers={"Set-Cookie":header});return True
    authorize=lambda ref,desc:_current(handler,service,project,ref,desc)
    if action=="renew":
        with slot(handler.server):
            old=tickets.get(project,service.principal,cookie)
            if old.reference!=ref:c.reject("Playback renewal belongs to another fixed media",code="conflict")
            value,header=tickets.renew(project,service.principal,cookie,authorize=authorize,secure=secure)
    else:
        with slot(handler.server):
            old=tickets.peek(project,service.principal,cookie)
            if old is not None:service.cancel_output(old.key)
            descriptor=service.media_inspect(project,ref,consumer_closed=handler._consumer_closed)
            value,header=tickets.prepare(project,service.principal,reference=ref,descriptor=descriptor,
                authorize=authorize,previous_cookie=cookie,secure=secure)
    handler._send(value,headers={"Set-Cookie":header});return True


def get(handler,*,head=False):
    target=endpoint(handler,playback=True)
    if target is None:return False
    project,binding=target
    expected=handler._request_origin()
    origins=handler.headers.get_all("Origin",[]);sites=handler.headers.get_all("Sec-Fetch-Site",[])
    if len(origins)>1 or origins and origins[0]!=expected or len(sites)>1 or sites and sites[0] not in {"none","same-origin"}:
        c.reject("Media playback requires its original same-origin page",code="operation_rejected")
    service=_service(handler);tickets=handler.server.media_tickets
    ticket=tickets.get(project,service.principal,handler.headers.get("Cookie"))
    if c.digest(ticket.reference)!=binding:
        c.reject("Playback URL belongs to another fixed media reference",code="conflict")
    authorize=lambda ref,desc:_current(handler,service,project,ref,desc)
    with slot(handler.server):
        tickets.current(ticket,authorize)
        if head:
            metadata=ticket.descriptor
            tickets.current(ticket,authorize)
            handler.send_response(200)
            for key,value in {"Content-Type":metadata["mime"],"Content-Length":str(metadata["bytes"]),"Accept-Ranges":"bytes","ETag":'"'+metadata["sha256"]+'"',
                "Cache-Control":"private, no-store","X-Content-Type-Options":"nosniff","Referrer-Policy":"no-referrer"}.items():handler.send_header(key,value)
            handler.end_headers();return True
        from demiflow.workbench.analysis.media import byte_range
        # Validate/size before dispatch, including open-ended native ranges.
        byte_range(handler.headers.get("Range"),ticket.descriptor["bytes"])
        value=service.read_output(project,"artifact-media" if "source_snapshot" in ticket.reference else "output-media",
            dict(reference=ticket.reference,range=handler.headers.get("Range")) if "source_snapshot" in ticket.reference else dict(ticket.reference,range=handler.headers.get("Range")),
            consumer_closed=handler._consumer_closed,cancellation_tag=ticket.key)
        tickets.current(ticket,authorize)
        if not isinstance(value,OutputRange) or value.metadata.get("descriptor")!=ticket.descriptor:
            c.reject("Playback bytes differ from the inspected immutable media",code="conflict")
        send_range(handler,value)
    return True


def send_range(handler,value):
    metadata=value.metadata
    headers={"Accept-Ranges":"bytes",
        "ETag":'"'+(metadata.get("descriptor",{}).get("sha256") or metadata.get("sha256"))+'"',
        "X-Forge-Binding":value.artifact["digest"]}
    if metadata["status"]==206:headers["Content-Range"]=f"bytes {metadata['start']}-{metadata['end']}/{metadata['total']}"
    if metadata.get("attachment"):headers["Content-Disposition"]='attachment; filename="notebook-output.json"'
    handler._send(value.data,metadata["status"],mime=metadata["mime"],headers=headers)
