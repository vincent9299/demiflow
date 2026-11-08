"""Closed project HTTP metadata adapter to the separately admitted helper.

No source URI, execution callback or principal can be supplied by a browser.
The existing authenticated HTTP project resolution is repeated by the helper's
live Web callback. Analysis requests have their own two bounded slots; its
independent stop uses one separate slot and never occupies pipeline control.
"""
from contextlib import contextmanager

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.metadata import SPECS


READ = frozenset({"documents", "notebook", "view", "case-group-bindings", "annotations", "annotation-heads", "annotation-definitions", "field-preview", "receipt", "source", "source-profile", "source-from-handle", "case-page","case-media","artifact-media","artifact-media-current", "output","output-original","output-media","output-current", "session", "execution", "execution-environment", "executions"})
READ = READ | {'notebook-sessions','session-generations'}
WRITE = frozenset(SPECS) - READ
GET_FIELDS = {"analysis-"+name: SPECS[name] for name in READ}
GET_FIELDS["analysis-output-inspect"] = (SPECS["output-media"][0]-{"range"},set())
POST_FIELDS = {"analysis-"+name: SPECS[name] for name in WRITE}
# This authenticated cancellation only addresses an already owned reader.
# It neither starts a job nor claims process cleanup in its acknowledgement.
POST_FIELDS['analysis-read-cancel']=SPECS['read-cancel']
# Declarative source reads use a body to keep a bounded typed filter/projection
# out of the URL. They have no write/execute grant and no mutation receipt.
POST_FIELDS["analysis-case-page"] = SPECS["case-page"]
GET_FIELDS.pop("analysis-case-page")
for _name in ("case-group-bindings","annotation-heads","annotation-definitions","field-preview","source-from-handle","case-media","artifact-media-current"):
    POST_FIELDS["analysis-"+_name]=SPECS[_name]
    GET_FIELDS.pop("analysis-"+_name)
for _name in ("output-original","output-media","artifact-media"):
    _required,_optional=GET_FIELDS["analysis-"+_name]
    GET_FIELDS["analysis-"+_name]=(_required-{"range"},_optional|{"range"})
POST_FIELDS["analysis-stop-generation"] = ({"request_id", "session_id", "generation"}, set())
POST_FIELDS["analysis-media"] = ({"action"},{"reference"})
PERMISSION = {name: "analysis_write" for name in {"notebook-create", "notebook-patch", "view-save"}}
PERMISSION.update({name: "analysis_execute" for name in {"session-start", "session-control", "cell-submit", "cell-cancel", "stop-generation"}})
PERMISSION["annotations-save"] = "case_annotate"
PERMISSION["media-capture"] = "analysis_write"
INTEGER_FIELDS = {"notebook": {"revision", "offset", "limit"}, "view": {"revision"},
    "documents": {"limit"}, "annotations": {"before", "limit", "watermark"}, "executions": {"after", "limit"},
    "output":{"offset","limit"},"output-media":{"output_index"},"output-inspect":{"output_index"}}


@contextmanager
def slot(server, *, stop=False):
    service = getattr(server, "analysis", None)
    if service is None:
        raise ForgeError("Analysis deployment is not connected; no host execution fallback", code="operation_rejected")
    slots = server.analysis_stops if stop else server.analysis_requests
    if not slots.acquire(blocking=False):
        raise ForgeError("Analysis request capacity is busy; no operation was dispatched", code="busy")
    try:
        yield service
    finally:
        slots.release()


def get(server, project, name, query, *, range_header=None, consumer_closed=None):
    if name not in GET_FIELDS:
        c.reject("Unknown analysis read endpoint")
    operation = name.removeprefix("analysis-")
    required, optional = GET_FIELDS[name]
    c.fields(query, required, optional, label=name)
    payload = dict(query)
    if operation in {"output-original","output-media","artifact-media"}:
        if range_header is not None and "range" in payload:c.reject("Use exactly one output range")
        payload["range"]=range_header if range_header is not None else payload.get("range")
        c.text(payload["range"],"output byte range",96)
    for field in INTEGER_FIELDS.get(operation, set()) & payload.keys():
        raw = payload[field]
        if type(raw) is not str or not raw.isascii() or not raw.isdecimal() or len(raw) > 19:
            c.reject("Analysis page/revision must be an unsigned decimal integer")
        payload[field] = c.integer(int(raw), "analysis page/revision", 2**63-1)
    if operation=="artifact-media":
        if type(payload['reference']) is str:
            c.text(payload['reference'],'fixed media reference',4096);payload['reference']=c.decode(payload['reference'].encode())
        from demiflow.workbench.analysis.registered_media import reference
        reference(payload['reference'])
    if operation == "annotations":
        c.text(payload["target"], "fixed Case target", 4096)
        payload["target"] = c.decode(payload["target"].encode())
    if operation=="source-profile":
        # Workspace HTTP's common query parser already decodes `columns`.
        # Direct adapter callers may still supply its JSON wire spelling.
        if type(payload["columns"]) is str:
            c.text(payload["columns"],"profile projection",4096)
            payload["columns"]=c.decode(payload["columns"].encode())
        c.encoded(payload["columns"],maximum=4096)
        c.sequence(payload["columns"],"profile projection",16,minimum=1)
    with slot(server) as service:
        if operation=="output-inspect":
            from demiflow.workbench.analysis.media_http import reference
            return {"descriptor":service.media_inspect(project,reference(payload),consumer_closed=consumer_closed)}
        if operation in {"output","output-original","output-media","artifact-media"}:
            return service.read_output(project,operation,payload,consumer_closed=consumer_closed)
        return service.call(project, operation, payload)


def post(server, project, root, name, payload,*,consumer_closed=None):
    if name not in POST_FIELDS:
        c.reject("Unknown analysis mutation endpoint")
    operation = name.removeprefix("analysis-")
    required, optional = POST_FIELDS[name]
    c.fields(payload, required, optional, label=name)
    c.encoded(payload)
    # No auto-grant from an available helper, project declaration or run mode.
    inventory = server.inventory()
    from demiflow.workbench.projects.registry import resolve_workspace
    current, _ = resolve_workspace(inventory, project)
    if current != root:
        c.reject("Registered project changed before analysis dispatch", code="conflict")
    permission = PERMISSION.get(operation)
    if permission is not None and not server.effective_permissions(inventory["projects"][project])[permission]:
        c.reject("Required analysis capability is disabled: " + permission, code="operation_rejected")
    with slot(server, stop=operation == "stop-generation") as service:
        if operation in {"case-page","media-capture"}:return service.read_output(project,operation,payload,consumer_closed=consumer_closed)
        return (service.stop_generation(project, **payload) if operation == "stop-generation"
            else service.call(project, operation, payload))
