"""Closed metadata API shared by the real helper and its Web adapter.

This layer owns no subprocesses and never accepts a verification callback from
an IPC request. Its controller supplies the admitted live authority/resolver.
Saving source or changing a view cannot enqueue code or a pipeline run.
"""
from __future__ import annotations

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.sessions import Sessions

SPECS = {
    "documents": ({"kind"}, {"after", "limit"}),
    "notebook": ({"notebook_id"}, {"revision", "offset", "limit"}),
    "notebook-create": ({"request_id", "document"}, set()),
    "notebook-patch": ({"request_id", "notebook_id", "expected_revision", "patch"}, set()),
    "view": ({"view_id"}, {"revision"}),
    "view-save": ({"request_id", "view_id", "expected_revision", "descriptor"}, set()),
    "case-group-bindings": ({"digest", "case_key"}, {"view", "variant"}),
    "annotations": ({"target", "definition"}, {"before", "limit", "watermark"}),
    "annotation-heads": ({"target", "definitions"}, {"watermark"}),
    "annotation-definitions": ({"target"}, {"before","watermark","limit"}),
    "field-preview": ({"format", "value"}, set()),
    "annotations-save": ({"request_id", "target", "entries", "origin"}, set()),
    "receipt": ({"request_id"}, set()),
    "source": ({"digest"}, set()),
    "source-from-handle": ({"handle"},set()),
    "source-register": ({"locator"}, set()),
    "source-profile": ({"digest", "columns"}, set()),
    "output": ({"execution_id","expected_output_id"}, {"expected_binding","offset","limit"}),
    "output-original": ({"execution_id","expected_output_id","expected_binding","range"}, set()),
    "output-media": ({"execution_id","expected_output_id","expected_binding","output_index","mime","expected_sha256","range"}, set()),
    "output-current": ({"execution_id","expected_output_id","expected_binding"},set()),
    "media-capture": ({"request_id","digest","case_key","field"},set()),
    "case-media": ({"digest","case_key","field"},set()),
    "artifact-media-current": ({"reference"},set()),
    "artifact-media": ({"reference","range"},set()),
    "case-page": ({"digest", "query", "view"}, {"cursor", "review"}),
    "session-start": ({"request_id", "notebook_id", "revision", "sources"}, set()),
    "session": ({"session_id"}, set()),
    'notebook-sessions':({'notebook_id'},set()),
    'session-generations':({'session_id'},set()),
    "session-control": ({"request_id", "session_id", "expected_generation", "action"}, set()),
    "cell-submit": ({"request_id", "session_id", "expected_generation", "revision", "cell_id", "source_sha256"}, set()),
    "cell-cancel": ({"request_id", "execution_id", "expected_generation"}, set()),
    "execution": ({"execution_id"}, set()),
    "execution-environment": ({'execution_id'}, set()),
    "executions": ({"session_id", "generation"}, {"after", "limit", "order"}),
    "read-cancel": ({'request_id','read_request_id'}, set()),
}


class Metadata:
    def __init__(self, records, sources, *, environment=None, reader_environment=None):
        if not isinstance(records, Records):
            c.reject("Analysis metadata requires its dedicated record store")
        if environment is not None and not callable(environment):
            c.reject('Generation environment provider must be server-owned')
        self.records, self.sources, self.sessions = records, sources, Sessions(records)
        self.environment = environment
        if reader_environment is not None and not callable(reader_environment):
            c.reject('Reader environment provider must be server-owned')
        self.reader_environment = reader_environment

    def route(self, authority, operation, payload):
        c.one_of(operation, SPECS, "analysis metadata operation")
        required, optional = SPECS[operation]
        c.fields(payload, required, optional, label="analysis " + operation)
        c.encoded(payload)
        access = authority.access()
        access.check()
        if (operation in {'session-start','cell-submit'} or operation=='session-control' and payload['action']=='restart'):
            if not getattr(self,'execution_available',lambda:True)():
                c.reject('Original startup owners are still being reconciled; no execution was replayed',code='busy')
        if operation=='read-cancel':
            return dict(request_id=c.identity(payload['request_id']),
                read_request_id=c.identity(payload['read_request_id']))
        if self.reader_environment is not None and operation in {'output','output-original','output-media',
                'artifact-media','case-page','media-capture'}:
            from demiflow.workbench.analysis.execution_facts import origin
            authority.reader_environment=origin(self.reader_environment(authority))
        verify = lambda source, target=None: self.sources.verify(authority, source, target)
        if operation == "source-register":
            return self.sources.register(authority, payload["locator"])
        if operation=="source-from-handle":
            return self.sources.private_source(access,payload["handle"])
        if operation == "source":
            return self.records.source(access, **payload)
        if operation=="output-current":
            from demiflow.workbench.analysis.presentations import binding_status
            return binding_status(self.records,access,**payload)
        if operation in {"output","output-original","output-media"}:
            from demiflow.workbench.analysis.output_reader import registered_witness
            from demiflow.workbench.analysis.dependencies import resolve
            if operation=="output":
                c.integer(payload.get("offset",0),"output page offset",256)
                c.integer(payload.get("limit",8),"output page size",16,1)
            else:
                if payload["range"] is not None:c.text(payload["range"],"output byte range",96)
                if operation=="output-media":
                    c.integer(payload["output_index"],"output media index",255)
                    c.sha(payload["expected_sha256"])
                    c.one_of(payload["mime"],{"image/png","image/jpeg","image/gif","image/webp","audio/wav","audio/mpeg","audio/ogg","audio/webm","video/mp4","video/webm","video/ogg"},"output media MIME")
            return registered_witness(self.records,access,payload["execution_id"],expected_output_id=payload["expected_output_id"],
                expected_binding=payload.get("expected_binding"),dependency_resolver=resolve)
        if operation=='media-capture':
            from demiflow.workbench.analysis.media_captures import Captures
            available=getattr(self,'media_available',lambda:True)
            captures=Captures(self.records,self.sources);captures.available=available
            return captures.admit(authority,**payload)
        if operation=='receipt':
            result=self.records.receipt(access,**payload)
            if result.get('operation')=='media.capture':
                from demiflow.workbench.analysis.media_captures import Captures
                return Captures(self.records,self.sources).receipt(authority,result)
            return result
        if operation=='case-media':
            from demiflow.workbench.analysis.case_media import lookup
            return lookup(self.records,self.sources,authority,**payload)
        if operation in {'artifact-media','artifact-media-current'}:
            from demiflow.workbench.analysis.case_media import registered_witness
            value=registered_witness(self.records,self.sources,authority,payload['reference'])
            if operation=='artifact-media-current':return dict(verified=True,descriptor=value['artifact']['descriptor'])
            from demiflow.workbench.analysis.media import byte_range
            byte_range(payload['range'],value['artifact']['bytes']);return value
        if operation == "field-preview":
            from demiflow.workbench.analysis.outputs import field_preview
            result=field_preview(payload["format"],payload["value"])
            c.encoded(result);access.check()
            return result
        if operation=="case-page":
            from demiflow.workbench.analysis.case_pages import page
            source,origin=self.sources.registered(access,payload["digest"])
            return page(self.records,authority,source,origin,payload)
        if operation == "source-profile":
            source,origin=self.sources.registered(access,payload["digest"])
            if origin["kind"]!="external":
                c.reject("Pinned source profiling needs the native artifact reader",code="operation_rejected")
            from demiflow.workbench.analysis.source_query import request
            query=request(dict(kind="profile",columns=payload["columns"]),source)
            value=authority.call("source-query",dict(source_locator=origin["locator"],source_snapshot=source["digest"],query=query,purpose="catalog"))
            if type(value) is not dict or value.get("source_snapshot")!=source["digest"] or value.get("kind")!="profile":
                c.reject("Source profile belongs to another fixed source",code="conflict")
            c.encoded(value,maximum=56*1024);access.check()
            return value
        methods = {"documents": self.records.list_documents, "notebook": self.records.notebook,
            "notebook-create": self.records.create_notebook, "notebook-patch": self.records.patch_notebook,
            "view": self.records.view, "view-save": self.records.save_view, "case-group-bindings": self.records.case_group_bindings, "annotations": self.records.annotations,
            "annotation-heads": self.records.annotation_heads,
            "annotation-definitions": self.records.annotation_definitions,
            "annotations-save": self.records.save_annotations, "receipt": self.records.receipt,
            "session-start": self.sessions.start, "session": self.sessions.get,
            'notebook-sessions':self.sessions.notebook_sessions,'session-generations':self.sessions.generations,
            "session-control": self.sessions.control, "cell-submit": self.sessions.submit,
            "cell-cancel": self.sessions.cancel, "execution": self.sessions.execution,
            'execution-environment':self.sessions.environment,"executions": self.sessions.page}
        arguments = dict(payload)
        if operation in {"view-save", "annotations-save", "session-start"}:
            arguments["verify"] = verify
        if operation=="annotations-save":
            arguments["verify_media"]=lambda source,target,witnesses:self.sources.verify_media(authority,source,target,witnesses)
        if operation == "session-start":
            arguments["web_parent"] = authority.peer.identity
        if self.environment is not None and (operation == 'session-start'
                or operation == 'session-control' and payload['action'] == 'restart'):
            # The admitted controller supplies fixed facts. Public request
            # fields cannot select an interpreter, root or execution mode.
            from demiflow.workbench.analysis.execution_facts import origin
            arguments['environment'] = origin(self.environment(authority))
        result = methods[operation](access, **arguments)
        c.encoded(result)
        return result
