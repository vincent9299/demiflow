"""Process-free, live-cell-bound reads of registered complex source fields.

Evaluated by the same asynchronous authority transcript as metadata. It never
imports Arrow/Lance, dispatches code or turns a client path into a source.
"""
import json

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.operations import Scope
from demiflow.workbench.analysis.operations import reference
from demiflow.workbench.analysis.dependencies import record as record_dependency
from demiflow.workbench.analysis.source_query import request


def alias_binding(bindings, reserved, payload):
    """Validate an explicit, generation-local SQL name without changing it."""
    c.fields(payload,{'alias','handle'},{'expected'},label='generation table alias')
    alias=c.sql_alias(payload['alias'])
    if alias in reserved:
        c.reject('Original Notebook source aliases cannot be replaced',code='conflict')
    previous=bindings.get(alias)
    if previous!=payload['handle']:
        if previous!=payload.get('expected'):
            c.reject('Table alias changed; supply its previous handle explicitly',code='conflict')
        if previous is None and len(set(reserved)|set(bindings))>=16:
            c.reject('A kernel generation supports at most16 explicit table/source names',code='resource_limit')
    return alias


class SourceReads:
    def __init__(self, records, sources, *, session_id, generation, execution_id, bindings=None):
        self.records, self.sources = records, sources
        self.session_id, self.generation, self.execution_id = session_id, generation, execution_id
        self.bindings={} if bindings is None else c.decode(c.encoded(bindings,maximum=8192))

    def route(self, authority, operation, payload):
        c.one_of(operation, {"source-info", "source-sample", "case-source", "handle-info", "table",'bind-table'}, "registered source read")
        required={'alias','handle'} if operation=='bind-table' else {"handle"} if operation in {"case-source","handle-info"} else {"alias"} if operation in {"source-info","table"} else {"alias","digest","query"}
        c.fields(payload,required,{'expected'} if operation=='bind-table' else set(),label="registered source read")
        alias = None if "alias" not in payload else c.text(payload["alias"], "session source alias", 128)
        scope = Scope(authority.access(), self.session_id, self.generation, self.execution_id)
        database = self.records.database
        with database.locked() as db:
            session = scope.active(db)
            bindings = json.loads(session["sources"])
        if operation=='bind-table':
            alias_binding(self.bindings,bindings,payload)
            if 'expected' in payload:
                with database.locked() as db:reference(db,scope.access.project,payload['expected'])
        if operation in {"case-source","handle-info","table",'bind-table'}:
            handle=payload.get("handle") if operation!="table" else self.bindings.get(alias)
            if handle is None:
                if alias in bindings:
                    c.reject("Rich source fields use source(alias).sample()/case_view(source(alias)); bind a separate primitive projection for SQL",code="operation_rejected")
                c.reject("Source alias is not bound to this generation",code="not_found")
            if operation=="case-source":
                result=self.sources.private_source(scope.access,handle)
                record_dependency(self.records,scope,handle=handle,source_snapshot=result["digest"])
            else:
                with database.locked() as db:result=reference(db,scope.access.project,handle)
                record_dependency(self.records,scope,handle=handle)
            with database.locked() as db:scope.active(db)
            return result
        digest = bindings.get(alias)
        if digest is None: c.reject("Source alias is not bound to the original kernel generation", code="not_found")
        source, origin = self.sources.registered(scope.access, digest)
        if operation=="source-info":
            if not self.sources.verify(authority, source):
                c.reject("Fixed source is unavailable", code="conflict")
            result = source
        else:
            if payload["digest"] != digest: c.reject("Source handle belongs to another binding", code="conflict")
            query = request(payload["query"], source)
            if query["kind"] != "sample": c.reject("Notebook source sample accepts only its bounded projection/page")
            if origin["kind"] != "external":
                c.reject("Pinned tables use their bounded Arrow handle", code="operation_rejected")
            result = authority.call("source-query", dict(source_locator=origin["locator"], source_snapshot=digest,
                query=query, purpose="notebook"))
            if type(result) is not dict or result.get("source_snapshot") != digest or result.get("kind") != "sample":
                c.reject("Source sample belongs to another binding", code="conflict")
        c.encoded(result, maximum=56*1024)
        with database.locked() as db:
            after = scope.active(db)
            if json.loads(after["sources"]) != bindings:
                c.reject("Original session source bindings changed", code="conflict")
        return result
