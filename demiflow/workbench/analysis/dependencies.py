"""Exact broker-admitted execution dependencies for immutable output witnesses.

Only controller/broker code calls remember with a live owning Scope. This is
not an API accepting producer manifests. Historic resolution runs inside the
output witness's existing SQLite read lock and adds no RPC or recursive lock.
"""
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.operations import reference
from demiflow.workbench.analysis.records import _source


def remember(db, scope, *, handle, source_snapshot=None):
    actual=reference(db,scope.access.project,handle)
    if source_snapshot is None:
        kind,key,body="handle",actual["id"],{name:actual[name] for name in ("id","generation","digest")}
    else:
        source=_source(db,scope.access.project,source_snapshot)
        if source["kind"]!="pinned_object" or source["retention"]!="pinned" or source["binding"]!=actual["digest"] or actual["retention"]!="pinned":
            c.reject("Case dependency is not the proven pinned handle",code="conflict")
        kind,key,body="source",source["digest"],dict(source_snapshot=source["digest"],handle=handle)
    raw=c.encoded(body,maximum=1024)
    previous=db.execute("SELECT body FROM analysis_execution_dependencies WHERE project=? AND execution_id=? AND kind=? AND reference_key=?",
        (scope.access.project,scope.execution_id,kind,key)).fetchone()
    if previous is not None:
        if bytes(previous[0])!=raw:c.reject("Execution dependency changed",code="conflict")
        return
    count=db.execute("SELECT count(*) FROM analysis_execution_dependencies WHERE project=? AND execution_id=? AND kind=?",
        (scope.access.project,scope.execution_id,kind)).fetchone()[0]
    if count>=16:c.reject("Execution dependency witness exceeds16 registered references per kind",code="resource_limit")
    rows=db.execute("SELECT kind,body FROM analysis_execution_dependencies WHERE project=? AND execution_id=? LIMIT 33",
        (scope.access.project,scope.execution_id)).fetchall()
    if len(rows)>32:c.reject("Execution dependency registry exceeds its policy",code="resource_limit")
    identifiers=set()
    for row in rows:
        previous_body=c.decode(bytes(row["body"]),maximum=1024)
        identifiers.add(previous_body["id"] if row["kind"]=="handle" else previous_body["handle"]["id"])
    if actual["id"] not in identifiers and len(identifiers)>=16:
        c.reject("Execution dependency witness exceeds16 distinct table handles",code="resource_limit")
    db.execute("INSERT INTO analysis_execution_dependencies VALUES (?,?,?,?,?)",(scope.access.project,scope.execution_id,kind,key,raw))


def record(records,scope,*,handle,source_snapshot=None):
    with records.database.transaction(payload_bytes=4096) as db:
        scope.active(db)
        remember(db,scope,handle=handle,source_snapshot=source_snapshot)
        # A pending fresh check rolls this transaction back before yielding.
        scope.active(db)


def resolve(db,access,execution):
    if execution["project"]!=access.project:c.reject("Dependency witness belongs to another project",code="conflict")
    c.identity(execution["id"],"dependency execution")
    result=dict(handles=[],sources=[])
    rows=db.execute("SELECT kind,body FROM analysis_execution_dependencies WHERE project=? AND execution_id=? ORDER BY kind,reference_key LIMIT 33",
        (access.project,execution["id"])).fetchall()
    if len(rows)>32:c.reject("Execution dependency count exceeds its registry policy",code="resource_limit")
    handles={}
    for row in rows:
        item=c.decode(bytes(row["body"]),maximum=1024)
        if row["kind"]=="handle":
            actual=reference(db,access.project,item);handles[actual["id"]]={name:actual[name] for name in ("id","generation","digest")}
        elif row["kind"]=="source":
            c.fields(item,{"source_snapshot","handle"},label="registered source dependency")
            source=_source(db,access.project,item["source_snapshot"])
            actual=reference(db,access.project,item["handle"])
            if source["kind"]!="pinned_object" or source["binding"]!=actual["digest"] or actual["retention"]!="pinned":
                c.reject("Registered dependency source no longer matches its pinned object",code="conflict")
            result["sources"].append(source["digest"])
            handles[actual["id"]]={name:actual[name] for name in ("id","generation","digest")}
        else:c.reject("Unknown execution dependency kind",code="conflict")
    if len(handles)>16 or len(result["sources"])>16:c.reject("Execution output witness exceeds16 sources/handles",code="resource_limit")
    result["handles"]=list(handles.values());c.encoded(result,maximum=8192)
    return result
