"""Case pages bound to the fixed source, full view, query and annotation cut.

Cursor digests are consistency fences, not authority or credentials. Every
page still resolves its project-owned source and checks fresh read access.
Private tables produce a controller-only fixed native read admission; no native decoder runs in metadata.
"""
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.source_query import request


def native_config(value):
    """Small closed request validation; no native decoder import."""
    from demiflow.workbench.projects.registry import identity
    from demiflow.workbench.analysis.oci import object_id
    from demiflow.workbench.analysis.case_query import specification
    c.fields(value,{"version","operation","witness","query"},label="private Case worker request")
    if type(value["version"]) is not int or value["version"]!=1 or value["operation"]!="case-page":c.reject("Private Case worker protocol differs")
    witness=value["witness"];c.fields(witness,{"project","artifact","source"},label="private Case witness");identity(witness["project"])
    artifact=witness["artifact"];c.fields(artifact,{"id","generation","digest"},label="private Case object")
    object_id(artifact["id"]);c.integer(artifact["generation"],"private Case object generation",2**31-1,1);c.sha(artifact["digest"])
    source=witness["source"];checked=c.source_snapshot({key:item for key,item in source.items() if key!="digest"})
    if (checked!=source or source["kind"]!="pinned_object" or source["retention"]!="pinned" or source["binding"]!=artifact["digest"]):
        c.reject("Private Case object/source witness differs",code="conflict")
    if "scope" not in value["query"]:c.reject("Private Case scope must be explicit")
    specification(value["query"],source);c.encoded(value,maximum=48*1024)
    return value


def prepare(records,authority,source,payload):
    access=authority.access()
    view=payload["view"]
    c.fields(view,{"id","revision","descriptor"},label="Case page view basis")
    descriptor=c.case_view(view["descriptor"],source)
    if view["id"] is None:
        if type(view["revision"]) is not int or view["revision"]!=0:c.reject("Local Case view has no saved revision")
    else:
        saved=records.view(access,view["id"],revision=view["revision"])
        if saved["document"]!=descriptor:c.reject("Case page differs from its saved view revision",code="conflict")
    view_digest=c.digest(dict(id=view["id"],revision=view["revision"],descriptor=descriptor))
    if type(payload['query']) is dict and 'annotation_rows' in payload['query']:
        c.reject('Annotation row selection is issued by the server, not a Case query parameter')
    query=request(payload["query"],source)
    if query["kind"]!="cases":c.reject("Case page accepts declarative Case reads only")
    if query.get("offset",0)!=0:c.reject("Subsequent Case pages require their bound cursor")
    expected=list(dict.fromkeys(field["field"] for field in descriptor["fields"]))
    if (query["columns"]!=expected or query.get("filters")!=descriptor["filters"]
            or query.get("grouping",[])!=descriptor["grouping"] or query.get("sort",[])!=descriptor["sort"]):
        c.reject("Case read differs from its view projection/filter/group/order",code="conflict")
    query=dict(kind="cases",columns=query["columns"],filters=query.get("filters"),grouping=query.get("grouping",[]),
        sort=query.get("sort",[]),scope=query.get("scope","full"),limit=query.get("limit",20))
    digest_basis=dict(source=source["digest"],view=view_digest,query=query)
    review=None
    if 'review' in payload:
        from demiflow.workbench.analysis.case_review import validate
        from demiflow.workbench.analysis.case_review import principal_scope
        review=validate(payload['review'],source,descriptor)
        digest_basis.update(review=review,principal=principal_scope(access))
    query_digest=c.digest(digest_basis)
    cursor=payload.get("cursor")
    if cursor is not None:
        c.fields(cursor,{"format","source_snapshot","view_digest","query_digest","annotation_watermark","scope_digest","offset"},label="Case cursor")
        for key in ("source_snapshot","view_digest","query_digest","scope_digest"):c.sha(cursor[key],"Case cursor "+key)
        c.integer(cursor["offset"],"Case cursor offset",1000,1)
        c.integer(cursor["annotation_watermark"],"Case cursor annotation watermark",2**63-1)
        if (cursor["format"]!="demiflow_case_cursor_v1" or cursor["source_snapshot"]!=source["digest"]
                or cursor["view_digest"]!=view_digest or cursor["query_digest"]!=query_digest):
            c.reject("Case cursor belongs to another source/view/query; begin a new query",code="conflict")
    context=dict(source_snapshot=source["digest"],view_digest=view_digest,query_digest=query_digest,cursor=cursor,
        query={**query,"offset":cursor["offset"] if cursor else 0})
    if review is not None:
        from demiflow.workbench.analysis.case_review import selection
        watermark=cursor['annotation_watermark'] if cursor else None
        freeze=getattr(authority,'case_review_watermark',None)
        if watermark is None and callable(freeze):
            access.check()
            def read_cut():
                with records.database.locked() as db:
                    return db.execute('SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?',(access.project,)).fetchone()[0]
            watermark=freeze(query_digest,read_cut)
        selected,report=selection(records,access,source,review,descriptor,
            watermark=watermark)
        context['query']['annotation_rows']=selected;context['review']=report
    return context


def page(records,authority,source,origin,payload):
    context=prepare(records,authority,source,payload)
    if origin["kind"]=="private":
        from demiflow.workbench.analysis.operations import reference
        with records.database.locked() as db:
            actual=reference(db,authority.project,origin["handle"])
        if actual["digest"]!=source["binding"] or actual["retention"]!="pinned":
            c.reject("Pinned Case source no longer matches its registered object",code="conflict")
        artifact={key:actual[key] for key in ("id","generation","digest")}
        config=dict(version=1,operation="case-page",witness=dict(project=authority.project,artifact=artifact,source=source),query=context["query"])
        native_config(config)
        result=dict(format="demiflow_private_case_read_v1",config=config,context=context)
        c.encoded(result,maximum=56*1024);return result
    if origin["kind"]!="external":c.reject("Unknown registered Case source")
    value=authority.call("source-query",dict(source_locator=origin["locator"],source_snapshot=source["digest"],query=context["query"],purpose="catalog"))
    return finish(records,authority,source,context,value)


def finish(records,authority,source,context,value):
    access=authority.access();cursor=context["cursor"];view_digest=context["view_digest"];query_digest=context["query_digest"]
    if (type(value) is not dict or value.get("source_snapshot")!=source["digest"] or value.get("kind")!="cases"):
        c.reject("Case page belongs to another fixed source",code="conflict")
    if (value.get("columns")!=context["query"]["columns"] or value.get("offset")!=context["query"]["offset"]
            or value.get("coverage",{}).get("requested_scope")!=context["query"]["scope"]):
        c.reject("Case worker projection/offset/scope differs from the original query",code="conflict")
    c.sha(value.get("scope_digest"),"Case scope digest")
    if cursor and cursor["scope_digest"]!=value["scope_digest"]:
        c.reject("Case query population changed between pages",code="conflict")
    review=context.get('review')
    if review is not None and value.get('annotation_selection_digest')!=review['selection_digest']:
        c.reject('Case reader omitted or changed its server annotation selection',code='conflict')
    access.check()
    with records.database.locked() as db:
        current=db.execute("SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?",(access.project,)).fetchone()[0]
        watermark=review['annotation_watermark'] if review is not None else cursor["annotation_watermark"] if cursor else current
        if watermark>current:c.reject("Case cursor annotation cut is unavailable",code="conflict")
    basis=dict(format="demiflow_case_cursor_v1",source_snapshot=source["digest"],view_digest=view_digest,
        query_digest=query_digest,annotation_watermark=watermark,scope_digest=value["scope_digest"])
    result=dict(value,view_digest=view_digest,query_digest=query_digest,annotation_watermark=watermark,
        **({'review':review} if review is not None else {}),
        next_cursor=None if value["next_offset"] is None else dict(basis,offset=value["next_offset"]))
    c.encoded(result)
    return result
