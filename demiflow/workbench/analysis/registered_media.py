"""Closed immutable media metadata; no decoder, source IO or authority grant.

Capture/export/probe owns the proof. This small validator is used by the same
canonical artifact publication protocol and by historical current-read checks.
"""
from demiflow.workbench.analysis import contracts as c

REFERENCE={"source_snapshot","case_key","field","artifact_id","artifact_generation","expected_binding","expected_sha256","mime"}


def reference(value):
    from demiflow.workbench.analysis.oci import object_id
    c.fields(value,REFERENCE,label="fixed Case media reference")
    for key in ("source_snapshot","expected_binding","expected_sha256"):c.sha(value[key],"media reference "+key)
    object_id(value["artifact_id"]);c.integer(value["artifact_generation"],"media artifact generation",2**31-1,1)
    c.text(value["field"],"media reference field",256)
    key=c.text(value["case_key"],"media reference Case",20)
    if not key.isascii() or not key.isdecimal() or str(int(key))!=key or int(key)>=2**64:c.reject("Media reference Case must be canonical uint64")
    c.one_of(value["mime"],{"image/png","image/jpeg","image/webp","image/gif","audio/wav","audio/mpeg","audio/ogg","audio/webm","video/mp4","video/webm","video/ogg"},"media reference MIME")
    c.encoded(value,maximum=2048);return value


def require_basis(db,access,source,target,refs):
    """Verify ready manifest facts inside the original annotation transaction.

    Returned source witnesses still need a current trusted resource check;
    manifest possession alone does not authorize external resource roots.
    """
    c.sequence(refs,"annotation media basis",16);c.encoded(refs,maximum=16*1024)
    result={};fields={item["name"] for item in source["schema"]}
    for ref in refs:
        reference(ref)
        if ref["source_snapshot"]!=source["digest"] or ref["case_key"]!=target["case_key"] or ref["field"] not in fields or ref["field"] in result:
            c.reject("Annotation media belongs to another source/Case/field",code="conflict")
        row=db.execute("SELECT generation,manifest_digest,manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'",(access.project,ref["artifact_id"])).fetchone()
        if row is None or row["generation"]!=ref["artifact_generation"] or row["manifest_digest"]!=ref["expected_binding"]:
            c.reject("Annotation media is not the registered ready object generation",code="conflict")
        manifest=c.decode(bytes(row["manifest"]))
        if c.digest(manifest)!=ref["expected_binding"] or manifest["project"]!=access.project or manifest["payload"].get("format")!="media":c.reject("Annotation media manifest differs",code="conflict")
        from demiflow.workbench.analysis.object_files import candidate_manifest
        seed={key:manifest[key] for key in ('project','object_id','generation','retention','execution_id','parents','source_binding')}
        if (candidate_manifest(seed,manifest['payload'],manifest['files'])!=manifest
                or seed['object_id']!=ref['artifact_id'] or seed['generation']!=ref['artifact_generation']
                or seed['source_binding']!=source['digest']):c.reject('Annotation media canonical object identity differs',code='conflict')
        media=payload(manifest["payload"],artifact_id=ref["artifact_id"])
        witness=media["source"]
        if (media["sha256"]!=ref["expected_sha256"] or media["mime"]!=ref["mime"]
                or witness["source_snapshot"]!=ref["source_snapshot"] or witness["case_key"]!=ref["case_key"] or witness["field"]!=ref["field"]):
            c.reject("Annotation media content or original resource witness differs",code="conflict")
        result[ref["field"]]=witness
    return result


def source_witness(value):
    c.fields(value,{"version","source_snapshot","case_key","field","declared_sha256","resource_binding"},label="fixed media source witness")
    if type(value["version"]) is not int or value["version"]!=1:c.reject("Fixed media source version differs")
    for key in ("source_snapshot","declared_sha256","resource_binding"):c.sha(value[key],"fixed media "+key)
    key=c.text(value["case_key"],"fixed media Case key",20)
    if not key.isascii() or not key.isdecimal() or str(int(key))!=key or int(key)>=2**64:c.reject("Fixed media Case key must be a canonical uint64 string")
    c.text(value["field"],"fixed media source field",256);c.encoded(value,maximum=2048)
    return value


def payload(value,*,artifact_id):
    from demiflow.workbench.analysis.media import verified_metadata
    c.fields(value,{"format","mime","bytes","sha256","complete","descriptor","source","chunk_sha256"},label="sealed media artifact payload")
    if value["format"]!="media" or value["complete"] is not True:c.reject("Media artifact requires a complete isolated probe before ready")
    source_witness(value["source"])
    if type(value["mime"]) is not str:c.reject("Media MIME is absent")
    kind=c.one_of(value["mime"].split('/')[0],{"image","audio","video"},"sealed media kind")
    c.integer(value["bytes"],"sealed media bytes",{"image":8,"audio":16,"video":64}[kind]*1024*1024,1);c.sha(value["sha256"])
    chunks=c.sequence(value["chunk_sha256"],"sealed media chunk hashes",64,minimum=1)
    if len(chunks)!=(value["bytes"]+1024*1024-1)//(1024*1024):c.reject("Sealed media chunk count differs from exact bytes",code="conflict")
    for digest in chunks:c.sha(digest,"sealed media chunk digest")
    descriptor=verified_metadata(value["descriptor"],kind)
    if (descriptor["artifact_id"]!=artifact_id or "output_index" in descriptor or descriptor["mime"]!=value["mime"]
            or descriptor["sha256"]!=value["sha256"] or descriptor["bytes"]!=value["bytes"]
            or value["sha256"]!=value["source"]["declared_sha256"]):
        c.reject("Sealed media probe/content/declaration identity differs",code="conflict")
    c.encoded(value,maximum=16*1024)
    return value
