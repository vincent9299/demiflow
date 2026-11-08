"""Immutable Case-definition origins proven by a trusted output read job.

No public endpoint accepts a witness/projection as evidence. Controller read
completion supplies its admitted registered_witness and matching worker page.
This module only validates small metadata and writes the private fact store;
it never parses the original16MiB Jupyter document or starts a job.
"""
import copy
import re

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.records import _source
from demiflow.workbench.analysis.sessions import _execution

CASE_MIME="application/vnd.demiflow.case-view+json"


def binding_status(records,access,*,execution_id,expected_output_id,expected_binding):
    c.identity(execution_id);c.identity(expected_output_id);c.sha(expected_binding)
    access.check()
    with records.database.locked() as db:
        execution=_execution(db,access.project,execution_id)
        row=db.execute("SELECT execution_id,manifest_digest FROM analysis_objects WHERE project=? AND id=? AND state='ready'",
            (access.project,expected_output_id)).fetchone()
        if (execution["state"]!="finished" or execution["output_id"]!=expected_output_id or row is None
                or row["execution_id"]!=execution_id or row["manifest_digest"]!=expected_binding):
            c.reject("Fixed output binding is unavailable or changed",code="conflict")
        access.check();return dict(verified=True)


def binding(witness,index,descriptor):
    c.integer(index,"Case output index",255)
    return dict(artifact_id=witness["artifact"]["id"],manifest_sha256=witness["artifact"]["digest"],
        output_index=index,descriptor_sha256=c.digest(descriptor),source_snapshot=descriptor["source_snapshot"],
        execution_id=witness["provenance"]["execution_id"],kernel_generation=witness["provenance"]["kernel_generation"],
        question_sha256=[c.digest(question) for question in descriptor["questions"]])


def current(db,access,witness):
    artifact=witness["artifact"];provenance=witness["provenance"]
    if provenance["project"]!=access.project:c.reject("Output witness belongs to another project",code="conflict")
    execution=_execution(db,access.project,provenance["execution_id"])
    if (execution["state"]!="finished" or execution["output_id"]!=artifact["id"]
            or execution["generation"]!=provenance["kernel_generation"]):
        c.reject("Original output execution witness changed",code="conflict")
    row=db.execute("SELECT execution_id,generation,manifest_digest,manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'",
        (access.project,artifact["id"])).fetchone()
    if (row is None or row["execution_id"]!=execution["id"] or row["manifest_digest"]!=artifact["digest"]
            or type(artifact["generation"]) is not int or row["generation"]!=artifact["generation"]
            or provenance["output_generation"]!=row["generation"]):
        c.reject("Original output object is no longer ready/bound",code="conflict")
    manifest=c.decode(bytes(row["manifest"]))
    if c.digest(manifest)!=artifact["digest"] or manifest["payload"].get("sha256")!=artifact["sha256"]:
        c.reject("Original output manifest witness differs",code="conflict")


def register(records,access,*,witness,projection):
    """Called only after current registered job/peer/frame identity checks."""
    c.encoded(projection,maximum=56*1024)
    if projection.get("version")!="output_v1" or projection.get("artifact")!=witness["artifact"] or projection.get("provenance")!=witness["provenance"]:
        c.reject("Output page differs from its original admitted witness",code="conflict")
    result=copy.deepcopy(projection);c.sequence(result.get("items"),"output page items",16)
    access.check()
    with records.database.transaction(payload_bytes=128*1024) as db:
        current(db,access,witness)
        for item in result["items"]:
            for representation in c.sequence(item.get("representations",[]),"output alternatives",16):
                if representation.get("mime")!=CASE_MIME or representation.get("available") is not True:continue
                descriptor=representation.get("value")
                if type(descriptor) is not dict:c.reject("Case representation omitted its descriptor")
                source=_source(db,access.project,descriptor.get("source_snapshot"))
                if witness["sources"].get(source["digest"])!=source:
                    c.reject("Case representation source is outside the proven execution dependencies",code="conflict")
                c.case_view(descriptor,source)
                proof=binding(witness,item["index"],descriptor);origin_id="no"+c.digest(proof)
                offered=representation.get("definition_witness")
                if offered is None or offered.get("binding")!=proof or offered.get("origin")!={"kind":"notebook_output","reference":origin_id}:
                    c.reject("Case definition witness differs from its registered output",code="conflict")
                raw_descriptor,raw_binding=c.encoded(descriptor),c.encoded(proof)
                previous=db.execute("SELECT descriptor,binding FROM analysis_output_presentations WHERE project=? AND origin_id=?",(access.project,origin_id)).fetchone()
                if previous is not None:
                    if bytes(previous["descriptor"])!=raw_descriptor or bytes(previous["binding"])!=raw_binding:
                        c.reject("Immutable output presentation changed",code="conflict")
                else:
                    if (db.execute("SELECT count(*) FROM analysis_output_presentations").fetchone()[0]>=4096
                            or db.execute("SELECT count(*) FROM analysis_output_presentations WHERE project=?",(access.project,)).fetchone()[0]>=512
                            or db.execute("SELECT count(*) FROM analysis_output_presentations WHERE project=? AND output_id=?",(access.project,witness["artifact"]["id"])).fetchone()[0]>=32):
                        c.reject("Output presentation registry quota is full",code="resource_limit")
                    for question in descriptor["questions"]:records._definition(db,access.project,question)
                    db.execute("INSERT INTO analysis_output_presentations VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (access.project,origin_id,proof["execution_id"],proof["artifact_id"],proof["manifest_sha256"],proof["output_index"],
                         proof["descriptor_sha256"],proof["source_snapshot"],raw_descriptor,raw_binding))
                from demiflow.workbench.analysis.case_groups import register as register_groups
                from demiflow.workbench.analysis.records import _time
                register_groups(records,db,access,descriptor,source,
                    origin=dict(kind="notebook_output",reference=origin_id),now=_time())
                representation["definition_witness"]={"origin":{"kind":"notebook_output","reference":origin_id},"binding":proof,"registered":True}
                representation["origin"]={"kind":"notebook_output","reference":origin_id}
        current(db,access,witness)
        c.encoded(result,maximum=56*1024);access.check()
    return result


def require(db,access,reference,source,definitions):
    if type(reference) is not str or not re.fullmatch(r"no[a-f0-9]{64}",reference):
        c.reject("Notebook output origin must identify its registered immutable representation")
    row=db.execute("SELECT descriptor,binding,source_snapshot,descriptor_digest FROM analysis_output_presentations WHERE project=? AND origin_id=?",
        (access.project,reference)).fetchone()
    if row is None:c.reject("Notebook output definition origin is not registered",code="operation_rejected")
    descriptor=c.decode(bytes(row["descriptor"]));proof=c.decode(bytes(row["binding"]))
    if (row["source_snapshot"]!=source["digest"] or descriptor.get("source_snapshot")!=source["digest"]
            or c.digest(descriptor)!=row["descriptor_digest"] or proof["descriptor_sha256"]!=row["descriptor_digest"]
            or "no"+c.digest(proof)!=reference or not definitions<=set(proof["question_sha256"])
            or proof["question_sha256"]!=[c.digest(question) for question in descriptor["questions"]]):
        c.reject("Question/source is not part of the registered output representation",code="conflict")

    return descriptor
