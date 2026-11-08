"""Fixed Case media metadata from the existing ready artifact ledger.

No byte/codec IO, path authority or implicit capture. Current source resource
proof comes from the existing bounded Web reader through live authority.
"""
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.registered_media import reference
from demiflow.workbench.analysis.registered_media import require_basis
from demiflow.workbench.analysis.object_files import candidate_manifest


def _registered(db,access,source,ref):
    target=dict(source_snapshot=source['digest'],case_key=ref['case_key'],variant=None,field=None)
    witness=require_basis(db,access,source,target,[ref])[ref['field']]
    row=db.execute("SELECT manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'",(access.project,ref['artifact_id'])).fetchone()
    manifest=c.decode(bytes(row['manifest']))
    seed={key:manifest[key] for key in ('project','object_id','generation','retention','execution_id','parents','source_binding')}
    if (candidate_manifest(seed,manifest['payload'],manifest['files'])!=manifest
            or seed['object_id']!=ref['artifact_id'] or seed['generation']!=ref['artifact_generation']
            or seed['source_binding']!=source['digest']):
        c.reject('Fixed media canonical registration differs',code='conflict')
    value=manifest['payload']
    artifact=dict(project=access.project,id=ref['artifact_id'],generation=ref['artifact_generation'],digest=ref['expected_binding'],
        **{key:value[key] for key in ('sha256','bytes','mime','descriptor','chunk_sha256')},source_digest=c.digest(witness))
    return dict(reference=ref,source=witness,artifact=artifact)


def registered_witness(records,sources,authority,ref):
    reference(ref);access=authority.access();access.check()
    source,_=sources.registered(access,ref['source_snapshot'])
    with records.database.locked() as db:value=_registered(db,access,source,ref)
    current=sources.media_sources(authority,source,ref['case_key'],[ref['field']])[0]
    if current['source']!=value['source'] or current['mime']!=value['artifact']['mime']:
        c.reject('Current source resource declaration differs from the fixed media',code='conflict')
    access.check();return value


def lookup(records,sources,authority,*,digest,case_key,field):
    from demiflow.forge.schema import ForgeError
    c.sha(digest);c.text(field,'fixed Case media field',256)
    access=authority.access();access.check();source,_=sources.registered(access,digest)
    try:current=sources.media_sources(authority,source,case_key,[field])[0]
    except ForgeError as error:
        if error.code!='operation_rejected':raise
        access.check()
        return dict(status='unavailable',reason='resource_unavailable',message=str(error)[:512])
    with records.database.locked() as db:
        row=db.execute("SELECT capture_id FROM analysis_media_captures WHERE project=? AND source_snapshot=? AND case_key=? AND field=? AND resource_binding=? AND state='ready' ORDER BY updated DESC,capture_id DESC LIMIT 1",
            (access.project,digest,case_key,field,current['source']['resource_binding'])).fetchone()
        if row is None:
            access.check();return dict(status='capture_required')
        from demiflow.workbench.analysis.media_captures import _get
        capture=_get(db,row['capture_id']);artifact=db.execute("SELECT generation,manifest_digest,manifest FROM analysis_objects WHERE project=? AND id=? AND state='ready'",(access.project,capture['object_id'])).fetchone()
        if artifact is None:c.reject('Original capture is missing its ready object',code='conflict')
        media=c.decode(bytes(artifact['manifest']))['payload']
        ref=dict(source_snapshot=digest,case_key=case_key,field=field,artifact_id=capture['object_id'],artifact_generation=artifact['generation'],expected_binding=artifact['manifest_digest'],expected_sha256=media['sha256'],mime=media['mime'])
        value=_registered(db,access,source,reference(ref))
        if value['source']!=current['source'] or value['artifact']['mime']!=current['mime']:
            c.reject('Fixed media lookup differs from its current source',code='conflict')
        access.check();return dict(status='ready',reference=value['reference'],descriptor=value['artifact']['descriptor'])
