"""Immutable output-group evidence in the existing document store/answer scope.

Only saved view and admitted Notebook presentation transactions register groups.
Resolving a target never registers data, executes code or grants row access.
"""
from __future__ import annotations

import re

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.records import Records
from demiflow.workbench.analysis.records import _document
from demiflow.workbench.analysis.records import _source

KIND = 'case_output_group'
SEMANTIC = 'demiflow_case_group_semantics_v1'
REGISTRATION = 'demiflow_case_group_registration_v1'
FORMAT = 'demiflow_case_group_bindings_v1'
VARIANT = re.compile(r'cg[0-9a-f]{64}\Z')


def definitions(descriptor, source):
    c.case_view(descriptor, source)
    if descriptor['version'] == 1:
        return []
    components = {item['id']: item for item in descriptor['fields']}
    result = []
    for group in descriptor['output_groups']:
        semantic = dict(format=SEMANTIC, source_snapshot=source['digest'], id=group['id'],
                        fields=sorted({components[name]['field'] for name in group['fields']}))
        c.encoded(semantic, maximum=16*1024)
        result.append((group, semantic, 'cg'+c.digest(semantic)))
    return result


def _origin_descriptor(db, access, source, registration):
    c.fields(registration, {'kind', 'reference', 'descriptor_sha256'}, label='group registration')
    c.one_of(registration['kind'], {'saved_view', 'notebook_output'}, 'group registration kind')
    c.sha(registration['descriptor_sha256']);c.text(registration['reference'], 'group registration reference', 256)
    if registration['kind'] == 'saved_view':
        identifier, separator, raw = registration['reference'].partition('@')
        if not separator or not raw.isascii() or not raw.isdecimal() or len(raw)>10 or str(int(raw))!=raw:
            c.reject('Group registration must identify an immutable view version')
        c.integer(int(raw), 'registered view revision', 2**31-1, 1)
        saved = _document(db, access.project, 'case_view', identifier, int(raw))
        descriptor = saved['document']
        if saved['digest'] != c.digest(descriptor):
            c.reject('Immutable group view digest differs', code='conflict')
    else:
        from demiflow.workbench.analysis.presentations import require
        descriptor = require(db, access, registration['reference'], source, set())
    c.case_view(descriptor, source)
    if descriptor['version'] != 2 or c.digest(descriptor) != registration['descriptor_sha256']:
        c.reject('Registered output group descriptor differs', code='conflict')
    return descriptor


def require(db, access, source, variant):
    c.text(variant, 'registered group variant', 96)
    if not VARIANT.fullmatch(variant):
        c.reject('Annotation variant has no registered output-group namespace', code='operation_rejected')
    size = db.execute('SELECT length(r.body) FROM documents d JOIN document_revisions r '
        'ON r.project=d.project AND r.kind=d.kind AND r.id=d.id AND r.revision=d.revision '
        'WHERE d.project=? AND d.kind=? AND d.id=?', (access.project, KIND, variant)).fetchone()
    if size is not None and size[0] > 16*1024:
        c.reject('Immutable output-group evidence exceeds its bound', code='resource_limit')
    stored = _document(db, access.project, KIND, variant)
    document = stored['document']
    c.fields(document, {'format', 'semantic', 'registration'}, label='immutable output group')
    if stored['revision'] != 1 or document['format'] != REGISTRATION or c.digest(document) != stored['digest']:
        c.reject('Immutable output-group registration changed', code='conflict')
    semantic = document['semantic']
    c.fields(semantic, {'format', 'source_snapshot', 'id', 'fields'}, label='output-group semantics')
    if semantic['format'] != SEMANTIC or semantic['source_snapshot'] != source['digest']:
        c.reject('Registered output group belongs to another source', code='conflict')
    c.identity(semantic['id'], 'output group ID')
    members = c.sequence(semantic['fields'], 'group source fields', 16, minimum=1)
    known = {item['name'] for item in source['schema']}
    for name in members:
        c.text(name, 'group source field', 256)
        if name not in known:c.reject('Registered group field is outside its source', code='conflict')
    if members != sorted(set(members)) or variant != 'cg'+c.digest(semantic):
        c.reject('Registered group semantics do not match their identity', code='conflict')
    descriptor = _origin_descriptor(db, access, source, document['registration'])
    matched = next((group for group, normalized, identity in definitions(descriptor, source)
                    if identity == variant and normalized == semantic), None)
    if matched is None:
        c.reject('Group is not in its immutable registration evidence', code='conflict')
    return dict(id=semantic['id'], label=matched['label'], definition=semantic,
                registration=document['registration'])


def register(records, db, access, descriptor, source, *, origin, now):
    """Internal: caller is in the original trusted registration transaction."""
    groups = definitions(descriptor, source)
    if not groups:return
    registration = dict(kind=origin['kind'], reference=origin['reference'], descriptor_sha256=c.digest(descriptor))
    actual = _origin_descriptor(db, access, source, registration)
    if actual != descriptor:c.reject('Output-group registration descriptor differs', code='conflict')
    for group, semantic, variant in groups:
        previous = db.execute('SELECT revision FROM documents WHERE project=? AND kind=? AND id=?',
                              (access.project, KIND, variant)).fetchone()
        if previous is not None:
            if require(db, access, source, variant)['definition'] != semantic:
                c.reject('Registered output group changed', code='conflict')
            continue
        if (db.execute('SELECT count(*) FROM documents WHERE kind=?', (KIND,)).fetchone()[0] >= 4096
                or db.execute('SELECT count(*) FROM documents WHERE project=? AND kind=?',
                              (access.project, KIND)).fetchone()[0] >= 512):
            c.reject('Immutable output-group quota is full; existing history is retained', code='resource_limit')
        value = dict(format=REGISTRATION, semantic=semantic, registration=registration)
        c.encoded(value, maximum=16*1024)
        records._put_document(db, access, KIND, variant, 0, value, now)


def require_target(db, access, source, target, *, origin=None):
    """Reads validate durable semantics; writes also use live Sources.verify."""
    Records._target(source, target)
    if target['variant'] is None:return None
    if target['field'] is not None:
        c.reject('Output-group targets cannot borrow a field scope', code='conflict')
    proof = require(db, access, source, target['variant'])
    if origin is not None and origin['kind'] in {'saved_view', 'notebook_output'}:
        # Question provenance and group provenance are separate, but a claimed
        # immutable display origin may not silently substitute a different group.
        if origin['kind'] == 'saved_view':
            identifier, separator, raw = origin['reference'].partition('@')
            if not separator or not raw.isascii() or not raw.isdecimal() or len(raw)>10 or str(int(raw))!=raw:
                c.reject('Saved group answer origin must name an immutable view')
            descriptor = _document(db, access.project, 'case_view', identifier, int(raw))['document']
        else:
            from demiflow.workbench.analysis.presentations import require as require_presentation
            descriptor = require_presentation(db, access, origin['reference'], source, set())
        if not any(variant == target['variant'] and semantic == proof['definition']
                   for _, semantic, variant in definitions(descriptor, source)):
            c.reject('Answer group is outside its immutable question/view origin', code='conflict')
    return proof


def _entry(proof, target, label=None):
    return dict(id=proof['id'], label=proof['label'] if label is None else label, registered=True,
                variant=target['variant'], target=target, definition=proof['definition'],
                registration=proof['registration'])


def media_fields(db, access, source, target, origin):
    """New explicit group evidence is its members plus verified shared input."""
    proof=require_target(db,access,source,target,origin=origin)
    descriptor=_origin_descriptor(db,access,source,proof['registration'])
    if origin['kind']=='saved_view':
        identifier,_,revision=origin['reference'].partition('@')
        descriptor=_document(db,access.project,'case_view',identifier,int(revision))['document']
    elif origin['kind']=='notebook_output':
        from demiflow.workbench.analysis.presentations import require as require_presentation
        descriptor=require_presentation(db,access,origin['reference'],source,set())
    return set(proof['definition']['fields'])|{item['field'] for item in descriptor['fields']
        if item['role'] in {'input','reference','context'}}


def bindings(records, access, *, digest, case_key, view=None, variant=None):
    access.check()
    if (view is None) == (variant is None):
        c.reject('Choose exactly one current view or historical registered group')
    with records.database.locked() as db:
        source = _source(db, access.project, digest)
        base = dict(source=digest, case_key=case_key, variant=None, field=None)
        Records._target(source, base)
        entries = [];view_digest = None
        if variant is not None:
            proof = require(db, access, source, variant)
            entries.append(_entry(proof, {**base, 'variant':variant}))
        else:
            c.fields(view, {'id', 'revision', 'descriptor'}, label='Case group view basis')
            descriptor = c.case_view(view['descriptor'], source)
            if view['id'] is None:
                if type(view['revision']) is not int or view['revision'] != 0:
                    c.reject('Local output-group view has no saved revision')
            else:
                saved = _document(db, access.project, 'case_view', view['id'], view['revision'])
                if saved['document'] != descriptor:
                    c.reject('Output-group view differs from its immutable revision', code='conflict')
            view_digest = c.digest(view)
            for group, semantic, identity in definitions(descriptor, source):
                known = db.execute('SELECT revision FROM documents WHERE project=? AND kind=? AND id=?',
                                   (access.project, KIND, identity)).fetchone()
                if known is None:
                    entries.append(dict(id=group['id'], label=group['label'], registered=False,
                                        variant=None, target=None, reason='save_or_register_required'))
                else:
                    proof = require(db, access, source, identity)
                    if proof['definition'] != semantic:
                        c.reject('Current output-group semantics differ', code='conflict')
                    entries.append(_entry(proof, {**base, 'variant':identity}, group['label']))
        result = dict(format=FORMAT, source_snapshot=digest, case_key=case_key,
                      view_digest=view_digest, groups=entries)
        c.encoded(result, maximum=48*1024);access.check()
        return result
