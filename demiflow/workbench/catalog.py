"""Declarative Case presentation and access to explicitly bound local resources.

These declarations never change Dataset identity or execution semantics. The
workspace worker rereads the fixed row before following a resource reference;
HTTP callers supply a field path, never a filesystem path or a request record.
"""
from pathlib import Path
import re
from urllib.parse import urlsplit
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlunsplit

from demiflow.forge.evidence import read_evidence, redact, validate_reference
from demiflow.forge.schema import ForgeError
from demiflow.forge.platform import bounded_json


def field_path(value, *, pattern=False):
    if (type(value) is not list or not 1 <= len(value) <= 8 or type(value[0]) is not str
            or value[0] == '*' or any(not (type(v) is str and 1 <= len(v) <= 128 and
            not re.search(r'[\x00-\x1f]', v) and (pattern or v != '*') or
            type(v) is int and 0 <= v < 10000) for v in value)):
        raise ForgeError('Field paths require 1..8 bounded names or list indices')
    return value


def validate(display, provenance, locations):
    if (type(display) is not dict or set(display) - {'title', 'fields'}
            or type(provenance) is not dict or set(provenance) - {'kind', 'note', 'source'}):
        raise ForgeError('Unknown table presentation or provenance fields')
    for key, value in [('title', display.get('title', '')), ('note', provenance.get('note', '')),
                       ('source', provenance.get('source', ''))]:
        if type(value) is not str or len(value) > (4096 if key == 'note' else 512):
            raise ForgeError('Table presentation text exceeds budget')
    if provenance and provenance.get('kind') != 'historical':
        raise ForgeError('Attached provenance can describe historical data, not certify a managed run')
    fields = display.get('fields', [])
    if type(fields) is not list or len(fields) > 16:
        raise ForgeError('Table display supports at most 16 fields')
    seen = set()
    for field in fields:
        if (type(field) is not dict or set(field) - {'path', 'label', 'kind', 'location_id', 'sha256_path', 'relocations', 'max_bytes','mime'}
                or not {'path', 'kind'} <= field.keys() or field['kind'] not in {'text', 'json', 'image', 'audio','video','evidence'}):
            raise ForgeError('Invalid table display field')
        path = field_path(field['path'], pattern=True)
        if tuple(path) in seen:
            raise ForgeError('Duplicate display field path')
        seen.add(tuple(path))
        if type(field.get('label', '')) is not str or len(field.get('label', '')) > 128:
            raise ForgeError('Invalid display field label')
        resource_keys = {'location_id', 'sha256_path', 'relocations', 'max_bytes','mime'} & field.keys()
        if field['kind'] in {'text', 'json'}:
            if resource_keys:
                raise ForgeError('Plain display fields cannot grant resource access')
            continue
        if field.get('location_id') not in locations:
            raise ForgeError('Media/evidence field needs an explicitly registered location')
        media=field['kind'] in {'image','audio','video'}
        maximum = {'image':8*1024**2,'audio':16*1024**2,'video':64*1024**2}.get(field['kind'],128*1024**3)
        if type(field.get('max_bytes')) is not int or not 1 <= field['max_bytes'] <= maximum:
            raise ForgeError('Image/evidence resource needs a finite byte budget')
        if media:
            hashes = field_path(field.get('sha256_path'), pattern=True)
            if [i for i, part in enumerate(path) if part == '*'] != [i for i, part in enumerate(hashes) if part == '*']:
                raise ForgeError('Image and SHA256 paths must have matching list positions')
            allowed={'image':{'image/png','image/jpeg','image/webp','image/gif'},'audio':{'audio/wav','audio/mpeg','audio/ogg','audio/webm'},'video':{'video/mp4','video/webm','video/ogg'}}
            if ('mime' in field or field['kind']!='image') and field.get('mime') not in allowed[field['kind']]:
                raise ForgeError('Media field needs a supported declared MIME')
        elif 'sha256_path' in field:
            raise ForgeError('Evidence uses its native request identity, not an invented content hash')
        elif 'mime' in field:raise ForgeError('Evidence does not declare media MIME')
        roots = field.get('relocations', [])
        if type(roots) is not list or len(roots) > 16:
            raise ForgeError('Too many explicit resource relocations')
        for root in roots:
            if type(root) is not str or len(root) > 2048 or not Path(root).is_absolute() or '..' in Path(root).parts:
                raise ForgeError('Resource relocation must name an absolute directory')
        if any(Path(a).is_relative_to(Path(b)) for i, a in enumerate(roots) for j, b in enumerate(roots) if i != j):
            raise ForgeError('Overlapping resource relocation roots')
    bounded_json({'display': display, 'provenance': provenance}, 12 * 1024)


def validate_fields(display, fields):
    """Validate presentation against actual fixed schema metadata, without rows."""
    def type_at(path):
        current = {'kind': 'record', 'fields': fields}
        for part in path:
            if current['kind'] == 'record' and type(part) is str and part in current['fields']:
                current = current['fields'][part]['type']
            elif current['kind'] == 'list' and (part == '*' or type(part) is int):
                current = current['item']
            else:
                raise ForgeError('Display path differs from fixed schema: ' + str(path))
        return current
    for field in display.get('fields', []):
        value = type_at(field['path'])
        if field['kind'] in {'image','audio','video'}:
            if value['kind'] != 'str' or type_at(field['sha256_path'])['kind'] != 'str':
                raise ForgeError('Image URI and hash fields must be strings')
        if field['kind'] == 'evidence':
            if value['kind'] != 'record' or not {'journal_path', 'request_id', 'kind'} <= value['fields'].keys():
                raise ForgeError('Evidence field must contain native PromptRecordRef fields')


def select_field(display, path, kind):
    field_path(path)
    choices = [field for field in display.get('fields', []) if field['kind'] == kind
               and len(path) == len(field['path']) and all(
                   type(actual) is int if expected == '*' else actual == expected and type(actual) is type(expected)
                   for expected, actual in zip(field['path'], path))]
    if len(choices) != 1:
        raise ForgeError('Resource field is not uniquely declared by this table')
    return choices[0]


def at(value, path):
    for part in path:
        if type(value) is dict and type(part) is str and part in value:
            value = value[part]
        elif type(value) is list and type(part) is int and 0 <= part < len(value):
            value = value[part]
        else:
            raise ForgeError('Declared Case field is absent')
    return value


def image(row, path, field, base):
    from demiflow.objects import local_object_uri
    from demiflow.objects import read_local_object
    from demiflow.lance.preview import image_bytes_preview
    hashes = [path[i] if part == '*' else part for i, part in enumerate(field['sha256_path'])]
    uri, sha = at(row, path), at(row, hashes)
    if type(uri) is not str:
        raise ForgeError('Image field must contain a local path or file URI')
    if not urlsplit(uri).scheme:
        uri = (base / uri).as_uri()
    ref = {'uri': uri, 'sha256': sha}
    resolved = local_object_uri(ref, root=base, relocations=field.get('relocations', []))
    raw = read_local_object(resolved, sha, root=base, max_bytes=field['max_bytes'])
    return {**image_bytes_preview(raw), 'media_identity': 'sha256:' + sha}


def evidence(row, path, field, base):
    from demiflow.forge.io import no_symlinks
    from demiflow.execution.host.inspection import resolve_local_artifact
    reference = at(row, path)
    validate_reference(reference)
    target = Path(reference['journal_path'])
    target = target if target.is_absolute() else base / target
    roots = [base, *(Path(r) for r in field.get('relocations', []))]
    matches = [root for root in roots if target.is_relative_to(root)]
    if '..' in target.parts or len(matches) != 1:
        raise ForgeError('Evidence must match exactly one declared root')
    target = base / target.relative_to(matches[0])
    # Native exact location manifests may relocate a historical file; validate
    # the resolved target too. No basename or undeclared directory fallback.
    no_symlinks(target)
    target = resolve_local_artifact(target)
    no_symlinks(target)
    if not target.is_relative_to(base) or not target.is_file() or target.stat().st_size > field['max_bytes']:
        raise ForgeError('Historical evidence is outside its declared bounded location')
    return {'reference': reference, 'format': 'demiflow_prompt_sqlite_v1',
            'document': read_evidence(reference, target)}
