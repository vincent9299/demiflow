"""Native reading API declaration and document-scope validation."""
from .documents import canonical
from .reading_contract import DOCUMENT, READ_DOCUMENTS_DESCRIPTION, read_documents_arguments
from demiflow.objects import ObjectRef
from demiflow.schema import validate_instance


def validate_resources(resources, limits):
    for record in resources.values():
        if not isinstance(record, dict) or set(record) != {'document_ref', 'url', 'bindings', 'eligible'}:
            raise ValueError('Resources must be native read_documents document records')
        validate_instance(record, DOCUMENT, label='environment resource')
        ObjectRef(**record['document_ref'])


def validate_arguments(arguments, resources, limits):
    request = arguments['request']
    if any(request[key] > limits.max_material_chars for key in ('new_chars', 'total_chars')):
        raise ValueError('Material budget exceeds the declared environment limit')
    for key, limit in (('documents', 4), ('questions', 8), ('retained', 256), ('requests', 8)):
        if len(request.get(key, [])) > limit:
            raise ValueError(key + ' exceeds per-call resource limit')
    allowed = {canonical(record['document_ref']) for record in resources.values()}
    supplied = set()
    for record in request['documents']:
        reference = canonical(record['document_ref'])
        if reference not in allowed or reference in supplied:
            raise ValueError('Document outside this row or duplicate document')
        supplied.add(reference)
    for record in request.get('retained', []) + request.get('requests', []):
        if canonical(record['document_ref']) not in supplied:
            raise ValueError('Block request must reference a supplied document')
    for record in request.get('retained', []):
        if record['evidence_id'] != record['document_ref']['sha256'] + ':' + record['block_id']:
            raise ValueError('Retained evidence_id must match its document and block')


DEFINITION = dict(
    name='read_documents', function='demiflow.collect.reading:read_documents',
    version='2',
    arguments=read_documents_arguments('chars'), description=READ_DOCUMENTS_DESCRIPTION,
    bindings={'context': 'context', 'document_concurrency': 'limits.document_concurrency',
              'timeout_s': 'limits.timeout_s', 'max_bytes': 'limits.max_document_bytes'},
    validate_resources='demiflow.collect.reading_api:validate_resources',
    validate_arguments='demiflow.collect.reading_api:validate_arguments',
    requires_resources=True, replay='verify')
