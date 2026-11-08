"""Discoverable run tools backed by the same operations as CLI and HTTP.

This module performs one explicit invocation. It does not own an Agent loop,
operation journal, retries or execution lifecycle. Discovery grants no rights.
"""
from demiflow.workbench.runs import operations as run_operations
from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError


def describe():
    result = []
    for spec in run_operations.OPERATIONS.values():
        properties, required = {}, []
        for name, field in spec.fields:
            kind = {'text': 'string', 'bool': 'boolean', 'int': 'integer', 'object': 'object'}.get(field.kind)
            schema = ({'anyOf': [{'type': 'string'}, {'type': 'object'}]} if kind is None else {'type': kind})
            if field.kind == 'int':
                schema.update(minimum=field.minimum, maximum=field.maximum)
            if field.kind == 'text':
                schema.update(minLength=1, maxLength=4096, description='At most 4096 UTF-8 bytes.')
            if field.nullable:
                schema = {'anyOf': [schema, {'type': 'null'}]}
            properties[name] = schema
            if field.required or name in spec.http_required or spec.name == 'prepare' and name == 'request_id':
                required.append(name)
        if spec.name == 'prepare':
            properties['request_id'] = {'type': 'string', 'minLength': 1, 'maxLength': 4096}
        result.append(dict(name='run.' + spec.name, version=run_operations.VERSION,
                           input_schema=dict(type='object', properties=properties, required=required, additionalProperties=False),
                           permission=spec.permission, effects=spec.effects, preconditions=spec.preconditions,
                           result=spec.result, retry=spec.retry, handler='run_operations.perform',
                           authority='Current authorization is rechecked; this description grants no access.'))
    bounded_json(result, 24576)
    return result


def invoke(name, root, arguments, *, authority=None):
    if type(name) is not str or not name.startswith('run.') or name[4:] not in run_operations.OPERATIONS:
        raise ForgeError('Unknown tool', code='invalid_request')
    operation = name[4:]
    spec = run_operations.OPERATIONS[operation]
    arguments = spec.validate(arguments)
    if (operation == 'prepare' and not arguments.get('request_id')
            or operation in {'start', 'cancel'} and 'expected_attempt' not in arguments):
        raise ForgeError('Tools require an explicit request/attempt identity', code='invalid_request')
    # Preserve the original result and errors, including partial resume and
    # unknown launch. No assertion of exactly-once execution or safe retry.
    return run_operations.perform(operation, root, authority=authority, **arguments)
