"""Agent-facing run.* operation and tool contracts owned by Forge.

Pure schema, argument validation, HTTP wire decoding, and tool dispatch contract.
Execution authority and actual run execution are provided by the host (Forge runs
or Workbench runs) via explicit callback without reverse imports.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType

from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_run_operations_v1'
ARGUMENT_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Field:
    kind: str
    required: bool = False
    minimum: int = 0
    maximum: int = 2**64 - 1
    nullable: bool = False
    wire_name: str | None = None

    def check(self, name, value):
        if value is None and self.nullable:
            return
        valid = (type(value) is str and 0 < len(value.encode()) <= 4096
                 if self.kind == 'text' else
                 type(value) is bool if self.kind == 'bool' else
                 type(value) is int and self.minimum <= value <= self.maximum
                 if self.kind == 'int' else
                 type(value) is dict if self.kind == 'object' else
                 type(value) is dict or type(value) is str and 0 < len(value.encode()) <= 4096
                 if self.kind == 'parameters' else False)
        if not valid:
            raise ForgeError('Invalid run argument: ' + name, code='invalid_request')


@dataclass(frozen=True)
class OperationSpec:
    name: str
    handler: str
    method: str
    endpoint: str
    fields: tuple[tuple[str, Field], ...]
    permission: str
    effects: str
    preconditions: str
    result: str
    retry: str
    http_required: frozenset[str] = frozenset()

    def wire_fields(self):
        required, optional = set(), set()
        for name, field in self.fields:
            key = field.wire_name or name
            (required if field.required or name in self.http_required else optional).add(key)
        return required, optional

    def validate(self, arguments):
        if type(arguments) is not dict:
            raise ForgeError('Run arguments must be an object', code='invalid_request')
        fields = dict(self.fields)
        if arguments.keys() - fields.keys() or any(f.required and k not in arguments for k, f in self.fields):
            raise ForgeError('Missing or unknown run arguments', code='invalid_request')
        for key, value in arguments.items():
            fields[key].check(key, value)
        try:
            bounded_json(arguments, ARGUMENT_BYTES)
        except (ValueError, TypeError, RecursionError) as exc:
            if isinstance(exc, ForgeError):
                raise
            raise ForgeError('Invalid run argument JSON', code='invalid_request') from exc
        return dict(arguments)


_RUN = ('run_id', Field('text', required=True))
_EXPECTED = ('expected', Field('text', required=True))
_ATTEMPT = ('attempt', Field('int', minimum=1, maximum=32, nullable=True))
_CONTROL = ('expected_attempt', Field('int', minimum=1, maximum=32, nullable=True))

_SPECS = (
    OperationSpec('prepare', 'prepare', 'POST', 'run-prepare', (
        ('entry', Field('text', required=True)), ('params', Field('parameters', required=True)),
        ('inputs', Field('object', nullable=True)), ('outputs', Field('object', nullable=True)),
        ('overrides', Field('object', nullable=True)), ('output_prefix', Field('text', nullable=True)),
        ('limits', Field('object', nullable=True)), ('new_outputs', Field('bool')),
        ('telemetry', Field('bool')), ('request_id', Field('text', nullable=True)),
    ), 'run_submit', 'Seal inputs and create a prepared run; optional durable prepare receipt.',
        'Registered inputs, effective program check, scope, fixed sources, outputs and budgets.',
        'Original run view; prepared/ready is not execution success.',
        'Same request_id and fingerprint only; pending/unknown requires receipt query or explicit reconcile.'),
    OperationSpec('start', 'start', 'POST', 'run-start', (_RUN, _EXPECTED, _CONTROL),
        'run_submit', 'Admit and launch the original prepared attempt.',
        'Expected manifest and attempt, ready receipt, current basis, outputs, capacity and native fence.',
        'Original run view; admission, execution and cleanup remain distinct.',
        'Unknown launch: query the same run/attempt; never automatically relaunch.', frozenset({'expected_attempt'})),
    OperationSpec('show', 'show', 'GET', 'run', (_RUN, _ATTEMPT,
        ('include_report', Field('bool', wire_name='report')), ('compact', Field('bool'))),
        'read', 'Read only.', 'Current project access and exact selected run/attempt.',
        'Original run view, with requested historical report or compact projection.', 'Read again with the same target.'),
    OperationSpec('list', 'list_runs', 'GET', 'runs', (
        ('offset', Field('int')), ('limit', Field('int', minimum=1, maximum=100)),
        ('cursor', Field('text', nullable=True)),
    ), 'read', 'Read only; no implicit index rebuild or reconciliation.', 'Current project access and bounded page.',
        'Original runs/total/next_cursor/index.', 'Invalidated cursor requires an explicit fresh page.'),
    OperationSpec('logs', 'logs', 'GET', 'run-logs', (_RUN, _ATTEMPT, ('offset', Field('int'))),
        'read', 'Read only.', 'Current project access; selected attempt and byte offset.',
        'Original text/next_offset/truncated/evicted; existing 64 KiB domain page limit.',
        'Do not reuse an offset across attempts; retained history may be evicted.'),
    OperationSpec('cancel', 'cancel', 'POST', 'run-cancel', (_RUN, _CONTROL, ('compact', Field('bool'))),
        'run_control', 'Request stopping for the exact attempt.', 'Current control permission and attempt CAS; no development recheck.',
        'Original run view or compact snapshot; requested stop is not confirmed exit/cleanup.',
        'Query the same attempt after disconnection; never follow a changed current attempt.', frozenset({'expected_attempt'})),
    OperationSpec('resume', 'resume', 'POST', 'run-resume', (_RUN, _EXPECTED,
        ('expected_attempt', Field('int', required=True, minimum=1, maximum=31))),
        'run_submit', 'Create next attempt, then invoke existing start.',
        'Original basis, compatible recovery, stopped execution, complete cleanup/accounting and remaining budget.',
        'Original run view; next attempt can exist even if subsequent start fails.',
        'Query original run; explicitly start a verified prepared next attempt, not blind resume of the old one.'),
)
OPERATIONS = MappingProxyType({spec.name: spec for spec in _SPECS})
HTTP_GET = MappingProxyType({s.endpoint: s for s in _SPECS if s.method == 'GET'})
HTTP_POST = MappingProxyType({s.endpoint: s for s in _SPECS if s.method == 'POST'})


def http_arguments(spec, values):
    """Decode the existing wire shape, then apply the shared typed contract."""
    required, optional = spec.wire_fields()
    if type(values) is not dict or not required <= values.keys() or values.keys() - required - optional:
        raise ForgeError('Missing or unknown request fields', code='invalid_request')
    result = {}
    for name, field in spec.fields:
        wire_name = field.wire_name or name
        if wire_name not in values:
            continue
        value = values[wire_name]
        if spec.method == 'GET':
            if field.kind == 'int':
                if type(value) is not str or not re.fullmatch(r'[0-9]{1,20}', value):
                    raise ForgeError('Invalid integer query field: ' + wire_name, code='invalid_request')
                value = int(value)
            elif field.kind == 'bool':
                if value not in {'0', '1'}:
                    raise ForgeError(wire_name + ' must be 0 or 1', code='invalid_request')
                value = value == '1'
        elif (value is None and field.kind != 'int') or (field.kind == 'parameters' and type(value) is not str):
            raise ForgeError('Invalid HTTP run argument: ' + wire_name, code='invalid_request')
        result[name] = value
    return spec.validate(result)


def describe():
    """Return bounded Agent-discoverable schema descriptions for all run.* tools."""
    result = []
    for spec in OPERATIONS.values():
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
        result.append(dict(name='run.' + spec.name, version=VERSION,
                           input_schema=dict(type='object', properties=properties, required=required, additionalProperties=False),
                           permission=spec.permission, effects=spec.effects, preconditions=spec.preconditions,
                           result=spec.result, retry=spec.retry, handler='run_operations.perform',
                           authority='Current authorization is rechecked; this description grants no access.'))
    bounded_json(result, 24576)
    return result


def validate_tool_call(name, arguments):
    """Validate an Agent run.* tool invocation and return (operation_name, validated_arguments)."""
    if type(name) is not str or not name.startswith('run.') or name[4:] not in OPERATIONS:
        raise ForgeError('Unknown tool', code='invalid_request')
    operation = name[4:]
    spec = OPERATIONS[operation]
    validated = spec.validate(arguments)
    if (operation == 'prepare' and not validated.get('request_id')
            or operation in {'start', 'cancel'} and 'expected_attempt' not in validated):
        raise ForgeError('Tools require an explicit request/attempt identity', code='invalid_request')
    return operation, validated


def invoke(name, root, arguments, *, authority=None, perform=None):
    """Validate a run.* tool call and dispatch once through the supplied perform callback."""
    operation, validated = validate_tool_call(name, arguments)
    if perform is None:
        raise ForgeError('Run operation dispatcher callback is required', code='operation_rejected')
    return perform(operation, root, authority=authority, **validated)
