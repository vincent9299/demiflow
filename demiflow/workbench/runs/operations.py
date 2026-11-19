"""Shared contracts for the existing seven workspace run operations.

This finite adapter validates input and calls the run domain once with trusted
composition. It owns no execution state, retry, queue or resource allocator.
HTTP authenticates before constructing its authority; local CLI retains its
host trust model. Current authority and engineering checks run at admission.
"""
from dataclasses import dataclass
from types import MappingProxyType
import re

from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_run_operations_v1'
ARGUMENT_BYTES = 1024 * 1024  # Existing one-shot inspector input ceiling.


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
    # Historical CLI start/cancel may omit this; HTTP must specify it.
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
            # HTTP v1 only accepted a parameter path; local/internal calls may
            # also use a closed parameter object. Do not broaden wire authority.
            raise ForgeError('Invalid HTTP run argument: ' + wire_name, code='invalid_request')
        result[name] = value
    return spec.validate(result)


def perform(name, root, *, authority=None, **arguments):
    """Call one existing use case once, preserving its result and exceptions."""
    spec = OPERATIONS.get(name)
    if spec is None:
        raise ForgeError('Unknown run operation', code='invalid_request')
    arguments = spec.validate(arguments)
    from demiflow.workbench.runs import manager as runs
    from pathlib import Path
    from demiflow.workbench.runs.authority import LocalAuthority
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, spec.permission)

    if name in {'start', 'cancel'} and 'expected_attempt' not in arguments:
        # Legacy local CLI: resolve once, then let the real domain CAS reject a
        # race. An explicit null remains a legacy identity, never "current".
        from demiflow.workbench.runs.view import control_state
        _, value = control_state(root, arguments['run_id'])
        arguments['expected_attempt'] = value['state'].get('attempt')
    if name in {'prepare', 'start', 'cancel', 'resume'}:
        arguments['authority'] = authority
    return getattr(runs, spec.handler)(root, **arguments)


def worker_arguments(name, arguments):
    """Validate existing inspector inputs without recursively dispatching.

    These internal phase calls retain runs' actual locks and checks. No new
    authority token or permission default is introduced into the worker.
    """
    if type(arguments) is not dict:
        raise ForgeError('Worker run arguments must be an object', code='invalid_request')
    if name == 'run_prepare':
        internal = {k: v for k, v in arguments.items() if k in {'reserved_run_id', 'prepare_request_id', 'native_deployment', 'capacity_domain', 'environment_names'}}
        public = {k: v for k, v in arguments.items() if k not in internal}
        # request_id belongs to the parent receipt wrapper, not _prepare.
        if 'request_id' in public:
            raise ForgeError('Preparation receipt is parent-owned', code='invalid_request')
        values = OPERATIONS['prepare'].validate(public)
        for key, value in internal.items():
            if key == 'native_deployment':
                from demiflow.workbench.runs.native_resources import validate
                validate(value)
            elif key == 'environment_names':
                from demiflow.workbench.runs.native_resources import environment_names
                environment_names(value)
            elif key == 'capacity_domain':
                from demiflow.workbench.runs.native_resources import capacity_domain
                capacity_domain(value)
            else:
                Field('text', nullable=True).check(key, value)
        return {**values, **internal}
    if name == 'run_preflight':
        if set(arguments) != {'run_id', 'expected'}:
            raise ForgeError('Invalid preflight arguments', code='invalid_request')
        return OPERATIONS['start'].validate(arguments)
    if name == 'run_resume':
        return OPERATIONS['resume'].validate(arguments)
    raise ForgeError('Unknown run inspection phase', code='invalid_request')
