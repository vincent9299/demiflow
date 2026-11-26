"""Shared contracts for the existing seven workspace run operations.

This finite adapter validates input and calls the run domain once with trusted
composition. It owns no execution state, retry, queue or resource allocator.
HTTP authenticates before constructing its authority; local CLI retains its
host trust model. Current authority and engineering checks run at admission.
"""
from demiflow.forge.bridge.run_tools import (
    ARGUMENT_BYTES,
    Field,
    HTTP_GET,
    HTTP_POST,
    OPERATIONS,
    OperationSpec,
    VERSION,
    http_arguments,
)
from demiflow.forge.schema import ForgeError


def perform(name, root, *, authority=None, **arguments):
    """Call one existing use case once, preserving its result and exceptions."""
    spec = OPERATIONS.get(name)
    if spec is None:
        raise ForgeError('Unknown run operation', code='invalid_request')
    arguments = spec.validate(arguments)
    from demiflow.forge.runs import manager as runs
    from pathlib import Path
    from demiflow.forge.runs.authority import LocalAuthority
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, spec.permission)

    if name in {'start', 'cancel'} and 'expected_attempt' not in arguments:
        # Legacy local CLI: resolve once, then let the real domain CAS reject a
        # race. An explicit null remains a legacy identity, never "current".
        from demiflow.forge.runs.context import control_state
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
        internal = {k: v for k, v in arguments.items() if k in {'reserved_run_id', 'prepare_request_id', 'native_deployment', 'training_location', 'capacity_domain', 'environment_names', 'executable_dirs'}}
        public = {k: v for k, v in arguments.items() if k not in internal}
        # request_id belongs to the parent receipt wrapper, not _prepare.
        if 'request_id' in public:
            raise ForgeError('Preparation receipt is parent-owned', code='invalid_request')
        values = OPERATIONS['prepare'].validate(public)
        for key, value in internal.items():
            if key == 'native_deployment':
                from demiflow.forge.runs.native_resources import validate
                validate(value)
            elif key == 'training_location':
                from pathlib import Path
                if (type(value) is not str or not 1 <= len(value.encode()) <= 4096
                        or not Path(value).is_absolute() or str(Path(value).resolve()) != value):
                    raise ForgeError('Training deployment requires a canonical absolute file path')
            elif key == 'executable_dirs':
                from demiflow.forge.runs.native_resources import executable_directories
                executable_directories(value)
            elif key == 'environment_names':
                from demiflow.forge.runs.native_resources import environment_names
                environment_names(value)
            elif key == 'capacity_domain':
                from demiflow.forge.runs.native_resources import capacity_domain
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
