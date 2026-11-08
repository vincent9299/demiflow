"""A closed mapping to existing Forge use cases, independent of any product."""
from pathlib import Path
from .schema import ForgeError

# Effects and prerequisites describe real handlers; discovery grants no rights.
TOOLS = {
    'forge.context': dict(arguments=['task'], effects='read_saved_project_facts', prerequisite='readable project'),
    'forge.check': dict(arguments=[], effects='inspect_sources', prerequisite='contracted project'),
    'forge.status': dict(arguments=[], effects='inspect_sources_and_saved_gates', prerequisite='contracted project'),
}


def describe():
    return [dict(name=name, version=1,
                 input_schema=dict(type='object', properties={key: {'type': 'string', 'minLength': 1, 'maxLength': 128}
                                                             for key in spec['arguments']}, additionalProperties=False),
                 effects=spec['effects'], prerequisite=spec['prerequisite'])
            for name, spec in TOOLS.items()]


def invoke(name, project, arguments=None):
    arguments = {} if arguments is None else arguments
    if name not in TOOLS or type(arguments) is not dict or set(arguments) - set(TOOLS[name]['arguments']):
        raise ForgeError('Unknown Forge tool or fields', code='invalid_request')
    if any(type(value) is not str or not 1 <= len(value) <= 128 for value in arguments.values()):
        raise ForgeError('Invalid Forge tool argument', code='invalid_request')
    if 'task' in arguments:
        from .development.project import safe_id
        try:
            safe_id(arguments['task'])
        except ValueError as exc:
            raise ForgeError(str(exc), code='invalid_request') from exc
    # Actual validation remains in the existing domain handler. No retries,
    # secondary state machine, or product callback registry lives here.
    if name == 'forge.context':
        from .context import query
        return query(Path(project), **arguments)
    if name == 'forge.check':
        from .checks import check_project
        return check_project(Path(project))
    from .lifecycle import status
    return status(Path(project))
