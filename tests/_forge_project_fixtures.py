"""Build an existing declared project for tests of downstream Workbench behaviour."""
from demiflow.forge.platform import content_digest
from demiflow.forge.project_io import control_lock, read_control, write_control


def declare_existing_flow(root, entry, *, params):
    """Install fixture data, including intentionally broken or multivariant declarations."""
    path = root / 'workspace.json'
    with control_lock(root):
        control = read_control(path)
        expected = content_digest(control)
        variants = control['entries'].setdefault(entry, [])
        if params not in variants:
            variants.append(params)
            write_control(path, control, expected=expected)


def grant_config_write(registry, workspace_id):
    from demiflow.workbench.projects import registry as projects
    projects.migrate(registry, expected=content_digest(projects.load_registry(registry)))
    value = projects.load_registry(registry)
    capabilities = projects.capabilities(value['projects'][workspace_id])
    projects.set_capabilities(registry, workspace_id, expected=content_digest(value),
                              capabilities={**capabilities, 'config_write': True})
