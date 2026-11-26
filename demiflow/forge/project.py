"""Explicit project declarations and standard source files shared by authors and consumers."""
from hashlib import sha256
import keyword
import os
from pathlib import Path
import re
import uuid

from .api_reference import MODULES
from .project_guard import publish_project_control
from .io import directory_handle, ensure_directory, no_symlinks, publish_bytes
from .platform import bounded_json, content_digest, module_scope
from .platform_environment import read_bounded
from .platform_snapshot import build_snapshot, load_snapshot, snapshot_complete, write_snapshot
from .project_io import (
    control_lock, identifier, read_control, relative_path, write_control,
)
from .project_rules import ROLE_DIRS
from .schema import ForgeError

FORMAT = "demiflow_forge_workspace_v1"
FORMAT_V2 = "demiflow_forge_workspace_v2"
FORMAT_V3 = "demiflow_forge_workspace_v3"
MODERN_FORMATS = {FORMAT_V2, FORMAT_V3}
STATE_PATH = ".forge/state.json"
_SLIM_MANIFEST_FIELDS = frozenset({"name", "entries", "budgets", "locations", "workflows"})
_DEFAULT_AUDIT = {"reports": [], "change_scope": None}

def project_name(value):
    if (type(value) is not str or not value.strip() or value != value.strip()
            or len(value.encode('utf-8')) > 256 or any(ord(c) < 32 for c in value)):
        raise ForgeError('Project name requires 1..256 UTF-8 bytes without control characters', code='invalid_request')
    return value


def validate_entry(root, entry):
    if not isinstance(entry, str) or entry.count(':') != 1:
        raise ForgeError('Entry requires project-relative file.py:function')
    path, symbol = entry.split(':')
    target = relative_path(root, path)
    if (target.suffix != '.py' or not symbol.isidentifier() or symbol.startswith('_')
            or any(not part.isidentifier() for part in path[:-3].split('/'))):
        raise ForgeError('Entry must name a local Python module and public function')
    return target, symbol


def validate_parameter_path(root, params):
    target = relative_path(root, params)
    if target.suffix != '.json' or any(part.startswith('.') for part in Path(params).parts):
        raise ForgeError('Parameters require a project-relative author JSON file', code='invalid_request')
    return target


def location_root(root, location):
    path = Path(location["root"])
    result = path if path.is_absolute() else relative_path(root, location["root"])
    no_symlinks(result)
    if not result.is_dir():
        raise ForgeError("Data location must be an existing directory")
    return result.absolute()


def validate_budget_declaration(values, *, label='Workspace'):
    """Project and run overrides name native limits and carry positive integral values."""
    from demiflow.execution.host.control import DEFAULT_LIMITS
    if type(values) is not dict or values.keys() - DEFAULT_LIMITS.keys():
        raise ForgeError('Unknown ' + label + ' budget')
    if any(type(value) is not int or value < 1 for value in values.values()):
        raise ForgeError(label + ' budgets must be positive integers')


def is_slim_manifest(value):
    return (
        type(value) is dict
        and "format" not in value
        and "name" in value
        and set(value) <= _SLIM_MANIFEST_FIELDS
    )


def _default_audit():
    return {"reports": [], "change_scope": None}


def _bootstrap_state(root):
    """Lazily initialize .forge/state.json and platform snapshot when absent (e.g. after git clone)."""
    snapshot = build_snapshot(MODULES, extras=())
    if not snapshot_complete(snapshot):
        raise ForgeError('Cannot initialize from an incomplete SDK snapshot', code='inspection_incomplete')
    digest = snapshot['digests']['snapshot']
    relative = '.forge/platform/' + digest + '.json'
    state = dict(format=FORMAT_V3, id='w' + uuid.uuid4().hex, platform=dict(path=relative, digest=digest))
    try:
        ensure_directory(root / '.forge')
        with control_lock(root):
            state_file = root / STATE_PATH
            if no_symlinks(state_file).exists():
                return read_control(state_file)
            snap_path = relative_path(root, relative)
            ensure_directory(snap_path.parent)
            if not snap_path.exists():
                write_snapshot(snap_path, snapshot)
            write_control(state_file, state, create=True)
    except OSError:
        # Read-only mount/sandbox: return in-memory state without failing read operations.
        pass
    return state


def load_project(root):
    """Validate local project declarations (slim workspace.json + .forge/state.json or legacy v1/v2/v3)."""
    root = Path(root).absolute()
    no_symlinks(root)
    raw = read_control(root / "workspace.json")
    if is_slim_manifest(raw):
        state_file = root / STATE_PATH
        state = read_control(state_file) if no_symlinks(state_file).exists() else _bootstrap_state(root)
        if (
            type(state) is not dict
            or state.get("format") != FORMAT_V3
            or set(state) - {"audit"} != {"format", "id", "platform"}
        ):
            raise ForgeError("Unsupported Forge state format")
        value = {
            "format": state["format"],
            "id": state["id"],
            "name": raw["name"],
            "entries": raw.get("entries", {}),
            "locations": raw.get("locations", {}),
            "platform": state["platform"],
            "audit": state.get("audit", _default_audit()),
        }
        if "budgets" in raw:
            value["budgets"] = raw["budgets"]
        if "workflows" in raw:
            value["workflows"] = raw["workflows"]
    else:
        value = raw
    modern = type(value) is dict and value.get('format') in (FORMAT_V2, FORMAT_V3)
    audit_fields = {'audit'} if type(value) is dict and value.get('format') == FORMAT_V3 else set()
    if (type(value) is not dict or value.get('format') not in (FORMAT, FORMAT_V2, FORMAT_V3)
            or set(value) - ({'budgets', 'workflows'} if modern else set()) != {'format', 'name', 'locations', 'platform'} | ({'id', 'entries'} if modern else set()) | audit_fields):
        raise ForgeError("Unsupported workspace format")
    if audit_fields:
        from demiflow.forge.audit import validate_policy
        validate_policy(value['audit'])
    if modern:
        if 'budgets' in value:
            validate_budget_declaration(value['budgets'])
        if 'workflows' in value:
            wf_list = value['workflows']
            if type(wf_list) is not list or len(wf_list) > 64 or any(type(w) is not str for w in wf_list) or len(set(wf_list)) != len(wf_list):
                raise ForgeError('Workspace workflows must be a list of at most 64 distinct relative paths')
            for wf_path in wf_list:
                wf_target = relative_path(root, wf_path)
                wf_parts = Path(wf_path).parts
                if (len(wf_parts) < 2 or wf_parts[0] != 'workflows' or wf_target.suffix != '.json'
                        or any(part.startswith('.') for part in wf_parts)):
                    raise ForgeError('Workflow declarations must reference workflows/*.json files')
        if not isinstance(value['id'], str) or not re.fullmatch(r'w[0-9a-f]{32}', value['id']):
            raise ForgeError('Invalid workspace identity')
        if type(value['entries']) is not dict or len(value['entries']) > 256:
            raise ForgeError('Invalid registered entry index')
        for entry, parameters in value['entries'].items():
            validate_entry(root, entry)
            if type(parameters) is not list or not 1 <= len(parameters) <= 64 or any(type(p) is not str for p in parameters) or len(set(parameters)) != len(parameters):
                raise ForgeError('Entry requires 1..64 distinct parameter paths')
            for params in parameters:
                validate_parameter_path(root, params)
    project_name(value["name"])
    if type(value["locations"]) is not dict or not 0 <= len(value["locations"]) <= 16:
        raise ForgeError("Invalid location count")
    for key, location in value["locations"].items():
        identifier(key)
        if (
            type(location) is not dict or set(location) not in ({"root", "writable"}, {"root", "writable", "write_modes"})
            or type(location["writable"]) is not bool
            or not isinstance(location["root"], str)
        ):
            raise ForgeError("Invalid data location")
        if 'write_modes' in location:
            modes = location['write_modes']
            if not modern or type(modes) is not list or len(modes) > 3 or any(m not in ('create', 'overwrite', 'checkpoint') for m in modes) or len(set(modes)) != len(modes) or bool(modes) != location['writable']:
                raise ForgeError('Invalid explicit location write modes')
        if not modern and location["writable"] and (key != "local" or location["root"] != "data"):
            raise ForgeError("Only workspace data/ may be writable")
        location_root(root, location)
    if (type(value["platform"]) is not dict or set(value["platform"]) != {"path", "digest"}
            or not isinstance(value["platform"]["digest"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", value["platform"]["digest"])):
        raise ForgeError("Invalid platform binding")
    path = relative_path(root, value["platform"]["path"])
    if path.parent != root / ".forge/platform":
        raise ForgeError("Platform snapshot must be in .forge/platform")
    return root, value


def save_project(root, value, *, expected=None, create=False):
    """Persist workspace declaration, splitting Forge internal state to .forge/state.json for slim projects."""
    root = Path(root).absolute()
    manifest = root / "workspace.json"
    state_file = root / STATE_PATH
    slim = (
        create
        or no_symlinks(state_file).exists()
        or (no_symlinks(manifest).exists() and is_slim_manifest(read_control(manifest)))
    )
    if not slim:
        if expected is not None and content_digest(read_control(manifest)) != expected:
            raise ForgeError('Control changed; refresh before replacing', code='conflict')
        publish_project_control(manifest, bounded_json(value, 8 * 1024**2), create=create or expected is None)
        return
    if expected is not None:
        if create:
            raise ForgeError('Cannot create with a previous control digest', code='conflict')
        try:
            _, current = load_project(root)
        except ForgeError as exc:
            if exc.code == 'not_found':
                raise ForgeError('Expected control no longer exists', code='conflict') from exc
            raise
        if content_digest(current) != expected:
            raise ForgeError('Control changed; refresh before replacing', code='conflict')
    ws_doc = {"name": value["name"], "entries": value.get("entries", {})}
    if "budgets" in value:
        ws_doc["budgets"] = value["budgets"]
    if value.get("workflows"):
        ws_doc["workflows"] = value["workflows"]
    if value.get("locations"):
        ws_doc["locations"] = value["locations"]
    state_doc = {
        "format": value["format"],
        "id": value["id"],
        "platform": value["platform"],
    }
    audit_val = value.get("audit")
    if isinstance(audit_val, dict) and (audit_val.get("reports") or audit_val.get("change_scope") is not None):
        state_doc["audit"] = audit_val
    ensure_directory(root / ".forge")
    publish_project_control(no_symlinks(state_file), bounded_json(state_doc, 8 * 1024**2), create=create)
    publish_project_control(no_symlinks(manifest), bounded_json(ws_doc, 8 * 1024**2), create=create)


def _referenced_location_ids(root, value):
    """Collect location_id keys still referenced by active parameter files or tables/*.json."""
    used = set()

    def visit(item, depth=0):
        if depth > 16:
            return
        if isinstance(item, dict):
            loc_id = item.get('location_id')
            if isinstance(loc_id, str) and loc_id:
                used.add(loc_id)
            for child in item.values():
                visit(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                visit(child, depth + 1)

    for paths in (value.get('entries') or {}).values():
        for rel in paths:
            try:
                visit(read_control(validate_parameter_path(root, rel), 65536))
            except Exception:
                continue
    tables_dir = Path(root) / 'tables'
    if tables_dir.is_dir() and not tables_dir.is_symlink():
        try:
            for child in sorted(tables_dir.iterdir()):
                if child.is_file() and not child.is_symlink() and child.suffix == '.json':
                    try:
                        visit(read_control(child, 16384))
                    except Exception:
                        continue
        except OSError:
            pass
    return used


def slim_project_manifest(root):
    """Migrate a legacy workspace.json into slim workspace.json + .forge/state.json and drop unused locations."""
    root = Path(root).absolute()
    manifest = root / "workspace.json"
    state_file = root / STATE_PATH
    with control_lock(root):
        raw_ws = read_control(manifest)
        _, value = load_project(root)
        if value['format'] != FORMAT_V3:
            raise ForgeError('slim_manifest requires a modern v3 project', code='invalid_request')
        used_locs = _referenced_location_ids(root, value)
        kept_locations = {k: v for k, v in (value.get('locations') or {}).items() if k in used_locs}
        removed_locations = sorted(set(value.get('locations') or {}) - set(kept_locations))
        ws_doc = {"name": value["name"], "entries": value.get("entries", {})}
        if "budgets" in value:
            ws_doc["budgets"] = value["budgets"]
        if value.get("workflows"):
            ws_doc["workflows"] = value["workflows"]
        if kept_locations:
            ws_doc["locations"] = kept_locations
        state_doc = {
            "format": value["format"],
            "id": value["id"],
            "platform": value["platform"],
        }
        audit_val = value.get("audit")
        if isinstance(audit_val, dict) and (audit_val.get("reports") or audit_val.get("change_scope") is not None):
            state_doc["audit"] = audit_val
        already_slim = (
            is_slim_manifest(raw_ws)
            and no_symlinks(state_file).exists()
            and raw_ws == ws_doc
        )
        if already_slim:
            return dict(
                status='unchanged',
                manifest_path='workspace.json',
                state_path=STATE_PATH,
                removed_fields=[],
                removed_locations=[],
                kept_locations=sorted(kept_locations),
            )
        removed_fields = sorted(set(raw_ws) - set(ws_doc))
        ensure_directory(root / ".forge")
        publish_project_control(no_symlinks(state_file), bounded_json(state_doc, 8 * 1024**2), create=False)
        publish_project_control(no_symlinks(manifest), bounded_json(ws_doc, 8 * 1024**2), create=False)
        return dict(
            status='slimmed',
            manifest_path='workspace.json',
            state_path=STATE_PATH,
            removed_fields=removed_fields,
            removed_locations=removed_locations,
            kept_locations=sorted(kept_locations),
        )


def load_project_snapshot(root):
    root, value = load_project(root)
    snap_path = relative_path(root, value["platform"]["path"])
    if snap_path.exists():
        snapshot = load_snapshot(snap_path)
    else:
        snapshot = build_snapshot(MODULES, extras=())
    if (
        snapshot["digests"]["snapshot"] != value["platform"]["digest"]
        or snapshot["scope"]["discovery_version"] not in (2, 3)
    ):
        raise ForgeError("Workspace platform binding requires a matching supported snapshot")
    return root, value, snapshot


_EMPTY_PARAMS = {}
_LEGACY_EMPTY_PARAMS = {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {}}
_PROMPT_BYTES = 1024**2


def discover_project_sources(root, *, exclude=(), include_roles=True, role_names=ROLE_DIRS):
    """List published entries separately from standard source files, without importing project code."""
    root = no_symlinks(Path(root).absolute())
    protected = set(exclude)

    def visible(relative):
        return not any(str(p) in protected for p in (Path(relative), *Path(relative).parents))

    if root.exists() and not root.is_dir():
        raise ForgeError('Project root must be a directory', code='invalid_request')
    if not visible('workspace.json'):
        raise ForgeError('Project declaration is outside the permitted scope')
    control = load_project(root)[1] if no_symlinks(root / 'workspace.json').exists() else None
    entries = {entry: [p for p in params if visible(p)]
               for entry, params in (control or {}).get('entries', {}).items()
               if visible(entry.partition(':')[0])}
    if not set(role_names) <= set(ROLE_DIRS):
        raise ForgeError('Unknown project source role', code='invalid_request')
    roles = {role: [] for role in role_names}
    if not include_roles:
        return dict(control=control, roles=roles, entries=entries)
    seen = 0
    for role in roles:
        pending = [root / role]
        suffixes = ({'.yaml', '.yml', '.json'} if role in {'prompts', 'configs'}
                    else {'.json'} if role == 'workflows'
                    else {'.ipynb'} if role == 'notebooks' else {'.py'})
        while pending:
            folder = pending.pop()
            if not visible(folder.relative_to(root)) or not no_symlinks(folder).exists():
                continue
            with directory_handle(folder) as parent, os.scandir(parent) as children:
                for child in children:
                    path = folder / child.name
                    relative = path.relative_to(root).as_posix()
                    seen += 1
                    if seen > 2048:
                        raise ForgeError('Project coding discovery exceeds entry budget', code='resource_limit')
                    if not visible(relative) or child.name.startswith('.') or child.name == '__pycache__':
                        continue
                    if child.is_dir(follow_symlinks=False):
                        pending.append(path)
                    elif path.suffix in suffixes and child.is_file(follow_symlinks=False):
                        roles[role].append(relative)
        roles[role].sort()
    return dict(control=control, roles=roles, entries=entries)


def _init_result(root, value, created):
    return dict(status='created' if created else 'unchanged', root=str(root), project_id=value['id'],
                manifest_path='workspace.json', manifest_sha256=sha256(read_bounded(root/'workspace.json', 1024**2)).hexdigest(),
                created_paths=created, platform_snapshot=value['platform'], diagnostics=[])


def initialize_project(root, *, name=None, existing=False, extras=None, modules=None):
    """Publish the project declaration last. Retry can safely reuse a complete matching snapshot."""
    root = no_symlinks(Path(root).absolute())
    if '..' in root.parts or not root.parent.is_dir():
        raise ForgeError('Choose a normalized project root in an existing directory', code='invalid_request')
    if name is not None:
        project_name(name)
    manifest = root / 'workspace.json'
    if root.exists() and not root.is_dir():
        raise ForgeError('Project root must be a directory')
    if existing and not root.exists():
        raise ForgeError('Existing project root is missing', code='not_found')
    snapshot = None
    if not no_symlinks(manifest).exists():
        title = project_name(root.name if name is None else name)
        if root.exists() and not existing:
            with directory_handle(root) as parent, os.scandir(parent) as children:
                if any(child.name != '.forge' for child in children):
                    raise ForgeError('Nonempty project requires explicit --existing', code='conflict')
        snapshot = build_snapshot(MODULES if modules is None else modules, extras=extras or ())
        if not snapshot_complete(snapshot):
            raise ForgeError('Cannot initialize from an incomplete SDK snapshot', code='inspection_incomplete')
    ensure_directory(root)
    ensure_directory(root / '.forge')
    with control_lock(root):
        if no_symlinks(manifest).exists():
            _, value, snapshot = load_project_snapshot(root)
            if (value['format'] != FORMAT_V3 or name is not None and name != value['name'] or
                    extras is not None and sorted(set(extras)) != snapshot['scope']['extras'] or
                    modules is not None and list(module_scope(modules)) != snapshot['scope']['modules']):
                raise ForgeError('Existing project has different identity or SDK scope', code='conflict')
            return _init_result(root, value, [])
        if snapshot is None:
            raise ForgeError('Project declaration disappeared during initialization', code='inputs_changed')
        if not existing:
            with directory_handle(root) as parent, os.scandir(parent) as children:
                if any(child.name != '.forge' for child in children):
                    raise ForgeError('Nonempty project requires explicit --existing', code='conflict')
        with directory_handle(root / '.forge') as parent, os.scandir(parent) as files:
            if any(item.name not in ('platform', 'control.lock') for item in files):
                raise ForgeError('Unrecognized controls in an uninitialized project', code='conflict')
        digest = snapshot['digests']['snapshot']
        relative = '.forge/platform/' + digest + '.json'
        path = relative_path(root, relative)
        ensure_directory(path.parent)
        created = []
        if path.exists():
            if load_snapshot(path)['digests']['snapshot'] != digest:
                raise ForgeError('Existing snapshot conflicts with its identity', code='conflict')
        else:
            write_snapshot(path, snapshot)
            created.append(relative)
        value = dict(format=FORMAT_V3, audit=_default_audit(), id='w'+uuid.uuid4().hex,
                     name=title, entries={}, locations={}, platform=dict(path=relative, digest=digest))
        save_project(root, value, create=True)
        created.append(STATE_PATH)
        if not existing:
            from .project_env import ensure_gitignore_env
            if ensure_gitignore_env(root):
                created.append('.gitignore')
        return _init_result(root, value, [*created, 'workspace.json'])


def _bounded_prompt_yaml(text):
    """Admit YAML before construction, including the expanded cost of repeated anchors."""
    import yaml
    # Each frame holds an optional anchor, expanded cost and subtree depth.
    stack = [[None, 0, 0]]
    anchors = {}
    for index, event in enumerate(yaml.parse(text, Loader=yaml.SafeLoader)):
        if len(stack) > 33 or index > 65536:
            raise ForgeError('Prompt YAML exceeds structure budget', code='resource_limit')
        if isinstance(event, yaml.events.CollectionStartEvent):
            stack.append([event.anchor, 1, 1])
            continue
        if isinstance(event, yaml.events.CollectionEndEvent):
            anchor, size, depth = stack.pop()
        elif isinstance(event, yaml.events.AliasEvent):
            if event.anchor not in anchors:
                raise ForgeError('Recursive or unresolved YAML alias', code='invalid_request')
            size, depth = anchors[event.anchor]
            anchor = None
        elif isinstance(event, yaml.events.ScalarEvent):
            anchor, size, depth = event.anchor, 16 + len(event.value.encode()), 1
        else:
            continue
        if anchor is not None:
            anchors[anchor] = size, depth
        stack[-1][1] += size
        stack[-1][2] = max(stack[-1][2], depth + 1)
        if stack[-1][1] > 4 * _PROMPT_BYTES or stack[-1][2] > 33:
            raise ForgeError('Expanded Prompt YAML exceeds the parsing budget', code='resource_limit')
    return yaml.safe_load(text)


def inspect_prompt_source(text):
    """Inspect an inline native Prompt or Agent document. Never import project code."""
    import yaml
    from demiflow.agent import inspect_agent_config_document
    from demiflow.operator_llm.parser import inspect_prompt_pack
    if not isinstance(text, str) or len(text.encode()) > _PROMPT_BYTES:
        raise ForgeError('Prompt source must be UTF-8 within 1 MiB', code='resource_limit')
    try:
        raw = _bounded_prompt_yaml(text)
        if not isinstance(raw, dict):
            raise ForgeError('Prompt source requires a native schema_version', code='invalid_request')
        version = raw.get('schema_version')
        if version == 'demiflow_agent_v2':
            names, tools = inspect_agent_config_document(raw)
        else:
            inspected = inspect_prompt_pack(text)
            if inspected.diagnostics:
                raise ForgeError('; '.join(item.message for item in inspected.diagnostics[:8]))
            names, tools = tuple(inspected.prompt_definitions), ()
        return version, names, tools
    except ForgeError:
        raise
    except (TypeError, ValueError, yaml.YAMLError, RecursionError) as exc:
        raise ForgeError('Invalid native prompt or agent document: ' + str(exc)[:1024]) from exc


def create_role_file(root, role, name, *, content=None, actor=False):
    root, _ = load_project(root)
    if role not in ('flow', 'operator', 'prompt'):
        raise ForgeError('Unknown project source role', code='invalid_request')
    if (not isinstance(name, str) or len(name.encode()) > 128 or not name.isidentifier()
            or name.startswith('_') or keyword.iskeyword(name)):
        raise ForgeError('Choose a public module name', code='invalid_request')
    if (content is not None) != (role == 'prompt') or actor and role != 'operator':
        raise ForgeError('Only prompt files accept content and only operators accept actor', code='invalid_request')
    directory = {'flow': 'flows', 'operator': 'operators', 'prompt': 'prompts'}[role]
    relative = directory + '/' + name + ('.yaml' if role == 'prompt' else '.py')
    target = relative_path(root, relative)
    if role == 'prompt':
        raw = content.encode() if isinstance(content, str) else content
        if type(raw) is not bytes or len(raw) > _PROMPT_BYTES:
            raise ForgeError('Prompt source must be UTF-8 within 1 MiB', code='resource_limit')
        try:
            version, definitions, _ = inspect_prompt_source(raw.decode('utf-8'))
        except UnicodeDecodeError as exc:
            raise ForgeError('Prompt source must be UTF-8', code='invalid_request') from exc
        result = dict(format=version, definitions=list(definitions), sha256=sha256(raw).hexdigest())
    elif role == 'flow':
        raw = b'"""Compose a native DemiFlow pipeline using the public Dataset API."""\n\n\ndef run():\n    raise NotImplementedError\n'
        result = dict(entry=relative+':run', registered=False)
    else:
        symbol = name
        if actor:
            symbol = ''.join(part[:1].upper()+part[1:] for part in name.split('_') if part)
            if keyword.iskeyword(symbol):
                symbol += 'Actor'
            source = f'class {symbol}:\n    def __call__(self, row):\n        raise NotImplementedError\n'
        else:
            source = f'def {name}(row):\n    raise NotImplementedError\n'
        raw = (source + f'\n\n__demiflow_operators__ = ["{symbol}"]\n').encode()
        result = dict(symbol=symbol)
    ensure_directory(target.parent)
    publish_bytes(target, raw)
    return dict(status='created', root=str(root), role=role, path=relative, created_paths=[relative],
                diagnostics=[], **result)


def register_flow(root, entry, *, params=None, authorize=None, table_resolver=None):
    """Publish a checked binding only while its file and declaration evidence still matches."""
    from .pipeline_check import check_project, verify_check_inputs
    root, value = load_project(root)
    if value['format'] != FORMAT_V3:
        raise ForgeError('Flow registration requires a v3 project', code='invalid_request')
    source, _ = validate_entry(root, entry)
    bound = value['entries'].get(entry, [])
    if bound:
        if params is None and len(bound) == 1:
            params = bound[0]
        if params not in bound:
            raise ForgeError('Choose an existing parameter binding for this entry', code='conflict')
    elif len(value['entries']) >= 256:
        raise ForgeError('Registered entry count exceeds the project limit', code='resource_limit')
    selected = params if params is not None else f'configs/{source.stem}.json'
    parameter_file = validate_parameter_path(root, selected)
    if params is None and any(selected in items for items in value['entries'].values()):
        raise ForgeError('Default parameter path is bound to another entry', code='conflict')

    check = check_project(root, entry=entry, params=params, table_resolver=table_resolver)
    if (check['status'] != 'passed' or check['native_flow']['status'] != 'passed'
            or check['project_layout']['status'] != 'passed' or len(check['entries']) != 1
            or check['entries'][0]['entry'] != entry or check['entries'][0]['status'] != 'checked'):
        detail = '; '.join(item['detail'][:256] for item in check['diagnostics'][:4])
        kind = 'inspection_incomplete' if check['status'] in ('incomplete', 'not_applicable') else 'operation_rejected'
        raise ForgeError('Flow registration requires a verified native flow and project layout: ' + detail, code=kind)

    manifest = root / 'workspace.json'
    with control_lock(root):
        if authorize is not None:
            authorize()
        verify_check_inputs(root, check['basis'])
        if load_project(root)[1] != value:
            raise ForgeError('Project declaration changed before registration', code='inputs_changed')
        created = []
        if not bound:
            expected = content_digest(value)
            value['entries'][entry] = [selected]
            bounded_json(value, 1024**2)
            if params is None:
                if no_symlinks(parameter_file).exists():
                    if read_control(parameter_file, 65536) not in (_EMPTY_PARAMS, _LEGACY_EMPTY_PARAMS):
                        raise ForgeError('Default parameter file has different contents', code='conflict')
                else:
                    ensure_directory(parameter_file.parent)
                    write_control(parameter_file, _EMPTY_PARAMS, create=True)
                    created.append(selected)
                # Publishing the default may involve a durable write. Confirm its bytes
                # and all checked inputs before recording the binding.
                verify_check_inputs(root, check['basis'])
                if read_control(parameter_file, 65536) not in (_EMPTY_PARAMS, _LEGACY_EMPTY_PARAMS):
                    raise ForgeError('Default parameter file changed during registration', code='inputs_changed')
            save_project(root, value, expected=expected)
        return dict(status='unchanged' if bound else 'registered', role='flow', root=str(root),
                    path=source.relative_to(root).as_posix(), entry=entry, params=selected,
                    registered=True, created_paths=created, check_digest=check['check_digest'],
                    manifest_sha256=sha256(read_bounded(manifest, 1024**2)).hexdigest(), diagnostics=[])


def register_workflow(root, path, *, authorize=None):
    """Validate and register a workflows/*.json file in workspace.json['workflows']."""
    from .workflow import MAX_WORKFLOW_BYTES, MAX_WORKFLOWS_PER_PROJECT, validate_workflow, validate_workflow_path
    root, value = load_project(root)
    if value['format'] != FORMAT_V3:
        raise ForgeError('Workflow registration requires a v3 project', code='invalid_request')
    target = validate_workflow_path(root, path)
    rel = target.relative_to(root).as_posix()
    spec = validate_workflow(root, rel, control=value)
    if spec['status'] != 'passed':
        detail = '; '.join(item['detail'][:256] for item in spec['diagnostics'][:4])
        raise ForgeError('Workflow registration requires a valid workflow definition: ' + detail,
                         code='operation_rejected')
    manifest = root / 'workspace.json'
    with control_lock(root):
        if authorize is not None:
            authorize()
        _, current = load_project(root)
        try:
            raw = read_bounded(validate_workflow_path(root, rel), MAX_WORKFLOW_BYTES)
        except (OSError, ValueError) as exc:
            raise ForgeError('Workflow changed before registration', code='inputs_changed') from exc
        if current != value or sha256(raw).hexdigest() != spec['digest']:
            raise ForgeError('Project declaration or workflow changed before registration', code='inputs_changed')
        existing = list(current.get('workflows') or ())
        already = rel in existing
        if not already:
            if len(existing) >= MAX_WORKFLOWS_PER_PROJECT:
                raise ForgeError('Registered workflow count exceeds the project limit', code='resource_limit')
            expected = content_digest(current)
            current['workflows'] = [*existing, rel]
            bounded_json(current, 1024**2)
            save_project(root, current, expected=expected)
        return dict(
            status='unchanged' if already else 'registered',
            role='workflow',
            root=str(root),
            path=rel,
            name=spec['name'],
            registered=True,
            workflow_digest=spec['digest'],
            manifest_sha256=sha256(read_bounded(manifest, 1024**2)).hexdigest(),
            diagnostics=[],
        )
