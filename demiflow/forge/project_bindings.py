"""Static and runtime binding of native parameters and declared resources."""
import hashlib
import re
from pathlib import Path

from .platform import bounded_json, content_digest
from .platform_environment import read_bounded
from .project import load_project, location_root, save_project
from .project_io import identifier, relative_path, read_control
from .schema import ForgeError


def bind_table(root, alias, raw_path, *, location_id=None, version=None):
    """Safely register a read-only input location (if needed) and pin tables/<alias>.json."""
    from .io import ensure_directory, no_symlinks
    from .platform_worker import describe_fixed_table
    from .project_io import control_lock, write_control
    from demiflow.lance.refs import DatasetRef

    root = Path(root).absolute()
    if not isinstance(alias, str) or not alias or not isinstance(raw_path, str) or not raw_path:
        raise ForgeError('bind_table requires alias and path to a .lance directory', code='invalid_request')
    identifier(alias)
    if version is not None and (type(version) is not int or version < 1):
        raise ForgeError('bind_table version must be a positive integer', code='invalid_request')
    target = Path(raw_path)
    target = (target if target.is_absolute() else (root / target)).absolute()
    no_symlinks(target)
    forbidden_roots = (Path('/etc'), Path('/proc'), Path('/sys'), Path('/dev'), Path('/root'))
    if (any(p.startswith('.') for p in target.parts[1:])
            or any(target == fb or fb in target.parents for fb in forbidden_roots)
            or not target.is_dir() or target.suffix != '.lance'):
        raise ForgeError('bind_table requires an existing non-hidden .lance directory', code='invalid_request')
    description = describe_fixed_table(target, version)
    parent_dir = target.parent
    with control_lock(root):
        _, value = load_project(root)
        expected_ws = content_digest(value)
        locations = value.setdefault('locations', {})
        chosen_loc = None
        rel_uri = None
        for existing_id, loc in locations.items():
            try:
                loc_root = location_root(root, loc)
                if loc_root in target.parents:
                    chosen_loc = existing_id
                    rel_uri = target.relative_to(loc_root).as_posix()
                    break
            except Exception:
                continue
        if chosen_loc is None:
            if location_id is not None:
                if not isinstance(location_id, str) or not location_id:
                    raise ForgeError('Invalid location_id', code='invalid_request')
                identifier(location_id)
                if location_id in locations:
                    raise ForgeError(f'location_id already exists with a different root: {location_id}', code='conflict')
                chosen_loc = location_id
            else:
                chosen_loc = 'input_' + hashlib.sha256(str(parent_dir).encode()).hexdigest()[:10]
            if len(locations) >= 16:
                raise ForgeError('Location budget exhausted (max 16)', code='resource_limit')
            locations[chosen_loc] = {'root': str(parent_dir), 'writable': False}
            save_project(root, value, expected=expected_ws)
            rel_uri = target.name
        ensure_directory(root / 'tables')
        ref = DatasetRef(
            alias,
            rel_uri,
            description['version'],
            alias,
            '1',
            description['schema_hash'],
            description['row_count'],
            store_id=chosen_loc,
        )
        control = {
            'format': 'demiflow_forge_table_v1',
            'alias': alias,
            'location_id': chosen_loc,
            'ref': ref.to_dict(),
        }
        table_path = root / 'tables' / f'{alias}.json'
        if table_path.exists():
            old_ctrl = read_control(table_path, 16384)
            write_control(table_path, control, expected=content_digest(old_ctrl))
        else:
            write_control(table_path, control, create=True)
    return dict(
        status='bound',
        alias=alias,
        location_id=chosen_loc,
        relative_uri=rel_uri,
        version=description['version'],
        row_count=description['row_count'],
        param_ref={'kind': 'table', 'alias': alias},
    )


def table_binding(root, value, alias):
    from demiflow.lance.refs import DatasetRef

    identifier(alias)
    control = read_control(root / "tables" / f"{alias}.json", 16384)
    modern = control.get('format') == 'demiflow_forge_table_v2'
    if (
        set(control) != {"format", "alias", "location_id", "ref"} | ({'display', 'provenance'} if modern else set())
        or control["format"] not in {"demiflow_forge_table_v1", 'demiflow_forge_table_v2'}
        or control["alias"] != alias
    ):
        raise ForgeError("Invalid table reference")
    if control["location_id"] not in value["locations"]:
        raise ForgeError("Unknown table location")
    ref = DatasetRef.from_dict(control["ref"])
    base = location_root(root, value["locations"][control["location_id"]])
    uri = relative_path(base, ref.relative_uri)
    if ref.store_id != control["location_id"]:
        raise ForgeError("Reference store and location differ")
    return control, ref, uri


def table_identity(control):
    """Presentation edits do not change a fixed Dataset binding or old v1 IDs."""
    return {key: 'demiflow_forge_table_v1' if key == 'format' else control[key]
            for key in ('format', 'alias', 'location_id', 'ref')}


_WRAPPED_PARAM_FORMATS = frozenset({
    "demiflow_forge_flow_parameters_v1",
    "demiflow_forge_flow_parameters_v2",
})
_FORBIDDEN_ABS_ROOTS = (Path('/etc'), Path('/proc'), Path('/sys'), Path('/dev'), Path('/root'))


def extract_arguments(parameters):
    """Extract raw flow arguments from flat JSON or legacy wrapped v1/v2 parameter documents.

    Returns ``(arguments_dict, format_tag)`` where ``format_tag`` is ``'flat'`` or one of
    the legacy wrapped format strings.
    """
    if not isinstance(parameters, dict):
        raise ForgeError("Invalid flow parameter format", code='invalid_request')
    if "format" in parameters:
        fmt = parameters.get("format")
        if (
            fmt not in _WRAPPED_PARAM_FORMATS
            or set(parameters) != {"format", "arguments"}
            or not isinstance(parameters["arguments"], dict)
        ):
            raise ForgeError("Invalid flow parameter format", code='invalid_request')
        return parameters["arguments"], fmt
    if (
        "arguments" in parameters
        and isinstance(parameters["arguments"], dict)
        and set(parameters) <= {"arguments", "run_directory"}
    ):
        raise ForgeError("Invalid flow parameter format", code='invalid_request')
    return parameters, "flat"


def is_direct_table_ref(item):
    """True for direct URI Lance input references without tables/<alias>.json."""
    if not isinstance(item, dict):
        return False
    if item.get("kind") == "table" and "uri" in item:
        return True
    if (
        "kind" not in item
        and set(item) in ({"uri"}, {"uri", "version"})
        and isinstance(item.get("uri"), str)
        and item["uri"].endswith(".lance")
        and ("version" not in item or item["version"] is None or type(item["version"]) is int)
    ):
        return True
    return False


def resolve_table_uri(root, raw_uri, version=None):
    from .io import no_symlinks

    if not isinstance(raw_uri, str) or not raw_uri or len(raw_uri.encode('utf-8')) > 4096:
        raise ForgeError("Invalid table URI", code='invalid_request')
    if version is not None and (type(version) is not int or not 1 <= version < 2**64):
        raise ForgeError("Table version must be a positive integer", code='invalid_request')
    candidate = Path(raw_uri)
    if candidate.is_absolute():
        target = candidate.absolute()
        no_symlinks(target)
        if (
            any(p.startswith('.') for p in target.parts[1:])
            or any(target == fb or fb in target.parents for fb in _FORBIDDEN_ABS_ROOTS)
        ):
            raise ForgeError("Table URI points to a forbidden or hidden path", code='operation_rejected')
    else:
        if any(p.startswith('.') for p in candidate.parts):
            raise ForgeError("Table URI must not use hidden path segments", code='invalid_request')
        target = relative_path(root, raw_uri)
    if target.suffix != ".lance":
        raise ForgeError("Input table URI must name a .lance directory", code='invalid_request')
    return target, version


def resolve_output_uri(root, raw_uri):
    if not isinstance(raw_uri, str) or not raw_uri or len(raw_uri.encode('utf-8')) > 4096:
        raise ForgeError("Invalid output URI", code='invalid_request')
    if any(p.startswith('.') for p in Path(raw_uri).parts):
        raise ForgeError("Output URI must not use hidden path segments", code='invalid_request')
    target = relative_path(root, raw_uri)
    data_root = Path(root).absolute() / "data"
    if not target.is_relative_to(data_root) or target == data_root:
        raise ForgeError("Output URI must stay inside project data/ directory", code='invalid_request')
    if target.suffix != ".lance":
        raise ForgeError("Output must name a .lance table", code='invalid_request')
    return target


def declared_table_uris(parameters):
    """Yield ``(pointer, uri, version)`` for every direct Lance table reference in parameters."""
    try:
        arguments, _ = extract_arguments(parameters)
    except ForgeError:
        return
    pending = [(arguments, '', 0)]
    visits = 0
    while pending:
        obj, pointer, depth = pending.pop()
        visits += 1
        if visits > 4096 or depth > 16:
            raise ForgeError("Parameter structure exceeds budget", code='resource_limit')
        if type(obj) is dict:
            if is_direct_table_ref(obj):
                yield pointer, obj["uri"], obj.get("version")
            elif len(obj) <= 256:
                pending.extend((v, pointer + '/' + str(k), depth + 1) for k, v in obj.items())
            else:
                raise ForgeError("Parameter member budget exceeded", code='resource_limit')
        elif type(obj) is list:
            if len(obj) > 256:
                raise ForgeError("Parameter item budget exceeded", code='resource_limit')
            pending.extend((v, pointer + '/' + str(k), depth + 1) for k, v in enumerate(obj))


def resolve_arguments(root, value, parameters, *, table_resolver, table_describer=None):
    from demiflow.lance.preview import describe_table

    arguments, param_format = extract_arguments(parameters)
    tables = {}
    targets = {}
    controls = {}

    def visit(item, depth=0):
        if depth > 16:
            raise ForgeError("Parameter nesting exceeds budget", code='resource_limit')
        if isinstance(item, dict):
            if item.get('kind') == 'location':
                if param_format not in {'demiflow_forge_flow_parameters_v2', 'flat'} or set(item) != {'kind', 'location_id'}:
                    raise ForgeError('Location arguments require v2 and an explicit location_id', code='invalid_request')
                location = value['locations'].get(item['location_id'])
                if location is None:
                    raise ForgeError('Unknown declared location', code='invalid_request')
                return str(location_root(root, location))
            if item.get("kind") == "table":
                if set(item) == {"kind", "alias"}:
                    alias = item["alias"]
                    control, ref, uri = table_resolver(root, value, alias)
                    description = (table_describer or describe_table)(uri, ref.lance_version)
                    if (
                        description["schema_hash"] != ref.schema_hash
                        or description["row_count"] != ref.row_count
                    ):
                        raise ForgeError("Pinned table metadata changed", code='invalid_request')
                    tables[alias] = {**description, "uri": str(uri), "reference": table_identity(control)}
                    if not alias.startswith("run:"):
                        controls["tables/" + alias + ".json"] = content_digest(table_identity(control))
                    return {"uri": str(uri), "version": ref.lance_version}
                if "uri" in item and set(item) <= {"kind", "uri", "version"}:
                    target, version = resolve_table_uri(root, item["uri"], item.get("version"))
                    description = (table_describer or describe_table)(target, version)
                    table_key = "uri:" + hashlib.sha256(str(target).encode()).hexdigest()[:16]
                    tables[table_key] = {
                        **description,
                        "uri": str(target),
                        "reference": {
                            "format": "demiflow_forge_table_uri_v1",
                            "uri": str(target),
                            "version": description["version"],
                            "schema_hash": description["schema_hash"],
                            "row_count": description["row_count"],
                        },
                    }
                    return {"uri": str(target), "version": description["version"]}
                raise ForgeError("Invalid table argument", code='invalid_request')
            if is_direct_table_ref(item):
                target, version = resolve_table_uri(root, item["uri"], item.get("version"))
                description = (table_describer or describe_table)(target, version)
                table_key = "uri:" + hashlib.sha256(str(target).encode()).hexdigest()[:16]
                tables[table_key] = {
                    **description,
                    "uri": str(target),
                    "reference": {
                        "format": "demiflow_forge_table_uri_v1",
                        "uri": str(target),
                        "version": description["version"],
                        "schema_hash": description["schema_hash"],
                        "row_count": description["row_count"],
                    },
                }
                return {"uri": str(target), "version": description["version"]}
            if item.get("kind") == "output":
                if "uri" in item and "location_id" not in item and set(item) <= {"kind", "uri", "mode", "expected_version"}:
                    path = resolve_output_uri(root, item["uri"])
                    mode = item.get("mode", "create")
                    expected = item.get("expected_version")
                    if (
                        mode not in {'create', 'overwrite', 'checkpoint'}
                        or mode in {'create', 'checkpoint'} and expected is not None
                        or mode == 'overwrite' and (type(expected) is not int or expected < 1)
                    ):
                        raise ForgeError('Output mode requires create/checkpoint or overwrite with a valid fixed CAS baseline', code='invalid_request')
                    if str(path) in targets:
                        raise ForgeError('Each output target must have one declaration', code='invalid_request')
                    targets[str(path)] = {'mode': mode, 'expected_version': expected}
                    return {'uri': str(path), 'mode': mode, 'expected_version': expected}
                modern = param_format in {'demiflow_forge_flow_parameters_v2', 'flat'}
                required = {'kind', 'location_id', 'relative_uri'} | ({'mode', 'expected_version'} if param_format == 'demiflow_forge_flow_parameters_v2' else set())
                allowed = {'kind', 'location_id', 'relative_uri', 'mode', 'expected_version'} if modern else required
                if not (required <= set(item) <= allowed):
                    raise ForgeError("Invalid output argument", code='invalid_request')
                location = value["locations"].get(item["location_id"])
                if not location or not location["writable"]:
                    raise ForgeError("Output location must be workspace-owned", code='invalid_request')
                mode, expected = item.get('mode', 'create'), item.get('expected_version')
                if (mode not in location.get('write_modes', ['create']) or mode in {'create', 'checkpoint'} and expected is not None
                        or mode == 'overwrite' and (type(expected) is not int or expected < 1)):
                    raise ForgeError('Output mode requires location permission and a valid fixed CAS baseline', code='invalid_request')
                path = relative_path(
                    location_root(root, location), item["relative_uri"]
                )
                if path.suffix != ".lance":
                    raise ForgeError("Output must name a .lance table", code='invalid_request')
                if str(path) in targets:
                    raise ForgeError('Each output target must have one declaration', code='invalid_request')
                targets[str(path)] = {'mode': mode, 'expected_version': expected}
                return {'uri': str(path), **({'mode': mode, 'expected_version': expected} if modern else {})}
            return {k: visit(v, depth + 1) for k, v in item.items()}
        if isinstance(item, list):
            if len(item) > 1024:
                raise ForgeError("Parameter sequence exceeds budget", code='resource_limit')
            return [visit(v, depth + 1) for v in item]
        if item is None or type(item) in (str, int, float, bool):
            return item
        raise ForgeError("Only finite JSON parameters are accepted", code='invalid_request')

    args = visit(arguments)
    bounded_json(args, 1024**2)
    return args, tables, targets, controls


def resolve_flow_resources(root, tree, references, bindings, *, control=None):
    """Read declared files under the selected project's source budget.

    Analyzers receive configuration text or immutable file metadata, never a
    workspace, SourceTree, connection or unrestricted reader.
    """
    if type(references) is not list or len(references) > 256:
        raise ForgeError("Declared resource count exceeds budget")
    result = {}
    for reference in references:
        if type(reference) is not dict or type(reference.get('parameter')) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', reference['parameter']) or reference['parameter'] in result:
            raise ForgeError('Invalid or duplicate resource parameter')
        if reference.get('kind') == 'objects':
            if set(reference)-{'access'} != {'kind', 'parameter', 'root', 'relocations', 'layout'}:
                raise ForgeError('Invalid object location reference')
            reference=dict(reference)
            from demiflow.objects import local_object_directory
            from .io import no_symlinks
            reference['root'] = str(local_object_directory(reference['root'], base=root))
            if reference.get('access') not in {None,'write'}:
                raise ForgeError('Unknown object resource access')
            workspace = control if control is not None else load_project(root)[1]
            declared_locations = workspace.get('locations') or {}
            binding = {k: reference[k] for k in ('kind', 'root', 'relocations', 'layout')}
            if declared_locations:
                matches = [key for key, location in declared_locations.items()
                           if str(location_root(root, location)) == reference['root']]
                if len(matches) != 1:
                    raise ForgeError('Object root must match one registered workspace location')
                if reference.get('access')=='write' and not declared_locations[matches[0]].get('writable',False):
                    raise ForgeError('Image object destination is not writable')
                binding['location_id'] = matches[0]
            else:
                obj_path = Path(reference['root']).absolute()
                no_symlinks(obj_path)
                proj_root = Path(root).absolute()
                if (any(p.startswith('.') for p in obj_path.parts[1:])
                        or any(obj_path == fb or fb in obj_path.parents for fb in _FORBIDDEN_ABS_ROOTS)
                        or obj_path == proj_root
                        or obj_path in proj_root.parents):
                    raise ForgeError('Object root points to a forbidden, hidden, or workspace-encompassing path', code='operation_rejected')
                if reference.get('access') == 'write':
                    data_root = proj_root / 'data'
                    if obj_path != data_root and not obj_path.is_relative_to(data_root):
                        raise ForgeError('Image object destination must stay inside project data/ directory', code='invalid_request')
            if 'access' in reference:binding['access']=reference['access']
            key = 'objects:' + content_digest(binding)
            if key not in bindings and len(bindings) >= 256:
                raise ForgeError('Declared resource closure exceeds count budget')
            bindings[key] = binding
            result[reference['parameter']] = binding
            continue
        if type(reference) is dict and reference.get('kind') == 'ledger':
            from demiflow.operator_llm.sqlite_journal import MAX_DISK_BYTES
            if set(reference) != {'kind', 'parameter', 'path', 'format', 'max_bytes'}:
                raise ForgeError('Invalid native ledger reference')
            path = reference['path']
            if type(path) is not str or not re.fullmatch(r'evidence/[A-Za-z][A-Za-z0-9_-]{0,63}\.sqlite', path):
                raise ForgeError('Run ledger must use evidence/<name>.sqlite')
            if type(reference['max_bytes']) is not int or not 4 * 1024**2 <= reference['max_bytes'] <= MAX_DISK_BYTES:
                raise ForgeError('Run ledger requires a 4 MiB..32 GiB disk ceiling')
            if type(reference['format']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', reference['format']):
                raise ForgeError('Invalid native ledger format')
            binding = {k: reference[k] for k in ('kind', 'path', 'format', 'max_bytes')}
            key = 'ledger:' + path
            if key in bindings and bindings[key] != binding:
                raise ForgeError('Shared ledger declarations must have identical limits and format')
            if key not in bindings and len(bindings) >= 256:
                raise ForgeError('Declared resource closure exceeds count budget')
            bindings[key] = binding
            result[reference['parameter']] = binding
            continue
        kind = reference.get('kind')
        required = {"kind", "parameter", "path"}
        if kind == 'file':
            required |= {'format', 'sha256', 'max_bytes'}
        if set(reference) != required:
            raise ForgeError("Invalid declared resource reference")
        if kind not in {'config', 'file'}:
            raise ForgeError("Declared resource kind has no workspace binding contract")
        maximum = 1024**2
        if kind == 'file':
            maximum = reference['max_bytes']
            if type(maximum) is not int or not 1 <= maximum <= 16 * 1024**2:
                raise ForgeError('Sealed file requires a byte ceiling <= 16 MiB')
            if type(reference['format']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', reference['format']):
                raise ForgeError('Invalid sealed file format')
            if type(reference['sha256']) is not str or not re.fullmatch('[0-9a-f]{64}', reference['sha256']):
                raise ForgeError('Sealed file requires an exact SHA256')
        name, path = reference["parameter"], reference["path"]
        physical = relative_path(root, path)
        if any(part.startswith('.') for part in Path(path).parts):
            raise ForgeError("Declared resource must be an author file within the project")
        allowance = maximum if path in bindings else min(maximum, tree.limits["total_bytes"] - tree.bytes)
        tree.require_visible(path)
        raw = read_bounded(physical, allowance)
        digest = hashlib.sha256(raw).hexdigest()
        binding = {"kind": kind, "path": path, "size": len(raw), "sha256": digest}
        if kind == 'file':
            if digest != reference['sha256']:
                raise ForgeError('Declared file SHA256 mismatch')
            binding.update(format=reference['format'], max_bytes=maximum)
        if path in bindings and bindings[path] != binding:
            raise ForgeError("Declared resource changed during analysis")
        if path not in bindings:
            if len(bindings) >= 256:
                raise ForgeError("Declared resource closure exceeds count budget")
            tree.bytes += len(raw)
            bindings[path] = binding
        result[name] = {**binding, "text": raw.decode("utf-8")} if kind == 'config' else dict(binding)
    return result
