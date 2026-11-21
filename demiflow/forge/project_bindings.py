"""Static and runtime binding of native parameters and declared resources."""
import hashlib
import re
from pathlib import Path

from .platform import bounded_json, content_digest
from .platform_environment import read_bounded
from .project import load_project, location_root
from .project_io import identifier, relative_path, read_control
from .schema import ForgeError


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


def resolve_arguments(root, value, parameters, *, table_resolver, table_describer=None):
    from demiflow.lance.preview import describe_table

    if (
        not isinstance(parameters, dict)
        or parameters.get("format") not in {"demiflow_forge_flow_parameters_v1", "demiflow_forge_flow_parameters_v2"}
        or set(parameters) != {"format", "arguments"}
        or not isinstance(parameters["arguments"], dict)
    ):
        raise ForgeError("Invalid flow parameter format", code='invalid_request')
    tables = {}
    targets = {}
    controls = {}

    def visit(item, depth=0):
        if depth > 16:
            raise ForgeError("Parameter nesting exceeds budget", code='resource_limit')
        if isinstance(item, dict):
            if item.get('kind') == 'location':
                if parameters['format'] != 'demiflow_forge_flow_parameters_v2' or set(item) != {'kind', 'location_id'}:
                    raise ForgeError('Location arguments require v2 and an explicit location_id', code='invalid_request')
                location = value['locations'].get(item['location_id'])
                if location is None:
                    raise ForgeError('Unknown declared location', code='invalid_request')
                return str(location_root(root, location))
            if item.get("kind") == "table":
                if set(item) != {"kind", "alias"}:
                    raise ForgeError("Invalid table argument", code='invalid_request')
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
            if item.get("kind") == "output":
                modern = parameters['format'] == 'demiflow_forge_flow_parameters_v2'
                required = {'kind', 'location_id', 'relative_uri'} | ({'mode', 'expected_version'} if modern else set())
                if set(item) != required:
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

    args = visit(parameters["arguments"])
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
            from demiflow.objects import _local_path
            value=str(reference['root'])
            logical=_local_path(value) if value.startswith('file:') else Path(value)
            reference['root']=str(logical if logical.is_absolute() else Path(root)/logical)
            workspace = control if control is not None else load_project(root)[1]
            matches = [key for key, location in workspace['locations'].items()
                       if str(location_root(root, location)) == reference['root']]
            if len(matches) != 1:
                raise ForgeError('Object root must match one registered workspace location')
            if reference.get('access') not in {None,'write'}:
                raise ForgeError('Unknown object resource access')
            if reference.get('access')=='write' and not workspace['locations'][matches[0]].get('writable',False):
                raise ForgeError('Image object destination is not writable')
            binding = {k: reference[k] for k in ('kind', 'root', 'relocations', 'layout')}
            binding['location_id'] = matches[0]
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
