"""Native, non-executable composition facts and structural transfer rules.

Declarations attach data to the existing function; they do not wrap it, register
business code, or create a second plan/executor. Unknown semantics stay unknown.
"""

from __future__ import annotations

import copy
from functools import partial
import json

FORMAT = "demiflow_api_flow_v2"
KINDS = {
    "read_table",
    "map",
    "filter",
    "join",
    "union",
    "project",
    "drop",
    "rename",
    "limit",
    "write_table",
    "materialize",
    "flat_map",
    "batch_map",
    "run_stream",
    "count",
    "reduce",
    "collect_summary",
    "from_items",
    "require_columns",
}


def handler_identity(function):
    """Identity only. Deserialized identities are never imported or invoked."""
    if isinstance(function, partial):
        return {"function": handler_identity(function.func)["function"],
                "arguments": list(function.args), "keywords": dict(function.keywords)}
    return {"function": function.__module__ + ":" + function.__qualname__,
            "arguments": [], "keywords": {}}


def flow_api(kind, *, datasets=(), callable=None, effects=(), execution="lazy",
             contract_id=None, role=None, normalize=None, resource_references=None,
             analyze=None, callable_ports=(), version=1):
    if callable is not None and callable_ports:
        raise ValueError('Declare callable ports once')
    ports = ([{'name': callable, 'role': 'transform', 'optional': False}]
             if callable is not None else [dict(p) for p in callable_ports])
    handlers = {
        "normalize": normalize or partial(normalize_arguments, kind, execution),
        "resource_references": resource_references or no_resources,
        "analyze": analyze or partial(analyze_fields, kind, execution),
    }
    declaration = {
        "format": FORMAT,
        "kind": kind,
        "datasets": list(datasets),
        "callable_ports": ports,
        "effects": list(effects),
        "execution": execution,
        "contract_id": contract_id or "demiflow." + kind + "." + execution,
        "version": version,
        "role": role or ("source" if not datasets else "bridge" if execution == "bridge"
                         else "action" if execution == "action" else "transform"),
        "handlers": {name: handler_identity(fn) for name, fn in handlers.items()},
        "owner": handler_identity(handlers["analyze"])["function"].split(":")[0],
    }

    def attach(function):
        function.__demiflow_flow__ = declaration
        function.__demiflow_flow_handlers__ = handlers
        return function

    return attach


def validate_semantics(value, parameters, *, historical=False):
    if value == {"status": "unknown"}:
        return value
    legacy = historical and type(value) is dict and value.get("format") == "demiflow_api_flow_v1"
    keys = {
        "format",
        "kind",
        "datasets",
        "effects",
        "execution",
    }
    if not legacy:
        keys.update({"contract_id", "version", "role", "handlers", "owner", "callable_ports"})
    else:
        keys.add('callable')
    if type(value) is not dict or set(value) != keys:
        raise ValueError("Invalid native flow declaration")
    if (value["format"] != FORMAT and not legacy) or not isinstance(value["kind"], str):
        raise ValueError("Unsupported native flow declaration format")
    if any(
        type(value[k]) is not list or any(type(x) is not str for x in value[k])
        for k in ("datasets", "effects")
    ):
        raise ValueError("Invalid native flow ports/effects")
    names = {p["name"] for p in parameters}
    ports = ([{'name': value['callable'], 'role': 'transform', 'optional': False}] if value.get('callable') else []) if legacy else value['callable_ports']
    if (type(ports) is not list or len(ports) > 16 or any(type(p) is not dict or set(p) != {'name', 'role', 'optional'}
            or type(p['name']) is not str or p['role'] not in {'transform', 'predicate'} or type(p['optional']) is not bool for p in ports)):
        raise ValueError('Invalid native callable ports')
    if len({p['name'] for p in ports}) != len(ports) or sum(p['role'] == 'transform' for p in ports) > 1:
        raise ValueError('Duplicate or ambiguous native callable ports')
    if set(value["datasets"]) - names or {p['name'] for p in ports} - names:
        raise ValueError("Native flow declaration references absent parameters")
    if value["execution"] not in {"lazy", "stream", "action", "bridge"}:
        raise ValueError("Invalid native execution declaration")
    if legacy:
        if len(json.dumps(value)) > 16384:
            raise ValueError("Native flow declaration exceeds budget")
        return copy.deepcopy(value)
    if (type(value["contract_id"]) is not str or not value["contract_id"]
            or type(value["version"]) is not int or value["version"] < 1
            or value["role"] not in {"source", "transform", "bridge", "action", "value", "query"}
            or type(value["owner"]) is not str or not value["owner"].startswith("demiflow.")):
        raise ValueError("Invalid native contract identity/role")
    if type(value["handlers"]) is not dict or set(value["handlers"]) != {"normalize", "resource_references", "analyze"}:
        raise ValueError("Invalid native semantic handlers")
    for handler in value["handlers"].values():
        if (type(handler) is not dict or set(handler) != {"function", "arguments", "keywords"}
                or type(handler["function"]) is not str or not handler["function"].startswith("demiflow.")
                or type(handler["arguments"]) is not list or type(handler["keywords"]) is not dict):
            raise ValueError("Invalid native semantic handler identity")
    if value["kind"] in KINDS:
        kind = value["kind"]
        arity = 0 if kind in {"read_table", "from_items"} else 2 if kind in {"join", "union"} else 1
        if len(value["datasets"]) != arity or len(set(value["datasets"])) != arity:
            raise ValueError(
                "Native flow dataset port arity conflicts with its semantics"
            )
        if bool(ports) != (kind in {"map", "filter", "flat_map", "batch_map", "reduce"}):
            raise ValueError("Native callable port conflicts with its semantics")
        modes = (
            {"lazy", "stream"}
            if kind == "map"
            else (
                {"action"}
                if kind in {"write_table", "run_stream", "count", "collect_summary", 'require_columns'}
                else {"bridge"} if kind in {"materialize", "from_items"} else {"stream"} if kind == "batch_map" else {"lazy"}
            )
        )
        if value["execution"] not in modes:
            raise ValueError("Native execution mode conflicts with its semantics")
    if len(json.dumps(value)) > 16384:
        raise ValueError("Native flow declaration exceeds budget")
    return copy.deepcopy(value)


def no_resources(arguments):
    return []


def normalize_guard(arguments):
    values = dict(arguments)
    condition, message = values['condition'], values['message']
    if type(condition) is not bool and not (type(condition) is dict and
            set(condition) in ({'runtime_value'}, {'value_expression', 'arguments'})):
        raise ValueError('Guard requires a boolean native value')
    if type(message) is not str or not 1 <= len(message.encode('utf-8')) <= 2048:
        raise ValueError('Guard message must contain 1..2048 UTF-8 bytes')
    return values


def analyze_guard(arguments, *, inputs, callables, resources):
    return {'fields': {}, 'driver': 'pull', 'value_type': scalar('none'),
            'cardinality': 'none', 'order': 'preserved'}


def normalize_stream_parameters(arguments, *, batch=False, effective=True):
    """Native scalar coercions; effective facts also resolve queue defaults."""
    result = dict(arguments)
    result['concurrency'] = int(1 if result.get('concurrency') is None else result['concurrency'])
    depth = result.get('queue_depth')
    result['queue_depth'] = (result['concurrency'] if effective else None) if depth is None else int(depth)
    if result['concurrency'] < 1 or result['queue_depth'] is not None and result['queue_depth'] < 1:
        raise ValueError('Stream concurrency and queue depth must be >= 1')
    if result.get('hard_timeout') is not None:
        result['hard_timeout'] = float(result['hard_timeout'])
    if batch:
        result['max_batch'] = int(result['max_batch'])
        if result['max_batch'] < 1:
            raise ValueError('batch_map max_batch must be >= 1')
        if result.get('flush_interval') is not None:
            result['flush_interval'] = float(result['flush_interval'])
    elif result.get('execution', 'inline') not in ('inline', 'thread'):
        raise ValueError("map_async execution must be 'inline' or 'thread'")
    return result


def normalize_arguments(kind, execution, arguments):
    """Pure native normalization; managed resource admission is separate."""
    result = dict(arguments)
    if kind == 'require_columns':
        names = result['columns']
        if (type(names) not in (list, tuple) or not 1 <= len(names) <= 2048
                or any(type(name) is not str or not 1 <= len(name) <= 256 for name in names)
                or len(set(names)) != len(names) or type(result['exact']) is not bool):
            raise ValueError('Column prerequisite requires bounded unique names and an exact flag')
        result['columns'] = list(names)
    if kind == 'collect_summary':
        if type(result['max_rows']) is not int or not 1 <= result['max_rows'] <= 128:
            raise ValueError('Summary row bound must be 1..128')
        if type(result['max_bytes']) is not int or not 1 <= result['max_bytes'] <= 256 * 1024:
            raise ValueError('Summary byte bound must be 1..256 KiB')
    if kind == 'run_stream':
        for key in ('on_progress', 'on_drain', 'cancellation', 'queue_factory', 'stop_file'):
            if result.get(key) is not None:
                raise ValueError(key + ' requires a declared native resource/callback contract')
    if kind == "read_table":
        result["columns"] = list(result.get("columns") or ())
        result["projection"] = dict(result.get("projection") or {})
    if kind in {"project", "drop"}:
        columns = result["cols"]
        result["cols"] = [columns] if isinstance(columns, str) else list(columns)
    if kind == "join":
        for name in ("on", "right_on"):
            keys = result.get(name) or result["on"]
            result[name] = [keys] if isinstance(keys, str) else list(keys)
    if kind == 'reduce':
        result['fn_kwargs'] = dict(result.get('fn_kwargs') or {})
        keys = result['on']
        result['on'] = [keys] if isinstance(keys, str) else list(keys)
        if not result['on'] or len(result['on']) != len(set(result['on'])):
            raise ValueError('Reduction requires unique nonempty keys')
        if type(result['chunk_bytes']) is not int or result['chunk_bytes'] < 1:
            raise ValueError('Reduction chunk_bytes must be a positive integer')
    if kind in {"map", "batch_map"}:
        if execution == "stream":
            result = normalize_stream_parameters(result, batch=kind == 'batch_map')
            result['execution'] = result.get('execution') or 'inline'
        elif any(result.get(key) is not None for key in ("concurrency", "queue_depth", "execution", "catch", "label", "callable_scope")):
            result["concurrency"] = result.get("concurrency") or 1
            result["queue_depth"] = result.get("queue_depth") or result["concurrency"]
            result["execution"] = result.get("execution") or "inline"
            result["callable_scope"] = result.get("callable_scope") or "stage"
    return result


def row_fields(returns, *, expansion=False, nullable=False, iterator=False):
    """Schema common to every emitted row, including conditional missing keys."""
    if returns.get("nullable") and not nullable:
        raise ValueError("Nullable callable result is not allowed for this node")
    variants = returns["options"] if returns["kind"] == "union" else [returns]
    rows = []
    for variant in variants:
        if variant["kind"] == "none" and nullable:
            continue
        if variant["kind"] == "list" and expansion or variant["kind"] in {"iterator", "tuple"} and iterator:
            variant = variant["item"]
        if variant["kind"] != "record" or variant.get("nullable"):
            raise ValueError("Callable must emit declared records")
        rows.append(variant["fields"])
    if not rows:
        raise ValueError("Callable must declare an emitted row type")
    result = {}
    for schema in rows:
        for name, value in schema.items():
            if name not in result:
                result[name] = copy.deepcopy(value)
            else:
                result[name]["type"] = union_type(result[name]["type"], value["type"])
    for name, value in result.items():
        value["required"] = all(name in schema and schema[name]["required"] for schema in rows)
        value["evidence"] = "derived"
    return result


def analyze_fields(kind, execution, arguments, *, inputs, callables, resources):
    """Non-executable facts shared by authoring and native capability checking."""
    if kind not in KINDS:
        raise ValueError("No native analyzer for this contract")
    schema = inputs[0] if inputs else {}
    if kind == 'require_columns':
        names = set(arguments['columns'])
        if not names <= schema.keys() or arguments['exact'] and names != schema.keys():
            raise ValueError('Dataset columns differ from the explicit prerequisite')
        return {'fields': {}, 'driver': 'pull', 'input_driver': 'pull', 'value_type': scalar('none'),
                'cardinality': 'none', 'order': 'preserved'}
    if kind == 'count':
        return {'fields': {}, 'value_type': scalar('int'), 'driver': 'pull',
                'input_driver': 'pull', 'cardinality': 'scalar', 'order': 'preserved'}
    if kind == 'collect_summary':
        def json_type(value):
            kind = value['kind']
            if kind in {'str', 'int', 'float', 'bool', 'none', 'never'}:
                return True
            if kind in {'list', 'tuple'}:
                return json_type(value['item'])
            if kind == 'record':
                return all(json_type(f['type']) for f in value['fields'].values())
            if kind == 'union':
                return all(json_type(v) for v in value['options'])
            return False
        if not all(json_type(f['type']) for f in schema.values()):
            raise ValueError('Bounded summaries require JSON scalar/list/record fields')
        return {'fields': {}, 'value_type': {**scalar('list'), 'item': {**scalar('record'), 'fields': schema}},
                'driver': 'pull', 'input_driver': 'pull', 'cardinality': 'bounded', 'order': 'preserved'}
    if kind == 'from_items':
        declared = resources['value_types']['items']
        if declared['kind'] != 'list' or declared.get('nullable'):
            raise ValueError('Managed from_items requires a bounded row list')
        return {'fields': row_fields(declared['item']), 'driver': 'pull', 'cardinality': 'bounded', 'order': 'preserved'}
    returns = callables[0]["returns"] if callables else None
    stream = execution == "stream" or kind == "map" and arguments.get("execution") is not None
    if kind == "read_table":
        schema = resources["fields"]
        if arguments.get("columns"):
            schema = transfer_fields("project", schema, {"cols": arguments["columns"]})
    elif kind == 'reduce':
        require_fields(schema, arguments['on'])
        schema = row_fields(returns, nullable=True)
    elif kind in {"flat_map", "batch_map"} or kind == "map" and execution == "stream":
        if kind == "flat_map" and returns["kind"] not in {"list", "tuple", "iterator"}:
            raise ValueError("flat_map must declare an iterable of rows")
        if kind == "batch_map" and returns["kind"] != "list":
            raise ValueError("batch_map must declare a list of rows, optionally nullable")
        schema = row_fields(returns, expansion=True, nullable=kind != "flat_map", iterator=kind == "flat_map")
    elif kind == "union":
        for other in inputs[1:]:
            schema = transfer_fields(kind, schema, arguments, other=other)
    elif kind != "run_stream":
        schema = transfer_fields(kind, schema, arguments, returns=returns,
                                 other=inputs[1] if len(inputs) > 1 else None)
    if kind == 'write_table' and arguments.get('schema') is not None:
        from ..lance.schema import build_schema
        schema = arrow_fields(build_schema(arguments['schema']['schema_declaration']))
    return {
        "fields": schema,
        "cardinality": ("zero_or_more" if kind in {"join", "flat_map", "batch_map"} or kind == "map" and execution == "stream"
                        else "zero_or_one_per_group" if kind == 'reduce' else "zero_or_one" if kind in {"filter", "limit"} else "one"),
        "order": "unspecified" if stream or kind in {"join", "union", "reduce"} else "preserved",
        "driver": "stream" if stream else "pull",
        "input_driver": "pull" if kind in {'join', 'union', 'reduce'} else 'stream' if kind == 'run_stream' else 'either',
        "callable_form": "awaitable" if callables and callables[0].get("async") else "sync" if callables else None,
        "placement": arguments.get("execution") or "inline",
        "scope": arguments.get("callable_scope") or "stage" if callables else None,
    }


def scalar(kind, *, nullable=False):
    return {"kind": kind, "nullable": nullable}


def literal_type(value):
    """Structural types of plain declaration values, without candidate code."""
    if value is None:
        return scalar('none')
    if type(value) in (bool, int, float, str, bytes):
        return scalar(type(value).__name__)
    if isinstance(value, dict) and all(type(k) is str for k in value):
        return {**scalar('record'), 'fields': {k: field(literal_type(v)) for k, v in value.items()}}
    if isinstance(value, (list, tuple)):
        item = literal_type(value[0]) if value else scalar('never')
        if any(not compatible(literal_type(v), item) for v in value):
            item = scalar('unknown')
        return {**scalar('list' if isinstance(value, list) else 'tuple'), 'item': item}
    return scalar('unknown')


def field(type_, *, required=True, evidence="declared"):
    return {"type": copy.deepcopy(type_), "required": required, "evidence": evidence}


def union_type(*types):
    """Canonical finite union. Missing fields are not represented by null."""
    options = []
    nullable = False
    for value in types:
        nullable = nullable or value.get("nullable", False) or value["kind"] == "none"
        for option in value.get("options", ()) if value["kind"] == "union" else (value,):
            if option["kind"] == "none":
                nullable = True
                continue
            option = dict(option, nullable=False)
            if option not in options:
                options.append(option)
            if len(options) > 8:
                raise ValueError("Union exceeds eight alternatives")
    if not options:
        return scalar("none")
    options.sort(key=lambda value: json.dumps(value, sort_keys=True))
    if len(options) == 1:
        return dict(options[0], nullable=nullable)
    return {**scalar("union", nullable=nullable), "options": options}


def compatible(source, target):
    if source["kind"] == "never":
        return True
    if source.get("nullable", False) and not target.get("nullable", False):
        return False
    if source["kind"] == "none":
        return target["kind"] == "none" or target.get("nullable", False)
    if source["kind"] == "union":
        return all(compatible(option, target) for option in source["options"])
    if target["kind"] == "union":
        return any(compatible(dict(source, nullable=False), option) for option in target["options"])
    if target["kind"] == "iterator" and source["kind"] in {"list", "tuple"}:
        return compatible(source["item"], target["item"])
    if source["kind"] != target["kind"]:
        return False
    if source["kind"] == "record":
        for name, expected in target["fields"].items():
            actual = source["fields"].get(name)
            if actual is None:
                if expected["required"]:
                    return False
            elif (expected["required"] and not actual["required"]) or not compatible(
                actual["type"], expected["type"]
            ):
                return False
    for key in ("item", "key", "value"):
        if key in target and (
            key not in source or not compatible(source[key], target[key])
        ):
            return False
    return True


def arrow_type(value, *, nullable=False, depth=0):
    import pyarrow as pa

    if depth > 16:
        raise ValueError("Schema depth exceeds budget")
    t = pa.types
    kind = (
        "bool"
        if t.is_boolean(value)
        else (
            "int"
            if t.is_integer(value)
            else (
                "float"
                if t.is_floating(value)
                else (
                    "str"
                    if t.is_string(value) or t.is_large_string(value)
                    else (
                        "bytes"
                        if t.is_binary(value) or t.is_large_binary(value)
                        else "none" if t.is_null(value) else None
                    )
                )
            )
        )
    )
    if kind:
        return {**scalar(kind, nullable=nullable), "physical": str(value)}
    if t.is_list(value) or t.is_large_list(value) or t.is_fixed_size_list(value):
        return {
            **scalar("list", nullable=nullable),
            "item": arrow_type(
                value.value_type, nullable=value.value_field.nullable, depth=depth + 1
            ),
        }
    if t.is_struct(value):
        return {
            **scalar("record", nullable=nullable),
            "fields": arrow_fields(value, depth=depth + 1),
        }
    raise ValueError(f"Unsupported Arrow port type: {value}")


def arrow_fields(schema, *, depth=0):
    if len(schema) > 2048 or len(set(schema.names)) != len(schema):
        raise ValueError("Schema field budget or duplicate field violation")
    return {
        f.name: field(
            arrow_type(f.type, nullable=f.nullable, depth=depth), evidence="metadata"
        )
        for f in schema
    }


def require_fields(schema, names):
    if not isinstance(names, (list, tuple)) or any(type(n) is not str for n in names):
        raise ValueError("Field names must be an explicit sequence")
    missing = set(names) - set(schema)
    if missing:
        raise ValueError(f"Missing field(s): {sorted(missing)}")


def transfer_fields(kind, schema, arguments, *, returns=None, other=None):
    """Apply native field semantics to declarations, never to candidate rows."""
    result = copy.deepcopy(schema)
    if kind == "map":
        if arguments.get("inputs") is None:
            if (
                arguments.get("output") is not None
                or arguments.get("outputs") is not None
            ):
                raise ValueError("Whole-row map cannot bind output(s) without inputs")
            if returns is None or returns["kind"] != "record":
                raise ValueError("Whole-row map requires a declared record return")
            return copy.deepcopy(returns["fields"])
        output, outputs = arguments.get("output"), arguments.get("outputs")
        if output is not None and outputs is not None:
            raise ValueError("map accepts either output or outputs, not both")
        if output is not None:
            if type(output) is not str or not output:
                raise ValueError("output must be a nonempty field")
            result[output] = field(returns)
        elif outputs is not None:
            if (
                returns is None
                or returns["kind"] != "record"
                or not isinstance(outputs, dict)
                or not outputs
            ):
                raise ValueError(
                    "outputs requires a declared record and a nonempty mapping"
                )
            for key, target in outputs.items():
                actual = returns["fields"].get(key)
                if not actual or not actual["required"]:
                    raise ValueError(f"Result key {key!r} must exist")
                if type(target) is not str or not target:
                    raise ValueError("outputs destination must be a nonempty field")
                result[target] = copy.deepcopy(actual)
        elif returns != scalar("none"):
            raise ValueError("Field-bound map without output(s) must return None")
    elif kind == "filter":
        if returns != scalar("bool"):
            raise ValueError("filter must declare a non-null bool return")
    elif kind == "join":
        if (
            type(arguments.get("chunk_bytes", 1)) is not int
            or arguments.get("chunk_bytes", 1) < 1
        ):
            raise ValueError(
                "join chunk_bytes must be positive; it is not a native RSS budget"
            )
        if type(arguments.get("suffix", "_right")) is not str:
            raise ValueError("join suffix must be a string")
        left = arguments["on"]
        right = arguments.get("right_on") or left
        left, right = [left] if isinstance(left, str) else left, (
            [right] if isinstance(right, str) else right
        )
        require_fields(schema, left)
        require_fields(other, right)
        how = arguments.get("how", "inner")
        if (
            how not in {"inner", "left", "semi", "anti"}
            or len(left) != len(right)
            or not left
        ):
            raise ValueError("Invalid join type or key arity")
        for a, b in zip(left, right):
            ta, tb = dict(schema[a]["type"], nullable=False), dict(
                other[b]["type"], nullable=False
            )
            if not compatible(ta, tb) or not compatible(tb, ta):
                raise ValueError(f"Incompatible join keys: {a}, {b}")
        if how in {"semi", "anti"}:
            return result
        if set(left) & set(right) and any(
            a != b and (a in right or b in left) for a, b in zip(left, right)
        ):
            raise ValueError(
                "Overlapping renamed join keys require data-dependent collision handling"
            )
        for name, value in other.items():
            if name in right and left[right.index(name)] == name:
                continue
            target = (
                name if name not in result else name + arguments.get("suffix", "_right")
            )
            if target in result:
                raise ValueError(f"Join column collision: {target}")
            result[target] = copy.deepcopy(value)
            if how == "left":
                result[target]["required"] = False
            result[target]["evidence"] = "derived"
    elif kind in {"project", "drop"}:
        names = arguments["cols"]
        names = [names] if isinstance(names, str) else names
        require_fields(schema, names)
        if len(names) != len(set(names)) or kind == "project" and not names:
            raise ValueError(
                "Projected columns must be nonempty; selected/dropped names must be unique"
            )
        result = {
            k: v for k, v in result.items() if (k in names) == (kind == "project")
        }
    elif kind == "rename":
        mapping = arguments["names"]
        if not mapping:
            raise ValueError("Renamed fields must be nonempty")
        if isinstance(mapping, (list, tuple)):
            if len(mapping) != len(schema):
                raise ValueError("Renamed column count differs")
            mapping = dict(zip(schema, mapping))
        require_fields(schema, list(mapping))
        if any(type(v) is not str or not v for v in mapping.values()):
            raise ValueError("Renamed fields must be nonempty strings")
        result = {mapping.get(k, k): v for k, v in result.items()}
        if len(result) != len(schema):
            raise ValueError("Renamed fields collide")
    elif kind == "union":
        if set(schema) != set(other) or any(
            not compatible(schema[k]["type"], other[k]["type"])
            or not compatible(other[k]["type"], schema[k]["type"])
            for k in schema
        ):
            raise ValueError("union requires compatible row schemas")
        for name in result:
            result[name]["required"] = (
                schema[name]["required"] and other[name]["required"]
            )
    elif kind not in {"limit", "write_table", "materialize"}:
        raise ValueError(f"Unsupported native transfer kind: {kind}")
    if len(result) > 2048:
        raise ValueError("Output schema exceeds field budget")
    return result
