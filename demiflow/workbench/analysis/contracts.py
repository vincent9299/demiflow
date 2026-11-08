"""Closed, size-bounded values shared by Notebook, Case UI and Python helpers."""
from __future__ import annotations

import hashlib
import json
import keyword
import math
import re

from demiflow.forge.schema import ForgeError

FORMATS = frozenset({"text", "md", "code", "json", "message", "dialogue",
                     "image", "audio", "video", "tag", "html"})
ROLES = frozenset({"input", "output", "reference", "context", "evaluation"})
LANGUAGES = frozenset({"python", "sql", "markdown"})
_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_SQL_ALIAS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}\Z")


def reject(message, *, code="invalid_request"):
    raise ForgeError(message, code=code)


def text(value, label, maximum=256, *, empty=False):
    if type(value) is not str or len(value) > maximum:
        reject(f"{label} exceeds its text limit")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        reject(f"{label} is not valid Unicode")
    if size > maximum or (not empty and not value.strip()) or "\x00" in value:
        reject(f"{label} is invalid")
    return value


def identity(value, label="ID"):
    if type(value) is not str or not _ID.fullmatch(value):
        reject(f"{label} must be an opaque identifier")
    return value


def sha(value, label="digest"):
    if type(value) is not str or not _SHA.fullmatch(value):
        reject(f"{label} must be SHA-256")
    return value


def sql_alias(value):
    if type(value) is not str or not _SQL_ALIAS.fullmatch(value):
        reject("SQL source alias must be a native SQL identifier")
    return value


def result_name(value):
    sql_alias(value)
    if keyword.iskeyword(value) or value.startswith("__"):
        reject("SQL result name must be an ordinary Python variable")
    return value


def integer(value, label, maximum, minimum=0):
    if type(value) is not int or not minimum <= value <= maximum:
        reject(f"{label} must be in {minimum}..{maximum}")
    return value


def fields(value, required, optional=(), *, label="object"):
    if type(value) is not dict or set(value) - (set(required) | set(optional)) or not set(required) <= set(value):
        reject(f"Invalid fields for {label}")
    return value


def sequence(value, label, maximum, *, minimum=0):
    if type(value) is not list or not minimum <= len(value) <= maximum:
        reject(f"{label} must contain {minimum}..{maximum} items")
    return value


def one_of(value, options, label):
    if type(value) is not str or value not in options:
        reject(f"Unsupported {label}")
    return value


def encoded_size(value, *, maximum=65536, nodes=8192, depth=16):
    """Exact canonical UTF-8 length, before allocating its JSON string/bytes.

    The only encoder remains json.dumps below. Counting includes string escape
    expansion, UTF-8, delimiters, keys and booleans. At most ``nodes`` native
    values and ``depth`` ancestors are visited; arbitrary encoders are rejected.
    The encoded byte budget does not claim to bound the input object's RSS.
    """
    count = 0
    size = 0
    ancestors = set()

    def add(amount):
        nonlocal size
        size += amount
        if size > maximum:
            reject("JSON exceeds its byte limit", code="resource_limit")

    def visit(item, level):
        nonlocal count
        count += 1
        if count > nodes or level > depth:
            reject("JSON exceeds its node/depth limit", code="resource_limit")
        kind = type(item)
        if kind is str:
            if len(item) > maximum:
                reject("JSON string exceeds its byte limit", code="resource_limit")
            add(2)
            for char in item:
                number = ord(char)
                if char in {'"', "\\", "\b", "\f", "\n", "\r", "\t"}:
                    add(2)
                elif number < 32:
                    add(6)
                elif number < 128:
                    add(1)
                elif number < 2048:
                    add(2)
                elif 0xD800 <= number <= 0xDFFF:
                    reject("JSON contains invalid Unicode")
                else:
                    add(3 if number < 65536 else 4)
        elif item is None:
            add(4)
        elif kind is bool:
            add(4 if item else 5)
        elif kind is int:
            if not -(2**63) <= item < 2**63:
                reject("JSON integer exceeds signed 64-bit range")
            add(len(str(item)))
        elif kind is float:
            if not math.isfinite(item):
                reject("JSON number must be finite")
            add(len(repr(item)))
        elif kind in (dict, list):
            if len(item) > nodes or id(item) in ancestors:
                reject("JSON collection is too large or cyclic", code="resource_limit")
            add(2 + max(0, len(item) - 1) + (len(item) if kind is dict else 0))
            ancestors.add(id(item))
            if kind is dict:
                for key, child in item.items():
                    if type(key) is not str:
                        reject("JSON object keys must be strings")
                    visit(key, level + 1)
                    visit(child, level + 1)
            else:
                for child in item:
                    visit(child, level + 1)
            ancestors.remove(id(item))
        else:
            reject("Only native JSON values are supported")

    visit(value, 0)
    return size


def encoded(value, *, maximum=65536, nodes=8192, depth=16):
    size = encoded_size(value, maximum=maximum, nodes=nodes, depth=depth)
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(data) != size:
        raise RuntimeError("Canonical JSON length calculation disagrees with the encoder")
    return data


def digest(value, **limits):
    return hashlib.sha256(encoded(value, **limits)).hexdigest()


def decode(data, *, maximum=65536, **limits):
    if type(data) is not bytes or len(data) > maximum:
        reject("JSON input exceeds its byte limit", code="resource_limit")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                reject("JSON contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(data, object_pairs_hook=unique,
                           parse_constant=lambda _: reject("JSON number must be finite"))
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, ForgeError):
            raise
        reject("Invalid JSON")
    encoded(value, maximum=maximum, **limits)
    return value


def cell(value):
    fields(value, {"id", "language", "source"}, {"result_name"}, label="Notebook cell")
    identity(value["id"], "cell ID")
    one_of(value["language"], LANGUAGES, "Notebook cell language")
    text(value["source"], "cell source", 16384, empty=True)
    if "result_name" in value:
        if value["language"] != "sql":
            reject("Only SQL cells have a separate result name")
        result_name(value["result_name"])
    # One cell must fit a64 KiB control envelope even with JSON escaping.
    encoded(value, maximum=48*1024)
    return dict(value)


def notebook(value):
    fields(value, {"title", "cells"}, label="Notebook")
    text(value["title"], "Notebook title")
    cells = [cell(item) for item in sequence(value["cells"], "cells", 64)]
    if len({item["id"] for item in cells}) != len(cells):
        reject("Notebook cell IDs must be unique")
    result = dict(title=value["title"], cells=cells)
    encoded(result, maximum=1024 * 1024, nodes=4096)
    return result


def notebook_patch(document, patch):
    """One bounded patch, with ordering explicit and stable IDs preserved."""
    encoded(patch)
    fields(patch, set(), {"title", "put_cells", "delete_cells", "order"}, label="Notebook patch")
    if not patch:
        reject("Notebook patch is empty")
    cells = {item["id"]: item for item in document["cells"]}
    puts = [cell(item) for item in sequence(patch.get("put_cells", []), "put_cells", 64)]
    deletes = [identity(item, "deleted cell ID") for item in sequence(patch.get("delete_cells", []), "delete_cells", 64)]
    if len(set(deletes)) != len(deletes) or len({item["id"] for item in puts}) != len(puts):
        reject("Repeated cell in patch")
    if set(deletes) & {item["id"] for item in puts}:
        reject("Cannot put and delete the same cell")
    if not set(deletes) <= set(cells):
        reject("Deleted cell is missing", code="conflict")
    for key in deletes:
        del cells[key]
    for item in puts:
        cells[item["id"]] = item
    order = patch.get("order", list(cells))
    sequence(order, "cell order", 64)
    if any(type(item) is not str for item in order) or len(set(order)) != len(order) or set(order) != set(cells):
        reject("Cell order must contain every current cell exactly once")
    return notebook(dict(title=patch.get("title", document["title"]), cells=[cells[key] for key in order]))


def export_notebook(document):
    document = notebook(document)
    cells = []
    for item in document["cells"]:
        language = item["language"]
        source = item["source"]
        metadata = {"demiflow": {"language": language}}
        if language == "sql":
            metadata["demiflow"]["sql_source"] = source
            metadata["demiflow"]["requires"] = "DemiFlow analysis helper and an authorized source binding"
            expression = "sql(" + repr(source) + ")"
            if "result_name" in item:
                metadata["demiflow"]["result_name"] = item["result_name"]
                expression = item["result_name"] + " = " + expression + "\n" + item["result_name"]
            source = "from demiflow.workbench.analysis.client import sql\n" + expression + "\n"
        cell_value = dict(id=item["id"], cell_type="markdown" if language == "markdown" else "code",
                          metadata=metadata, source=source)
        if language != "markdown":
            cell_value.update(execution_count=None, outputs=[])
        cells.append(cell_value)
    return dict(nbformat=4, nbformat_minor=5, metadata={"kernelspec": {
        "name": "python3", "language": "python", "display_name": "Python 3"},
        "demiflow": {"title": document["title"], "outputs_imported": False}}, cells=cells)


def import_notebook(value, *, title="Imported Notebook"):
    """Explicit source-only import. Never imports output, widget or kernel state."""
    encoded(value, maximum=2 * 1024 * 1024, nodes=32768, depth=16)
    if type(value) is not dict or value.get("nbformat") != 4:
        reject("Only nbformat 4 Notebook source import is supported")
    items = sequence(value.get("cells"), "import cells", 64)
    result = []
    for index, item in enumerate(items):
        if type(item) is not dict or item.get("cell_type") not in {"code", "markdown"}:
            reject("Unsupported imported cell type")
        metadata = item.get("metadata", {})
        if type(metadata) is not dict or type(metadata.get("demiflow", {})) is not dict:
            reject("Invalid imported cell metadata")
        extension = metadata.get("demiflow", {})
        language = extension.get("language", "markdown" if item["cell_type"] == "markdown" else "python")
        source = extension.get("sql_source") if language == "sql" else item.get("source", "")
        if type(source) is list:
            if len(source) > 16384 or any(type(line) is not str for line in source):
                reject("Invalid Notebook source lines")
            if sum(len(line) for line in source) > 16384:
                reject("Notebook cell source exceeds limit", code="resource_limit")
            source = "".join(source)
        cell_value = dict(id=item.get("id", f"cell-{index + 1}"), language=language, source=source)
        if "result_name" in extension:
            cell_value["result_name"] = extension["result_name"]
        result.append(cell(cell_value))
    return notebook(dict(title=title, cells=result))


def question(value):
    fields(value, {"id", "title", "type", "required"}, {"options", "minimum", "maximum"}, label="question")
    identity(value["id"], "question ID")
    text(value["title"], "question title")
    if type(value["required"]) is not bool:
        reject("question required must be boolean")
    kind = one_of(value["type"], {"choice", "multi", "rating", "text"}, "question type")
    expected = {"id", "title", "type", "required"}
    if kind in {"choice", "multi"}:
        expected.add("options")
        options = sequence(value.get("options"), "question options", 32, minimum=1)
        keys = set()
        for item in options:
            fields(item, {"id", "label"}, label="option")
            identity(item["id"], "option ID")
            text(item["label"], "option label")
            if item["id"] in keys:
                reject("Repeated question option ID")
            keys.add(item["id"])
    elif kind == "rating":
        expected |= {"minimum", "maximum"}
        integer(value.get("minimum"), "rating minimum", 10000, -10000)
        integer(value.get("maximum"), "rating maximum", 10000, value["minimum"])
    elif kind != "text":
        reject("Unsupported question type")
    if set(value) != expected:
        reject("Question fields do not match its type")
    encoded(value, maximum=16384)
    return value


def answer(definition, value):
    definition = question(definition)
    kind = definition["type"]
    if value is None:
        if definition["required"]:
            reject("Required question has no answer")
        return value
    if kind == "text":
        return text(value, "answer", 8192, empty=not definition["required"])
    if kind == "rating":
        return integer(value, "answer", definition["maximum"], definition["minimum"])
    options = {item["id"] for item in definition["options"]}
    if kind == "choice":
        if type(value) is not str or value not in options:
            reject("Answer is not a defined option")
    else:
        sequence(value, "answer selections", 32, minimum=int(definition["required"]))
        if any(type(item) is not str or item not in options for item in value) or len(set(value)) != len(value):
            reject("Answer contains an undefined or repeated option")
    return value


def typed_filter(value, schema):
    """Source fields only. No raw SQL, regex or Python expressions."""
    count = 0

    def scalar(item, field):
        kind = schema[field]["type"]
        if item is None:
            reject("Use is_null for null predicates")
        if kind == "bool" and type(item) is not bool:
            reject("Filter type mismatch")
        if kind == "int64" and (type(item) is not int or not -(2**63) <= item < 2**63):
            reject("Filter type mismatch")
        if kind == "float64" and (type(item) not in {int, float} or not math.isfinite(item)):
            reject("Filter type mismatch")
        if kind == "utf8":
            text(item, "filter value", 1024, empty=True)
        if kind not in {"bool", "int64", "float64", "utf8"}:
            reject("This field type does not support predicates")

    def walk(item, level):
        nonlocal count
        count += 1
        if level > 4 or count > 32:
            reject("Filter exceeds depth/node limit", code="resource_limit")
        if type(item) is not dict:
            reject("Invalid filter")
        op = one_of(item.get("op"), {"and", "or", "eq", "contains", "in", "range", "is_null"}, "filter operation")
        if op in {"and", "or"}:
            fields(item, {"op", "items"}, label="compound filter")
            for child in sequence(item["items"], "filter items", 16, minimum=1):
                walk(child, level + 1)
            return
        fields(item, {"op", "field", "value"}, label="filter predicate")
        field = item["field"]
        if type(field) is not str or field not in schema:
            reject("Unknown filter field")
        if op in {"eq", "contains"}:
            scalar(item["value"], field)
            if op == "contains" and schema[field]["type"] != "utf8":
                reject("contains requires a text field")
        elif op == "in":
            for part in sequence(item["value"], "filter values", 32, minimum=1):
                scalar(part, field)
        elif op == "range":
            fields(item["value"], {"lower", "upper"}, label="range")
            if schema[field]["type"] not in {"int64", "float64"}:
                reject("range requires a numeric field")
            scalar(item["value"]["lower"], field)
            scalar(item["value"]["upper"], field)
            if item["value"]["lower"] > item["value"]["upper"]:
                reject("Range lower bound exceeds upper bound")
        elif op == "is_null":
            if type(item["value"]) is not bool:
                reject("is_null requires a boolean")
        else:
            reject("Unsupported filter operation")

    if value is not None:
        walk(value, 0)
    return value


def output_groups(value, components):
    """A bounded display map; a browser declaration grants no group target."""
    known = {item["id"]: item for item in components}
    identifiers, used = set(), set()
    total = 0
    for group in sequence(value, "output groups", 8, minimum=1):
        fields(group, {"id", "label", "fields"}, label="output group")
        identity(group["id"], "output group ID");text(group["label"], "output group label", 256)
        if group["id"] in identifiers:reject("Repeated output group ID")
        identifiers.add(group["id"])
        selected = sequence(group["fields"], "output group members", 32, minimum=1)
        total += len(selected)
        if total > 32:reject("Output group member budget exceeded", code="resource_limit")
        has_output = False
        for member in selected:
            identity(member, "output group display field ID")
            item = known.get(member)
            if member in used or item is None or item["role"] not in {"output", "evaluation"}:
                reject("Repeated, unknown or non-output group member")
            used.add(member);has_output = has_output or item["role"] == "output"
        if not has_output:reject("An output group needs at least one real output member")


def case_view(value, source):
    fields(value, {"version", "source_snapshot", "case_key", "fields", "grouping", "filters", "sort", "layout", "questions"},
           {"variant_key", "output_groups", "name"}, label="Case view")
    if 'name' in value:
        text(value['name'],'Case view name',256)
        if '\n' in value['name'] or '\r' in value['name']:
            reject('Case view name must be a single line')
    if type(value["version"]) is not int or value["version"] not in {1, 2}:
        reject("Unsupported Case view version")
    if value["version"] == 1 and "output_groups" in value:
        reject("Output groups require Case view version 2")
    if value["version"] == 2 and ("output_groups" not in value or "variant_key" in value):
        reject("Version 2 needs explicit output groups, not a free variant column")
    if value["source_snapshot"] != source["digest"] or value["case_key"] != source["case_key"]:
        reject("Case view source binding differs", code="conflict")
    schema = {item["name"]: item for item in source["schema"]}
    if "variant_key" in value and (type(value["variant_key"]) is not str or value["variant_key"] not in schema):
        reject("Unknown variant field")
    keys = set()
    for item in sequence(value["fields"], "Case fields", 32, minimum=1):
        fields(item, {"id", "field", "label", "role", "format"}, {"language"}, label="Case field")
        identity(item["id"], "display field ID")
        text(item["label"], "display label")
        if item["id"] in keys or type(item["field"]) is not str or item["field"] not in schema:
            reject("Repeated display ID or unknown source field")
        keys.add(item["id"])
        one_of(item["role"], ROLES, "Case role")
        one_of(item["format"], FORMATS, "Case format")
        if "language" in item:
            if item["format"] != "code":
                reject("language is only valid for code fields")
            identity(item["language"], "code language")
    if value["version"] == 2:
        output_groups(value["output_groups"], value["fields"])
        if len({item["field"] for item in value["fields"]}) > 16:
            reject("Case view exceeds its real projection field budget", code="resource_limit")
    for field in sequence(value["grouping"], "grouping fields", 4):
        if type(field) is not str or field not in schema:
            reject("Unknown grouping field")
    if len(set(value["grouping"])) != len(value["grouping"]):
        reject("Repeated grouping field")
    seen = set()
    for item in sequence(value["sort"], "sort fields", 4):
        fields(item, {"field", "direction"}, label="sort")
        if type(item["field"]) is not str or item["field"] not in schema or item["field"] in seen or type(item["direction"]) is not str or item["direction"] not in {"asc", "desc"}:
            reject("Invalid sort field or direction")
        seen.add(item["field"])
    typed_filter(value["filters"], schema)
    one_of(value["layout"], {"card", "comparison", "gallery"}, "Case layout")
    questions = [question(item) for item in sequence(value["questions"], "questions", 16)]
    if len({item["id"] for item in questions}) != len(questions):
        reject("Repeated question ID")
    encoded(value)
    return value


def source_snapshot(value):
    """A trusted resolver supplies this after verifying an immutable binding."""
    fields(value, {"kind", "binding", "schema", "rows", "case_key", "retention"}, label="source snapshot")
    one_of(value["kind"], {"fixed_lance", "pinned_object"}, "fixed source kind")
    one_of(value["retention"], {"external", "pinned"}, "source retention")
    if (value["kind"] == "pinned_object") != (value["retention"] == "pinned"):
        reject("Pinned source kind and retention must agree")
    sha(value["binding"], "source binding")
    integer(value["rows"], "source rows", 2**63 - 1)
    names = set()
    for item in sequence(value["schema"], "source schema", 256):
        fields(item, {"name", "type", "nullable"}, label="source field")
        text(item["name"], "source field name")
        text(item["type"], "source field type", 128)
        if item["name"] in names or type(item["nullable"]) is not bool:
            reject("Invalid source schema")
        names.add(item["name"])
    fields(value["case_key"], {"kind", "field"}, label="Case key")
    key = value["case_key"]
    if key["kind"] == "row_id":
        if key["field"] is not None:
            reject("Lance row_id is independent of a visible field")
    elif key["kind"] == "unique_field":
        if type(key["field"]) is not str or key["field"] not in names:
            reject("Unknown stable Case key field")
    else:
        reject("Unsupported stable Case key")
    encoded(value)
    return {**value, "digest": digest(value)}
