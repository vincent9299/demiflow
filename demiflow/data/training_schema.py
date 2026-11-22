"""Field contracts shared by training declarations, static inspection and bound data."""
from __future__ import annotations

PREDICTION_ROW_ID = "__demiflow_prediction_input_id"


def _fields(body):
    """Split named nested Arrow fields, leaving nested types and decimal parameters intact."""
    parts = []
    start = depth = 0
    for i, char in enumerate(body):
        if char in "<([":
            depth += 1
        elif char in ">)]":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(body[start:i].strip())
            start = i + 1
        if depth < 0:
            raise ValueError("malformed Arrow field declaration")
    if depth:
        raise ValueError("unclosed Arrow field type")
    parts.append(body[start:].strip())
    return parts


def _canonical(text):
    import pyarrow as pa
    text = text.strip()
    if text.startswith("struct<") and text.endswith(">"):
        fields = []
        for member in _fields(text[7:-1]):
            name, sep, kind = member.partition(":")
            if not sep or not name.strip():
                raise ValueError("struct fields require a name and an Arrow type")
            fields.append((name.strip(), _canonical(kind.removesuffix(" not null"))))
        return ("struct", tuple(sorted(fields)))
    for kind in ("list", "large_list", "list_view", "large_list_view"):
        if text.startswith(kind + "<") and text.endswith(">"):
            child = text[len(kind) + 1:-1].strip()
            # Arrow may call the list's value field "item" or "element".
            name, sep, rest = child.partition(":")
            if sep and "<" not in name:
                child = rest.strip()
            return kind, _canonical(child.removesuffix(" not null"))
    aliases = {"str": "string", "integer": "int64", "boolean": "bool", "bytes": "binary"}
    try:
        return str(pa.type_for_alias(aliases.get(text, text)))
    except ValueError:
        # Decimal, temporal and extension types may have additional parameters.
        # Their exact Arrow spelling still certifies a known physical field.
        return text


def validate_columns(schema, columns):
    """Require the declared named fields and Arrow types. A source may carry other fields."""
    for name, expected in columns.items():
        if schema.get_field_index(name) < 0:
            raise ValueError("Missing or ambiguous training field " + name)
        actual = str(schema.field(name).type)
        if actual != expected and _canonical(actual) != _canonical(expected):
            raise ValueError(f"Training field {name} requires {expected}, found {actual}")

def prediction_columns(bindings, fields):
    """Map named predictor inputs to physical columns, preserving every type constraint."""
    columns = {}
    for name, column in bindings.items():
        declared = fields[name]
        previous = columns.setdefault(column, declared)
        if previous != declared and _canonical(previous) != _canonical(declared):
            raise ValueError(f"Conflicting predictor types for column {column}: {previous} and {declared}")
    return columns
