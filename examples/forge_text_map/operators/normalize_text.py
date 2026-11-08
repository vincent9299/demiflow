from demiflow.forge import operator


@operator(
    name="normalize_text",
    version="1.0.0",
    description="Trim surrounding whitespace and apply the configured case conversion.",
    input_schema={
        "type": "object",
        "properties": {"id": {"type": "integer"}, "text": {"type": "string"}},
        "required": ["id", "text"],
        "additionalProperties": False,
    },
    output_schema={
        "type": "object",
        "properties": {"id": {"type": "integer"}, "text": {"type": "string"}},
        "required": ["id", "text"],
        "additionalProperties": False,
    },
    config_schema={
        "type": "object",
        "properties": {"uppercase": {"type": "boolean"}},
        "required": ["uppercase"],
        "additionalProperties": False,
    },
)
def normalize_text(row, *, uppercase):
    text = row["text"].strip()
    return {**row, "text": text.upper() if uppercase else text}
