"""Development-record serialization, using the shared durable text writer."""
import json

from ..io import atomic_write_text


def atomic_write_json(path, value):
    # Retain the existing task format, including default=str, NaN handling and
    # no trailing newline. Forge's other JSON contracts use stricter semantics.
    atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2,
                                      sort_keys=True, default=str))
