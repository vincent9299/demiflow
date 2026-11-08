"""Shared bounded JSON primitives; callers own protocol and lifecycle semantics."""
import json
import math


def encode(value, limit):
    """Bound encoded accumulation, including JSON escaping and image data URLs."""
    chunks, size = [], 0
    for part in json.JSONEncoder(ensure_ascii=False, separators=(',', ':'), allow_nan=False).iterencode(value):
        encoded = part.encode('utf-8')
        size += len(encoded)
        if size > limit:
            raise ValueError('Codex message exceeds configured byte budget')
        chunks.append(encoded)
    return b''.join(chunks)


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key: ' + key)
            result[key] = value
        return result
    def constant(value):
        raise ValueError('Non-finite JSON value: ' + value)
    def number(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError('Non-finite JSON number')
        return parsed
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)

