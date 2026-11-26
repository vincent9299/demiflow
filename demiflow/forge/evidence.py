"""Bounded native evidence reference validation, reading, and secret redaction."""
from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError


def validate_reference(reference):
    if (type(reference) is not dict or set(reference) - {'journal_path', 'request_id', 'kind', 'attempt'}
            or not {'journal_path', 'request_id', 'kind'} <= reference.keys()
            or any(type(reference[k]) is not str or not 1 <= len(reference[k]) <= 4096 for k in ('journal_path', 'request_id', 'kind'))
            or reference['kind'] not in {'request', 'response', 'error'}
            or type(reference.get('attempt', 1)) is not int or not 1 <= reference.get('attempt', 1) <= 64):
        raise ForgeError('Invalid native evidence reference')


def redact(value, depth=0):
    if depth > 24:
        raise ForgeError('Evidence nesting exceeds display budget')
    sensitive = {'authorization', 'proxyauthorization', 'apikey', 'accesstoken', 'refreshtoken',
                 'password', 'secret', 'clientsecret', 'cookie', 'setcookie', 'token'}

    def secret(key):
        return re.sub('[^a-z0-9]', '', key.lower()) in sensitive

    if type(value) is dict:
        return {key: '[redacted]' if secret(key) else redact(item, depth + 1) for key, item in value.items()}
    if type(value) is list:
        return [redact(item, depth + 1) for item in value]
    if type(value) is str:
        if value.startswith('data:') and ';base64,' in value[:256]:
            return '[inline binary omitted]'
        if value.startswith(('http://', 'https://')):
            parsed = urlsplit(value)
            host = parsed.netloc.rsplit('@', 1)[-1]
            query = urlencode([(key, '[redacted]' if secret(key) else val) for key, val in parse_qsl(parsed.query, keep_blank_values=True, max_num_fields=128)])
            return urlunsplit((parsed.scheme, host, parsed.path, query, parsed.fragment))
    return value


def read_evidence(reference, path):
    """Read only stored JSON: never expand offline binary references."""
    from demiflow.operator_llm.call_ref import PromptRecordRef
    validate_reference(reference)
    document = PromptRecordRef(**{**reference, 'journal_path': str(path)}).read(
        max_bytes=1024**2, restore_inputs=False, resolve_locations=False)
    bounded_json(document, 1024**2)
    return redact(document)
