"""Closed, data-only representations of Python temporal/decimal scalar values.

No type imports from payloads, pickle, object reconstruction hooks, or arbitrary
``tzinfo`` calls. Existing primitive JSON/bytes encodings remain in their codec.
"""
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import sys
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SCALAR_TAGS = frozenset({'date', 'datetime', 'time', 'timedelta', 'decimal'})
_TYPES = frozenset({date, datetime, time, timedelta, Decimal})


def _timezone_node(value, max_bytes):
    if value is None:
        return None
    if value is timezone.utc:
        return {'kind': 'utc'}
    if type(value) is timezone:
        offset = value.utcoffset(None)
        name = value.tzname(None)
        if len(name.encode('utf-8')) > max_bytes:
            raise OverflowError('timezone name exceeds byte budget')
        return {'kind': 'fixed', 'microseconds': ((offset.days * 86400 + offset.seconds)
                * 1000000 + offset.microseconds), 'name': name}
    if type(value) is ZoneInfo:
        key = value.key
        if type(key) is not str or not key:
            raise ValueError('ZoneInfo requires a named timezone key')
        if len(key.encode('utf-8')) > min(max_bytes, 4096):
            raise OverflowError('timezone key exceeds byte budget')
        return {'kind': 'zoneinfo', 'key': key}
    raise ValueError('unsupported custom tzinfo in data value')


def scalar_node(value, *, max_bytes=8 * 1024**2):
    """Return a closed JSON descriptor, or None for other types (no coercion)."""
    kind = type(value)
    if kind not in _TYPES:
        return None
    # In particular, inspect Decimal's existing storage before creating a string
    # of its coefficient. The scalar plus its encoded representation are charged.
    if sys.getsizeof(value) > max_bytes:
        raise OverflowError('scalar value exceeds byte budget')
    if kind is Decimal:
        text = str(value)
        if len(text) > max_bytes:
            raise OverflowError('decimal coefficient exceeds byte budget')
        return {'t': 'decimal', 'v': text}
    if kind is date:
        return {'t': 'date', 'v': [value.year, value.month, value.day]}
    if kind is timedelta:
        return {'t': 'timedelta', 'v': [value.days, value.seconds, value.microseconds]}
    fields = [value.hour, value.minute, value.second, value.microsecond]
    if kind is datetime:
        fields = [value.year, value.month, value.day] + fields
    zone = _timezone_node(value.tzinfo, max_bytes)
    offset = value.utcoffset()  # only the exact built-in tz types passed above
    micros = None if offset is None else ((offset.days * 86400 + offset.seconds)
                                         * 1000000 + offset.microseconds)
    return {'t': 'datetime' if kind is datetime else 'time', 'v': fields,
            'fold': value.fold, 'tz': zone, 'offset_us': micros}


def _timezone_from_node(node):
    if node is None:
        return None
    if type(node) is not dict:
        raise ValueError('timezone descriptor must be a closed object')
    if node == {'kind': 'utc'}:
        return timezone.utc
    if set(node) == {'kind', 'microseconds', 'name'} and node['kind'] == 'fixed':
        if type(node['microseconds']) is not int or type(node['name']) is not str:
            raise ValueError('invalid fixed timezone value')
        return timezone(timedelta(microseconds=node['microseconds']), node['name'])
    if set(node) == {'kind', 'key'} and node['kind'] == 'zoneinfo':
        if type(node['key']) is not str or not 1 <= len(node['key'].encode('utf-8')) <= 4096:
            raise ValueError('invalid timezone key')
        try:
            return ZoneInfo(node['key'])
        except ZoneInfoNotFoundError as exc:
            raise ValueError('unknown named timezone') from exc
    raise ValueError('unsupported timezone descriptor')


def decode_scalar(node):
    """Decode only the explicit built-in domain; constructor validation is kept."""
    if type(node) is not dict or node.get('t') not in SCALAR_TAGS:
        raise ValueError('unknown scalar data tag')
    tag = node['t']
    fields = {'t', 'v', 'fold', 'tz', 'offset_us'} if tag in ('datetime', 'time') else {'t', 'v'}
    if set(node) != fields:
        raise ValueError('invalid scalar descriptor fields')
    if tag == 'decimal':
        if type(node['v']) is not str:
            raise ValueError('invalid decimal coefficient')
        try:
            value = Decimal(node['v'])
        except InvalidOperation as exc:
            raise ValueError('invalid decimal coefficient') from exc
        if str(value) != node['v']:
            raise ValueError('noncanonical decimal coefficient')
        return value
    count = {'date': 3, 'timedelta': 3, 'time': 4, 'datetime': 7}[tag]
    values = node['v']
    if type(values) is not list or len(values) != count or any(type(v) is not int for v in values):
        raise ValueError('invalid temporal scalar fields')
    if tag == 'date':
        return date(*values)
    if tag == 'timedelta':
        if not 0 <= values[1] < 86400 or not 0 <= values[2] < 1000000:
            raise ValueError('noncanonical timedelta fields')
        return timedelta(days=values[0], seconds=values[1], microseconds=values[2])
    if type(node['fold']) is not int or node['fold'] not in (0, 1):
        raise ValueError('invalid temporal fold')
    constructor = datetime if tag == 'datetime' else time
    expected_offset = node['offset_us']
    if expected_offset is not None and type(expected_offset) is not int:
        raise ValueError('invalid temporal UTC offset')
    value = constructor(*values, fold=node['fold'], tzinfo=_timezone_from_node(node['tz']))
    offset = value.utcoffset()
    micros = None if offset is None else ((offset.days * 86400 + offset.seconds)
                                         * 1000000 + offset.microseconds)
    if micros != expected_offset:
        raise ValueError('named timezone offset changed since encoding')
    return value
