"""Run explicitly with the separately built benchmark extension on PYTHONPATH."""
from collections import UserDict
from copy import deepcopy
import random

import pytest

pytest.importorskip('_demiflow_schema_probe')
from schema_probe import SnapshotValidator, variants
from demiflow.schema import validate_instance


def outcome(fn, value, schema):
    try:
        fn(value, schema, label='response')
        return None
    except Exception as exc:
        return type(exc), str(exc)


def compare(schema, values):
    methods = variants(schema)
    for value in values:
        expected = outcome(validate_instance, value, schema)
        for name, method in methods.items():
            assert outcome(method, value, schema) == expected, (name, value, schema)


@pytest.mark.parametrize('kind', ['integer', 'number', 'boolean', 'null'])
def test_numeric_and_type_boundaries(kind):
    values = [True, False, None, 0, 1, -1, 0.5, float('nan'), float('inf'), -float('inf'), 2**1024, -(2**1024), '1']
    compare({'type': kind}, values)
    if kind in ('integer', 'number'):
        for bound in [0, 2**1024, float('nan'), float('inf')]:
            for key in ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum']:
                compare({'type': kind, key: bound}, values)


def test_unicode_and_error_compatibility():
    values = ['', 'x', '中文', '😀', 'e\u0301', '\ud800', 'abc', 5]
    compare({'type': 'string', 'minLength': 1, 'maxLength': 2}, values)
    compare({'type': 'string', 'enum': ['中文', '\ud800']}, values)
    compare({'type': 'string', 'const': '😀'}, values)
    compare({'type': 'string', 'format': 'date-time'}, ['2026-11-01T12:00:00Z', 'bad', None])
    schema = {'type':'object', 'properties':{'x':{'type':'integer'}}, 'required':['x']}
    compare(schema, [{'x':1}, {'x':True}, {}, {'y':3}, {'x':1,'y':2}, UserDict(x=1), {'x':1, 2:4, 'a':2}])


def test_mutation_cannot_reuse_a_stale_schema():
    schema = {'type':'object','properties':{'x':{'type':'integer'}},'required':['x']}
    for backend in ['rust','python']:
        current = deepcopy(schema); check = SnapshotValidator(current, backend)
        check({'x':1}, current)
        current['properties']['x']['minimum'] = 2
        assert outcome(check, {'x':1}, current) == outcome(validate_instance, {'x':1}, current)
        class EqualString(str):
            def __eq__(self, other): return True
            __hash__ = str.__hash__
        current['properties']['x'] = {'type':EqualString('string')}
        assert outcome(check, {'x':1}, current) == outcome(validate_instance, {'x':1}, current)
        current = deepcopy(schema); current['properties']['x']['type'] = ['integer','null']
        assert outcome(check, {'x':None}, current) == outcome(validate_instance, {'x':None}, current)


def test_budget_and_unsupported_fallback():
    schema = {'type':'array','items':{'type':'integer'}}
    check = SnapshotValidator(schema)
    values = [0]*65537
    assert not check.predicate(values)
    assert outcome(check, values, schema) is None
    schemas = [
        {'type':'object','properties':{str(i):{'type':'integer'} for i in range(513)}},
        {'type':'string','const':'x'*65537},
        {'type':['integer','null']},
        {'type':'object','properties':{'x':{}}},
        {'type':'number','enum':[1,True,float('nan')]},
    ]
    for schema in schemas:
        assert SnapshotValidator(schema).predicate is None
        compare(schema, [{}, {'x':42}, None, 1, True, float('nan'), 'x'])


def test_seeded_differential_cases():
    rng = random.Random(20261101)
    def create(depth=0):
        kind = rng.choice(['object','array','string','integer','number','boolean','null'] if depth < 3
                          else ['string','integer','number','boolean','null'])
        schema = {'type':kind}
        if kind == 'object':
            children = {str(i):create(depth+1) for i in range(rng.randrange(4))}
            schema.update(properties={k:s for k,(s,v) in children.items()}, required=list(children), additionalProperties=rng.choice([False,True]))
            value = {k:v for k,(s,v) in children.items()}
        elif kind == 'array':
            child,value = create(depth+1); schema['items'] = child; value = [value]*rng.randrange(4)
            schema.update(minItems=0,maxItems=4)
        elif kind == 'string':
            value = rng.choice(['','中文','😀','\ud800','abc']); schema.update(minLength=0,maxLength=5)
            if rng.randrange(2): schema['enum'] = [value]
        elif kind in ('integer','number'):
            value = rng.choice([0,1,-100,2**128])
            schema.update(minimum=-(2**129),maximum=2**129)
            if kind == 'number' and rng.randrange(2): value = float('nan')
        elif kind == 'boolean': value = rng.choice([True,False])
        else: value = None
        if rng.randrange(8) == 0: schema['type'] = [kind,'null'] if kind != 'null' else kind
        return schema,value
    for _ in range(1500):
        schema, good = create()
        compare(schema, [good, None, True, [], {'unexpected':1}, 'wrong'])
