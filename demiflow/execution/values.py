"""Bounded scalar expressions over completed native action facts.

These describe checks and arguments, never execution topology. Python still
executes the Flow; the checker compares its actual arguments to these facts.
"""
import operator

_BINARY = {'eq': operator.eq, 'ne': operator.ne, 'lt': operator.lt,
           'le': operator.le, 'gt': operator.gt, 'ge': operator.ge,
           'add': operator.add, 'sub': operator.sub}


def resolve(value, completed, *, depth=0):
    if depth > 32:
        raise ValueError('Native value expression exceeds depth budget')
    if type(value) is list:
        return [resolve(item, completed, depth=depth + 1) for item in value]
    if type(value) is not dict:
        return value
    if set(value) == {'runtime_value'}:
        ref = value['runtime_value']
        if type(ref) is not dict or set(ref) not in ({'node'}, {'node', 'field'}) or ref['node'] not in completed:
            raise ValueError('Native value is not from a completed action')
        result = completed[ref['node']]
        if 'field' in ref:
            if type(result) is not dict or ref['field'] not in result:
                raise ValueError('Native completion fact has no requested field')
            result = result[ref['field']]
        return result
    if set(value) == {'value_expression', 'arguments'}:
        op, arguments = value['value_expression'], value['arguments']
        if type(arguments) is not list or not 1 <= len(arguments) <= 2:
            raise ValueError('Invalid native value expression')
        args = [resolve(v, completed, depth=depth + 1) for v in arguments]
        if any(type(v) not in (int, float, bool, str, type(None)) for v in args):
            raise ValueError('Native expressions require scalar values')
        if op == 'not' and len(args) == 1 and type(args[0]) is bool:
            return not args[0]
        if op in _BINARY and len(args) == 2:
            return _BINARY[op](*args)
        raise ValueError('Unsupported native value expression')
    return {key: resolve(item, completed, depth=depth + 1) for key, item in value.items()}
