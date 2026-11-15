"""Opt-in checks of actual native callable values, using the static type facts.

No business registration, coercion, payload logging or alternate callable API.
The checks travel with CallableSpec to the worker that actually invokes it.
"""
import inspect
import sys
import types


class RuntimeContractError(TypeError):
    pass


def check_size(value, maximum=8 * 1024**2):
    remaining = maximum
    count = 0

    def visit(item, depth):
        nonlocal remaining, count
        count += 1
        if depth > 16 or count > 100000:
            raise RuntimeContractError('Value nesting/item budget exceeded')
        remaining -= sys.getsizeof(item)
        if remaining < 0:
            raise RuntimeContractError('Value byte budget exceeded')
        if type(item) is dict:
            for key, child in item.items():
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(item) in (list, tuple, set, frozenset):
            for child in item:
                visit(child, depth + 1)
        elif item is not None and type(item) not in (str, bytes, int, float, bool):
            from ..execution.contracts.scalar_values import scalar_node
            try:
                node = scalar_node(item, max_bytes=maximum)
            except OverflowError as exc:
                raise RuntimeContractError('Value byte budget exceeded') from exc
            except ValueError as exc:
                raise RuntimeContractError('Unsupported runtime value type: ' + str(exc)) from exc
            if node is None:
                raise RuntimeContractError('Unsupported runtime value type: ' + type(item).__name__)
            # Account for timezone names/Decimal coefficients as well as their
            # native scalar storage before the input deep copy is admitted.
            visit(node, depth + 1)

    visit(value, 0)
    return maximum - remaining


def check_value(value, expected, label, depth=0):
    if depth > 16:
        raise RuntimeContractError(label + ': type depth exceeded')
    kind = expected['kind']
    if value is None:
        if kind == 'none' or expected.get('nullable'):
            return
        raise RuntimeContractError(label + ': null is not allowed')
    if kind == 'union':
        options = expected.get('options', ())
        if not 1 <= len(options) <= 8:
            raise RuntimeContractError(label + ': invalid union alternatives')
        for option in options:
            try:
                check_value(value, option, label, depth + 1)
                return
            except RuntimeContractError:
                pass
        raise RuntimeContractError(label + ': value matches no union alternative')
    scalar = {'str': str, 'bytes': bytes, 'int': int, 'float': float, 'bool': bool}
    if kind in scalar:
        if type(value) is not scalar[kind]:
            raise RuntimeContractError(label + ': expected ' + kind + ', got ' + type(value).__name__)
        return
    if kind in {'record', 'dict'}:
        if type(value) is not dict:
            raise RuntimeContractError(label + ': expected mapping')
        if kind == 'record':
            for name, field in expected['fields'].items():
                if name not in value:
                    if field['required']:
                        raise RuntimeContractError(label + '.' + name + ': missing required key')
                else:
                    check_value(value[name], field['type'], label + '.' + name, depth + 1)
        else:
            for key, child in value.items():
                check_value(key, expected['key'], label + '.key', depth + 1)
                check_value(child, expected['value'], label + '.value', depth + 1)
        return
    if kind in {'list', 'tuple'} and type(value) is (list if kind == 'list' else tuple):
        if 'length' in expected and len(value) != expected['length']:
            raise RuntimeContractError(label + ': sequence length differs from native declaration')
        for child in value:
            check_value(child, expected['item'], label + '[]', depth + 1)
        return
    raise RuntimeContractError(label + ': unsupported or mismatched type ' + kind)


class CallValidation:
    def __init__(self, function, contract):
        self.contract = contract
        self.signature = inspect.signature(function)
        self.label = contract['label']

    def inputs(self, args, kwargs):
        self._size((args, kwargs), self.contract.get('max_input_bytes'))
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        for p in self.contract['parameters']:
            if p['name'] in bound.arguments:
                check_value(bound.arguments[p['name']], p['type'], self.label + '.' + p['name'])

    def output(self, result):
        expected = self.contract['returns']
        if expected['kind'] == 'iterator':
            if type(result) not in (types.GeneratorType, list, tuple):
                raise RuntimeContractError(self.label + ': unsupported iterator object')
            if type(result) in (list, tuple):
                self._size(result, self.contract.get('max_return_bytes'))
            return self._items(result, expected['item'])
        self._size(result, self.contract.get('max_return_bytes'))
        check_value(result, expected, self.label + '.return')
        return result

    def _items(self, result, expected):
        iterator = iter(result)
        try:
            for index, value in enumerate(iterator):
                if index >= self.contract.get('max_output_rows', 200000):
                    raise RuntimeContractError(self.label + ': expansion row budget exceeded')
                self._size(value)
                check_value(value, expected, self.label + '.yield')
                yield value
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                close()

    def _size(self, value, maximum=None):
        try:
            check_size(value, maximum or self.contract['max_row_bytes'])
        except RuntimeContractError as exc:
            raise RuntimeContractError(self.label + ': ' + str(exc)) from exc


def validator(function, contract):
    return CallValidation(function, contract) if contract is not None else None
