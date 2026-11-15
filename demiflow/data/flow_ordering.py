"""Pure public ordering declarations; execution stays in the v3 registry."""
import copy


def normalize_ordering(kind, arguments):
    from .dataset import _columns, _optional_seed
    values = dict(arguments)
    if kind == 'sort':
        values['key'] = list(_columns(values['key'], 'Dataset.sort'))
        descending = values['descending']
        descending = [descending] * len(values['key']) if isinstance(descending, bool) else list(descending)
        if len(descending) != len(values['key']) or any(type(v) is not bool for v in descending):
            raise ValueError('Dataset.sort descending must be a bool or one bool per key')
        values['descending'] = descending
        if values['boundaries'] is not None:
            values['boundaries'] = list(values['boundaries'])
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in values['boundaries']):
                raise TypeError('Dataset.sort boundaries must be numeric')
    elif kind == 'repartition':
        if (values['num_blocks'] is None) == (values['target_num_rows_per_block'] is None):
            raise ValueError('Dataset.repartition requires exactly one block-count argument')
        for name in ('num_blocks', 'target_num_rows_per_block'):
            if values[name] is not None:
                values[name] = int(values[name])
                if values[name] < 1:
                    raise ValueError('Dataset.repartition block count must be positive')
        if values['keys'] is not None:
            values['keys'] = list(_columns(tuple(values['keys']), 'Dataset.repartition keys'))
        values['shuffle'], values['sort'] = bool(values['shuffle']), bool(values['sort'])
    elif kind in {'shuffle', 'block_order', 'sample'}:
        values['seed'] = _optional_seed(values['seed'], 'Dataset.' + kind)
        if kind == 'shuffle' and values['num_blocks'] is not None:
            values['num_blocks'] = int(values['num_blocks'])
            if values['num_blocks'] < 1:
                raise ValueError('Dataset.random_shuffle num_blocks must be positive')
        if kind == 'sample':
            values['fraction'] = float(values['fraction'])
            if not 0 <= values['fraction'] <= 1:
                raise ValueError('Dataset.random_sample fraction must be in [0, 1]')
    else:
        raise ValueError('Unknown ordering declaration')
    return values


def analyze_ordering(kind, arguments, *, inputs):
    from .flow_contract import require_fields
    schema = inputs[0]
    if kind == 'sort':
        require_fields(schema, arguments['key'])
    if kind == 'repartition' and arguments['keys'] is not None:
        require_fields(schema, arguments['keys'])
    return {'fields': copy.deepcopy(schema), 'driver': 'pull', 'input_driver': 'either',
            'cardinality': 'zero_or_one' if kind == 'sample' else 'one',
            'order': 'preserved' if kind == 'sample' else 'sorted' if kind == 'sort' else 'unspecified'}
