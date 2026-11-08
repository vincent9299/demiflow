"""Greedy, bounded Prompt input measurement using the native renderer.

Returns input ranges; does not choose candidates, alter their order, construct
business IDs, call a model, or declare successfully measured input accepted.
"""
from .parser import parse_prompt_pack, resolve_prompt
from .flow_contract import _value, validate_prompt_inputs
from ..data.flow_contract import scalar, field


def normalize_packing(arguments):
    from ..data.plan import normalize_bound_inputs
    a = dict(arguments)
    a['inputs'] = normalize_bound_inputs(a['inputs'])
    for name in ('prompt', 'config', 'item_argument', 'output', 'label'):
        if type(a[name]) is not str or not 1 <= len(a[name]) <= 1024:
            raise ValueError('Invalid prompt packing argument: ' + name)
    if a['item_argument'] not in a['inputs']:
        raise ValueError('Packing requires an explicitly bound item argument')
    a['queue_depth'] = a['concurrency'] if a['queue_depth'] is None else a['queue_depth']
    for name, maximum in [('max_items', 4096), ('max_input_items', 10000), ('max_input_chars', 16 * 1024**2),
        ('max_request_bytes', 64 * 1024**2), ('max_expansion_bytes', 128 * 1024**2),
        ('max_measure_bytes', 4 * 1024**3), ('concurrency', 64), ('queue_depth', 256)]:
        if type(a[name]) is not int or not 1 <= a[name] <= maximum:
            raise ValueError('Invalid finite prompt packing limit: ' + name)
    return a


def packing_resources(a):
    return [{'kind': 'config', 'parameter': 'config', 'path': a['config']}]


def _prompt(pack, a):
    prompt = resolve_prompt(pack, a['prompt'])
    if prompt.response_format == 'image':
        raise ValueError('Image generation has no prompt packing contract')
    if prompt.model.transport == 'systemone' and 'for_each' in prompt.systemone:
        policy = prompt.systemone['for_each']
        if a['item_argument'] != policy['input'] or a['max_items'] > policy['max_questions']:
            raise ValueError('Packing item argument/count differs from the native questions contract')
    return prompt


def _facts(prompt, arguments):
    return {'contract': 'demiflow.prompt.packing.v1', 'prompt': _value(prompt), 'arguments': dict(arguments)}


def analyze_packing(a, *, inputs, callables, resources):
    prompt = _prompt(parse_prompt_pack(resources['config']['text']), a)
    schema = inputs[0]
    validate_prompt_inputs(prompt, schema, a['inputs'])
    if schema[a['inputs'][a['item_argument']]]['type']['kind'] != 'list':
        raise ValueError('Prompt packing requires a list input')
    if a['output'] in schema:
        raise ValueError('Packing output cannot overwrite an input')
    result = {**scalar('list'), 'item': {**scalar('record'), 'fields': {
        'start': field(scalar('int')), 'stop': field(scalar('int')), 'error': field(scalar('str', nullable=True))}}}
    return {'fields': {**schema, a['output']: field(result)}, 'cardinality': 'one', 'order': 'unspecified',
        'driver': 'stream', 'kind': 'async_map', 'callable_form': 'sync', 'placement': 'thread', 'scope': 'stage',
        'native': _facts(prompt, a), 'stream_arguments': dict(concurrency=a['concurrency'], queue_depth=a['queue_depth'],
            catch=[], label=a['label'], hard_timeout=None, execution='thread'),
        'reserved_bytes': a['concurrency'] * (16 * a['max_request_bytes'] + 16 * 1024**2), 'resources': []}


class PackPromptInputs:
    def __init__(self, pack, **arguments):
        self.arguments = normalize_packing(arguments)
        self.prompt = _prompt(pack, self.arguments)

    def __call__(self, row):
        from ..data.runtime_contract import check_size
        from .tokens import CharacterCounter
        a = self.arguments
        if a['output'] in row:
            raise ValueError('Packing output overlaps input')
        values = {name: row[column] for name, column in a['inputs'].items()}
        check_size(values, 32 * 1024**2)
        items = values[a['item_argument']]
        if type(items) is not list or len(items) > a['max_input_items']:
            raise ValueError('Packing input item count exceeds its budget')
        if not items:
            # No request is rendered for an empty item sequence. Optional media
            # can therefore remain absent in skipped or failed input rows.
            return {**row, a['output']: []}
        image_bytes = 0
        for placeholder in self.prompt.template.placeholders:
            if placeholder.kind.value not in {'image', 'numbered_image'}:
                continue
            images = values[placeholder.name]
            for image in images if isinstance(images, list) else [images]:
                if type(image) is str and image.isascii() and image.startswith('data:image/'):
                    image_bytes += len(image)
                elif type(image) is bytes:
                    image_bytes += 4 * ((len(image) + 2) // 3) + 64
                else:
                    raise ValueError('Packing requires image bytes or data URLs')
        counter, measured = CharacterCounter(), 0

        def fits(start, stop):
            nonlocal measured
            if stop - start > a['max_items']:
                return False
            candidate = {**values, a['item_argument']: items[start:stop]}
            # Charge before rendering. This is a conservative work allowance,
            # separate from the exact native character count used for grouping.
            charge = 6 * check_size(candidate, 32 * 1024**2) + 6 * len(self.prompt.template.source) + 4096
            if measured + charge > a['max_measure_bytes']:
                raise ValueError('Prompt packing measurement work budget exceeded')
            measured += charge
            try:
                chars = counter.prompt(self.prompt, candidate)
            except ValueError:
                return False
            # Conservative JSON escaping/framing estimate, not tokenizer or
            # provider admission. The actual model node validates independently.
            wire = chars * 6 + image_bytes + 4096
            return chars <= a['max_input_chars'] and wire <= a['max_request_bytes'] and wire * (stop - start) <= a['max_expansion_bytes']

        ranges, start = [], 0
        for index in range(len(items)):
            if index > start and not fits(start, index + 1):
                ranges.append({'start': start, 'stop': index, 'error': None})
                start = index
            if not fits(index, index + 1):
                ranges.append({'start': index, 'stop': index + 1, 'error': 'input_budget_exceeded'})
                start = index + 1
        if start < len(items):
            ranges.append({'start': start, 'stop': len(items), 'error': None})
        return {**row, a['output']: ranges}


def packing_facts(actor):
    if type(actor) is not PackPromptInputs:
        raise TypeError('Unknown native packing actor')
    a = actor.arguments
    return {'native': _facts(actor.prompt, a),
            'reserved_bytes': a['concurrency'] * (16 * a['max_request_bytes'] + 16 * 1024**2)}
