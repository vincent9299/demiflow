"""Bounded, caller-owned chat messages. No history or conversation policy."""
from collections.abc import Mapping
import json

DEFAULT_LIMITS = {'max_messages': 64, 'max_parts': 256, 'max_images': 16,
                  'max_bytes': 64 * 1024 * 1024}


def message_limits(value=None):
    if value is not None and (not isinstance(value, Mapping) or set(value) - set(DEFAULT_LIMITS)):
        raise ValueError('message_limits has unsupported fields')
    result = {**DEFAULT_LIMITS, **(value or {})}
    if any(type(v) is not int or v < 1 or v > DEFAULT_LIMITS[k] for k, v in result.items()):
        raise ValueError('message_limits must be positive integers within platform ceilings')
    return result


def validate_messages(value, limits=None):
    """Validate before copying; bound UTF-8 values plus conservative JSON framing.

    Strings are immutable and shared. This limits admitted payload, not upstream
    allocation or process RSS. HTTP serialization/journaling add bounded copies.
    """
    limits = message_limits(limits)
    if not isinstance(value, (list, tuple)) or not 1 <= len(value) <= limits['max_messages']:
        raise ValueError('messages must be a nonempty bounded list')
    used, parts, images = 2, 0, 0

    def text(item):
        nonlocal used
        if not isinstance(item, str):
            raise ValueError('message text/URLs must be strings')
        if len(item) > limits['max_bytes'] - used:
            raise ValueError('messages exceed max_bytes')
        # Bounded temporary chunks, including escaping; never serialize the
        # entire unvalidated input just to discover that it exceeds the budget.
        for start in range(0, len(item), 8192):
            used += len(json.dumps(item[start:start + 8192], ensure_ascii=False).encode('utf-8')) - 2
            if used > limits['max_bytes']:
                raise ValueError('messages exceed max_bytes')

    for message in value:
        if not isinstance(message, dict) or set(message) != {'role', 'content'}:
            raise ValueError('each message requires only role and content')
        if message['role'] not in ('system', 'user', 'assistant'):
            raise ValueError('messages support system/user/assistant roles only')
        used += 64
        content = message['content']
        if isinstance(content, str):
            text(content)
            continue
        if not isinstance(content, (list, tuple)) or not content:
            raise ValueError('message content must be text or a nonempty list of parts')
        parts += len(content)
        if parts > limits['max_parts']:
            raise ValueError('messages exceed max_parts')
        for part in content:
            used += 128
            if not isinstance(part, dict):
                raise ValueError('message parts must be mappings')
            if part.get('type') == 'text' and set(part) == {'type', 'text'}:
                text(part['text'])
            elif part.get('type') == 'image_url' and set(part) == {'type', 'image_url'}:
                images += 1
                if images > limits['max_images']:
                    raise ValueError('messages exceed max_images')
                image = part['image_url']
                if not isinstance(image, dict) or not {'url'} <= set(image) <= {'url', 'detail'}:
                    raise ValueError('image_url requires url and optional detail')
                url = image['url']
                if not isinstance(url, str) or not url.startswith(('https://', 'http://', 'data:image/')):
                    raise ValueError('image_url must be HTTP(S) or image data URL')
                if 'detail' in image and image['detail'] not in ('auto', 'low', 'high'):
                    raise ValueError('unsupported image detail')
                text(url)
            else:
                raise ValueError('unsupported message content part')
    if used > limits['max_bytes']:
        raise ValueError('messages exceed max_bytes')
    # Copy only containers after validation; never mutate caller-owned history.
    return tuple({'role': m['role'], 'content': m['content'] if isinstance(m['content'], str) else [
        dict(p) if p['type'] == 'text' else {'type': 'image_url', 'image_url': dict(p['image_url'])}
        for p in m['content']]} for m in value)


def render_input(prompt, values):
    if prompt.input_mode == 'messages':
        if set(values) != {'messages'}:
            raise ValueError('messages mode requires only the messages binding')
        return (), validate_messages(values['messages'], prompt.message_limits)
    from .template import render_template
    if set(values) != set(prompt.arguments):
        from .errors import PromptArgumentMissingError, PromptArgumentUnexpectedError
        missing = set(prompt.arguments) - set(values)
        if missing:
            raise PromptArgumentMissingError(f'missing prompt arguments: {sorted(missing)}')
        raise PromptArgumentUnexpectedError(f'unexpected prompt arguments: {sorted(set(values) - set(prompt.arguments))}')
    return render_template(prompt.template, {key: values[key] for key in prompt.template.arguments}), None


def template_input_limits(value):
    """Image counts/encoded image bytes are independent of text/token budgets."""
    ceilings = {'max_images': 64, 'max_image_bytes': 64 * 1024**2, 'max_total_image_bytes': 128 * 1024**2}
    if type(value) is not dict or set(value) != set(ceilings):
        raise ValueError('input_limits requires max_images/max_image_bytes/max_total_image_bytes')
    if any(type(v) is not int or not (0 if k == 'max_images' else 1) <= v <= ceilings[k] for k, v in value.items()):
        raise ValueError('Image input limits exceed deployment ceilings')
    if value['max_image_bytes'] > value['max_total_image_bytes']:
        raise ValueError('Single image budget exceeds total image budget')
    return dict(value)


def admit_template(prompt, values, options):
    """Bound expansion before rendering; client still checks the exact wire body.

    Explicit image limits cover already loaded bytes/data URLs. Without them,
    legacy HTTP URLs retain wire-size accounting only. Decoded pixels belong to
    the native image reader's separate limits.
    """
    from ..data.runtime_contract import check_size, RuntimeContractError
    maximum = options.get('max_request_bytes')
    if maximum is None:
        return
    if prompt.input_mode != 'template' or set(prompt.input_modalities) - {'text', 'image'}:
        raise RuntimeContractError('Bounded rendering requires a text/image template')
    limits = template_input_limits(options['input_limits']) if 'input_limits' in options else None
    from .model import ImageValue
    image_names = {p.name for p in prompt.template.placeholders if p.kind.value in {'image', 'numbered_image'}}
    measured = dict(values)
    for name in image_names:
        images = values.get(name)
        images = images if type(images) in (list, tuple) else (images,)
        if len(images) > (limits['max_images'] if limits else 64):
            raise RuntimeContractError('Image input count budget exceeded')
        measured[name] = [(image.data if image.data is not None else image.uri) if type(image) is ImageValue else image for image in images]
    check_size(measured, maximum)
    reserved = 6 * len(prompt.template.source) + 512
    count = total = 0
    for placeholder in prompt.template.placeholders:
        value = values.get(placeholder.name)
        if placeholder.kind.value in {'image', 'numbered_image'}:
            images = value if type(value) in (list, tuple) else (value,)
            count += len(images)
            if count > (limits['max_images'] if limits else 64):
                raise RuntimeContractError('Image input count budget exceeded')
            for image in images:
                if type(image) is ImageValue:
                    image = image.data if image.data is not None else image.uri
                if type(image) is bytes:
                    size, wire = len(image), 4 * ((len(image) + 2) // 3) + 64
                elif type(image) is str:
                    comma = image.find(',', 0, 129)
                    prefix = image[:comma] if comma >= 0 else ''
                    if limits is None and image.startswith(('http://', 'https://')):
                        size, wire = 0, 6 * len(image)
                    elif not prefix.startswith('data:image/') or not prefix.endswith(';base64') or not image.isascii():
                        raise RuntimeContractError('Bounded image inputs require bytes or image data URLs')
                    else:
                        # No decode or payload substring before admission.
                        size, wire = (len(image) - comma - 1 + 3) // 4 * 3, len(image)
                else:
                    raise RuntimeContractError('Bounded image inputs require bytes or image data URLs')
                total += size
                if limits and (size > limits['max_image_bytes'] or total > limits['max_total_image_bytes']):
                    raise RuntimeContractError('Image input byte budget exceeded')
                reserved += wire + 128
        else:
            reserved += 6 * (len(value) if type(value) is str else check_size(value, maximum))
        if reserved > maximum:
            raise RuntimeContractError('Rendered prompt reservation exceeds max_request_bytes')
    if reserved > maximum:
        raise RuntimeContractError('Rendered prompt reservation exceeds max_request_bytes')
