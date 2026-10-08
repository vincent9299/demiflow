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
    return render_template(prompt.template, values), None
