"""Image wire formats and bounded media conversion, not an execution backend.

PromptActor, AsyncOperatorLLMRuntime and AsyncOperatorLLMClient own all scheduling,
budgets, journals, HTTP and service lifecycle. This module has none of those.
"""
import base64
import io
import json
import math
import re
from collections.abc import Mapping
from contextlib import contextmanager

from PIL import Image

from .model import ImageValue, PlaceholderKind, TextPart


IMAGE_RESPONSE_SCHEMA = {
    'type': 'object', 'additionalProperties': False, 'required': ['image'],
    'properties': {'image': {
        'type': 'object', 'additionalProperties': False, 'required': ['uri', 'sha256'],
        'properties': {'uri': {'type': 'string'}, 'sha256': {'type': 'string'}},
    }},
}
MEDIA_LIMITS = dict(max_images=16, max_text_bytes=256 * 1024,
    max_image_bytes=32 * 1024 * 1024, max_pixels=24_000_000,
    max_total_pixels=48_000_000, max_request_bytes=64 * 1024 * 1024,
    max_response_bytes=64 * 1024 * 1024)
WIRE_FORMATS = {'JPEG': ('image/jpeg', 'jpg'), 'PNG': ('image/png', 'png'),
                'WEBP': ('image/webp', 'webp')}


@contextmanager
def open_picture(raw):
    try:
        with Image.open(io.BytesIO(raw)) as picture:
            yield picture
    except (OSError, Image.DecompressionBombError) as exc:
        raise ValueError(f'Invalid image: {exc}') from exc


def validate_provider(configuration):
    allowed = {'backend', 'model', 'api', 'base_url', 'api_key_env', 'provider_model',
               'revision', 'parameters', 'timeout_s', 'model_path', 'device', 'seed'}
    if set(configuration) - allowed:
        raise ValueError('Unknown image provider configuration fields')
    if configuration.get('backend') not in {'modelhub', 'diffusers'}:
        raise ValueError('Image provider backend must be modelhub or diffusers')
    if not isinstance(configuration.get('model'), str) or not configuration['model']:
        raise ValueError('Image provider requires a model name')
    parameters = configuration.get('parameters', {})
    if not isinstance(parameters, Mapping) or set(parameters) & {
            'prompt', 'image', 'image[]', 'input_references', 'messages', 'generator', 'model', 'n'}:
        raise ValueError('Parameters cannot override rendered model input')
    timeout = configuration.get('timeout_s', 600)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 3600:
        raise ValueError('Image timeout must be in (0,3600]')
    if configuration['backend'] == 'modelhub':
        if configuration.get('api') not in {'images', 'images_json', 'chat', 'chat_interleaved', 'openrouter_images'}:
            raise ValueError('Unknown image provider API')
        from urllib.parse import urlsplit
        endpoint = urlsplit(configuration.get('base_url', ''))
        if endpoint.scheme not in {'http', 'https'} or not endpoint.netloc or endpoint.username or endpoint.password:
            raise ValueError('Image HTTP provider requires an HTTP(S) base_url without credentials')
        environment = configuration.get('api_key_env')
        if not isinstance(environment, str) or (environment and not re.fullmatch('[A-Z_][A-Z0-9_]*', environment)):
            raise ValueError('Image provider requires api_key_env, empty for no authentication')
    else:
        if any(not isinstance(configuration.get(key), str) or not configuration[key]
               for key in ('model_path', 'device')) or type(configuration.get('seed')) is not int:
            raise ValueError('Diffusers requires model_path, device and integer seed')


def media_limits(options):
    declared = (options or {}).get('limits', {})
    if not isinstance(declared, Mapping) or set(declared) - set(MEDIA_LIMITS):
        raise ValueError('Unknown image limit')
    limits = {**MEDIA_LIMITS, **declared}
    if any(type(value) is not int or not 1 <= value <= MEDIA_LIMITS[key]
           for key, value in limits.items()):
        raise ValueError('Image limits must be positive integers within platform ceilings')
    return limits


def execution_options(prompt, options, max_requests):
    options = dict(options or {})
    allowed = {'sqlite_journal', 'object_store', 'image_encoding', 'limits',
               'io_workers', 'prepare_workers', 'response_workers', 'collect_journal_totals',
               'keepalive_expiry_s', 'max_connections', 'max_keepalive_connections'}
    if set(options) - allowed:
        raise ValueError('Unsupported image prompt options')
    if type(max_requests) is not int or max_requests < 0:
        raise ValueError('Image output requires a finite max_requests')
    journal = options.get('sqlite_journal')
    if not isinstance(journal, Mapping) or not journal.get('path') or set(journal) - {'path', 'timeout_s', 'read_only'}:
        raise ValueError('Image output requires options.sqlite_journal with a path')
    if not options.get('object_store'):
        raise ValueError('Image output requires options.object_store')
    if options.get('image_encoding', 'png') not in {'png', 'preserve'}:
        raise ValueError('image_encoding must be png or preserve')
    limits = media_limits(options)
    return {**options, 'sqlite_journal': {**journal, 'max_requests': max_requests},
            'timeout_s': prompt.model.provider.get('timeout_s', 600),
            'max_response_bytes': limits['max_response_bytes'],
            'max_event_bytes': min(1024 * 1024, limits['max_response_bytes']), 'trust_env': False}


def bounded_values(prompt, values, options):
    limits = media_limits(options)
    result = dict(values)
    text_size, image_count, image_bytes, pixels = 0, 0, 0, 0
    for name, value in values.items():
        kind = prompt.template.arguments.get(name)
        if kind in (PlaceholderKind.IMAGE, PlaceholderKind.NUMBERED_IMAGE):
            if not isinstance(value, (list, tuple)):
                raise ValueError('Image placeholders require lists of image bytes')
            image_count += len(value)
            if image_count > limits['max_images']:
                raise ValueError('Image input exceeds max_images')
            bound = []
            for raw in value:
                if not isinstance(raw, bytes) or len(raw) > limits['max_image_bytes']:
                    raise ValueError('Image placeholders require bounded image bytes')
                image_bytes += len(raw)
                if image_bytes > limits['max_request_bytes']:
                    raise ValueError('Image input exceeds request budget')
                with open_picture(raw) as picture:
                    pixels += picture.width * picture.height
                    if picture.width * picture.height > limits['max_pixels']:
                        raise ValueError('Image exceeds max_pixels')
                    if pixels > limits['max_total_pixels']:
                        raise ValueError('Image input exceeds aggregate pixel budget')
                    bound.append(ImageValue(data=raw, media_type=Image.MIME.get(picture.format, 'application/octet-stream')))
            result[name] = bound
        else:
            if kind == PlaceholderKind.JSON:
                chunks = json.JSONEncoder(ensure_ascii=False).iterencode(value)
            else:
                chunks = (str(value),)
            for chunk in chunks:
                if len(chunk) > limits['max_text_bytes'] - text_size:
                    raise ValueError('Template text exceeds max_text_bytes')
                text_size += len(chunk.encode())
                if text_size > limits['max_text_bytes']:
                    raise ValueError('Template text exceeds max_text_bytes')
    return result


class BoundedImageBuffer(io.BytesIO):
    def __init__(self, limit):
        super().__init__()
        self.limit = limit

    def write(self, value):
        if self.tell() + len(value) > self.limit:
            raise ValueError('Encoded image exceeds max_image_bytes or request budget')
        return super().write(value)


def checked_image(raw, limits):
    if len(raw) > limits['max_image_bytes']:
        raise ValueError('Image exceeds max_image_bytes')
    with open_picture(raw) as picture:
        if picture.width * picture.height > limits['max_pixels']:
            raise ValueError('Image exceeds max_pixels')
        picture.load()
        return picture.convert('RGB')


def wire_format(raw):
    with open_picture(raw) as picture:
        return WIRE_FORMATS[picture.format]


def encoded_parts(parts, options):
    limits = media_limits(options)
    encoding = options.get('image_encoding', 'png')
    texts, images, text_bytes, pixels, total_bytes = [], [], 0, 0, 0
    for part in parts:
        if isinstance(part, TextPart):
            text_bytes += len(part.text.encode())
            if text_bytes > limits['max_text_bytes']:
                raise ValueError('Rendered prompt exceeds max_text_bytes')
            texts.append(part.text)
            continue
        raw = part.image.data
        if raw is None or len(raw) > limits['max_image_bytes']:
            raise ValueError('Image output requires bounded binary image inputs')
        with open_picture(raw) as picture:
            if picture.width * picture.height > limits['max_pixels']:
                raise ValueError('Image exceeds max_pixels')
            pixels += picture.width * picture.height
            if pixels > limits['max_total_pixels'] or len(images) >= limits['max_images']:
                raise ValueError('Image input exceeds aggregate pixel/count budget')
            original_format = picture.format
            if encoding == 'preserve':
                try:
                    picture.seek(1)
                except EOFError:
                    picture.seek(0)
                else:
                    raise ValueError('image_encoding=preserve requires a single-frame image')
            remaining = limits['max_request_bytes'] - total_bytes - 64
            byte_limit = min(limits['max_image_bytes'], max(0, remaining // 4 * 3))
            preserve = encoding == 'preserve' and original_format in WIRE_FORMATS
            if preserve and len(raw) > byte_limit:
                raise ValueError('Image request exceeds max_request_bytes')
            picture.load()
            if not preserve:
                converted = picture.convert('RGB')
                try:
                    with BoundedImageBuffer(byte_limit) as buffer:
                        converted.save(buffer, format='PNG')
                        raw = buffer.getvalue()
                finally:
                    converted.close()
            total_bytes += 4 * ((len(raw) + 2) // 3) + 64
            if total_bytes > limits['max_request_bytes']:
                raise ValueError('Image request exceeds max_request_bytes')
            images.append(raw)
    return ''.join(texts), images


def request_record(request, model, options):
    configuration = model.provider
    if configuration.get('api') == 'chat_interleaved' and len(request.parts) > 256:
        raise ValueError('Interleaved image request exceeds 256 content parts')
    prompt, images = encoded_parts(request.parts, options)
    urls = ['data:' + wire_format(raw)[0] + ';base64,' + base64.b64encode(raw).decode() for raw in images]
    parameters = configuration.get('parameters', {})
    if model.transport == 'diffusers':
        endpoint = 'diffusers'
        body = {'prompt': prompt, **parameters, 'seed': configuration['seed']}
        if urls:
            body['image'] = urls
    else:
        body = {'model': model.name, 'prompt': prompt, **parameters, 'n': 1}
        api = configuration['api']
        if api == 'openrouter_images':
            endpoint = '/images'
            if urls:
                body['input_references'] = [{'type': 'image_url', 'image_url': {'url': url}} for url in urls]
        elif api in {'images', 'images_json'}:
            endpoint = '/images/edits' if urls else '/images/generations'
            if urls:
                body['image' if api == 'images_json' else 'image[]'] = urls
        else:
            endpoint = '/chat/completions'
            if api == 'chat_interleaved':
                image_urls = iter(urls)
                content = [({'type': 'text', 'text': part.text} if isinstance(part, TextPart)
                            else {'type': 'image_url', 'image_url': {'url': next(image_urls)}})
                           for part in request.parts]
            else:
                content = [{'type': 'text', 'text': prompt}] + [
                    {'type': 'image_url', 'image_url': {'url': url}} for url in urls]
            body = {'model': model.name, 'messages': [{'role': 'user', 'content': content}], **parameters}
    record = {'template': {'name': request.prompt_name, 'version': request.prompt_version, 'template': request.template},
        'backend': configuration['backend'], 'endpoint': endpoint,
        'base_url': configuration.get('base_url') if model.transport != 'diffusers' else None,
        'model': configuration['model'], 'model_path': configuration.get('model_path'),
        'revision': configuration.get('revision'), 'body': body}
    if images and options.get('image_encoding', 'png') != 'png':
        record['image_encoding'] = options['image_encoding']
    if len(json.dumps(record, ensure_ascii=False).encode()) > media_limits(options)['max_request_bytes']:
        raise ValueError('Serialized image request exceeds max_request_bytes')
    return record


def wire_request(source, configuration):
    body = source['body']
    if source['endpoint'] == '/images/edits' and configuration['api'] == 'images':
        files = []
        for number, url in enumerate(body['image[]'], 1):
            raw = base64.b64decode(url.split(',', 1)[1], validate=True)
            mime, extension = wire_format(raw)
            files.append(('image[]', (f'reference_{number}.{extension}', raw, mime)))
        return {'data': {key: str(value) for key, value in body.items() if key != 'image[]'}, 'files': files}
    return {'json': body}


def response_location(body, configuration, options):
    if configuration.get('api') in {'chat', 'chat_interleaved'}:
        message = body['choices'][0]['message']
        parts = message.get('images') or message.get('content')
        urls = []
        for part in parts if isinstance(parts, list) else []:
            url = part.get('image_url')
            url = url.get('url') if isinstance(url, dict) else url
            if url and url not in urls:
                urls.append(url)
        if len(urls) != 1:
            raise ValueError('Chat API must return exactly one image')
        url = urls[0]
    else:
        outputs = body.get('data', [])
        if len(outputs) != 1:
            raise ValueError('Image API must return exactly one image')
        url = ('data:image/png;base64,' + outputs[0]['b64_json']
               if outputs[0].get('b64_json') else outputs[0]['url'])
    if url.startswith('data:image/'):
        encoded = url.split(',', 1)[1]
        if len(encoded) > 4 * ((media_limits(options)['max_image_bytes'] + 2) // 3):
            raise ValueError('Image response exceeds max_image_bytes')
        return base64.b64decode(encoded, validate=True)
    if not url.startswith(('http://', 'https://')):
        raise ValueError('Image response requires image data or HTTP(S) URL')
    return url


def store_image(raw, options):
    from demiflow.objects import LocalObjectStore
    picture = checked_image(raw, media_limits(options))
    picture.close()
    return LocalObjectStore(options['object_store']).put(raw).to_dict()
