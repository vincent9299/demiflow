"""Template-bound image generation, shared by the native Dataset image node.

One request/one image, no retry/fallback. Binary request parts are persisted before
transport; replay is keyed by the complete rendered request and prompt version.
"""
import asyncio
import base64
from copy import deepcopy
from contextlib import contextmanager, nullcontext
import io
import json
import os
from pathlib import Path
import time

from PIL import Image
from .execution.artifacts import digest
from .execution.file_ref import save_json_artifact
from .objects import LocalObjectStore
from .operator_llm.model import ImageValue, TextPart, PlaceholderKind
from .operator_llm.template import compile_template, render_template
from .operator_llm.sqlite_journal import SQLitePromptJournal
from .services import ManagedHTTPService, ModelServiceError
from .services.shared_http import SharedHTTPService


_WIRE_FORMATS = {'JPEG': ('image/jpeg', 'jpg'), 'PNG': ('image/png', 'png'),
                 'WEBP': ('image/webp', 'webp')}


class _BoundedImageBuffer(io.BytesIO):
    """Reject oversized encoding as it is written, before copying the buffer."""
    def __init__(self, limit):
        super().__init__()
        self.limit = limit

    def write(self, value):
        if self.tell() + len(value) > self.limit:
            raise ValueError('Encoded image exceeds max_image_bytes or remaining request budget')
        return super().write(value)


def _wire_format(raw):
    with Image.open(io.BytesIO(raw)) as image:
        return _WIRE_FORMATS[image.format]


class ImageGenerator:
    """Bounded HTTP actor; native GPU work is serial and drains before cleanup."""
    concurrency = 1
    label = 'map_image_async'

    def __init__(self, *, template, model, inputs, output, call_output, error_output,
                 journal_path, object_store, max_requests, when=None, limits=None, service=None,
                 image_encoding='png'):
        self.spec, self.config = deepcopy(template), deepcopy(model)
        if image_encoding not in ('png', 'preserve'):
            raise ValueError('image_encoding must be png or preserve')
        self.image_encoding = image_encoding
        if set(template) != {'name', 'version', 'template'} or not all(isinstance(v, str) and v for v in template.values()):
            raise ValueError('Image template requires name, version and template')
        self.template = compile_template(template['template'])
        if set(inputs) != set(self.template.arguments):
            raise ValueError('Image inputs must exactly bind template arguments')
        self.inputs, self.output, self.call_output, self.error_output = dict(inputs), output, call_output, error_output
        if len({output, call_output, error_output}) != 3 or not all(isinstance(v, str) and v for v in (output,call_output,error_output)):
            raise ValueError('Image output, call and error columns must be distinct nonempty names')
        if type(max_requests) is not int or max_requests < 0:
            raise ValueError('Image generation requires a finite max_requests')
        self.limits = dict(max_images=16, max_text_bytes=256*1024, max_image_bytes=32*1024*1024,
            max_pixels=24_000_000, max_total_pixels=48_000_000, max_request_bytes=64*1024*1024,
            max_response_bytes=64*1024*1024)
        if set(limits or {}) - set(self.limits):
            raise ValueError('Unknown image generation limit')
        self.limits.update(limits or {})
        if any(type(v) is not int or v < 1 for v in self.limits.values()):
            raise ValueError('Image limits must be positive integers')
        backend = model.get('backend')
        if backend not in {'diffusers', 'modelhub'}:
            raise ValueError('Native image generation supports diffusers and modelhub')
        if backend == 'modelhub' and model.get('api') not in {'images', 'images_json', 'openrouter_images', 'chat'}:
            raise ValueError('Unknown image API')
        if set(model.get('parameters', {})) & {'prompt','image','input_references','messages','generator','model','n'}:
            raise ValueError('Parameters cannot override rendered model input')
        if not 0 < model.get('timeout_s',600) <= 3600:
            raise ValueError('Image timeout must be in (0,3600]')
        if service is not None:
            if backend != 'modelhub' or not isinstance(service, (ManagedHTTPService, SharedHTTPService)):
                raise ValueError('Image service requires a modelhub backend and an HTTP service declaration')
            declaration = ManagedHTTPService(**service.configuration) if isinstance(service, SharedHTTPService) else service
            if declaration.base_url != model.get('base_url', '').rstrip('/'):
                raise ValueError('Image endpoint differs from managed service declaration')
            if declaration.expected_model not in (None, model.get('provider_model', model['model'])):
                raise ValueError('Image model differs from managed service declaration')
        self.service = service.bind() if service is not None else None
        self.when, self.model = when, None
        self.store = LocalObjectStore(object_store)
        self.journal = SQLitePromptJournal(journal_path, max_requests=max_requests)
        self.artifacts = Path(journal_path).with_suffix('.inputs')

    def _checked_image(self, raw):
        if len(raw) > self.limits['max_image_bytes']:
            raise ValueError('Image exceeds max_image_bytes')
        with Image.open(io.BytesIO(raw)) as im:
            if im.width * im.height > self.limits['max_pixels']:
                raise ValueError('Image exceeds max_pixels')
            im.load()
            return im.convert('RGB')

    def render(self, values):
        # Bound text/binary inputs before renderer allocation; reject external URLs.
        values = dict(values)
        for name, value in values.items():
            if isinstance(value, str) and len(value.encode()) > self.limits['max_text_bytes']:
                raise ValueError('Template text input exceeds max_text_bytes')
            if self.template.arguments.get(name) in (PlaceholderKind.IMAGE, PlaceholderKind.NUMBERED_IMAGE) and isinstance(value, (list, tuple)):
                if len(value) > self.limits['max_images']:
                    raise ValueError('Image input exceeds max_images')
                if any(not isinstance(v, bytes) or len(v) > self.limits['max_image_bytes'] for v in value):
                    raise ValueError('Image placeholders require bounded image bytes')
                if self.image_encoding == 'preserve':
                    # The image node decodes formats beyond the text renderer's
                    # signature allowlist. Inspect headers, never trust a suffix.
                    bound = []
                    for raw in value:
                        with Image.open(io.BytesIO(raw)) as im:
                            bound.append(ImageValue(data=raw, media_type=Image.MIME.get(im.format, 'application/octet-stream')))
                    values[name] = bound
        for name, value in values.items():
            if self.template.arguments.get(name) == PlaceholderKind.JSON:
                if len(json.dumps(value,ensure_ascii=False).encode()) > self.limits['max_text_bytes']:
                    raise ValueError('Template JSON input exceeds max_text_bytes')
        parts = render_template(self.template, values)
        prompt, images, total_pixels, total_bytes = '', [], 0, 0
        for part in parts:
            if isinstance(part, TextPart):
                prompt += part.text
            else:
                if part.image.data is None:
                    raise ValueError('Image generation requires binary image inputs')
                raw = part.image.data
                if len(raw) > self.limits['max_image_bytes']:
                    raise ValueError('Image exceeds max_image_bytes')
                with Image.open(io.BytesIO(raw)) as im:
                    if im.width * im.height > self.limits['max_pixels']:
                        raise ValueError('Image exceeds max_pixels')
                    total_pixels += im.width * im.height
                    if total_pixels > self.limits['max_total_pixels'] or len(images) >= self.limits['max_images']:
                        raise ValueError('Image input exceeds aggregate pixel/count budget')
                    # All checks above precede pixel allocation. Preserve never
                    # silently drops frames; png retains the legacy first-frame policy.
                    source_format = im.format
                    if self.image_encoding == 'preserve':
                        try:
                            im.seek(1)
                        except EOFError:
                            im.seek(0)
                        else:
                            raise ValueError('image_encoding=preserve requires a single-frame image')
                    remaining = self.limits['max_request_bytes'] - total_bytes - 64
                    byte_limit = min(self.limits['max_image_bytes'], max(0, remaining // 4 * 3))
                    preserve = self.image_encoding == 'preserve' and source_format in _WIRE_FORMATS
                    if preserve and len(raw) > byte_limit:
                        raise ValueError('Image request exceeds max_request_bytes')
                    im.load()  # Validate even when sending the original encoded bytes.
                    if not preserve:
                        rgb = im.convert('RGB')
                        try:
                            with _BoundedImageBuffer(byte_limit) as buf:
                                rgb.save(buf, format='PNG')
                                raw = buf.getvalue()
                        finally:
                            rgb.close()
                    total_bytes += 4 * ((len(raw) + 2) // 3) + 64
                    if total_bytes > self.limits['max_request_bytes']:
                        raise ValueError('Image request exceeds max_request_bytes')
                    images.append(raw)
        if len(prompt.encode()) > self.limits['max_text_bytes']:
            raise ValueError('Rendered prompt exceeds max_text_bytes')
        return prompt, images

    def request(self, prompt, images):
        cfg = self.config
        urls = ['data:' + _wire_format(raw)[0] + ';base64,' + base64.b64encode(raw).decode() for raw in images]
        parameters = cfg.get('parameters', {})
        if cfg['backend'] == 'diffusers':
            endpoint = 'diffusers'
            body = {'prompt': prompt, **parameters, 'seed': cfg['seed']}
            if urls: body['image'] = urls
        else:
            body = {'model': cfg.get('provider_model', cfg['model']), 'prompt': prompt, **parameters, 'n': 1}
            if cfg['api'] == 'openrouter_images':
                endpoint = '/images'
                if urls: body['input_references'] = [{'type':'image_url','image_url':{'url':url}} for url in urls]
            elif cfg['api'] in {'images', 'images_json'}:
                endpoint = '/images/edits' if urls else '/images/generations'
                if urls: body['image' if cfg['api']=='images_json' else 'image[]'] = urls
            else:
                endpoint = '/chat/completions'
                content = [{'type':'text','text':prompt}] + [{'type':'image_url','image_url':{'url':u}} for u in urls]
                body = {'model': cfg.get('provider_model', cfg['model']), 'messages':[{'role':'user','content':content}], **parameters}
        request = {'template':self.spec, 'backend':cfg['backend'], 'endpoint':endpoint,
            'base_url':cfg.get('base_url') if cfg['backend']=='modelhub' else None,
            'model':cfg['model'], 'model_path':cfg.get('model_path'), 'revision':cfg.get('revision'),
            'body':body}
        # Keep legacy png request identities stable. Non-default policy is part
        # of the journal contract as well as the actual bytes and MIME types.
        if images and self.image_encoding != 'png':
            request['image_encoding'] = self.image_encoding
        if len(json.dumps(request, ensure_ascii=False).encode()) > self.limits['max_request_bytes']:
            raise ValueError('Serialized image request exceeds max_request_bytes')
        return request

    def load_model(self):
        import torch
        from diffusers import DiffusionPipeline
        self.model = DiffusionPipeline.from_pretrained(self.config['model_path'],
            torch_dtype=torch.bfloat16, local_files_only=True).to(self.config['device'])

    def local(self, request, images):
        import torch
        if self.model is None: self.load_model()
        refs = [self._checked_image(raw) for raw in images]
        try:
            result = self.model(prompt=request['body']['prompt'], **({'image':refs} if refs else {}),
                **self.config['parameters'], generator=torch.Generator('cpu').manual_seed(self.config['seed'])).images
            if len(result) != 1: raise ValueError('Image model must return exactly one image')
            if result[0].width * result[0].height > self.limits['max_pixels']:
                raise ValueError('Generated image exceeds max_pixels')
            buf = io.BytesIO(); result[0].save(buf,format='PNG'); return buf.getvalue()
        finally:
            for im in refs: im.close()

    def remote(self, request, images):
        import httpx
        cfg, body = self.config, request['body']
        deadline = time.monotonic() + cfg.get('timeout_s',600)
        def read(client, method, url, **kwargs):
            with client.stream(method, url, **kwargs) as response:
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if time.monotonic() > deadline: raise TimeoutError('Image HTTP total timeout exceeded')
                    if len(raw) + len(chunk) > self.limits['max_response_bytes']:
                        raise ValueError('Image HTTP response exceeds max_response_bytes')
                    raw.extend(chunk)
                response.raise_for_status()
                return bytes(raw)
        headers = {'Authorization':'Bearer '+os.environ.get(cfg['api_key_env'],'anything')}
        with httpx.Client(timeout=cfg.get('timeout_s',600),trust_env=False,follow_redirects=False) as client:
            if request['endpoint'] == '/images/edits' and cfg['api']=='images':
                kwargs = {'data':{k:str(v) for k,v in body.items() if k!='image[]'},
                    'files':[('image[]',(f'reference_{i}.{_wire_format(raw)[1]}',raw,_wire_format(raw)[0]))
                             for i,raw in enumerate(images,1)]}
            else: kwargs = {'json':body}
            raw = read(client,'POST',cfg['base_url'].rstrip('/')+request['endpoint'],headers=headers,**kwargs)
            save_json_artifact(self.artifacts / 'responses', {'body':raw.decode()})
            response = json.loads(raw)
            if cfg['api'] != 'chat':
                outputs = response.get('data',[])
                if len(outputs) != 1: raise ValueError('Image API must return exactly one image')
                if outputs[0].get('b64_json'): return base64.b64decode(outputs[0]['b64_json'],validate=True)
                url = outputs[0]['url']
            else:
                message=response['choices'][0]['message']; parts=message.get('images') or message.get('content')
                urls=[]
                for part in parts if isinstance(parts,list) else []:
                    url=part.get('image_url'); url=url.get('url') if isinstance(url,dict) else url
                    if url and url not in urls: urls.append(url)
                if len(urls)!=1: raise ValueError('Chat API must return exactly one image')
                url=urls[0]
            if url.startswith('data:image/'): return base64.b64decode(url.split(',',1)[1],validate=True)
            return read(client,'GET',url)  # no gateway credentials on result downloads

    def generate(self, values, *, before_transport=None):
        started = time.monotonic()
        prompt, images = self.render(values)
        request = self.request(prompt, images)
        call = {**self.journal.references(request),
            'template_name':self.spec['name'],'template_version':self.spec['version']}
        try:
            prior = self.journal.lookup(request)
        except Exception as exc:
            exc.image_call = {**call, **getattr(exc,'call',{})}
            raise
        if prior is not None:
            return prior['image'], {**call,'input_ref':prior['input_ref'],'reused':True}, prior.get('seconds',0)
        if not self.journal.reserve(request):
            prior = self.journal.lookup(request)
            return prior['image'], {**call,'input_ref':prior['input_ref'],'reused':True}, prior.get('seconds',0)
        try:
            # Reservation bounds artifact growth; exact bytes are durable before transport.
            ref = save_json_artifact(self.artifacts, request)
            call['input_ref'] = ref.to_dict()
            if self.service is not None and before_transport is None:
                raise ValueError('Managed image services require the async actor lifecycle')
            with before_transport() if self.service is not None else nullcontext():
                raw = self.local(request,images) if self.config['backend']=='diffusers' else self.remote(request,images)
            im = self._checked_image(raw); im.close()
            image = self.store.put(raw).to_dict()
            elapsed = time.monotonic()-started
            self.journal.response(request, {'image':image,'seconds':elapsed,'input_ref':call['input_ref']})
            return image, call, elapsed
        except Exception as exc:
            exc.call = call
            self.journal.failed(request, exc, time.monotonic()-started)
            exc.image_call = call
            raise

    async def __call__(self, row):
        if self.when is not None and not self.when(row): return row
        loop = asyncio.get_running_loop()
        @contextmanager
        def ready():
            # Native work runs on a worker thread; the shared owner stays on the
            # actor loop. Start only after a cache miss and durable reservation.
            try:
                asyncio.run_coroutine_threadsafe(self.service.ensure_ready(), loop).result()
            except Exception as exc:
                raise ModelServiceError(str(exc)) from exc
            slot = self.service.request_slot() if hasattr(self.service, 'request_slot') else None
            if slot is not None:
                asyncio.run_coroutine_threadsafe(slot.__aenter__(), loop).result()
            try:
                yield
            finally:
                if slot is not None:
                    asyncio.run_coroutine_threadsafe(slot.__aexit__(None,None,None), loop).result()
        task = asyncio.create_task(asyncio.to_thread(self.generate,
            {key:row[col] for key,col in self.inputs.items()},
            before_transport=ready if self.service is not None else None))
        try:
            image, call, seconds = await asyncio.shield(task)
            return {**row,self.output:image,self.call_output:{**call,'seconds':seconds},self.error_output:None}
        except asyncio.CancelledError:
            try: await task
            except Exception: pass
            raise
        except ModelServiceError:
            raise  # Deployment failure stops the node, not a series of failed images.
        except Exception as exc:
            return {**row,self.output:None,self.call_output:getattr(exc,'image_call',getattr(exc,'call',None)),
                    self.error_output:f'{type(exc).__name__}: {exc}'}

    async def aclose(self):
        try:
            if self.service is not None:
                await self.service.aclose()
        finally:
            self.journal.close()
            if self.model is not None:
                import gc
                import torch
                self.model=None; gc.collect(); torch.cuda.empty_cache()
