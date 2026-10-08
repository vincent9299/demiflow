"""Local processor token admission for bounded, inline raster images.

Uses the deployment's complete native processor, including image placeholders;
never fetches a URL or loads model weights. Caller must bind the same revision
and processor defaults as the serving process. One CPU preprocessing at a time.
"""
import base64
import io
import threading

from .tokens import HuggingFaceTokenCounter


class HuggingFaceImageTokenCounter(HuggingFaceTokenCounter):
    def __init__(self, tokenizer_path, *, max_images=1, max_image_bytes=8*1024**2,
                 max_image_pixels=1024**2, **kwargs):
        for value in (max_images, max_image_bytes, max_image_pixels):
            if type(value) is not int or value < 1:
                raise ValueError('Image admission limits must be positive integers')
        super().__init__(tokenizer_path, **kwargs)
        from transformers import AutoProcessor
        self.max_images, self.max_image_bytes, self.max_image_pixels = (
            max_images, max_image_bytes, max_image_pixels)
        self.processor = AutoProcessor.from_pretrained(tokenizer_path, local_files_only=True,
                                                       trust_remote_code=False)
        self._lock = threading.Lock()

    def messages(self, messages):
        from PIL import Image
        converted, pictures = [], []
        try:
            for message in messages:
                content = message['content']
                if isinstance(content, str):
                    converted.append(dict(message))
                    continue
                parts = []
                for part in content:
                    if part.get('type') == 'text':
                        parts.append(dict(part))
                        continue
                    if part.get('type') != 'image_url' or len(pictures) >= self.max_images:
                        raise ValueError('Unsupported modality or image count exceeds budget')
                    url = part['image_url']['url']
                    if len(url) > 128 + 4*((self.max_image_bytes+2)//3):
                        raise ValueError('Encoded image exceeds byte budget')
                    header, sep, body = url.partition(',')
                    if not sep or header not in {'data:image/jpeg;base64', 'data:image/png;base64'}:
                        raise ValueError('Image counter requires inline JPEG/PNG; no network reads')
                    raw = base64.b64decode(body, validate=True)
                    if len(raw) > self.max_image_bytes:
                        raise ValueError('Image exceeds byte budget')
                    picture = Image.open(io.BytesIO(raw))
                    pictures.append(picture)
                    if picture.width*picture.height > self.max_image_pixels or getattr(picture, 'n_frames', 1) != 1:
                        raise ValueError('Image exceeds pixel/frame budget')
                    picture.load()
                    parts.append({'type': 'image', 'image': picture})
                converted.append({**message, 'content': parts})
            if not pictures:
                text_messages = [{**m, 'content': m['content'] if isinstance(m['content'], str)
                    else ''.join(p['text'] for p in m['content'])} for m in converted]
                return super().messages(text_messages)
            with self._lock:
                result = self.processor.apply_chat_template(converted, tokenize=True,
                    add_generation_prompt=True, return_dict=True, **self.template_kwargs)
                ids = result['input_ids']
                return len(ids[0]) if ids and isinstance(ids[0], list) else len(ids)
        finally:
            for picture in pictures:
                picture.close()
