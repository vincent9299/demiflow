"""Field-declared operator images; native results are never rewritten."""
import base64
import io

from .objects import ObjectRef, _local_path
from .execution.artifacts import resolve_local_artifact
from .operator_llm.client import _journal_io
from .operator_llm.errors import PromptBudgetExceededError


def image_fields(value):
    if (not isinstance(value, dict) or set(value) != {'items_field', 'uri_field', 'sha256_field'}
            or any(not isinstance(v, str) or len(v) > 128 for v in value.values())
            or not value['uri_field'] or not value['sha256_field']):
        raise ValueError('result_images requires items_field, uri_field and sha256_field')
    return dict(value)


def _read_image(ref, max_bytes, max_pixels):
    from PIL import Image
    raw = ObjectRef(**ref).read(max_bytes=max_bytes)
    # Inspect the bounded image header, without allocating decoded raster data.
    # Tool consumers decode the image; this is a byte/pixel admission check.
    with Image.open(io.BytesIO(raw)) as image:
        if image.width * image.height > max_pixels:
            raise ValueError('Tool image exceeds max_tool_image_pixels')
        mime = {'PNG': 'image/png', 'JPEG': 'image/jpeg', 'WEBP': 'image/webp', 'GIF': 'image/gif'}.get(image.format)
        if mime is None:
            raise ValueError('Unsupported tool image format')
        width, height = image.size
        image.verify()
    return raw, mime, width, height


class OperatorImages:
    def __init__(self, limits):
        self.limits = limits
        self.count = self.bytes = 0

    async def render(self, result, fields):
        """Return receipt metadata separately from transient model image bytes."""
        if fields is None:
            return [], []
        items = result[fields['items_field']] if fields['items_field'] else result
        if not isinstance(items, list):
            raise ValueError('Operator result_images must select a list')
        if len(items) > self.limits.max_tool_image_candidates:
            raise PromptBudgetExceededError('Operator images exceed max_tool_image_candidates')
        receipts, images = [], []
        for position, item in enumerate(items, 1):
            ref = {'uri': item[fields['uri_field']], 'sha256': item[fields['sha256_field']]}
            ObjectRef(**ref)
            receipt = {'image_id': 'sha256:' + ref['sha256'], 'object_ref': ref, 'position': position}
            remaining = self.limits.max_tool_image_total_bytes - self.bytes
            if self.count >= self.limits.max_tool_images or remaining < 1:
                receipts.append({**receipt, 'status': 'not_attached_budget'})
                continue
            try:
                raw, mime, width, height = await _journal_io(_read_image, ref,
                    min(remaining, self.limits.max_tool_image_bytes), self.limits.max_tool_image_pixels)
            except (OSError, ValueError) as exc:
                receipts.append({**receipt, 'status': 'unavailable', 'error': str(exc)[:512]})
                continue
            self.count += 1
            self.bytes += len(raw)
            if ref['uri'].startswith('file:'):
                receipt['local_path'] = str(resolve_local_artifact(_local_path(ref['uri'])))
            receipts.append({**receipt, 'status': 'attached', 'byte_size': len(raw),
                             'attachment_index': self.count,
                             'mime': mime, 'width': width, 'height': height})
            images.append('data:' + mime + ';base64,' + base64.b64encode(raw).decode('ascii'))
        return receipts, images

    async def verify(self, receipts):
        """Verify exactly the pictures delivered in the saved session, without APIs."""
        if not isinstance(receipts, list) or len(receipts) > self.limits.max_tool_image_candidates:
            raise ValueError('Invalid cached image receipts')
        for item in receipts:
            if item['status'] != 'attached':
                continue
            remaining = self.limits.max_tool_image_total_bytes - self.bytes
            if self.count >= self.limits.max_tool_images or remaining < item['byte_size']:
                raise ValueError('Cached tool images exceed session budget')
            if item['attachment_index'] != self.count + 1:
                raise ValueError('Cached tool image attachment order changed')
            raw, mime, width, height = await _journal_io(_read_image, item['object_ref'],
                min(remaining, self.limits.max_tool_image_bytes, max(1, item['byte_size'])),
                self.limits.max_tool_image_pixels)
            if (len(raw), mime, width, height) != (item['byte_size'], item['mime'], item['width'], item['height']):
                raise ValueError('Cached tool image changed')
            self.count += 1
            self.bytes += len(raw)
