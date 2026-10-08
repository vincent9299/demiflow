"""Declarative image selection and bounded, decode-free header inspection."""
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class ImageFilters:
    """Inclusive pixel minima, combined with AND; None disables a condition."""
    min_width: int | None = None
    min_height: int | None = None
    min_long_side: int | None = None
    min_short_side: int | None = None
    min_pixels: int | None = None

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if value is not None and (type(value) is not int or not 1 <= value <= 2**63-1):
                raise ValueError('Image filter must be a positive int64 or None: ' + field.name)

    @property
    def enabled(self):
        return any(getattr(self, field.name) is not None for field in fields(self))

    def rejection(self, width, height):
        values = {'min_width': width, 'min_height': height,
                  'min_long_side': max(width, height), 'min_short_side': min(width, height),
                  'min_pixels': width * height}
        for key, observed in values.items():
            required = getattr(self, key)
            if required is not None and observed < required:
                return f'{key}: observed={observed}, required={required}'
        return None


def declared_dimensions(request):
    width, height = request.get('declared_width'), request.get('declared_height')
    if width is None and height is None:
        return None
    if any(type(v) is not int or not 1 <= v <= 2**31-1 for v in (width, height)):
        raise ValueError('declared_width and declared_height must both be positive int32 values')
    return width, height


def header_dimensions(data):
    """Read only scalar header fields. Unknown/truncated headers defer to decode.

    Called on a prefix of at most 128 KiB, at geometrically spaced sizes.
    No Pillow decoder, decompression or allocation based on file dimensions.
    """
    n = len(data)
    dims = None
    if data[:8] == b'\x89PNG\r\n\x1a\n' and n >= 24 and data[8:16] == b'\x00\x00\x00\rIHDR':
        dims = int.from_bytes(data[16:20], 'big'), int.from_bytes(data[20:24], 'big')
    elif data[:6] in (b'GIF87a', b'GIF89a') and n >= 10:
        dims = int.from_bytes(data[6:8], 'little'), int.from_bytes(data[8:10], 'little')
    elif data[:2] == b'BM' and n >= 26:
        dib = int.from_bytes(data[14:18], 'little')
        if dib == 12:
            dims = int.from_bytes(data[18:20], 'little'), int.from_bytes(data[20:22], 'little')
        elif dib in (40, 52, 56, 64, 108, 124):
            dims = (int.from_bytes(data[18:22], 'little', signed=True),
                    abs(int.from_bytes(data[22:26], 'little', signed=True)))
    elif data[:2] == b'\xff\xd8':
        pos = 2
        while pos < n:
            if data[pos] != 255:
                break
            while pos < n and data[pos] == 255:
                pos += 1
            if pos >= n:
                break
            marker = data[pos]
            pos += 1
            if marker in (0xD9, 0xDA):
                break
            if marker == 0x01 or 0xD0 <= marker <= 0xD8:
                continue
            if pos + 2 > n:
                break
            length = int.from_bytes(data[pos:pos+2], 'big')
            if length < 2:
                break
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
                          0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                if length >= 8 and pos + 7 <= n:
                    dims = int.from_bytes(data[pos+5:pos+7], 'big'), int.from_bytes(data[pos+3:pos+5], 'big')
                break
            pos += length
    elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        pos = 12
        while pos + 8 <= n:
            kind = data[pos:pos+4]
            size = int.from_bytes(data[pos+4:pos+8], 'little')
            start = pos + 8
            if kind == b'VP8X' and size >= 10 and start + 10 <= n:
                dims = 1 + int.from_bytes(data[start+4:start+7], 'little'), 1 + int.from_bytes(data[start+7:start+10], 'little')
            elif kind == b'VP8 ' and size >= 10 and start + 10 <= n and data[start+3:start+6] == b'\x9d\x01\x2a':
                dims = int.from_bytes(data[start+6:start+8], 'little') & 0x3FFF, int.from_bytes(data[start+8:start+10], 'little') & 0x3FFF
            elif kind == b'VP8L' and size >= 5 and start + 5 <= n and data[start] == 0x2F:
                bits = int.from_bytes(data[start+1:start+5], 'little')
                dims = 1 + (bits & 0x3FFF), 1 + ((bits >> 14) & 0x3FFF)
            if dims:
                break
            pos = start + size + (size & 1)
    return dims if dims and all(v > 0 for v in dims) else None


class ImageHeaderProbe:
    """Per-response state; at most 128 KiB of additional retained bytes."""
    max_bytes = 128 * 1024

    def __init__(self, filters):
        self.filters = filters
        self.prefix = bytearray()
        self.next_check = 24
        self.done = False

    def feed(self, chunk):
        if self.done:
            return
        remaining = self.max_bytes - len(self.prefix)
        self.prefix.extend(memoryview(chunk)[:remaining])
        if len(self.prefix) < self.next_check:
            return
        dims = header_dimensions(self.prefix)
        self.next_check = min(self.max_bytes, max(self.next_check * 2, len(self.prefix) + 1))
        if dims or len(self.prefix) == self.max_bytes:
            self.done = True
            self.prefix.clear()
        if dims:
            reason = self.filters.rejection(*dims)
            if reason:
                from .web import BodyRejected
                raise BodyRejected(reason, {'width': dims[0], 'height': dims[1], 'filter_stage': 'header'})
