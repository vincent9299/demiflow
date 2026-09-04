"""demiflow.collect.images 测试：verify_image 的解码/规范表/拒收语义。"""

from __future__ import annotations

import io

from demiflow.collect.images import verify_image


def _png(w: int, h: int) -> bytes:
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (w, h), (1, 2, 3)).save(buf, "PNG")
    return buf.getvalue()


def test_png_metadata():
    m = verify_image(_png(320, 240))
    assert m == {"format": "PNG", "width": 320, "height": 240,
                 "mime": "image/png", "ext": "png"}


def test_jpeg_metadata():
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (10, 10), (4, 5, 6)).save(buf, "JPEG")
    m = verify_image(buf.getvalue())
    assert m["mime"] == "image/jpeg" and m["ext"] == "jpg"


def test_truncated_rejected():
    assert verify_image(_png(64, 64)[:32]) is None    # 截断：全量解码暴露


def test_not_image_rejected():
    assert verify_image(b"plain text bytes") is None


def test_empty_rejected():
    assert verify_image(b"") is None
