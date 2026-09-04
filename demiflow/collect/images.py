"""demiflow 图像内容校验原语：字节 → 图像元数据（fetch_tiers 的 verify 钩子）。

自 collect_v2.op_download 的 _verify_image 沉淀（2026-09-04，机制归引擎）：
- Pillow 全量解码（im.load()）强制暴露截断/损坏，而非仅读头部；
- format → mime / ext 规范表（ext 不带点，与常见清单口径一致）；
- 失败/截断/非图返回 None（= fetch_tiers 的拒收语义，不轮转）；
- Pillow 惰性导入（extras [images]），未安装时调用才报错。
"""

from __future__ import annotations

import io
from typing import Optional

# Pillow 解码格式 → mime（通用规范表，非业务口径）
_FORMAT_MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "GIF": "image/gif",
    "WEBP": "image/webp",
    "BMP": "image/bmp",
    "TIFF": "image/tiff",
}
# mime → 扩展名（无点）
_MIME_EXT = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
    "image/bmp": "bmp",
    "image/tiff": "tiff",
}


def verify_image(data: bytes) -> Optional[dict]:
    """verify 钩子：全量解码提取 {format, width, height, mime, ext}。

    解码失败/截断/非图返回 None；未知格式 mime 回落
    application/octet-stream、ext 回落 bin。
    """
    from PIL import Image
    try:
        with Image.open(io.BytesIO(data)) as im:
            im.load()   # 强制全量解码，截断/损坏在此暴露
            mime = _FORMAT_MIME.get(im.format or "",
                                    "application/octet-stream")
            return {"format": im.format, "width": im.width,
                    "height": im.height, "mime": mime,
                    "ext": _MIME_EXT.get(mime, "bin")}
    except Exception:  # noqa: BLE001 - 解码失败一律按拒收
        return None
