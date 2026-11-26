# 图片搜索、原始获取与独立解码

`Dataset.fetch` 每行获取一个原始对象，完整契约、预算、队列结算和迁移见
[原始对象获取](dataset-fetch.md)。`Dataset.fetch_images` 已移除；fetch 不做
图片格式判断、尺寸过滤或解码。业务先从搜索候选选择 `img_src`，保留来源页面
`url`，将一个图片链接映射为一行 `request`，获取后再显式解码与判断。

```python
from demiflow import data
from demiflow.collect import FetchPolicy
from demiflow.objects import LocalObjectStore

raw = data.from_items([{
    'id': 'image-1', 'page_url': 'https://example.org/page',
    'request': {'uri': 'https://example.org/image.jpg'},
}]).fetch(store=LocalObjectStore('/local/raw-images'),
         policy=FetchPolicy(max_object_bytes=20*1024*1024), concurrency=4)
# raw.materialize() 或持久 sink；实际解码使用独立图片原语及其像素/超时预算。
```

图片搜索沿用原生来源，不另起搜索服务。候选新增可空 `img_src`、`thumbnail_src`；`url` 仍是来源页面。同一页面的不同原图不再按页面 URL 合并；相同页面＋原图的重复结果合并 engines。完整 `response_json` 保留。网页结果历史投影不强加图片字段。默认引擎和真实访问是否可用须由消费者验收。

业务可声明 `google images`、`bing images`、`yandex images`、`baidu images`、`brave.images`，主备和代理通过 `WebSession` 配置。标准百度适配器取来源的 `replaceUrl[].ObjURL`，Brave 取 `properties.url`；缩略图独立保留，不能作为缺原图的替代。Brave 当前图片接口不支持分页，后续页返回 `unsupported_parameters`。2026-10-03 的 Brave 页面改为 Svelte 标量变量引用，平台以有界字面量解析器读取：页面至多 8 MiB、深度 64、20 万节点、4096 个标量参数，不执行页面 JavaScript；未知语法报解析失败。线上中英文原图检索已验证，但出口限流和网络失败仍按回执处理，不保证所有请求成功。

## 独立验证与质量判断

`WebSession.fetch_image`、`ImageClient`、`ImageFetchPolicy` 及下载/缓存/解码组合已移除。
fetch 只记录原始获取事实。业务通过后续普通 fn/actor 写独立质量字段；需要过滤/扩行时
先完成获取结算，再读固定历史结果。声明尺寸只帮助选择候选，不能代替实际解码。

`image_decode.decode_image(uri, sha256, policy)` 校验完整 SHA 并在独立小进程解码。
policy 必须提供 `max_bytes`、`max_pixels`、`max_frames`、`decode_memory_bytes`、
`decode_timeout_s`。调用方用普通 map 的并发或有界 gate 限制同时解码数。
子进程应用内存/CPU/墙钟限制，失败不改写获取回执，不重新 HTTP。
尺寸等过滤条件由业务在解码之后自行判断，不再藏在下载策略中。

```python
from demiflow.collect.image_decode import decode_image

def quality(row):
    result = row['result']
    if result['status'] != 'ok':
        return {**row, 'quality': {'accepted': False, 'reason': 'fetch_failed'}}
    ref = result['raw_ref']
    info = decode_image(ref['uri'], ref['sha256'], {
        'max_bytes': 20*1024**2, 'max_pixels': 40_000_000, 'max_frames': 1,
        'decode_memory_bytes': 256*1024**2, 'decode_timeout_s': 30,
    })
    reason = None if max(info['width'], info['height']) >= 1024 else 'min_long_side'
    return {**row, 'quality': {'accepted': reason is None, 'reason': reason, **info}}
# 对结算后的固定 history Dataset 执行 history.map(quality)。
```

## 历史读取

历史对象使用前由 `ObjectRef.read/verify` 或 `decode_image` 核验实际内容。
旧索引命中不替代内容验证，不重新获取来掩盖对象丢失或损坏。

IIIF 3 档位选择器仍仅用服务声明的尺寸、资源上限和缩放能力；门槛由消费者提供。
最多检查64档，不放大原图，不把协议计算值伪装成已验证尺寸。
搜索适配器保留来源元数据，业务用普通 map/flat_map/filter 选择一个 URI/行。

## 已验证原图的模型输入缩图

`demiflow.collect.image_resize.resize_image(raw, max_side=1536, ...)` 接收调用方已校验的编码字节，在独立 Pillow 进程输出等比缩放、EXIF 校正、透明区域白底的 JPEG（默认质量90），不修改原对象。PNG/JPEG/WEBP/GIF/BMP/TIFF 均可用；动图或多页图只取首帧，与普通模型输入一致。这是消费时的通用图像处理原语，不修改 Dataset.fetch 已提交的原始字节，也不判断图片是否为正例。

默认源图上限1.6亿像素、编码32MiB；独立进程地址空间1.5GiB、CPU/墙钟45秒、输出8MiB，关闭core dump。每个调用进程最多2个并行解码器，等待解码名额计入墙钟预算。每项临时输入/输出/诊断文件最多48MiB加小型配置，退出或超时清理；调用方已有编码字节、父进程及跨多个调用进程的并发另计。子进程地址空间上限不代表整个pipeline的RSS上限。损坏、超时、源像素或内存预算不足明确报错。

T2I作者正例在主进程可安全解码时沿用原编码路径；大JPEG先使用draft，PNG等不能预降采样的大图转入上述隔离路径。定向验证覆盖25.2MP带透明/EXIF的PNG、源像素预算、损坏图、超时退出和名额回收；真实29.7MP、138.7MP PNG均已成功缩图。
