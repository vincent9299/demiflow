# Dataset 图片搜索与获取

网络算子自动取得连接管理器的使用权，最后一个共享算子结束时关闭连接和维护任务。业务只声明出口策略及可选 `connection_policy`；生命周期、预算、观测和原生搜索后端的边界见 [连接管理](connection_management.md)。

`search_web` 统一返回线索；`fetch_documents` 获取文档；`fetch_images` 获取并验证原始图片。平台不认识概念 ready、正例数量或关系审核，这些由业务 Dataset pipeline 决定。

```python
from pathlib import Path
from demiflow import data
from demiflow.collect import SearchConfig
from demiflow.collect.session import WebSession
from demiflow.collect.image_library import ImageLibrary
from demiflow.collect.image_fetch import ImageFetchPolicy

objects = str(Path('shared/images/objects').absolute())
web = WebSession(
    cache_path='runs/acquisition/requests.sqlite', object_directory=objects,
    search=SearchConfig(engines=({'name':'bing images','categories':['images']},), retries=0),
    image_library=ImageLibrary(objects, str(Path('shared/images/library/index.sqlite').absolute())),
    image_policy=ImageFetchPolicy(max_bytes=20*1024*1024, max_pixels=40_000_000,
                                 max_frames=1, concurrency=2),
    retries=0, host_interval_s=1, timeout_s=30,
)
# 声明无 I/O。消费者从 search_web 的 candidates[].img_src 构造 requests。
stream = data.from_items([{'requests':[
    {'request_id':'image-1', 'url':'https://example.org/image.jpg', 'bindings':['opaque-id']},
]}]).fetch_images(requests='requests', output='images', session=web,
                 max_requests=64, max_request_bytes=256*1024,
                 request_concurrency=2, concurrency=4, queue_depth=8)
# 后接 save_lance(...).run_stream()；Dataset 执行器负责关闭 session。
```

## 输入输出

图片搜索沿用原生来源，不另起搜索服务。候选新增可空 `img_src`、`thumbnail_src`；`url` 仍是来源页面。同一页面的不同原图不再按页面 URL 合并；相同页面＋原图的重复结果合并 engines。完整 `response_json` 保留。网页结果历史投影不强加图片字段。默认引擎和真实访问是否可用须由消费者验收。

业务可声明 `google images`、`bing images`、`yandex images`、`baidu images`、`brave.images`，主备和代理通过 `WebSession` 配置。标准百度适配器取来源的 `replaceUrl[].ObjURL`，Brave 取 `properties.url`；缩略图独立保留，不能作为缺原图的替代。Brave 当前图片接口不支持分页，后续页返回 `unsupported_parameters`。2026-10-03 的 Brave 页面改为 Svelte 标量变量引用，平台以有界字面量解析器读取：页面至多 8 MiB、深度 64、20 万节点、4096 个标量参数，不执行页面 JavaScript；未知语法报解析失败。线上中英文原图检索已验证，但出口限流和网络失败仍按回执处理，不保证所有请求成功。

`fetch_images` 接收有界列表 `{request_id, url?, sha256?, image_uri?, bindings?, declared_width?, declared_height?, declared_file_bytes?, mime_type?}`。request_id 在当前行唯一，bindings 不透明。url 必须是 HTTP(S)；没有 url 时必须提供已知 SHA；本地 image_uri 必须是绝对 file URI 并附 expected SHA。输出保持行粒度与请求顺序，每请求附 result：status/reason、url/final_url、image_ref(uri+sha256)、content_type、format、width/height、size_bytes、filter_stage、retrieved_at、origin(local/download)、attempts。未知技术字段保持 null；失败或过滤没有成功 image_ref。

优先级：已知 SHA 的共享对象 → 显式本地 URI → 共享 URL 索引 → 下载。URL 索引只登记已解码成功的文件，URL 不等于图中身份。既有 CAS 可直接按 SHA 复用；没有 URL→SHA 记录的历史对象，不能从任意新 URL 推断同一内容。需要历史来源复用时，消费者可把实际已知的 SHA／URI 随请求传入，首次成功会登记 URL。不能只按文件名或图片相似度猜映射。

## 可选下载过滤

调用方在标准 `ImageFetchPolicy` 中声明条件；平台不内置训练任务的分辨率门槛，也不判断水印或概念。未配置或 `filters={}` 时不启用过滤，保留原有获取行为与回执身份。

来源元数据通过标准搜索候选及 Arrow 契约保留 `declared_width/height`、`declared_file_bytes`、`mime_type`。调用方可在选取下载名额前调用纯函数方法 `ImageFetchPolicy.declared_rejection(candidate)`；原生获取在需要 HTTP 时执行同一方法，不能由各业务另写一套判断。方法使用调用方的 `filters`、`max_bytes`、`max_pixels` 和可选 `allowed_mime_types`，返回拒绝原因或 None；None 只表示元数据未触发规则，不代表图片已验证。MIME 白名单默认不启用；未知 MIME、`application/octet-stream` 和未知文件大小继续传输/解码验证，实际解码格式仍须满足已配置白名单。宽高为一对正 int32，文件大小为正 int64，MIME 配置最多64项、每项255字符；无额外 HTTP HEAD。已知本地对象优先按真实文件验证，不用不可靠的远端声明覆盖事实。新元数据及 MIME 规则参与回执身份；完全不传新规则/声明的请求保留原身份。

IIIF 3 档位选择器只使用服务声明的尺寸、资源上限与支持的缩放能力；短边门槛和像素上限默认不设，由消费方配置。最多检查64个声明档位，绝不选择超过原尺寸的档位；协议计算出的缩放请求不伪造确切宽高，实际尺寸在获取后核对。Getty 与 Rijksmuseum 适配器每页最多4项、每项分别最多2/4次详情请求；平台不固化本批训练素材的1024门槛。

```python
image_policy = ImageFetchPolicy(
    filters={"min_long_side": 1024, "min_short_side": 512},
    max_bytes=20*1024*1024, max_pixels=40_000_000,
)
# 也支持 WebSession(image_policy={"filters": {"min_long_side": 1024}}, ...)
# 只有确认声明尺寸对应这个图片直链时才填写；不确定就省略这两个字段。
request = {
    "request_id": "image-1", "url": "https://example.org/image.jpg",
    "declared_width": 1920, "declared_height": 1080,
}
```

支持 `min_width`、`min_height`、`min_long_side`、`min_short_side`、`min_pixels`，值为正整数或 None；边界包含等号，多项条件须同时满足。宽高指编码文件的像素尺寸，长短边不受横竖方向影响，总像素为宽×高。`max_bytes/max_pixels/max_frames` 仍是独立的资源限制。当前不接受任意 Python 谓词或视觉模型作为下载过滤配置。

执行顺序：

1. **声明尺寸（declared）**：没有可复用本地文件时，检查请求显式提供的尺寸，未达门槛则不发 HTTP 请求。宽高须同时提供、均为正 int32，或同时省略。平台不从 URL 猜尺寸、不把搜索结果标称尺寸当成已验证事实；错误声明可能误排除图片，来源不可靠时应省略。
2. **文件头（header）**：GET 接收过程中，在有界前缀中读取 PNG/JPEG/GIF/BMP/WebP 尺寸，未达门槛立即关闭响应，不继续下载、不写 CAS、不进行解码，不重试也不轮换代理。这是图片文件头，不是 HTTP 响应头，不增加 HEAD 请求。仅检查成功的 2xx 响应；重定向、HTTP 错误和网络重试仍走原有流程。
3. **实际解码（decoded）**：完整下载或本地复用后，用隔离解码结果再次检查全部条件。声明或文件头通过不代表最终通过。TIFF、未识别或前缀内找不到尺寸的文件继续受原有字节／像素／帧／解码时间上限控制，最终以实际解码结果判断，不把“未知”当成合格。

被过滤返回 `status="filtered"`、`filter_stage`、包含条件/观测值/门槛的 `reason`、对应阶段的 `width/height` 和已有 attempts，`image_ref=null`。声明阶段的宽高只是调用方声明，header 阶段是未完整验证的容器字段，只有 decoded 阶段经过完整验证。过滤不计作代理故障，不登记成功 URL 索引；已有库存不删除。下载后才排除的文件可能已写入 CAS，遵循下文原有清理规则。

读取前缀最多额外保留 128 KiB，每个响应单独持有；按几何递增的前缀长度检查，累计扫描量有界，不在下载进程调用图像解码器。启用时传输按 16 KiB 块向检查器交付，底层网络缓冲及压缩编码可能读入更多字节，因此不承诺线上只传输文件头。总驻留仍须计入 `concurrency × max_bytes`、传输副本和解码预算。

过滤条件、过滤实现版本、显式声明尺寸进入获取回执身份；改变条件不会复用此前“已通过”或“被过滤”的判断。本地文件会重新验证，不自动重复 HTTP；相同配置及请求的终态回执仍可复用，失败不会获得隐含重试额度。`tests/test_image_filters.py` 覆盖声明零请求、虚高声明、分块提前关闭、重定向、gzip、HTTP 错误、未知格式回退、本地复用、并发隔离及配置边界；这些为本地可控测试，不代表真实站点下载成功率。

## 存储与恢复

`ImageLibrary` 声明可共享的 CAS 根和 SQLite 索引，二者与每 run 请求账本分离。原始字节以 SHA 命名、不修改像素。相同字节并发写入收敛到同一对象；同一 URL 的跨 run 获取使用共享分片锁，重查索引后复用；锁文件最多 4096 个。URL 精确标准化，不做模糊匹配。

持久请求账本复用完整结果，包括失败；未知中断请求不会自动获得免费重试。缓存身份包括解码实现、获取策略、库路径、URL／expected SHA、网络参数。成功复读仍检查实际文件 SHA 和字节上限，文件丢失或损坏返回 integrity_error，不自动补一次网络。不同 run 可复用已验证 URL 文件；新策略仍重新解码检查。没有 freshness／自动 refresh 策略，变更 URL 的新快照需显式另行设计，不通过删除账本隐藏重试。

下载先写 CAS 后在隔离进程解码，失败可能留下可寻址的原始字节，但不会进入成功 URL 索引，也不会作为成功图片交付；清理必须按引用和回执另行执行。元数据/磁盘/索引错误传播，不能冒认为成功下载。

## 资源与错误边界

HTTP 复用 WebClient 的流式字节限制、重定向检查、域名策略、代理、host 准入和重试回执。图片独立 fetch semaphore，默认并发 2。单图默认 20 MiB、4000 万像素、1 帧。Pillow 先校验容器，再完整解码每帧；JPEG/PNG/WEBP/GIF/BMP/TIFF 可用，HTML 或不支持容器失败。解码在当前执行器拥有的短命子进程中进行，默认地址空间 1 GiB、时间 30 秒，并限制 CPU 时间；取消等待已有有界工作退出后释放文件和库锁。

解码器直接以 `python -I image_decode.py` 启动，仅加载标准库后设置地址空间／CPU／单文件16 KiB及core禁用限制，再导入Pillow。不能先通过完整demiflow模块启动再降低地址空间上限：Arrow／BLAS等已映射空间可能超过1 GiB，导致正常图片也报MemoryError。父子进程仅交换32 KiB请求和16 KiB标量回执；临时目录不复制图片，单次元数据磁盘预算64 KiB。文件SHA、字节、像素、帧和完整解码检查保留。修复启动方式不改变请求身份，不重置预算，也不自动重试已提交失败。

默认每行最多 64 请求、256 KiB 元数据；字段长度、bindings 数量、并发、子进程寿命均有上限。只在有界 HTTP 层短暂持有 bytes，Dataset 输出只保存引用及标量。SQLite 每连接缓存 8 MiB、单条回执最多 64 KiB，不缓存全库。库要求本地 POSIX 路径、可靠 flock 与 SQLite 锁；不声明跨对象存储的分布式锁语义。

`tests/test_image_fetch.py` 覆盖真实 Dataset 生命周期与隔离解码：合法文件、坏内容、字节／像素／帧限制、预期 SHA 冲突、丢失／损坏文件、同/跨 run URL 去重、本地零 HTTP 复用、请求顺序及取消锁等待。

## 搜索与下载独立选择出口池

`WebSession(search_session_pool=..., search_route_attempts=...)` 配置搜索会话池；对应的 `fetch_session_pool=...` 配置文档／图片下载的主会话池。两边也可以各自选择静态出口池，不以搜索或下载用途固定池类型。平台不内置供应商、账号、IP，也不把搜索租约带给下载器。供应商会话工厂由消费者通过 import 路径声明。

| 选择 | 搜索 | 文档／图片下载 |
| --- | --- | --- |
| 主静态出口池 | `search_routes=[...]` | `fetch_proxy_routes={'*': {'pool': [...]}}` |
| 主会话池 | `search_session_pool={...}` | `fetch_session_pool={...}` |
| 备用 | 显式 `search_fallback` | 静态池内显式 `fallback_session_pool` |

这次接口对齐补齐了下载主会话池；备用组合仍保留各自已有契约，不声明支持任意池之间的递归降级。主会话池不需要先构造一个必定失败的静态出口。搜索与下载各自持有租约、连接、并发和创建额度；工厂接口共享，池实例不共享。

```python
web = WebSession(
    cache_path='runs/acquisition/requests.sqlite', object_directory=objects,
    fetch_session_pool={
        'factory': 'my_project.network:sticky_proxy',
        'factory_options': {'base_env': 'IMAGE_SESSION_PROXY'},
        'identity_envs': ['IMAGE_SESSION_PROXY'],
        'size': 4, 'ttl_s': 300, 'interval_s': 3,
        'creation_window_s': 60, 'max_creations_per_window': 12,
        'acquisition_timeout_s': 30,
    },
    fetch_proxy_routes={'internal.example': None},
    retries=1, timeout_s=30,
)
```

`fetch_session_pool` 是默认域名规则 `fetch_proxy_routes['*']={'session_pool': ...}` 的等价简写，两种写法采用相同请求身份，可互相复读已完成回执。也可为某个具体域名单独声明 `{'session_pool': ...}`。具体域名规则优先，并在每次重定向后重新选择；不会把源站的会话租约直接带给另一个域名规则。默认会话池与显式 `'*'` 或 `fetch_proxy_url` 同时出现会在无 I/O 的声明阶段报冲突，不能暗中改变主路径。

主会话池每 URL 仍只有 `retries+1` 次尝试。429 将当前代际标为不可用，关闭后在剩余额度内申请另一代际，不把 Retry-After 用作整个会话池的等待；创建速率／容量／租约等待仍有独立上限，额度耗尽明确失败。主路径与静态后备用路径均复用下述生命周期和持久创建账本。新 SID 由供应商映射到出口，不能把 SID 不同视为已验证 IP 不同。

业务可以继续声明“搜索会话池＋下载静态出口池”，无需启用下载会话池。新增主路径、域名与重定向、429 替换、创建额度、凭据身份及正式 Dataset 断点复用由 `tests/test_fetch_primary_session.py` 覆盖。

下载路由支持显式 `'*'` 默认规则。最长具体域名优先，其次 `'*'`，都不存在时才使用旧 fetch_proxy_url／直连行为。设置默认静态池后，新发现的图片域名和重定向目的地都会进入该池；具体域名可声明 `None` 表示显式直连。例：

```python
fetch_proxy_routes = {
    '*': {'pool': [
        {'name':'static-1', 'proxy':{
            'chain':['http://configured-entry.example:3128', {'secret_env':'IMAGE_STATIC_PROXY'}],
            'allowed_domains':['*'],
        }, 'interval_s':3},
    ], 'failure_limit':2, 'cooldown_s':1800, 'cooldown_wait_s':30},
    'internal.example': None,
}
```

`allowed_domains=['*']` 是调用方显式允许该代理链接收任意目的域名；本机 relay 的认证、连接数量和寿命限制仍然生效。该通配声明仅用于代理配置，blocked_domains 继续拒绝通配符。多跳 relay 对 HTTPS 使用 CONNECT、对普通 HTTP GET/HEAD 使用同链的末级转发代理；不改原 URL 的协议。HTTP 每连接仅转发一个无请求体的请求，移除逐跳及本机代理认证头，末级使用自己的凭据；拒绝超出目的域名／端口声明的请求，不透传后续流水请求。重定向仍由获取器逐跳审查，不自动直连。

### 静态池按网站选择出口

出口清单和调度策略由业务通过 `fetch_proxy_routes` 声明，平台每次 HTTP 动态租用可用出口，不将一个网站或 URL 永久绑定某个 IP。设置 `health_scope='host'` 后，429/403 和网络失败的健康状态按“出口＋目标 host”保存；路由并发和发起间隔仍在所有 host 之间共享，不能为每个新网站复制一份连接容量。不同供应商声明不等于已验证不同公网 IP。

`rotate_on_rate_limit=True` 要求 host 作用域。收到 429 后，`Retry-After` 只限制刚失败的出口／host；在显式 `WebSession(retries=...)` 额度内立即重新选其他可用出口，不先做整池等待。所有出口对该 host 都不可用时，才应用该 host 的 `cooldown_wait_s`。超时／无等待额度返回该 URL 的 `route_unavailable`，其他网站保持可用；包括重定向进入冷却池的情况。磁盘／元数据错误和整体资源不足仍中止 action，不能伪装成网络失败。

```python
fetch_proxy_routes['*'].update(
    health_scope='host', rotate_on_rate_limit=True,
    max_host_health_entries=4096, cooldown_wait_s=0,
)
# six attempts = initial plus five retries, shared by all declared static exits
web = WebSession(..., fetch_proxy_routes=fetch_proxy_routes, retries=5)
```

`max_host_health_entries` 限制每个声明域名池的持久健康记录，默认 4096、最大 65536；每次准入只加载当前 host 的最多 128 条出口状态。成功的全新 host 不新增健康记录；过期状态在写入前清理。额度已满且无法回收时明确报资源不足，不提前删除未到期冷却以换取容量。此处不申请新 SID，也不隐式启用下载会话池；如需要会话池，须单独显式声明下述配置。

## 静态下载失败后使用会话池

在某条静态池路由上显式设置 `fallback_session_pool` 后，403、429、5xx、连接/超时失败或静态出口全部冷却可触发会话池获取；404、业务排除、字节/内容错误和正常成功不触发。原静态获取最多 `retries+1` 次，随后最多 **1 次**会话池获取，每次仍受重定向、时间和字节限制。原失败与后续结果都在 attempts 中，触发降级时新增 `route_kind=static/session`。服务端 Retry-After 在 60 秒内会等待后再尝试；超过 60 秒保留失败，不绕过该等待换出口。

```python
fetch_proxy_routes['*']['fallback_session_pool'] = {
    'factory': 'my_project.network:sticky_proxy',
    'factory_options': {'base_env': 'IMAGE_SESSION_PROXY'},
    'identity_envs': ['IMAGE_SESSION_PROXY'],
    'size': 4, 'ttl_s': 300, 'interval_s': 3,
    'creation_window_s': 60, 'max_creations_per_window': 12,
    'acquisition_timeout_s': 30,
}
```

工厂与搜索池使用同一调用协议：`factory(token=32位不透明十六进制串, ttl_s=..., options=...)`，返回普通代理或 Secret 代理链声明。供应商语法、环境变量及允许域名由项目实现；平台不引用特定项目。`identity_envs` 中真实值只参与凭据摘要，变化会改变获取身份，原值不写回执。声明阶段不调用工厂或读取密钥。

下载会话池按需创建，不启动搜索 worker 或后台补池。每 WebSession/域名池最多 size 个租约、每租约一个 HTTP client/同时一个请求；client 最多保留一个连接，relay 最多两个隧道以允许旧连接收尾。到期或失败的代际先关闭连接再替换；关闭时清理工厂新建且在返回声明中引用的环境密钥，不删除调用方原有密钥。工厂需在返回前完成自身失败清理，不得创建未声明的外部资源。创建节奏通过 run journal 持久准入，最多 4096 个窗口内时间戳；重启不能重置创建额度。槽位和并发上限属于单 WebSession，多个会话不共享并发信号量。

失败后的重复执行仍复用同一获取回执；新建会话不是额外无限重试机制。代理事件、会话生成和使用数量可在 `fetch_session_pools` 指标及 journal 中查看。测试覆盖标准 Dataset 下载、静态→会话成功/失败、旧回执复用、永久错误、长 Retry-After、创建额度跨重启、到期替换、取消释放和声明无副作用。


## 可配置的静态出口并发与来源页面过滤

静态 `pool` 成员可声明 `concurrency=1..64`（默认 1），与 `interval_s` 独立：前者限制该出口在途 HTTP，后者限制发起间隔。实际仍受 WebClient 全局／host 准入限制。等待者在取得名额前重查冷却；取消归还名额，不增加网络重试。调度参数不改变既有请求身份或清除健康历史。指标新增 active_requests、peak_active、concurrency；原 active 布尔保留。

静态成员也支持 `reuse_connections`（默认 `True`），与搜索静态路由同名。设为 `False` 后，每次 HTTP 结束不保留空闲连接；出口 IP、代理链和会话策略保持原声明，不申请 SID。HTTP 客户端连接数受该出口的 concurrency 限制，复用开启时空闲连接至多 `min(20, concurrency)`。此开关用于显式管理连接生命周期，不改变已完成请求身份；禁用复用会增加握手开销，是否改善吞吐须按实际运行验证。

所属 action 关闭 WebClient 时取消未完成的获取任务，不再等待排队请求逐个启动和耗完重试。已预留但未提交结果的请求仍保留 interrupted 回执；重启不自动恢复其额度。正在进行的有界文件／解码工作按自身取消契约收尾，之后关闭客户端与代理中继。测试覆盖关闭后无继续请求、相同账本不重发和两种连接复用模式的真实 TCP 行为。

代理 CONNECT 后的 TLS 握手失败另有连接生命周期边界：当前 httpcore 1.0.9 可能留下 ACTIVE 的隧道对象，即使底层 socket 已关闭仍占池容量（[上游记录 #921](https://github.com/encode/httpcore/discussions/921)）。平台 `collect/http_transport.py` 在建立隧道失败／取消时关闭所属连接，让既有池机制回收容量；HTTPX 的异常映射、证书校验、连接和请求上限保持。该局部 transport 用于 WebClient 代理获取和下载会话客户端，不修改安装环境或全局猴子补丁。适配点依赖 HTTPX 0.28／httpcore 1.0 的 transport 接口，升级依赖时须保留真实套接字回归；上游正式修复后可移除。测试以容量 2 连续制造握手错误／超时，随后不同来源的正常请求必须仍可成功。关闭 keep-alive 不能修复这种 ACTIVE 对象泄漏，两者须分开验证。

`demiflow.collect.web.URLPolicy(blocked_domains=..., fetch_url_rules=...)` 是无 I/O 的声明和判定对象，其 `exclusion_reason(url)` 与 WebClient 使用同一实现。消费者可在标准 Dataset.map 中同时审查图片来源网页和 img_src，保留 excluded 原因后再送 fetch_images。规则名单和取舍属于业务，不内置电商名单。域名最多 4096、路径规则最多 256、待判断 URL 最多 16384 字符；只判断声明的 URL，不能据此声称已读来源网页或证实版权、事实身份。真实下载重定向仍逐跳检查策略。

## 搜索与原图下载流式衔接

可将 `search_web → save_lance(output_ref='search_source') → flat_map → 来源 filter → fetch_images → save_lance` 放在一个 `run_stream` 中。每批搜索成功提交后，`output_ref` 为该批下游行附上固定 uri/version，不写入自身 schema；字段冲突在该批写入前拒绝。下游据此读取真实版本，不必等待所有搜索结束，也不猜当前表头。

流式 `flat_map` 对迭代器逐项拉取并受下游有界队列背压，失败／取消时关闭迭代器，不先完整转成 list。连续过滤时定期让出事件循环以响应取消；用户行函数自身的分配仍需遵守字节和数量预算。原生搜索／下载共用同一 action 的 session 生命周期。业务在调度前保留总请求上界，并在固定阶段成功后核对实际行数。

### 下载与解码分别限流

`ImageFetchPolicy.concurrency` 限制整项图片获取及 HTTP 请求并发；`decode_concurrency`（默认 2，范围 1–256）另外限制隔离解码进程。实际解码并发不超过二者较小值。例：下载 128、解码 8，可以让慢网络请求并行等待，同时把隔离解码声明上界控制在 8×decode_memory_bytes。每张原始字节仍受 max_bytes 限制，传输副本、等待解码的元数据及其他执行器内存另计。取消时等待已启动的解码子进程退出后才归还解码名额；只修改 decode_concurrency 不改变图片获取回执身份。

## 已验证原图的模型输入缩图

`demiflow.collect.image_resize.resize_image(raw, max_side=1536, ...)` 接收调用方已校验的编码字节，在独立 Pillow 进程输出等比缩放、EXIF 校正、透明区域白底的 JPEG（默认质量90），不修改原对象。PNG/JPEG/WEBP/GIF/BMP/TIFF 均可用；动图或多页图只取首帧，与普通模型输入一致。这是消费时的通用图像处理原语，不改变 `fetch_images` 获取原始字节的契约，也不判断图片是否为正例。

默认源图上限1.6亿像素、编码32MiB；独立进程地址空间1.5GiB、CPU/墙钟45秒、输出8MiB，关闭core dump。每个调用进程最多2个并行解码器，等待解码名额计入墙钟预算。每项临时输入/输出/诊断文件最多48MiB加小型配置，退出或超时清理；调用方已有编码字节、父进程及跨多个调用进程的并发另计。子进程地址空间上限不代表整个pipeline的RSS上限。损坏、超时、源像素或内存预算不足明确报错。

T2I作者正例在主进程可安全解码时沿用原编码路径；大JPEG先使用draft，PNG等不能预降采样的大图转入上述隔离路径。定向验证覆盖25.2MP带透明/EXIF的PNG、源像素预算、损坏图、超时退出和名额回收；真实29.7MP、138.7MP PNG均已成功缩图。
