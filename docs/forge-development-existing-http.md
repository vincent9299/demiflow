# 复用既有开发服务

`demiflow_development_existing_http_v1` 将已登记项目的开发界面接回一个已经运行的开发服务。原服务继续独占原 Store、provider 和 host partition；本连接不构造 Store、不获取 `service.lock`、不创建 state root 或开发预算槽。

配置精确引用原 deployment 的路径和 SHA-256、原进程 PID/start/boot、literal `http://127.0.0.1:端口`、原私有 console token 文件，以及允许的 workspace/root/目录身份。项目必须已在原 deployment 授权。每条请求和等待响应期间检查原 policy、进程、root 与 token 文件元数据；漂移拒绝，不能自动采用另一进程或授权。

`existing_grants` 保留原公开开发 GET/POST：持续对话、历史、context/output、归档、输入、取消和恢复。原 backend 逐次执行真实权限、provider、CAS 和 durable-intent 检查。桥保留所有 request/submission/session/turn ID 和 expected revision；发送前查询原 capabilities，`start:false` 不妨碍原本允许的取消/恢复。未获开发授权的项目返回明确的项目级禁用状态；原 identity 的授权不会变成 demo 的开发权限。

`history_only` 是显式较低权限连接：所有 POST 在本桥拒绝，页面禁止发送、归档、取消、恢复与输入。它仍读取原历史，不替代完整开发能力验收。

## 传输和资源

只复用原公开路由与字段，不直接访问远端 Store 的内部方法。尤其 context 通过原 `dev-context` 读取。固定 loopback、不使用环境 proxy、不跟随重定向，不透传调用者的 Authorization/Host/Cookie。原 console token 只在固定请求内存中使用；本桥不读取 native provider credentials 的正文。

| 边界 | 原接口依据 | 本桥行为 |
|---|---|---|
| 请求 | Workspace HTTP body 64 KiB；开发正文另有 32 KiB 业务限制 | 同一 64 KiB 编码上限，原 backend 继续检查各字段业务限制 |
| 响应 | Workspace `_send` 编码上限 9 MiB；事件页最多 50 条且累计存储 wire 至多 256 KiB，每事件受原预算控制 | 完整读取最多 9 MiB，不截断；要求原 JSON/Content-Length/API-contract，拒绝 chunked/redirect/重复 JSON key/非有限数 |
| header | 原服务发送有限固定 header | 聚合最多 64 KiB；每行仍使用标准库 HTTP parser |
| deadline | 页面写入等待 65 秒；原执行异步返回受理 | 读取至多 5 秒，写入至多 60 秒；准入、capabilities 和 POST 共用一个绝对期限，嵌套不重置 |
| 并发 | 本桥没有执行器或开发队列 | 最多 2 条桥端在途请求/连接、1 个监视线程、1 个至多 250 ms 的控制准入等待 |

显式取消、输入和恢复可释放本桥自己的一条纯读取连接，为控制请求让出名额；不打断写入，不向原 owner 自动发送任何控制操作。原服务可能仍结束已断开的只读 HTTP handler，该尾部受其原 HTTP/订阅限额管理。本桥的 `active_connections` 是桥端统计，不声称结束了原服务线程，也不代表远端模型任务总数。

关闭或消费者断连会 shutdown 原 socket（包括 HTTP/1.0 已转交给 response 文件的 socket），释放本桥连接。`close()` 只做本地传输清理，最多等待 2 秒；不 cancel/recover/stop 原 owner。尚未退出的本桥请求记录为 pending，不伪造 complete。

诊断只保留最近 16 条方法/耗时/dispatch/退出记录，不含 prompt、令牌、正文或凭据。buffer peak 是编码请求/header/body 的保守预留统计，明确不包含解码 JSON、标准库对象及 native 分配，不是 RSS 硬上限。

## 不确定结果和权限变化

POST 在首次可能写入前记录 dispatched。已发写入的超时、断连、关闭、畸形回复或来源漂移返回 outcome unknown，保留原 ID，绝不自动重试。上游真实错误保留 HTTP 状态、原 CAS 错误和 `operation_started` 事实；上游 console 认证错误的 code 加 `upstream_` 命名，防止误判成当前工作台令牌失效。

现有开发页定期读取原 capabilities，拒绝或来源变化关闭相应操作。连接关闭不会接管、压缩、重放或重新归属任何旧会话；原不在配置中的遗留 workspace 保持排除状态。

## 验证和发布

传输 fixture 验证全部 GET/POST 字段、ID/CAS、不确定结果、动态授权、进程/policy/root 漂移、断连/关闭、并发和边界。fixture 不是模型或业务验证。真实原服务验证只发 GET，核原历史、配置、DB 字节与原身份；不启动真实模型/CLI 业务 turn。

本地 Service factory 分支维持原语义。后继完整源码和 wheel 需集中 Review；当前安装版及在途 demo/output 未退出前不安装、不替换。后续仅在自己的工作台登记原项目，7 项 native/analysis/config 权限为 false，开发权限仍由明确引用的原部署决定；正式服务和业务文件不变。
