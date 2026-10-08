# 算子执行期间的连接管理

`search_web`、`fetch_documents`、`fetch_images` 实际执行时自动取得 WebSession 的使用权。只声明 Dataset 或 WebSession 不创建客户端、连接或后台任务。每个执行会话拥有一个 `ConnectionManager`，维护协程运行在其事件循环中，没有独立守护进程或连接管理线程。

## 生命周期

同一次 action 的多个网络算子可以共享 WebSession。执行器先登记这些使用者，每个算子的全部 worker 完成并送出结果后调用 `astop`。最后一个使用者结束，才关闭搜索后端、下载客户端、代理资源及连接维护任务；无需等后面的模型或其他慢算子结束。同一个 actor 被多个节点复用时，等这些节点全部结束再释放。

异常和取消停止行调度后走同一释放路径。action 级 `aclose` 是部分启动失败的兜底，关闭幂等；清理失败会报告。最终指标保留到 `stats.metrics.resources`。顺序重用 WebSession 执行新 action 会创建新的管理器。不同 action/event loop 不得同时共享实例，也不能关闭对方持有的资源。

直接调用异步 `WebSession.search/fetch/fetch_image` 时，第一次调用惰性启动管理器，调用者仍须在 `finally` 中 `await web.aclose()`。不支持把手工调用混入由另一个 action 管理的执行会话。

## 出口与连接的职责

IP 出口池选择已配置的出口，按目标 host 保存健康及限流状态；代理会话池管理供应商 SID 的创建、期限和退役。它们不代替 HTTP 库的连接池。

`ConnectionManager` 统一创建、登记和关闭下载/旧 HTTP 搜索使用的 HTTPX 客户端。HTTPX/httpcore 继续负责 TCP/TLS 复用和请求分配。静态出口保有独立客户端；代理会话退役时关闭客户端、归还容量。TLS 建连失败仍使用 `RetrievalTransport` 的异常清理，不能依赖定时任务弥补漏释放。

原生搜索在隔离 worker 中使用 curl/浏览器连接，仍由相应后端管理，并在最后一个使用者结束时随搜索后端回收。下面的 HTTPX 预算不包含 worker 内的连接；SearchConfig 的 worker、HTTP 并发和代理隧道预算分别生效，不把 HTTPX 统计称为跨后端或跨进程总数。

## 有限预算与维护

通过 `WebSession(connection_policy={...})` 配置，业务无需手工创建管理器：

| 字段 | 默认值 | 含义 |
| --- | ---: | --- |
| `max_clients` | 128 | 本会话 HTTPX 客户端数量上限 |
| `max_total_connections` | 2048 | 所有这些客户端声明的连接容量之和上限 |
| `max_connections_per_client` | 128 | 单客户端允许声明的连接容量上限 |
| `keepalive_expiry_s` | 5 | 空闲连接过期时间 |
| `maintenance_interval_s` | 5 | 后台清理间隔，至少 0.01 秒 |
| `close_timeout_s` | 10 | 单客户端关闭及单池维护的超时 |

分配前检查数量和容量。容量按声明上限预留，不是当前打开连接数；关闭成功才归还，关闭失败保留占用，避免不断补建资源。独立客户端并行关闭，任务数最多为 `max_clients`，避免慢关闭时间按客户端数串行累加。不同 WebSession 的预算相加，不声明进程级统一硬限制。操作系统硬杀或不遵守取消的自定义资源不提供 Python finally 必达保证。

维护协程调用底层连接池已有的过期、空闲淘汰和排队唤醒规则，不按时长强杀有效的在途请求。执行结束停止维护；维护失败只保存异常类型，在后续准入或关闭时报错。扩展点按 HTTPX 0.28 / httpcore 1.0 验证，升级依赖须重跑真实连接测试。

已从池中摘除的空闲连接并行关闭，数量不超过该客户端声明容量。多个慢关闭不再串行累加到同一维护期限；某个关闭失败时，其余已摘除连接仍各自执行关闭，最后汇总异常。2026-10-04 生产出现维护超时后补充此保护，相关慢关闭、单连接失败及真实 CONNECT/TLS 生命周期测试共 9 项通过；不扩大连接容量、超时或业务重试额度。

管理器不发起业务重试，不改变 HTTP/搜索/模型预算和缓存身份。`capacity_error` 表示本地连接容量不足或连接池等待超时，不把出口记为网站限流，不自动换 IP 或增加重试；网站 429 继续由出口/会话调度策略处理。

## 观测与测试

`WebSession.snapshot_metrics()['connections']` 提供客户端创建/关闭数、当前和峰值声明容量、实际连接数、使用中/空闲连接数、排队请求数、空闲回收次数及后台维护状态。指标不含代理 URL、认证内容或逐请求历史；扫描规模受客户端和连接上限约束。

`tests/test_connection_manager.py` 用真实本地 HTTP 检查无新请求时的空闲回收、取消、容量拒绝、关闭失败不归还容量，以及池超时不污染出口健康。`tests/test_web_operator_lifecycle.py` 通过正式 Dataset 覆盖共享、慢下游、空输入、过滤、重复执行、同 actor 多节点、启动失败、运行失败、取消和 action 归属。已有 CONNECT/TLS 失败、429 轮换、下载、原生搜索及流式取消测试继续回归。
