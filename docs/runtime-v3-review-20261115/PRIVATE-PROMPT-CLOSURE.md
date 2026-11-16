# 原生 Prompt 私有缓存、结果提交与统一资源验收

## 范围与版本

隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`  
分支：`engine/gpt-v3-regression-20261115`  
完整功能候选：`45a61b6`；`d0591cc` 只收窄测试观察范围。最后 `aa3673b` 修复私有调用准入身份，并对受影响路径单独固定验收。本文件后的文档提交不改变生产或测试输入。

本轮自身提交按顺序：`3115f21`、`8320f59`、`06c6f43`、`45a61b6`、`d0591cc`、`aa3673b`。
依赖 `61bf436` / `18e98b5` / `1c8a505` / `27de688` 分别是总控已持有的
Embedding 私有缓存、预算错误、合作停止，以及 `5707e1b` 宽 literal 身份修复。
总控不应重复合入这些依赖。七件冻结文件保持 `49b666f` 字节；未改主仓、共享环境或服务，未推送。

## 对用户的实际能力

- 原 `map_prompt` / `map_prompt_async` 显式 SQLite 请求缓存可以在统一引擎的实际 worker 上执行。
  仍由用户指定模型、输入和路由；普通与 Routed 两种缓存都实测 Local/process 和同机 Ray Core。
- 显式私有缓存继续沿用原 HTTP request key、原 SQLite 请求/响应行及 call refs。
  已付费响应重用不会再发 HTTP，也不产生本次运行的新请求借记。
  首次调用、重试、run / 节点 / route / 私有历史请求上限各自保持原含义。
- 普通 Prompt 可以把**本次完整 attempt** 的输出物化到原 Dataset 结果 manifest；
  原最终 ACK 成功后，重读同一 ResultRef 不运行模型、不发 HTTP。
  这项能力是本轮补齐的目标能力，不宣称它曾在 `267b914` 通过。
- 私有缓存没有原生 HTTP pending-call 恢复能力。HTTP 前即封存 `attempt_only`，
  中途崩溃或原非 final ACK 后中断仍拒绝跨 attempt 恢复，不能把已确认结果缓存当恢复授权。
- 显式私有缓存仍不进入 Forge 受管执行、acquisition/queue、SaveLance 等原生写出路径。
  Embedding 私有缓存的原 delivery-only 合同未放开。Routed 私有缓存尚未授予 materialize。

## 唯一权威与故障边界

worker 仅使用原 authenticated bounded journal RPC；driver 从真实封存 graph 选择既有私有路径，
核 worker lease / action / occurrence / 已声明 route。RPC 不接受另一个路径、未知 route、
额外 native recovery 字段或未声明重试策略。

RPC operation id 另外绑定原行 scope 和 route，仅用于传输幂等；它不写入原 HTTP key。
同一 reserve 重传得到同一成功回执；不同业务行即使 HTTP 内容相同，也不能复用前一行的
发送许可。原 pending / unknown 保持拒绝，旧 classic client 的 requeue 也不能夺走活跃 writer。

最后补齐了一个实际付费请求边界：原 Embedding RPC 没有 batch/invocation 身份。
真实 driver + 原 SQLite 对照中，在显式 requeue 后再次收到 reserve=True，
但新 intent=0、原 run 借记仍为1；这是旧发送许可被误复用，不能以缓存幂等解释。

`aa3673b` 将 Prompt 的真实 row_scope、Embedding 的真实 batch RowScope + 原 request_part
同时绑定到 RPC 与原 RunJournal 准入；不进入 HTTP/cache key。Embedding 只在原 worker binder
用薄 async wrapper 包原 _exchange，原 await / return / exception / finally 保留，
ContextVar 随原 journal I/O pool 的 context 传播。分段数受 sealed max_batch 约束：
原 byte packing 每次至少消费一行，不接受越界 part。没有改 embeddings/runtime.py，
没有设一个虚假的 native recovery adapter，也未改变原 Embedding 预算异常或恢复能力。
真实批3行按字节拆成 part0/1/2，原成员事实仍在原 journal；第二次缓存重用零 HTTP/新借记。
旧 classic requeue 无法夺活 writer；writer 结束后，显式授权的新 scope/part 必须新准入，
而同一 RPC 重传仍只借记一次。

私有 SQLite 是 HTTP 意图/响应的唯一账本；RunJournal 是有限资源准入、执行与结果提交的唯一权威。
两数据库没有被包装成原子事务：原 RunJournal 先准入，私有 SQLite 后准入。
若在中间失败，保守保留原借记；不退款、不伪造原子性、不自动恢复。
旧 classic 文件锁协议保留，没有静默切成 throughput row lease。
关闭时继续使用原一个 owned close thread，先等待已接受的 RPC，再关闭原两个模型的数据库。

原生 Prompt 的 `_CapacityPending` 已加入固定控制异常词表。原异步 client.execute 包装本就会
释放 journal slot 后重试；现在该信号跨 RPC 保留类型，而非变成 INTERNAL_PROTOCOL。
暂拒不会写意图/扣预算或被缓存为永久成功/失败回执；driver 不阻塞等待容量。
真实八并发、原 64 MiB control 额度的测试观察到暂拒并继续完成，最终仅八次请求借记和八个完整响应。

## 私有缓存与原 artifact 同时驻留

新增 `dataflow_private_storage.bind_private_storage` 在两个模型 binder 后只执行一次：

1. 从封存配置解析规范化真实路径；不同节点、路由和符号链接 alias 指向同一文件时去重。
2. 在原 RunJournal fence transaction / `runtime_reservations` /
   `artifact_staging_bytes` 内预约每路径上限，没有另造 allocator、文件或账本。
3. 私有路径合计最多分配 run spill 上限的一半，为原 artifact/exchange 留另一半。
   新路径平分尚未分配的私有份额；每路径至少 4 MiB，最多 32 GiB。
   比如 32 MiB run、两个新私有路径，各 8 MiB；原 SQLite 再保留 rollback/page 开销。
4. reservation 绑定规范化路径，与 action/attempt 无关。后续 action 重用同路径、同 cap，
   不重复计费、不静默扩额或缩减旧 cap；无法容纳新增路径时明确拒绝。
   持久缓存的预约在本 run 内不退款。
5. 原 SQLite DELETE 模式 / max_page_count 约束继续生效。已有历史数据库超过 cap、
   WAL 历史、异常 lease 文件或过大历史目录在写入前拒绝，不删、不截断、不转换历史。
   检查原文件最多 100000 个 lease 条目；原 run 的有限请求额度限制新增文件数量。
   **文件系统 inode/目录元数据不是可移植的字节硬限制**，已在封存事实中明确 False，
   不把 SQLite payload/page 约束冒称完整文件系统元数据硬隔离。

真实混合 Prompt→Embedding、同路径别名、两个 action、原 artifact 共驻均有验证；
历史过大和已有 artifact 占额的反例在任何 HTTP / 新私有 DB 创建前拒绝。

## 原测试与第一手红绿证据

`8320f59` 固定验收：74 passed / 4 failed / 2 deselected（300.67s），同机 Ray 2 passed（42.03s）。
四红原样保留，没有删掉失败用例：

- 大图像 input 在旧 source identity 的 1 MiB metadata 文档上限失败：
  总控 `5707e1b` 流式保留旧 literal 摘要，未提高默认 metadata 上限。
- 私有普通 Prompt materialize 被本候选初版拒绝：已按总控裁决补齐当前 attempt 结果提交。
  原两测试在真实 `267b914` 和 `f15016a` 均在入口私有 journal guard 失败；
  日志证明它们不是 267 已通过的能力回归。原 nodeid/业务断言仍保留。
- standalone factory 被强传 `native_journal=None`：无真实绑定时恢复原调用形态。
- 模型验证原 monkeypatch 留在父进程，worker 真 GET 得到 501：
  改用原 loopback server 真 GET /models 观察，保留一 GET、八 POST、八并发断言；
  随后暴露并修复原生容量信号跨 RPC 丢失。

宽源修复后大图像旧探针又得到四次 hash，而非两次。第一手取证表明它观察的是 driver
lookup/reserve/response/replay 认证，未观察到 spawn worker 的原准备阶段。
最终该身份专项使用真实 thread worker，ContextVar 仅圈定真实 AsyncClient.prepare 的观测范围，
保留 `len(hashes)==2`、非主线程、HTTP 一次、replay、原 cache key、不复制原图像的全部断言。
driver 认证次数另外写入 JUnit property；没有把四改成二或删断言。
独立的真实 process 与 Ray 缓存链路保持覆盖。

`06c6f43` 固定原生/私有组合：27 passed / 7 deselected（146.31s），含 enum 1024/1025/1500
原 Git golden、真正容量暂拒、普通 materialize、非 final ACK 后拒恢复及私有缓存边界。
`27de688` 原四红组合：6 passed / 1 hash 观测红（55.54s）；随后真实 prepare 观测单例通过。
上述开发期、基线和固定失败证据都保留在配套 JSON，不用最终绿覆盖。

## 最终候选结果

| 固定候选 | 验收范围 | 结果 | 证据前缀 |
|---|---|---|---|
| `45a61b6` | 原 SQLite 全14项、旧 routes、共同storage、native capacity、普通materialize、历史golden | 31 passed / 107.70s | `/tmp/demiflow-private-prompt-complete-local` |
| `45a61b6` | 真 Ray：普通/Routed缓存重用、普通materialize最终结果重复读取 | 3 passed / 24 deselected / 63.08s | `/tmp/demiflow-private-prompt-complete-ray` |
| `d0591cc` | 大图像原用例，分别观察 client prepare / driver认证 / RPC序列化 | 1 passed / 12.20s | `/tmp/demiflow-private-prompt-image-audit` |
| `aa3673b` | 受影响的私有Embedding全Local组、Prompt重排/所有预算/重试、原停止入口 | 32 passed / 1 deselected / 130.50s | `/tmp/demiflow-private-scopes-fixed-local` |
| `aa3673b` | 真Ray：原Embedding实际batch分段/缓存重读、普通Prompt物化重读 | 2 passed / 41.04s | `/tmp/demiflow-private-scopes-fixed-ray` |

全部无 skipped/xfail。Ray 的24个 deselected 是其余 Local/协议用例；aa Local的1个 deselected 是另行真Ray运行的Embedding packing。不同候选的统计分别列出，不相加冒充同一HEAD的全套通过。
每份固定证据含2147个输入摘要；前后无变化。原四个红 nodeid 在最终功能候选均通过。
`45a61b6` 初版新增的 `driver_request_authentication_hashes=8` 属性实际覆盖了所有
prepare 之外的 canonical 序列化，命名不准确；这不是八次客户端哈希。
`d0591cc` 仅新增实际 DriverJournalService.native_request 的动态观测范围，
最终 JUnit 明确记录 client exact hashes=2、driver认证=4、worker RPC serialization=4。
原业务断言一直保留；两提交之间所有生产/打包输入逐字相同。

精确命令、真实模块路径、固定 HEAD、每个源码/测试输入 SHA256、逐项结果与冻结七件
见配套 JSON 的 verification artifacts。测试期间无输入修改；最终文档不算生产变更。

ManagedHTTPService/部署型 Prompt 的后续兼容由总控另接，本轮不代为声明其完成。

本交付没有把 V017 物理 target/broadcast 全路径恢复门、受管 Routed 准入、多机、
任意外部模型或 filesystem metadata 硬隔离宣称完成。同机 Ray Core 为真实执行；
本轮没有在每个子进程安装 Ray Data import 计数器，不能声称测得子进程零次导入。


