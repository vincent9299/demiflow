# Journal CPU 与线程交接优化设计

范围：显式平台工作，只修改 demiflow；保留既有 dirty 文件，不操作业务数据、
既有服务和 GPU 2/3，不 commit/push。起始 122 个 dirty 文件与 284 个平台 Python
文件已做哈希快照，证据目录 `/tmp/demiflow-journal-cpu-20261101`。

最终实现 review、端到端结果与限制见 [验收报告](journal-cpu-review-20261101.md)。

## 问题与边界

上一轮真实重放中，2048 行的 journal 工作占约 423.5 ms 客户端 CPU，涉及
lookup、attempt 引用构造、metadata 和线程交接。本轮复查发现：

1. lookup 与 references 分两条 SQL 查询；不仅重复查询，重试/恢复恰好发生在
   两次读之间时，响应和 attempt 引用还可能属于不同快照。
2. 每条引用经 dataclass `asdict` 递归复制固定的 3 个字符串和 1 个整数，重复
   两次创建字典；无需对已知不可变标量做通用深复制。
3. runtime 已传入 `_PreparedCall`，发送前仍 offload `record()`，实际只取出
   `.source`。reserve 发现另一调用已完成时，又多交接一次执行 lookup。
4. throughput journal 每次创建数据库租约都会读相同 PID 的 `/proc` start time。

保留三个有界、client-owned pool 的职责：prepare 构造/哈希，journal 持久化
I/O，response 解码/解释。不能把大输入哈希、重响应解码放到 event loop，不能
靠合并不同职责的 pool 改变 `prepare_workers/io_workers/response_workers` 上限。
不修改 BlockingIOPool 的背压、取消排空和关闭实现。

## 最小实现与实现前 review

* 新增可选 `lookup_with_references`，SQLite 用同一 SELECT 的关联子查询取得
  响应与 attempt，再返回 `(saved, refs)`；无结果为 `(None, None)`。普通 lookup
  API 保持原返回类型，旧/自定义 journal 仍走原接口。引用不做跨请求缓存。
* 原生 PromptRecordRef 的精确 str/int 字段直接构造 fresh dict，保留 attempt=1
  省略规则。其他类型/子类保持原 `asdict` 行为，避免改变公共兼容性。
* 实现 review 发现单条 `_save` 提交后也会再次查询 attempt。把 attempt 合入
  事务内已有的 SELECT，提交后使用捕获的整数构造引用；省去查询，也避免别的
  client 紧接着 retry 时返回新 attempt 的引用。批量提交已在事务内取 attempt，
  沿用其实现。
* `_PreparedCall` 直接解包；未准备请求仍进入 prepare pool。reserve 与“已被别的
  调用完成时的 lookup”在同一次 journal 工作中执行，保留 reserve/commit 边界。
* 进程启动身份仅在 shard 生命周期内复用，按 PID 检查、fork 后刷新；读取失败
  不缓存，仍保守处理。每 shard 一个固定大小身份，不引入按请求增长的缓存。

review 结论：允许这四项小改动。模型输入、请求 hash、数值、schema 质量校验、
请求额度、FULL commit、逐行记录、writer lease、显式恢复及历史 attempt 可读性
必须保持。新 SELECT 的跨连接快照一致性、旧只读数据库兼容、导入补 metadata、
取消时持久化完成但不发布行，都要检查。暂不改事务 batching 参数或磁盘耐久性。

## 资源和验收

新读取只增加一个 attempt 整数字段，没有额外载荷复制/队列；用已有
`recovery_request_attempt` 索引，不扫描全部调用。引用 dict 独立可变；固定路径
由既有 journal 生命周期确定。最多 8 shard，新增状态 O(shards)，无全局缓存。

差分覆盖 request id、输出、usage、引用、attempt、异常、预算、未确定调用、
恢复、legacy import、read-only、图片 state、取消/关闭及分片字节上限。
先跑无 GPU 回归，再做端到端 A/B：保存原始模块快照、独立子进程装载 baseline
或 candidate，交替顺序，关闭 profiling。真实旧 journal 只读重放，临时 journal
写入/重放与 Dataset checkpoint，最后用自有 GPU 1 服务校验真实模型链路。

每阶段至多 2000 行、并发至多 8；案例文件最多 32 MiB、单行 1 MiB，报告不保存
无限延迟样本。数据库/输出仅在实验目录，总磁盘预算 1 GiB。服务一次至多一个，
GPU 0/1 空闲检查与原有锁继续生效，GPU 2/3 不使用。计时阶段不同时运行其他
benchmark 或测试；外部任务保持运行并报告测量波动，不承诺 GPU 吞吐必然增加。
