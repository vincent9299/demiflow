# G：混放位置与 P1 保留对象

本表独立于 18 条 import；不把「6 条环外函数内导入」写成「6 类运行对象」。固定源为 `122edf4bd0c827338846abd0114f0b36cf8c1b6d`，逐文件 SHA 与类 AST SHA 在 `source-inventory.json`。

以下为 2026-11-20 的历史裁定；关系声明的现行归属和兼容范围见文末。三处混放当时均保留当前模块，不另开实施任务。逻辑职责按当前 owner 说明，不因目录名把运行代码称为声明，也不为了换目录名承担历史迁移。

| 位置 | 真实消费者与职责 | 状态、资源、身份 | 决策和负例 |
|---|---|---|---|
| `execution/local_kernel.py:45` PlanInput、`:52` UnionSource、`:58` KeyedSource | `data/relational_declaration.py:11`、Dataset 的 join/reduce/groups/union 等，planning/engine 读取同一类型。声明 helper 接 PlanInput，返回同一 KeyedSource/UnionSource。 | 三个原 dataclass 持 source/plan/executor、端口/键/reducer；不是新运行 owner。原有 union 仅展开空计划；关系预检复用原 compiler。模块整体还有运行政策，但三类本身保持。 | 不搬类、不改 `__module__`、不加影子 DTO。P1 relational/declaration 守卫、原 source/plan/executor 相同性、重复/null/端口、无额外排序、union 非空计划不展开；身份/序列化类路径逐字保持。 |
| `data/local_relational.py` | 引擎 `local_tasks` 和 `dataset_native` 两条 import 语句共 10 个符号（架构 v3 的 10 不是 10 条语句）；排序/连接/分组/checkpoint 的实际运行算法。`keys` 已由 P1 归到 relational_declaration，共用一份。 | spill 文件、外部排序/合并、ProcessPool/FD/取消属于现有任务执行生命周期；`_IO_BYTES=1MiB`、`_BLOCK_BYTES=256KiB`、`_MERGE_FAN_IN=32`，这些局部常量不是整个路径 RSS 的证明。进程可按模块名载入函数；改路径还会改变函数身份。 | 保留，不能为了目录观感再搬一次。后续只有实际替换某算法/消费者的任务才逐函数复审。原 keys 单实现守卫、慢消费者/宽行/取消清理/重复多重性与已有算法检查；不据 G 重跑无关吞吐。 |
| `data/result_cache.py` 的 CacheCall/CachedMap/CachedSyncMap | Dataset.map_cached 原包内构造；引擎 6 条 import 语句共 9 个符号，来自 dataflow_model_factory/models/usage、kernels.operator。P1 的 Cached* → native_model_declaration → CacheFactory 仍原两阶段。 | SQLiteKV、cache key/version/namespace、跨进程锁、frontier/state适配/replay 由原缓存和引擎 owner 协作；默认输入/结果各 8MiB、entries 2M、cache 8GiB、wait120s 是原局部上限；不等于全任务内存硬界。 | 不建 native_authoring，不将 SDK→data 改成 SDK→execution→data 绕行；类体/状态方法/私有访问保留。P1 精确构造/方法引用 guard、cache state restore/unknown/pending 负例与历史固定 bytes；不以 cache miss 隐藏恢复不确定性。 |

P1 六类运行对象单列如下。所有六项均保留当前构造和路径；这是仍然存在的运行依赖，不能宣称「SDK 完全不持运行实例」。构造不等于已执行，但可能规范化真实路径、分配可变容器，不能纳入「纯预检无副作用」保证。

| 类型 / SDK 调用点 | 输入、输出、实际所有者 | 状态 / 并发 / 资源 / 错误 | 历史、旧入口与验收 |
|---|---|---|---|
| `queue_producer.QueueProducer:74`；Dataset.enqueue:883，导入889 | 原 QueueConfig + 实际 ProducerRef + messages/queue_depth -> 原 actor，map_async concurrency=1；worker 原 configure/bind_execution 才取得 parent/channel/io。 | 构造校验 ProducerRef 精确类型、depth1..256；astart 打开原资源，固定 source unit/occurrence/chunk/游标/EOS 原逻辑，aclose 排空原 IO。原 identity、producer generation/消息预算不改。 | 不改类/recovery_identity/源 completion，不另造 producer 声明。guard 固定原 constructor/方法用途；反例旧 producer/源变化、未确认提交、重复消息和停止须由原 queue tests 证明。 |
| `queue_ack.QueueAcknowledge:7`；Dataset.ack_queue:899，导入905 | 原 config/result/depth/keep_row -> actor；bind_execution 只接已经验证的 graph-tail source。 | depth1..256；source.owner 管 delivery/io。__call__ 必须有 RowScope.delivery，经 source.io 调原 queue_execution.finalize_task；输出原 row 或 completion。未知/丢失 delivery 拒绝，不自行 ACK。 | queue_execution 是冻结件；不迁算法/类路径。guard 固定 constructor/keep_row，负例未绑定source、外来scope、重复/未确认完成，禁止在 SDK 新增 restore/close/ACK 调用。 |
| `raw_fetch.RawFetch:19`；Dataset.fetch:777，导入785 | 真实 LocalObjectStore、request/output、原 policy/concurrency/depth -> 原 actor，后续原 map_async。 | 构造只校验/绑定已有 store，concurrency1..64/depth1..256；astart/bind_execution 才获取 run/io/transfer/namespace。原 HTTP/object 提交/queue settlement 与取消排空原 owner；不可把失败当完整结果。 | 整个 raw_fetch.py 冻结，不动任何字节。P1精确构造guard+freeze hash；未知HTTP结果/部分对象/取消/恢复与原 scope 负例保持，不运行真实网络业务。 |
| `stream_lookup.LanceLookup:10`；Dataset.lookup_lance:979，导入986 | 固定 source/on/columns/output/max_matches/max_bytes -> 原 actor，SDK concurrency=1/depth=1。 | 构造 normalize_lookup、保存真实 source；astart/_open 读固定表，128-key/8MiB结果缓存，单匹配/批读取遵守原 max_matches/max_bytes；这些不等于Lance内部内存硬上限。aclose 清原表/缓存。 | 类路径/固定版本/源校验保留；无新抽象查询服务。负例版本变化、多匹配/宽行、缓存边界、取消；不允许声明默认读取 latest。 |
| `stream_admission.StreamAdmission:112`；Dataset.admit_rows:913，导入927 | path/key/unique_on/quotas/when 与预算 -> 原 actor；SDK concurrency=1，原 queue_depth。 | 构造 resolve路径/验证字段/生成policy，db/lock未开；astart/start 持原唯一写锁。原最多10M entries、key<=65536 bytes、disk<=16GiB准入；delivered集合随已准入键受限；已预留计数不因下游失败退款。 | 原 policy/key/digest、SQLite/首次到达与replay owner保持；不能转成普通缓存。负例duplicate/limited/skipped/冲突policy/disk不足/同源重放，关闭释放自己的db/lock。 |
| `stream_lance.StreamLanceSink:275`；Dataset.save_lance:1027，导入1041 | URI/schema/key/stage/options -> 同一原 sink；SDK 取 actor.__call__、actor.uri 并构建原 SaveLanceOp/stages。 | 构造规范化本地URI、校验schema/key，不发布table；astart取得原writer/锁，原批量/flush/queue与rows/row/key/index上限，tail flush、失败后on_drain固定receipt保持。源写入成功不能当未写重新覆盖。 | 类/父类方法路径、method reference、schema/阶段/原receipt identity不变；guard保留对uri/__call__的例外，禁止泛化方法访问。负例变更同key、盘满、已提交后响应异常、append固定头、提前结束/取消。 |

共同实施前提：若未来目标是消除六类实例表示或移动三处混放中的序列化对象，必须提出精确对象/载体、旧读取和原版本恢复办法，再按 P1 §9.1/P2 §5.2 和 C 对应范围评审。这不是当前保留决策的阻塞，也不授权新 S2/冻结或跨版本恢复。不得保留永久垫片假装历史已经迁移。

现有保留守卫已在本 G 树跑过：`test_p1_native_dependencies.py` + `test_p1_relational_boundary.py`，23 passed，1 个插件配置 warning，原始结果见 baseline.json。这是原 main 的基线，不是 G1/迁移后的运行验收。


## 2026-11-21 D1 关系声明的最新裁定

`PlanInput`、`KeyedSource`、`UnionSource` 的唯一定义在 `demiflow.data.sources`。Dataset 组装、规划与执行直接消费这三个声明。Python 可见的 `__module__` 和新 pickle 的类路径采用新模块名。原 `demiflow.execution.local_kernel` 仍提供三个旧名字，解析为同一批类，供历史程序解码和原来的显式导入使用。

v3 封存继续使用原模块标签计算三个精确类型、其实例及生成方法的身份；字段、方法代码、业务代码、输入、原始封存与恢复点仍按原规则校验。子类和同名用户类沿用自己的身份。旧类名解析与旧身份复算分别验证。上述裁定仅替换历史表第一行对三个关系声明物理位置的限制。

v3 的函数身份还包含整个定义文件的 SHA-256，捕获公开的 Dataset 类会封存全部方法的代码。为使旧运行能用相同业务代码续跑，v3 身份编码对这次改动的四份确切源文件保留原来的身份字节。适用范围为 data.dataset、data.sources、execution.local_kernel、data.result_cache 中仍可从定义模块解析且与实测源码一致的函数。捕获 Dataset 类时，仅在其五个关系方法上把唯一一条由 data.sources 导入 PlanInput 的指令按原包名编码；其余指令、常量、字段和相对导入层级都参与实际代码摘要。方法变更会改变类身份。模块其他位置的编辑不改变类身份。缓存的 _state_policy 已改变，保留实际新身份。完整的新旧文件摘要对列在 execution/recovery_sdk_v3.py。未知文件版本、同名仿造和更改的函数使用实际身份；测试覆盖这些反例。

设计选择适用于同一份 v3 写入与复核契约。构造 Dataset action、绑定旧日志、在工作进程复核程序都要从当前对象生成同一个身份，部分引用只保留身份摘要。新运行记录这四份已核实 SDK 实现的 v3 逻辑身份，封存程序及业务源码仍写入并复核实际内容摘要。历史文件原样保留。
