# Journal CPU 优化 review 与验收（2026-11-01）

## 决定与架构

保留三处平台模块的小范围 Python 改进，不引入 Rust、新调度器或配置开关。
设计及实现前 review 见 [设计](journal-cpu-design-20261101.md)。

| 组件 | 本轮修改 | 保留的边界 |
|---|---|---|
| `operator_llm.client` | 已准备请求直接解包；reserve 发现已完成时，同一 I/O 工作内 lookup；优先使用带引用的联合读取 | prepare / journal / response 三个 client-owned 有界 pool；重工作不移入 event loop |
| `sqlite_journal` | lookup 的响应与 attempt 同一 SELECT 读取；save 在已有事务查询中取得 attempt；复用 shard 的当前 PID 启动身份 | FULL commit、writer lease、预算、队列、逐行提交/重放、分片与恢复协议 |
| `call_ref` | 原生不可变标量直接构造 fresh dict，其他类型与子类保留 asdict | 引用字段、attempt=1 省略规则、深复制兼容和历史记录读取 |

没有跨请求响应缓存。新增缓存只有每 shard 的一个 PID/start-time tuple，PID
变化即刷新，读取失败不缓存；这不宣称整个 SQLite 对象支持跨 fork 复用。
用户无需改变业务 prompt 或部署配置，通用客户端/journal 路径自动受益。

## 实现 review 发现并修复的问题

原先 `lookup → references` 分两次查询，单条 `save → references` 也在提交后重新
查询 attempt。另一客户端若在中间完成显式 HTTP retry，前一次响应可能附上
下一 attempt 的引用。新实现使用本次观察/提交时的 attempt 整数，不跨请求缓存。
新增测试用两条连接在引用构造时插入下一次 retry，验证返回的旧引用仍能读到
原来的 429，新的引用能读到后来的 200。

联合 SELECT 是两个索引查询的一次 SQLite 调用，不是“磁盘读取减半”。实测
EXPLAIN 使用 `sqlite_autoindex_calls_1` 和覆盖索引
`recovery_request_attempt`，没有全表扫描。额外结果只是一列 attempt 整数。
批量提交原本已经在事务内绑定引用，沿用原实现。

review 特别排除了把准备、journal 和响应工作合到一个 pool 的方案：它会让慢
I/O 阻塞响应处理，并改变三个已有 workers 限额。此次只删除无工作量的交接，
以及把同类、顺序依赖的 I/O 放进同一个已受限的工作项。BlockingIOPool 的
背压、重复取消排空、关闭顺序均没有修改。

原生引用序列化仍返回独立可变 dict；类型/子类回退测试确保通用 asdict 深复制
行为保留。导入 metadata、read-only、uncertain、HTTP retry 的预算与历史引用、
图片摘要、逐行失败和响应提交后才发布等路径通过相关回归。

## 对照方法

证据目录：`/tmp/demiflow-journal-cpu-20261101`。`baseline/modules/` 保存修改前
三个模块，每个子进程在导入 runtime 前装载 baseline 或 candidate；其余平台
源码、解释器与配置相同。每个结果包含三个模块的 SHA256，避免拿旧统计当新证据。

计时关闭 profiling；单个 scenario 内两种实现逐轮交换先后顺序，顺序执行，
没有并行运行本任务的其他测试或 benchmark。已有外部训练保持运行，因此给出
轮次配对区间并区分稳定收益和噪声。bootstrap 区间是本机样本的描述性估计，
不保证其他硬件、文件系统或负载相同。

* **真实重放**：1000 条历史 JEV journal，12 轮/实现，先预热；read-only，
  请求预算 0，并发 4。计时包括完整 runtime、journal 读取、解码、schema 与关闭。
* **CPU 在线链路**：真实 loopback HTTP，500 条/阶段，并发 8，6 轮/实现。
  服务返回由输入确定的合法 SystemOne 概率，用于隔离 CPU/存储开销，不是模型
  性能。计时包含 Dataset、HTTP、逐条 journal 和 checkpoint。另做只读重放与
  引用读取，输出、请求身份、usage、attempt 引用逐条做跨实现哈希比较。
* legacy 与 throughput 分开测；throughput 使用相同的默认 `{}` 配置，不改
  flush 窗口、队列、shard、workers、WAL 或同步设置。不同模式间的绝对吞吐
  不是本轮配置选择建议。
* GPU 对照使用一个自有 JEV 实例，200 条不同业务输入、两个执行通道、无合批。
  客户端用同一物理地址接入该托管实例，避免每个子进程重复启动或更换缓存。

在线 p95 在 `call_with_trace` 入口/出口计时，包含 journal 提交和 schema 校验；
不把 HTTP 内部计时冒充完整请求延迟。action wall 还包含 Dataset checkpoint。
计时边界相同；独立线程池提交计数只在额外诊断轮次开启，不用于吞吐汇总。

## CPU 端到端结果

下表为各轮中位数；吞吐单位行/秒，CPU 为单阶段整个客户端进程线程累计时间。

| 链路 | baseline → candidate 吞吐 | CPU 时间 | p95 | 吞吐差异 |
|---|---:|---:|---:|---:|
| 1000 行真实重放 | 1939.9 → 2156.8 | 626.7 → 566.2 ms | 2.51 → 2.29 ms | +11.2% |
| 500 行 HTTP + legacy journal + checkpoint | 284.2 → 290.8 | 1776.8 → 1746.4 ms | 27.69 → 26.84 ms | +2.3% |
| 500 行 HTTP + throughput journal + checkpoint | 76.42 → 80.31 | 1661.7 → 1414.8 ms | 106.47 → 97.51 ms | +5.1% |

按轮次配对的吞吐比及描述性 95% 区间：

* 真实重放：1.139，[1.086, 1.197]，方向稳定；CPU 中位数减少约 9.7%。
* legacy 在线：1.036，[0.976, 1.097]，区间跨 1，**不能认定稳定的在线吞吐提升**。
* throughput 在线：1.050，[1.042, 1.057]；CPU 减少约 14.9%，p95 减少约 8.4%。

线程池计数证明减少了真实交接：500 条首次请求加 500 条重放，prepare 提交
**1500 → 1000**；journal 保持 2002、response 保持 1500。每个首次调用省一次
仅解包 `_PreparedCall` 的提交；重放保留三个职责隔离的交接。reserve 竞争命中时
还省一次 journal 交接，由专门回归测试覆盖。

OS 自愿上下文切换中位数分别从 42560 → 34787（重放）、65738 → 60664
（legacy 在线）、44836 → 34804（throughput 在线）。它包含 GIL、锁和 I/O
等待，不能与线程池提交数等同。客户端峰值 RSS 中位数约 205–209 MB，候选没有
增加常驻载荷或连接。没有放宽任何内存/并发上限来换取性能。

## GPU、回归与最终保留审计

`gpu-jev/report.json`：JEV 单实例、GPU 1、并发 2，6 轮/实现，每轮 200 条。
1000 条预热请求结束后，captures 固定为 32，计时期间不再捕获新图；生产默认
配置未改。所有阶段使用相同输入，未合批。

| 指标 | baseline | candidate |
|---|---:|---:|
| Dataset action 吞吐中位数 | 20.531 行/s | 20.342 行/s |
| 客户端 CPU / 200 行 | 1106.9 ms | 1017.3 ms |
| 请求 p95 中位数 | 164.22 ms | 174.49 ms |
| 自愿上下文切换中位数 | 12758 | 12044 |
| 客户端峰值 RSS 中位数 | 205.51 MB | 205.51 MB |

按轮次配对吞吐比为 **1.001**，描述性 95% 区间 [0.991, 1.012]，不能认定 GPU
吞吐增加。p95 的配对 baseline/candidate 比为 1.001，区间 [0.899, 1.109]，
也没有稳定的尾延迟收益。客户端 CPU 中位时间减少约 8.1%；配对比为 1.057，
区间 [1.034, 1.080]。GPU 推理仍支配该场景耗时，不能把 CPU 改进包装成模型加速。

2400 条正式 A/B 请求及 1000 条预热请求精确一致，各阶段另做只读 journal
重放、Dataset checkpoint 与原生引用读取。比较包含模型结果、request_id、usage
和引用 kind/attempt；仅排除预期不同的输出目录与执行耗时。worker 48 次在线
质量复核，0 mismatch / 0 failed。最终图缓存 3,991,846,978 bytes < 4 GiB，
allocator 峰值约 70.31 GiB < 74 GiB。

回归与保留审计：

* **253 项相关 CPU 回归通过**：新增引用快照/竞争、类型兼容、PID 身份、空转
  交接与 reserve 竞争命中测试，以及 journal、批量提交、只读重放、HTTP retry、
  线程池取消/隔离、图片、SystemOne、Dataset、引用检查与 embeddings 消费者。
* 本轮没有修改模型数学或质量门槛；GPU 选 JEV 验证通用客户端链路，不把它
  当作其他三个模型重新测过的速度数据，也不宣称解决历史 JEV 跨启动漂移。
* 起始 284 个平台 Python 文件中只有 client、sqlite_journal、call_ref 三个变化。
  起始 122 个 dirty 文件中，只追加修改了 client 和旧 CPU profiler 的 lookup
  探针适配；其余保持起始哈希，无文件丢失。systemone 图片 state 与上一轮
  deployment 复用改动完整保留。
* 自有服务、模型和 benchmark 子进程已清理，GPU 0/1 均为 0 MiB；未操作
  既有服务、业务数据或 GPU 2/3。所有新产物约 118 MiB，位于实验目录。
* HEAD 与起始相同，没有 commit/push；`git diff --check` 与 benchmark 语法检查通过。

最终决定：保留这些通用 CPU 改进。真实重放和本机 throughput 在线链路有可重复
收益；legacy 在线和 GPU 吞吐差异仍在噪声范围内。没有通过增加线程、放宽
队列或降低持久化标准取得这些结果。

## 复现入口与资源限制

`benchmarks/journal_cpu_experiment.py` 为顺序控制器，
`benchmarks/journal_cpu_worker.py` 为隔离执行器，均可通过 `--help` 查看参数。
示例（在平台仓、现有平台解释器内，输出目录必须不存在）：

```bash
PYTHONPATH=. /yzp/zhaozy/yangzepeng/0905/env/bin/python benchmarks/journal_cpu_experiment.py \
  --scenario replay --rounds 12 --rows 1000 --workers 4 \
  --baseline /tmp/demiflow-journal-cpu-20261101/baseline/modules \
  --source /tmp/demiflow-jev-integration-20261101/service-v6-fixed \
  --prompt /tmp/demiflow-jev-integration-20261101/service-v6-fixed/prompt.yaml \
  --cases /tmp/demiflow-graphs/native/cases.jsonl \
  --output /tmp/journal-replay-new-run
```

HTTP 场景用 `--scenario http --rows 500 --workers 8 --rounds 6`，cases 换成
`/tmp/demiflow-four-models/business-200.jsonl`；批量提交另加 `--storage throughput`。
GPU 场景额外需要显式本地模型路径、模型解释器与空闲 GPU 0/1，已测参数记录在
`gpu-jev/report.json`。只读历史 journal 不迁移；其他写入全在新的实验目录。

案例文件最多 32 MiB、切分前最多 512 个换行、单行最多 1 MiB，阶段最多 2000 行/并发 8。在线响应上限
8 KiB（足够本轮 noul 协议）；写入前按阶段数、实际输入与响应上限做 1 GiB
磁盘准入，保留 SQLite/引用/checkpoint 余量。子进程 240 秒、父进程 30 分钟超时，
异常清理自有子进程和服务，所有待测数据和延迟集合均有行数上限。
