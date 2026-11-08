# 自动调优实现复核（2026-11-03，Asia/Singapore）

本轮工作区：`../_demiflow/model-autotune-20261103/`。设计见
[自动调优方案](model-autotune-design-20261103.md)。以下是实现自审和可重复测试记录，
不是独立人工 reviewer 的签字。GPU 结果绑定本轮源码和各自独立的原生参考。

## 架构和边界复核

- Dataset / `map_prompt_async` 仍负责业务行调度、预算、重试与 journal；未增加第二套
  业务执行器。`tuning.py` 仅定义有界候选、身份、排序和档案校验；
  `tuning_runner.py` 是显式部署实验，复用现有 `build_service` 和服务 owner。
- `prepare` 仍 CPU-only。`auto` 缺失或过期时拒绝未命中请求的服务启动；完整命中
  journal 时不要求 GPU 和调优档案。普通 native/optimized 不自动应用档案。
- 执行配置单独进入 worker 的 execution identity，不替换逻辑模型契约。测试通过
  对应 optimized → auto 的真实 HTTP journal 离线重放，服务 bind 被禁用仍成功。
- 跨请求 batch 始终关闭；四个 backend 的原生协议、隔离状态和读出保持原样。
  新 backend 必须提供质量配方，未知模型不靠名字猜测或自动启用未经验证的内核。

## 质量、异常和资源复核

- 原生参考禁用候选图/内核，固定一个执行 lane；搜索和留出集不重叠。候选预热及
  每轮测量都精确比较完整 model/answers/usage；另一进程复验最终留出集。
- 修复请求语料摘要可能丢失 criteria 顺序的问题：请求使用保序摘要；配置仍使用
  规范摘要。只要原生编码可能变化，就不能当成同一个请求集合。
- 修复档案复用只检查通过标记的漏洞：必须有独立原生参考覆盖、完整轮次、留出集、
  零输出差异、内部零 mismatch、实际图重放和未超预算的缓存/分配器峰值证据。
- 在 CPU 测试中注入留出集错误，验证不覆盖旧档案；在已启动的真实 HTTP 服务上
  取消调优，验证 owner 关闭、线程退出、旧档案保留。
- GPU 准入只检查指定卡，繁忙则拒绝；服务沿用进程/GPU/端口锁。取消只清理本次
  owner。文件、字节、请求 worker、候选数、时间、日志和历史目录均有明确上限。
- `memory_limit_mib` 约束 Torch 分配器，额外保留设备余量；不冒充第三方分配或
  进程 RSS 的硬限制。调优档案使用者可写，摘要用于完整性校验，不是数字签名。

## pplx / 通用图后端的执行改动

捕获前的独立原生参考移入现有共享 reader；捕获、淘汰及释放仍独占。两个 lane
同时需要捕获时，先完成并同步各自原生输出，再排空 reader 捕获，避免不必要的
GPU 等待。CPU barrier 测试覆盖两 lane 的参考确实能重叠；JEV 的全局原生状态
没有套用这项改动。未删除结果同步、未改变精度和质量校验频率。

## CPU 验证

- 原有 13 个相关测试文件合计 **177 passed**；加入 Triton 并发保护和有界错误诊断后，
  14 个文件合计 **181 passed**，覆盖部署、四种后端、图缓存、服务副本、
  SystemOne、批量门槛和 journal；日志 `cpu-regression-v2.log`。
- 最终加入 AMOS 已知不安全宽度的准入与档案拒绝回归后，**183 passed**，
  日志 `cpu-regression-v3.log`。不能靠一轮碰巧成功的指标解除已知并发缺陷。
- CLI 输出补充实际调优证据后的定向回归见 `cli-regression.log`。
- 本轮开始前备份 243 个已有改动/未跟踪文件；中间核对没有删除文件，既有
  `systemone.py` 保持原始摘要。同期 Forge 文件的其他变化未被本轮覆盖；最终
  核对见文末保留审计。

## GPU 验证状态

四个模型各准备 512 个不同的真实 triage 输入，256 个搜索、256 个留出。
pplx / AMOS 分别来自自身完成的业务 journal；D3 / JEV 使用同一批真实业务题目，
各自重新建立原生参考，不能把模型之间的标签差异当成优化误差。

本轮 GPU 0/1 曾被其他图像任务占用，等待其自然释放后开始实验。使用现有模型
解释器只读接入；没有安装包、重启原有服务或占用 GPU 2/3。

### 最终版本 v3

源码摘要 `6c9e377492ff60f640b679f240e2b2595b7a361754410f695494accdf0334876`。
因复核发现已知不安全配置仍可能偶尔通过有限测试，AMOS 配方现在只准入 1/2-stream；
候选生成和冻结档案校验均拒绝 3/4-stream，不能仅靠吞吐排序跳过此边界。
此前两个 v2 任务由本次 owner 正常取消，原有服务未被停止。
四个模型使用新 revision 重新准备和验证，不拿 v2 档案冒充最终源码验收。
最终调优和真实 Dataset 验收均通过。汇总见
[最终数据](../../_demiflow/model-autotune-20261103/final-results.json)。

| 模型 | 候选数 | streams / 在途数 | 图缓存上限 | 原生搜索吞吐，两轮 | 独立留出吞吐 | 留出 p95 |
| --- | --- | --- | --- | --- | --- | --- |
| pplx | 13 | 2 / 4 | 4 GiB | 10.85 / 10.73 条/秒 | 22.52 条/秒 | 216.6 ms |
| AMOS | 9 | 2 / 4 | 2 GiB | 10.98 / 10.85 条/秒 | 23.35 条/秒 | 198.1 ms |
| D3 | 5 | 1 / 2 | 4 GiB | 5.81 / 5.76 条/秒 | 12.86 条/秒 | 251.6 ms |
| JEV | 5 | 1 / 2 | 4 GiB | 14.64 / 14.87 条/秒 | 27.18 条/秒 | 112.6 ms |

每个模型均有 256 条独立留出，完整 `model/answers/usage` 零差异。四个模型的
512 条原生输出摘要也与旧验证版本一致，见
[跨版本原生对照](../../_demiflow/model-autotune-20261103/native-baselines-final.json)；
JEV 还覆盖了 GPU 0 → GPU 1 的跨卡比较。

均为单卡单副本、跨请求 batch=false。D3 / JEV 的 1–4 streams 都通过本轮质量
验证，但有限测量中单流的稳定吞吐与尾延迟更合适；并行能力保留，不强制选择更多
streams。D3 / JEV 仅搜索原生及四种宽度，pplx / AMOS 还覆盖排队深度、CPU
线程及缓存策略。有限候选不是所有参数或长期流量下的全局最优证明。

四个模型分别通过真实 `map_prompt_async` + SQLite journal + checkpoint 的
200 条验证，输出与 HTTP 完整结果一致。模型退出后禁止 service bind，200 条
离线重放成功；每个 journal 都是 200 requests / 200 responses / 0 uncertain。
四个 `doctor` 均报告 `native-and-holdout-passed`；AMOS 实际重复 tune 命令返回
`reused=true`，没有再次加载模型。

| 模型（端到端报告） | 首次 ready 前 | ready 后至 Dataset 完成¹ | 冷运行总耗时 | 预热 HTTP GPU 平均 / 最低² | 该窗口显存峰值 |
| --- | --- | --- | --- | --- | --- |
| [pplx](../../_demiflow/model-autotune-20261103/pplx/e2e-final/report.json) | 67.53 s | 14.99 s | 82.52 s | 94.25% / 9% | 53,549 MiB |
| [AMOS](../../_demiflow/model-autotune-20261103/amos/e2e-final/report.json) | 64.01 s | 17.38 s | 81.39 s | 95.83% / 39% | 55,683 MiB |
| [D3](../../_demiflow/model-autotune-20261103/d3/e2e-final/report.json) | 70.41 s | 23.18 s | 93.59 s | 91.49% / 47% | 54,879 MiB |
| [JEV](../../_demiflow/model-autotune-20261103/autotrust-jev/e2e-final/report.json) | 120.55 s | 12.57 s | 133.12 s | 92.84% / 41% | 66,580 MiB |

¹ 含冷图、算子、journal、checkpoint 和服务退出，不等于纯 GPU 推理耗时。
² 是预热后连续三轮 200 条 HTTP 的短窗口 NVML 样本，样本数分别 24 / 23 / 41 / 19；
不是整段冷 Dataset 运行利用率，也不是峰值算力效率，不能宣称持续 100%。原始采样
位于各模型的 `e2e-final-gpu.json`。冷启动仍有明显成本，持续服务可使用既有
`lifetime: persistent` 生命周期；本轮隔离测试统一使用 action 生命周期。

### 本轮 pplx 新增收益的口径

同一最终源码和搜索集，固定 2 streams / 4 在途 / CPU 8 / 捕获阈值 8 的两轮为
22.05 / 22.14 条/秒，p95 354.54 / 292.96 ms；自动选中相同宽度、捕获阈值 2，
两轮为 22.69 / 23.43 条/秒，p95 222.82 / 171.64 ms。较慢轮吞吐提升约 2.9%，
较差轮 p95 降低约 37.2%。四流吞吐优势在 2% 以内，但尾延迟更高，因此未选。
这才是本轮参数选择相对固定配置的搜索阶段对照；上表原生/优化之差不能冒充相对
上一版 optimized 再次获得同等倍数提升。

独立端到端验证没有完全复现搜索阶段的尾延迟收益。先完成固定参数控制组，再在
GPU 0/1 没有其他实验负载时复验自动配置，其他执行参数相同，只有捕获阈值 8 → 2。
两组各 200 条完整响应一致，journal/replay 均通过。见
[隔离端到端对照](../../_demiflow/model-autotune-20261103/pplx-end-to-end-comparison-isolated.json)：

| 指标 | 固定参数 | 自动配置 |
| --- | --- | --- |
| ready 后 200 条 Dataset（含冷图与退出） | 16.04 s | 14.40 s |
| 预热 HTTP 三轮吞吐中位数 | 23.30 条/秒 | 23.97 条/秒 |
| 三轮中最差 p95 | 211.21 ms | 273.48 ms |
| 模型启动 | 39.77 s | 28.69 s |
| 冷运行总耗时 | 55.81 s | 43.09 s |

ready 后耗时减少约 10.2%（对应吞吐约 +11.4%），预热吞吐中位数约 +2.9%；
这里只是一组顺序对照，不能作为统计置信结论。启动时间存在较大缓存/运行条件波动，
不能把总耗时减少都归因于捕获策略。最差 p95 在复验中反而增大，因此不宣称尾延迟
已稳定改善。首次非隔离对照保留在 `pplx-end-to-end-comparison.json`，没有删除不利结果。

[pplx 隔离运行报告](../../_demiflow/model-autotune-20261103/pplx/e2e-isolated/report.json)
的约 26 秒预热窗口有 23 个 NVML 样本，平均 **98.87%**、最低 **92%**，显存峰值
53,421 MiB。它说明这个案例在持续供给下接近打满，仍不等于所有时刻 100%，也不
代表冷启动和任意业务前后处理都已消除。

## Triton 竞争定位与修复复核

AMOS 初次多 stream 候选出现 CUDA illegal memory access。仅恢复本轮改动前的
`NativeGraphAdapter.call` 仍失败；`CUDA_LAUNCH_BLOCKING=1` 和只清理当前 stream
cuBLAS workspace 也没有解决，不能归因于本轮捕获锁调整或宣称同步已经修好。

进一步诊断捕获到 FLA l2norm 调用 Triton 3.8 `Autotuner.run` 时的具体异常：
`full_nargs = {**self.nargs, ...}` 读取到另一线程清空的 `None`。源码中 `nargs`
是内核实例共享的临时参数，调用结束写回 `None`，原实现没有线程隔离。

`triton_dispatch.py` 在多 stream worker 接单前安装实例级 CPU 提交锁，生命周期
随内核对象和 worker，不加 GPU 同步，不锁整个 forward。CPU 测试验证共享参数
隔离、不同内核提交重叠、异步完成仍 pending 时提交能返回、异常释放及幂等安装。
Triton/FLA 源码进入准备身份。独立原生基线仍是单 lane 且不安装此保护。

诊断模式连续 2,048 次并发请求无崩溃只作为稳定性线索，不算独立质量验收；正式
v2 调优的 AMOS 2-stream 候选已通过完整原生输出比较；3/4-stream 仍能触发 CUDA
非法地址，v3 已从配方准入中排除这两种宽度。进一步诊断在关闭 CUDA Graph、直接执行适配后
原生模型的 4-stream 路径上也复现故障，因此不能将剩余问题只归因于图捕获。
剩余非法地址尚未定位到具体 GPU 内核，不能宣称 Triton 参数修复解决了全部根因。

恢复旧捕获流程、局部 cuBLAS workspace 清理、关闭 FLA tensor cache、JIT 提交锁、
关闭候选图内核和串行化 decoder 原生阶段等诊断均未可靠消除宽并行故障；没有把
这些猜测性补丁并入平台。诊断代码只留在实验目录，不修改外部包或生产模型实现。
最终 v3 的双流配置通过新进程留出集与真实 Dataset 回归，保留单卡并行。

## pplx 捕获调整的独立对照

v1 源码 ABBA 顺序、每次 512 冷请求 + 512 热请求，共 4,096 次完整响应与原生
输出一致。原实现冷吞吐 17.98 / 16.85，新实现 17.63 / 17.68；热吞吐分别
22.95 / 22.25 和 23.40 / 23.42 条/秒。存在轮次/缓存波动，不宣称冷阶段显著
提速，热阶段的小幅差异也不能代表整条业务性能。报告 `pplx/capture-ab-v1/report.json`。
独立 8 请求 profiler 片段 GPU 活跃约 98.1%，仅用于定位，不能冒充持续 NVML 利用率。
没有引入尚未证明收益的 pinned-memory 流水或 Rust 实现。

## 工作区保留与交付边界

[保留审计](../../_demiflow/model-autotune-20261103/preservation-audit.json) 核对了起始
快照中的 243 个文件：没有删除，HEAD 未变，`systemone.py` 摘要未变。本轮对既有
文件的修改限于审计中的 11 个服务集成、文档及测试文件，另新增调优、Triton 保护、
相应测试和设计/复核文档。同期 Forge 和 `execution/run_journal.py` 的其他改动未被
覆盖；后者改动为 Forge `execution_view` 读取投影，不在本轮普通 Dataset 测量路径。
没有 commit/push，没有修改共享模型安装包或业务 prompt，没有停止原有服务。
[最终清理检查](../../_demiflow/model-autotune-20261103/cleanup-final.json) 确认本次模型
worker 全部退出，检查时 GPU 0/1 无计算进程。

本轮完成显式部署调优和四模型验收；已知剩余边界包括 AMOS 3/4-stream 的底层
非法地址、较大的模型冷启动成本、有限样本下的性能不确定性，以及持续 100% GPU
利用率尚未实现。新后端仍需提供原生参考、状态隔离和质量配方；普通 vLLM profile
及多副本调度未被冒充为已支持自动调优。
