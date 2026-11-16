# I08：同一 cut 的纯算子状态物理打包

生产提交：`89a8b83` + `aa5d650`；基线：`3f348e0`。隔离树：
`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_perf_mainpath_20261116`，
分支 `engine/gpt-v3-perf-mainpath-20261116`。本报告不宣称 I08 总性能门完成。

## 结果

原 20,000 输入 → 10,000 输出、5 个逻辑节点、1 个物理 stage 的真实 Dataset 流水线，
使用相同预算、同一个已修正 harness、相同实际文件系统、交替次序运行 5 对。
最终生产版本的中位耗时 **14.4325s → 13.2541s，下降 8.16%**；5 对均更快。
基线范围 13.8574–14.5719s，候选范围 13.1226–13.9894s，仍有测量波动。
前一轮候选测到 4.86%；不要把本轮 8.16% 宣称为所有环境中的固定收益。

| 实际事实，每一轮 | 基线 | 候选 |
|---|---:|---:|
| 原 journal ACK | 40 | 40 |
| 输出行 | 10,000 | 10,000 |
| 原 store 的物理 publication | 416 | 336 |
| 独立 kernel image publication | 120 | 0 |
| 同 cut kernel pack publication | 0 | 40 |
| 退出后原资源池 active / charged | 0 / 0 | 0 / 0 |
| 退出后仍存活的已观测子进程 | 0 | 0 |

输入、输出 sum256/xor256 每轮一致；`checkpoint_rows=512`、checkpoint 时间策略、CPU、
内存、行/批/状态/临时磁盘上限均未改变。10 份 trial 的真实文件系统核验返回 `errors=[]`。
性能比较的生产 HEAD 分别为 `3f348e0`、`aa5d650`，每轮测试前后源码摘要一致且生产树干净。

采样峰值的五轮中位数：RSS 143,474,688 → 143,114,240 bytes；磁盘逻辑字节
5,477,759 → 5,409,852；文件数 547 → 479；FD 26 → 25；进程数均为 2。
这些是有采样间隔的观测值，不能代替 RSS、物理复制次数或瞬时磁盘峰值的硬保证。

## 改动与边界

`dataflow_kernel_pack.py` 将最多 8 个相邻、已排空、纯且无状态的同步 `OperatorKernel`
原 image 放在一个有版本的原 store 对象中。每个成员保留原字节、SHA-256、occurrence、
partition、长度；`IMAGE_VERSION`、image 内容及其恢复验证不变。
状态 actor、native、缓存 wrapper、异步生命周期、并发调用、batch、ports、groups、
聚合与尚在 finish 的算子继续使用原独立 image。

第一次逐 image 编码只留下最多 8 个有界描述符；第二次逐 image 编码、核对原摘要后流式
送入原 `RunArtifactStore.put`。期间不执行用户 snapshot，不拼出整包 bytes。
`RunArtifactStore` 在推进 chunk 迭代器前释放旧 chunk 和 memoryview，避免构造下一 image
时仍保留上一块。独立原生 store 对照探针证明：旧 chunk 的引用数由 4 降为 2（仅生成器
自身与 `getrefcount` 参数），两侧均写入并计费 80,000 bytes。

包装上限为原 state/4、control/4、resident/64、batch/4 和 64 KiB 中的最小值；
构造前还要求原 state ≥ 1 MiB、resident ≥ 1 MiB、batch ≥ 256 KiB、control ≥ 16 KiB。
少于两个 image 或窗口不足就直接使用原路径。原物理规划已为每个纯算子预留一个输入
batch 窗口；runner 在同步 push/poll 返回、队列排空并完成输出确认后才调用 snapshot。
至少两个原窗口承载一个 metadata 编码工作区及有界描述符；恢复时先耗尽并关闭流，再
解码所选成员，读缓冲与解码峰值不会整体叠加。这没有新增 allocator、grant 或额度。

磁盘仍在写前经过原 begin/grow 准入，以及 prepare、publish/fsync、settle。
仅明确的初始 `artifact_staging_bytes` 不准入可回退原分件路径；开始写入后的异常、
未知 prepare/publication/settlement 回执原样传播，保留原计费与租约恢复依据。
少 80 次 put，省去了这 80 次原 begin/prepare/settle 路径；没有改变 ACK 或 fsync 规则。

统一读取函数覆盖 kernel restore、提交前恢复检查、durable residency 和产品 callback
观测恢复。它验证包版本、成员数量/尺寸、重复键、每个摘要、尾部、整个原对象 SHA，
选中成员的原 image 身份也必须匹配 occurrence/partition。旧单 image 原样读取。
GC 遍历全部成员中的原引用；包括 actor snapshot、pending、buffer payload 的引用测试。
后一个成员损坏时，不会因前面的成员已读完就授权 GC。实际 actor 不进入本次 pack，
因此其独立 `restore_actor_state` 读取协议未被换成新格式。

## 验收证据

当前固定生产 HEAD 的 **40 项通过**：18 项新增 pack 验收 + 22 项原 kernel 测试。
命令、三个私有域、私有 TMPDIR、测试与生产源码摘要均在
`/tmp/demiflow-perf-pack-final-tests/verification.json`，日志为同目录 `tests.log`。

新增反例包括：成员路由不匹配、包头冒充另一原 image、晚到成员摘要损坏、截断、尾部、
版本/重复成员、两遍编码期间状态变化、有限状态/输入窗口回退、总磁盘及剩余额度回退、
未知发布不退款、GC 嵌套引用保护、部分 trace 不授权回收、上一块实际引用释放。
两条真实 Dataset 测试在原 ACK 后中断，分别从 packed image 与旧独立 image 恢复，
继续完成相同输出，原提交前缀不改，后续新 cut 使用 pack。

额外相关验证保留于 `/tmp/demiflow-perf-pack-tests-r2/` 与 `...-r4/`：原 artifact
计费、5 个真实进程崩溃窗口、GC、callback 观测、durable residency。r2 当时的唯一红项是
新增紧额度测试，发现 store 会收紧 max_bytes；已增加打包前的原 spill 上限判断，且
当前 18 项全部重验通过。r4 为 31 passed / 1 skipped；跳过项是当前 `env` 无 Ray 包的
durable residency Ray 用例，没有把该项计作通过。

正式五对结果：`/tmp/demiflow-perf-pack-comparison-r2/summary.json`；每轮 `trial.json`
包含实际命令、前后文件系统、原始计划/限制、40 ACK、输出摘要、资源释放和源码摘要。
同目录 `compare.py` 可复核执行顺序，`environment-verification.json` 为实际存储一致性核验。
本目录配套 JSON 记录这些输入的 SHA-256 和承重七件字节冻结核验。

本次没有把同机 Local 证据扩展为 Ray/多机收益，没有打包有状态 actor 或 native pending，
没有减少公共 Record 验证、跨 UDF 复用可变数据、修改 checkpoint 频率，也没有解决剩余
Record/编码 CPU 成本。相对旧引擎的全场景 I08 门仍需继续推进。
