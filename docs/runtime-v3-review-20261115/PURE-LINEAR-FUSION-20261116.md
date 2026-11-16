# 受原 worker 上限约束的纯同步线性合驻

最终生产候选 `7159715`，独立树 `engine/gpt-v3-residency-20261115`。不宣称 I08 性能已达到原意图：旧 fast_cpu 参考约 1.87s，前候选约 26–27s，本轮首诊断约 18.75s，仍有明显差距。

## 原问题与取舍

原 fast_cpu：20,000 输入 → 10,000 输出，算术 map → filter → projection → iter_rows，单源、单终端、单分区，4 worker 是上限。原规划已将 CPU 后缀合到一个 GraphRunner；source 与后缀间仍有 20,000 条数据传输、驱动中转、编码和信用握手。共享 JSON header 实验之前已因无整链收益撤回；本次直接消除不必要的物理边界。

自动融合不是所有 CPU 工作负载的最优选择。一个源本身也可能有 CPU 开销，源与 CPU 后缀重叠执行有时会更好；不能猜 UDF 成本。总控裁定采用保守范围：

- 显式 `max_active_workers=1` 保留原支持。
- 新默认规则只在原 stage 分区数超过实际 worker 上限时考虑合驻；资源足够的短链、不设上限的链保持原布局。
- 仅整个 DAG 是单源、单终端、单分区的纯同步线性链，没有共享、分支、多输入。
- 原 `none` state / `pure` effect / mergeable incremental CPU 算子；async/streaming_map、timer/flush_interval、GPU/其他资源、显式 limits/requirements/isolation、`fusion=False` 全部拒绝自动合驻。
- 单 host 的 CPU 与内存必须真实可容纳；CPU 同线程取最大请求，内存窗口仍求和。多 host 的资源总和不作为单 worker 可以放下的依据。

## 实现

`planning/dataflow_fusion.py::fuse_single_worker_pipeline(..., hosts=None)` 复用原合驻逻辑与原 StageSpec；`planning/dataflow_place.py` 的原编译调用传入已验证 hosts。没有另建执行器或调度器，逻辑 occurrence/边、任务与 batch 声明、原资源准入和所有 ACK/提交/恢复仍使用同一 GraphRunner。

现有两个 `plain_iterator_map_early_close` 的物理布局断言经总控明确裁定迁移为 1 stage/1 allocation，**原 take(1) 结果、源读取 1..25 条、实际 CPU/内存 grant 保持**。逻辑节点全部仍可见。新真实阻塞源测试覆盖 process/thread × async/timer：源必须等用户读到第一条才能继续，第一条在源 EOI 前确实到达，且源保留独立 stage。没有用只检查配置替代实际首条进展。

另锁定：已经封存的原 2 stage 布局在新优化器下恢复时仍按原计划执行，plan_id、物理 DTO 和既有 ACK 历史完全保留。测试在同一平台字节下选择旧布局后恢复；不是宣称跨平台代码版本可绕过原 CODE_MISMATCH 守卫。

## 证据

宽候选初始功能集合：59 passed / 102.47s，日志 `/tmp/demiflow-pure-fusion-r2/`。一次 GPU 负例 fixture 错用了不受支持的 `gpu` 顶层资源字段，纠正为原 `accelerators=[{'type':'gpu','count':1}]` 后该反例通过；未改生产消红。

首个原参数诊断：`/tmp/demiflow-perf-pure-linear-r1/fast_cpu/verification.json` / `trial.json`，18.747s，20,000 → 10,000 原结果 sum256/xor256 正确，源码前后摘要一致；原 journal 只有 1 个真实 allocation，CPU 1000 / memory 1638400，CONFIRMED，40 个 cut 全部 acked。不是强制修改 workers=1，封存 policy 仍为 max_active_workers=4。

该初诊断使用收窄前候选，不能冒称最终字节证明。最终 `7159715` 同字节 **62 passed / 106.08s**，完整命令/JUnit/全部生产与测试前后摘要位于 `/tmp/demiflow-pure-fusion-fixed-r1/`；快 CPU 原参数最终单次 **17.49593021s**，10,000 行 sum/xor 正确、source_before=after，原 journal 1 个 CONFIRMED worker、40 个 ACK；证据 `/tmp/demiflow-perf-pure-linear-r2/fast_cpu/`。七个冻结件在固定验收目录 `frozen-seven.json` 与362d8528逐字相等。同一 `7159715` 在真实私有 Ray（4 CPU / 0 GPU / 128MiB object store）按原 fast_cpu 参数通过：**25.581888502s**，20,000 → 10,000、sum/xor 与 Local 一致，原 journal 1 个 CONFIRMED allocation / 40 个 ACK；实际 actor PID/birth、完整 allocation 集合与环境由原观测器核验。`fast_cpu/trial.json` 的源码前后摘要一致、returncode=0，原 actor/子进程全部退出；`cluster.json` 显示 children=[]、ray_initialized_after=false。证据目录 `/tmp/demiflow-perf-pure-linear-ray-r1/`。这是单次真实运行，不是五轮性能通过。

剩余问题必须分开：performance lane 的 **V012 Ray 部分资源公平场景仍在原120s失败**（48 worker/0输出）；本纯链结果不覆盖该共享分支/停驻路径。其导出位于 `/tmp/demiflow-resource-final-ray-evidence/ray_core/fairness_deadlock/partial-slots/`。后续单独修 checkpoint 与阶段退租边界，不能把这里的 Ray green 当作全引擎 green。
