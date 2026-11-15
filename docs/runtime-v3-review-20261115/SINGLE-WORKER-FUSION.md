# 单 worker 的真实物理融合

用户明确只要一个 worker 时，能在同一额度内驻留的纯线性 source → 同步无状态算子 → 读取/物化 terminal 可由原 WorkerGraphRunner 一起执行。CPU/GPU、模型效果、带状态、异步、多输入与分支不因此合并。请求内存为各节点工作集之和，CPU 为实际串行最大请求；仍保留控制预留，由原 ResourceManager 分配。

逻辑节点、边与原封存身份解释保持独立；物理计划如实记录共同宿主。每条原边仍在 sealed exchange 描述中，stage assignment 只装配外部边界，内部边直接调用原 kernel，避免原实现把同一内部通道作为输入/输出打开两次。恢复仍用原 cut/source/kernel/terminal image。

真实结果：300 行 source/map/take_all 用一次 allocation，每个逻辑节点有 300 行观测；checkpoint_rows=2 时已确认 cut 后中断，恢复使用同一 plan_id、原确认前缀及完整结果。两项共 7.47 秒（`/tmp/demiflow-fusion-r3.xml`）。这不是完整九类性能基准，也不是多机证明。

128 KiB 下三个默认 64 KiB 工作集不能共驻，仍采用原有持久交接，未为了性能放宽内存。之前用该预算测融合的测试被主动中断且不计通过（`/tmp/demiflow-fusion-integration-r1.log`）。此类小额度下重复启动的成本还需线程宿主与后续性能基准衡量。

组合证据：`/tmp/demiflow-fusion-integration-r2.xml` 有 50 passed、2 failed，失败是已定位的内部通道重复装配；修后上述两专项通过。接入空 typed schema 后，C/K/P、公开预算、空类型与原 durable 恢复组合 ` /tmp/demiflow-empty-fusion-r4.xml` 为 159 passed / 1 skipped（322.25 秒）。唯一跳过是当前 Python 无 Ray，该项未计通过，后续用独立 Ray 环境补跑。
