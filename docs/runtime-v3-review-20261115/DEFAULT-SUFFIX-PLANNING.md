# 默认窄流水线：按实际 worker 预算选择已有安全后缀布局

隔离基线 `d2cf631`（合入 root `c05a71e`）；本包只修改 `planning/dataflow_thread_fusion.py`、`planning/dataflow_place.py::compile_physical_dataflow`，新增定向测试和本报告。主仓、业务断言、原测试预算、运行时 kernel/runner、承重七件均未改。

## 缺陷与修复

原 31 行 `range→map→flat_map→filter→map_batches→limit→take_all` 默认最多 4 个 worker，却保留 7 个独立 stage，被迫选 6 条 durable_incremental 交接逐阶段停放。已有安全后缀融合对普通任务只在 `workers==2` 启用，导致默认值反而慢。修前本树只读编译实测 `original_stages=7, default_workers=4, selected_stages=7`；未为重复计时再跑一次已知约 92 秒的旧完整案例。

现在同一条纯 CPU 线性图只要超过有限 worker 预算（至少 2 个），即可选择“独立 source + 原逻辑后缀”的既有布局。每个逻辑 kernel、任务批次、状态、定时器和恢复身份保持；物理合并 grant 仍是原 CPU、内存、状态/输出窗口的和。源不融合，防止阻塞读取饿死计时输出。

新规则只覆盖一源一终端、无分叉/多输入的纯 CPU 图；旧 workers=2 共享 DAG、prepend/async 特例保持原规则。明确 GPU、isolation、limits/custom resources 不被吞掉，终端自身的显式约束也检查。融合必须在某一个真实 host 的 CPU/内存容量内，集群合计资源不能冒充单 host；不合适就保留原 independently placeable stages。现有 Ray Core 不启用新增 Local 优化，其原合法多 host 规划不被改变。

`hosts` 由原 `compile_physical_dataflow` 传入，不增加 allocator/账本，不修改旧封存计划。

## 实际验收

- 原 `tests/test_local_kernel.py::test_narrow_chain_batch_boundaries_and_limit_match_serial` 未改，**5.357 秒通过**。精确 batch 边界、limit 与 thread 路线对照保持。
- 新默认公开案例未设置 workers workaround，真实原 RunJournal ACK；封存图仍 7 个逻辑节点，但物理 **2 个 stage、2 次真实 worker 分配**，全部交接 memory_incremental。两次独立执行耗时约 3.7 / 5.5 秒（仅报告观测，不把并行机器的墙钟当固定性能契约）。原完整 plan 已导出 `/tmp/demiflow-default-suffix-physical.json`，摘要与原 run 路径见 JSON。
- 编译矩阵 workers=2/3/4/6、process/thread 均保持源独立及原逻辑顺序，CPU/内存/state/kernel refs 精确求和。预算足够或 fusion=False 不强制更改布局。
- 多 host 单机 CPU/内存不足、显式 isolation、GPU、分叉共享源、Ray policy 均保留合法独立 stage。
- 原共享 prepend 只读源一次、无限 main 不预物化、batch/save/embedding 首批早于 EOI、任务类/闭包/方法、显式 stage/worker scope、flat_map/filter 任务边界、单 worker timer 禁止与源同驻、finite fusion 保护共 **17 项原测试通过**。
- 新真实 actor 例使用默认 4 worker 和显式 3 行任务批次：原计数 `[1,2,3,1,2,3,1]`，真实 3 次构造/关闭、7 行各调用一次；封存物理布局仍 2 stage。该新测试的行/批窗口声明为 64 KiB，状态预算保留默认，不冒称默认宽窗口下所有有状态链均能共驻。

最新有效集合 **30 passed、0 failed、0 skipped**：13 项新用例 + 17 项原保护。不是伪称同一次 30 全绿；原始 XML 和中间新夹具错误全部保留在 JSON：

- `/tmp/demiflow-default-suffix-r2.xml`：27 passed / 1 failed；唯一失败是新测试错误构造了契约不允许的 Ray+thread policy，后来改为真实 Ray policy 边界。
- `/tmp/demiflow-default-suffix-r4.xml`：12 passed / 1 failed；唯一失败是新 actor 夹具误把总 checkpoint 状态预算设成 4 KiB，尚未测试到任务语义。改为声明行/批 64 KiB、保留原默认状态预算。
- `/tmp/demiflow-default-suffix-actor.xml`：修正的新 actor 专项 1 passed。
- 早期 `red/r1/r3` 另有新增测试参数/手算答案错误；未修改任何原业务断言以过关。历史无效 Ray nodeid 的替代关系单独记录，旧失败仍可查。

固定生产代码验证命令统一为 `PYTHONPATH=$PWD:$PWD/tests DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-default-suffix-<round>-capacity /yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest ... --basetemp=/tmp/demiflow-default-suffix-<round> --junitxml=/tmp/demiflow-default-suffix-<round>.xml`。R2 包含上述原节点和新测试文件；R4 重验完整新文件；actor 为单个新增 actor nodeid。运行期间未改 Python 或 HEAD。

范围限制：这是默认规划选择的具体性能退步修复，不是全局性能门、真实多机/GPU验收或任意 DAG 最优融合证明。没有扩大共享/分叉融合范围，也没有通过提高 worker/内存预算实现提速。
