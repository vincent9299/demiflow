# 原生资源、模型与 GPU 的本轮集成验收

产品以真实 Dataset 的计划、运行、结果和历史为依据；引擎复用唯一 RunJournal、CommitCoordinator、资源池和 ACK。本轮修复与验证服务于这两条原始意图，不把测试数量换算成完成百分比。

隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`；分支：`engine/gpt-runtime-v3-review-20261115`。下表逐项写实际版本，不将不同版本冒称一次完整终验。回执、原始 XML 和摘要见 [RESOURCE-AND-GPU-PRIMARY-VERIFICATION.json](RESOURCE-AND-GPU-PRIMARY-VERIFICATION.json)。

| 实际版本 | 实际执行 | 结果 |
| --- | --- | --- |
| `0712606` | 完整原 `test_embeddings.py`、`test_inference_execution.py`、`test_sqlite_journal.py` | 100 passed，339.72 秒；1856 输入摘要前后相同 |
| `ba4fd42` | Arrow 预分配、终端 metadata、原 Arrow callback 失败、正式工件安全门、Prompt 托管服务缓存/取消组合 | 18 passed，88.99 秒；1861 输入摘要前后相同 |
| `2f5fa42` | V011：真实 Local CPU→驻留 CUDA Linear→慢 IO→原 ACK | 1 passed，23.10 秒；1862 输入摘要前后相同 |
| `abeaa7b` | 同一 V011 业务，真实同机 Ray Core + 原停驻窗口 5 项反例 | 6 passed，29.17 秒；1863 输入摘要前后相同 |

## 生产修复及独立核对

Arrow 转换现在先调用原有有界准入，再分配输出，后检查保留。真实 typed-null 反例原先在 4096 字节批预算下分配 133184 字节；Lance 声明宽类型在 65536 字节预算下分配 2097216 字节。修复见 [RESOURCE-PREALLOCATION-20261116.md](RESOURCE-PREALLOCATION-20261116.md)，总控核 `100aca1` 的 1052 份输入摘要及 16 个原附件，全等，集成为 `f1046fb`。另核原 115 项相关回归 XML；这不是将其冒称本树同 HEAD 重跑。

终端原 grant 增补其本就同时驻留的 block/root metadata；payload/state/run 的上限不增加。新执行计划封存 W+M，旧封存没有该字段时保持原读取窗口。合法 4 KiB 批正例和超限前拒绝一起验证。

低预算停驻修复已集成为 `3ce2e06`。只读原信用表，在可用窗口内排空可重放的 finish，原 pause/cut、ACK、路由身份和原子 reserve 保持。最终同字节原公平案例 64×4KiB、1 worker、16MiB、120 秒上限完成 128 行；35 次原 worker 分配/ACK，65.09 秒，全部释放。总控核最终 1053/1230 份输入摘要全等；测试辅助文件按明确的 `938a78c` 核对。详细限制和 43 个不同测试覆盖见 [PARKED-WINDOW-REPAIR-20261116.md](PARKED-WINDOW-REPAIR-20261116.md)。这不表示轻量 CPU 性能差距消失。

V033 正式工件安全门已接真实公开 Dataset、原 worker/store/loader。14 个反例覆盖代码摘要、工件摘要、ZIP 路径/符号链接、域、租约、历史材料缺失和控制载体；不从当前主仓补历史。总控核 Local/Ray 各 2177 个输入与 14 个附件，全等；另在本树组合中重跑正式门。见 [ARTIFACT-SECURITY-CLOSURE.md](ARTIFACT-SECURITY-CLOSURE.md)。

## V011 的真实用户链路

公开 Dataset 从 64 行 4KiB 输入开始，经异步 CPU 准备、普通类工厂的驻留 `torch.nn.Linear(16,16)`、异步 IO，最后 `take_all()` 与原提交。GPU 使用真实 H100 原 UUID grant，未另造设备池或 worker。

CPU 准备第 2 行前等待 GPU 对第 1 行的真实计算证据：若两个阶段挤在同一个阻塞执行上下文，该用例不能推进。第一个 IO 暂时阻塞，观察真实背压；随后释放并验证全部 64 个数学结果。状态 hooks 只保存调用计数，固定权重由同一个代码固定的类工厂恢复，不把 CUDA 模型句柄写入快照。

Local 和 Ray 均观察到：

- 5 个原物理 stage，CPU 准备、GPU、IO 的实际 PID 各不相同。
- 同一个模型实例/同一个 GPU worker 完成 64 次真实 CUDA 调用，仅 1 份原 GPU allocation。
- IO 阻塞时 GPU 仅完成 2 行、CPU 准备 3 行。原输出 credit 为 1 行，连同两端有界 active/queue/pending 的保守窗口为 7 行，明确小于全部 64 行。
- 真实 worker 内原信用表采样 64 次，最大 4546 字节、1 个在途 lease；observer 先调用原 reserve，再只读检查，没有替换准入结果。
- 完整结果、3 个原 ACK、模型关闭、原 GPU allocation RELEASED；实际 GPU worker PID 已退出。Ray 集群只在独立子解释器中私有启动，随后 shutdown 并退出子解释器。

这是同机 Local/Ray 的实际驻留/并发证明。既有六卡、两宿主专项有各自固定版本，不能用本例冒充本轮跨机或硬显存隔离验收。

GPU 模型声明原 worker 主存预约 2 GiB、run 8 GiB；CUDA 库工作区单独观测，上界 64 MiB。这不声称原分配器能硬限制任意 CUDA 分配。初版夹具错误沿用了旧点乘小例的 1/8 MiB 张量阈值；独立运行同一个 Linear 证实其实际 cuBLAS 工作区约 32 MiB，随后修正新夹具的有限窗口，原引擎预算未改。初次硬件空闲前置失败、新夹具阈值失败、误选无 Torch 的 Ray 解释器均保留失败/NotRun 回执，未计入通过。

实际 Ray GPU 使用已存在的私有 `/tmp/demiflow-ray-physical-gpu-env-20261116/bin/python`，没有安装或修改共享环境；Local 使用原 `env/bin/python`。完整命令在原回执；物理 GPU 正式运行还设置 `DEMIFLOW_PHYSICAL_GPU=1`、`PYTORCH_NVML_BASED_CUDA_CHECK=1`，Ray 设置 `DEMIFLOW_V3_PROVIDER=ray_core`。三个 RESOURCE_ROOT/RESOURCE_HOME/FORGE_DOMAIN 均独立指向 `/tmp/demiflow-*`。

## 未关闭项

仍需完成轻量 CPU/跨机正式性能、完整物理 target/broadcast 恢复覆盖、最后同版本 Local/Ray/工作台和全回归。V012/V023 负责人正在将新增等宽 cast 共驻反例及最终停驻代码一起复验；V027 负责人已交实际 actor owner 损失、原 ACK 恢复和未知模型请求不重发，待本树独立核对集成。外部 PostgreSQL 仍未提供实际 DSN，不算通过。

本轮主仓只读核验：`e40a6dd`、原 11 项 dirty 状态、18 个原文件摘要保持；七份冻结文件与 `362d8528` 逐字相同。未在主仓运行测试、未 push/合主仓。该边界结论不覆盖前文已披露的早期共享目录/截图事故。
