# R组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

本组同时拥有buffer未冻结协议的闭合和实现，详见EXECUTION-START。先交精确payload/参与者/EOF/兼容样本和可运行反例，再按函数装配；不要等另一轮全局文档互审。seal/ACK遵守C13，buffer不成为提交权威。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [R001](../tasks/R001.md) | `demiflow/execution/stage_buffers.py:StageBufferAdapter.__init__` | implement |
| [R002](../tasks/R002.md) | `demiflow/execution/stage_buffers.py:StageBufferAdapter.align` | implement |
| [R003](../tasks/R003.md) | `demiflow/execution/stage_buffers.py:StageBufferAdapter.restore` | implement |
| [R004](../tasks/R004.md) | `demiflow/execution/stage_buffers.py:StageBufferAdapter.close` | implement |
| [R005](../tasks/R005.md) | `demiflow/execution/stage_buffers.py:make_stage_buffer_adapter` | implement |
| [R006](../tasks/R006.md) | `demiflow/execution/stream_materialize.py:StreamStageProgress.bind_stages` | implement |
| [R007](../tasks/R007.md) | `demiflow/execution/stream_materialize.py:StreamStageProgress.align` | implement |
| [R008](../tasks/R008.md) | `demiflow/execution/stream_materialize.py:StreamStageProgress.restore_stages` | implement |
| [R009](../tasks/R009.md) | `demiflow/execution/stream_materialize.py:prepare_stream_blocks` | implement |
| [R010](../tasks/R010.md) | `demiflow/execution/stream.py:_Stage.__init__` | implement |
| [R011](../tasks/R011.md) | `demiflow/execution/stream.py:_materialize` | implement |
| [R012](../tasks/R012.md) | `demiflow/execution/stream.py:_arun` | implement |
| [R013](../tasks/R013.md) | `demiflow/execution/run_context.py:RunContext.action` | verify_preserve |
| [R014](../tasks/R014.md) | `demiflow/execution/run_context.py:create_target_absent_binding` | verify_preserve |
| [R015](../tasks/R015.md) | `demiflow/execution/stream.py:_make_worker` | implement |
| [R016](../tasks/R016.md) | `demiflow/execution/stream.py:run_stream` | verify_preserve |
| [R017](../tasks/R017.md) | `demiflow/execution/stream_barrier.py:_CheckpointBarrier.__init__` | implement |
| [R018](../tasks/R018.md) | `demiflow/execution/stream_barrier.py:_CheckpointBarrier.tokens` | implement |
| [R019](../tasks/R019.md) | `demiflow/execution/stream_barrier.py:_CheckpointBarrier.arrive` | implement |
| [R020](../tasks/R020.md) | `demiflow/execution/stream_barrier.py:_CheckpointBarrier.fail` | implement |
| [R021](../tasks/R021.md) | `demiflow/execution/stream_barrier.py:CheckpointControl.run` | verify_preserve |
| [R022](../tasks/R022.md) | `demiflow/execution/stream.py:_Stage.__slots__` | implement |
| [R023](../tasks/R023.md) | `demiflow/execution/stream_materialize.py:StreamMaterialization.seal` | implement |
| [R024](../tasks/R024.md) | `demiflow/execution/stream_materialize.py:StreamMaterialization._commit` | verify_preserve |
| [R025](../tasks/R025.md) | `demiflow/execution/stream_materialize.py:StreamMaterialization.finish` | verify_preserve |
| [R026](../tasks/R026.md) | `demiflow/execution/worker/progress.py:WorkerStreamProgress.seal` | implement |
| [R027](../tasks/R027.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress.seal` | implement |
| [R028](../tasks/R028.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress.request_cut` | verify_preserve |
| [R029](../tasks/R029.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress.note_driver_cut` | verify_preserve |
| [R030](../tasks/R030.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress.align_cut` | verify_preserve |
| [R031](../tasks/R031.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress._channel_cuts` | verify_preserve |
| [R032](../tasks/R032.md) | `demiflow/execution/worker/graph_progress.py:GraphWorkerProgress.note_contribution_acked` | verify_preserve |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：A, L-workers, M, S。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：BOUNDS, BUFFER, EMBED, GRAPH, HISTORY, LAYOUT, RECOVERY, STATE, UNKNOWN, VECTOR, WRITE。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `R.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
