# Q组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

优先signed-zero及tuple/bytes保真，再闭合预算、幂等预留与Q008联合credit。默认credit收紧与显式冲突拒绝区分；同一结果给Local/Ray两个真实调用点，不能扩大grant。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [Q001](../tasks/Q001.md) | `demiflow/execution/block_sink.py:_canonical_row` | implement |
| [Q002](../tasks/Q002.md) | `demiflow/execution/block_sink.py:BlockSink.append` | implement |
| [Q003](../tasks/Q003.md) | `demiflow/execution/block_sink.py:BlockSink.prepare` | verify_preserve |
| [Q004](../tasks/Q004.md) | `demiflow/execution/payload_limits.py:stream_limits` | implement |
| [Q005](../tasks/Q005.md) | `demiflow/execution/payload_limits.py:payload_scope` | implement |
| [Q006](../tasks/Q006.md) | `demiflow/execution/stream_resources.py:StreamResources.make_queue` | implement |
| [Q007](../tasks/Q007.md) | `demiflow/execution/reservation.py:derive_prefetch` | implement |
| [Q008](../tasks/Q008.md) | `demiflow/execution/reservation.py:resolve_prefetch_credit` | implement |

## 工作节奏（真人最新决定）

直接完成本组代码和自测，中途不安排GPT/Qwen Review，也不设置等外部批准的阶段。当前未冻结的组内细节由本组在既定边界内自主闭合；真正无法继续的跨组接口阻塞才报告，非阻塞事项随最后交付汇总。

全部12组交代码后，GPT合流，GPT与Qwen集中Review，GPT统一修改和复验。不要把交付自测等同于整体通过；V组的组合测试可以随代码交付待集成执行，不得因依赖未合流放宽断言。保持原任务范围和主仓/历史保护。

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：F。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：ADMISSION, BOUNDS, CODEC, GRAPH, RECOVERY, STREAM。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `Q.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；全部交付后GPT与Qwen集中Review，GPT统一修复并按真实组合证据验收。
