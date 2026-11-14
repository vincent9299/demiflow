# L-workers组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

Local与Ray的graph prefetch两个调用点都改；worker load/assignment/code pin不得绕过。L034/L035用有限异步ACK等待，错或迟到回执不推进新cut。progress类文件归R，按C13消费。queue纯转换worker继续零提交权。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [L017](../tasks/L017.md) | `demiflow/execution/graph.py:run_local_graph` | implement |
| [L018](../tasks/L018.md) | `demiflow/execution/graph.py:run_ray_graph` | implement |
| [L019](../tasks/L019.md) | `demiflow/execution/queue_chain.py:run_local_queue_chain` | verify_preserve |
| [L020](../tasks/L020.md) | `demiflow/execution/worker/layout.py:load_worker_layout` | implement |
| [L021](../tasks/L021.md) | `demiflow/execution/worker/execution.py:WorkerChainExecutor._load_plan` | verify_preserve |
| [L022](../tasks/L022.md) | `demiflow/execution/worker/execution.py:WorkerChainExecutor.start` | verify_preserve |
| [L023](../tasks/L023.md) | `demiflow/execution/worker/execution.py:WorkerChainExecutor._run` | verify_preserve |
| [L024](../tasks/L024.md) | `demiflow/execution/worker/graph_execution.py:GraphWorkerExecutor._load_plan` | verify_preserve |
| [L025](../tasks/L025.md) | `demiflow/execution/worker/graph_execution.py:GraphWorkerExecutor.start` | verify_preserve |
| [L026](../tasks/L026.md) | `demiflow/execution/worker/graph_execution.py:GraphWorkerExecutor._run` | verify_preserve |
| [L027](../tasks/L027.md) | `demiflow/execution/worker/queue_transform.py:QueueTransformExecutor._load_plan` | verify_preserve |
| [L028](../tasks/L028.md) | `demiflow/execution/worker/queue_transform.py:QueueTransformExecutor.start` | verify_preserve |
| [L029](../tasks/L029.md) | `demiflow/execution/worker/queue_transform.py:QueueTransformExecutor._run` | verify_preserve |
| [L034](../tasks/L034.md) | `demiflow/execution/worker/execution.py:WorkerChainExecutor._request_commit` | implement |
| [L035](../tasks/L035.md) | `demiflow/execution/worker/graph_execution.py:GraphWorkerExecutor._request_contribution` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：L-chain, L-contract, Q, R。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：BOUNDS, GRAPH, HISTORY, LAYOUT, QUEUE, RAY, RECOVERY。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `L-workers.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
