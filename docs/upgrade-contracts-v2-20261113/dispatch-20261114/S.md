# S组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

保留factory构造前豁免，构造后真实实例必须判状态；与operator身份/共享所有权同源。不能因无钩子直接判无状态或整体回退E。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [S001](../tasks/S001.md) | `demiflow/execution/operator_policy.py:classify_operator_state` | implement |
| [S002](../tasks/S002.md) | `demiflow/execution/operator_policy.py:snapshot_auto_state` | implement |
| [S003](../tasks/S003.md) | `demiflow/execution/operator_policy.py:restore_auto_state` | implement |
| [S004](../tasks/S004.md) | `demiflow/execution/operator_precheck.py:precheck_operator_state` | implement |
| [S005](../tasks/S005.md) | `demiflow/execution/operator_precheck.py:precheck_plan_state` | implement |
| [S006](../tasks/S006.md) | `demiflow/execution/operator_state.py:OperatorStates.validate` | implement |
| [S007](../tasks/S007.md) | `demiflow/execution/operator_state.py:OperatorStates.register` | implement |
| [S008](../tasks/S008.md) | `demiflow/execution/operator_state.py:OperatorStates.snapshot` | implement |
| [S009](../tasks/S009.md) | `demiflow/execution/contracts/operator.py:_adapter_present` | implement |
| [S010](../tasks/S010.md) | `demiflow/execution/contracts/operator.py:compile_operator_spec` | implement |
| [S011](../tasks/S011.md) | `demiflow/planning/traits.py:unified_operation_traits` | implement |
| [S012](../tasks/S012.md) | `demiflow/execution/stream_map.py:_MapWorker.prepare` | implement |
| [S013](../tasks/S013.md) | `demiflow/execution/recovery_identity.py:describe` | implement |
| [S014](../tasks/S014.md) | `demiflow/lance/search.py:VectorSearch.snapshot_state` | implement |
| [S015](../tasks/S015.md) | `demiflow/lance/search.py:VectorSearch.restore_state` | implement |
| [S016](../tasks/S016.md) | `demiflow/execution/operator_state.py:OperatorStates.__init__` | implement |

## 工作节奏（真人最新决定）

直接完成本组代码和自测，中途不安排GPT/Qwen Review，也不设置等外部批准的阶段。当前未冻结的组内细节由本组在既定边界内自主闭合；真正无法继续的跨组接口阻塞才报告，非阻塞事项随最后交付汇总。

全部12组交代码后，GPT合流，GPT与Qwen集中Review，GPT统一修改和复验。不要把交付自测等同于整体通过；V组的组合测试可以随代码交付待集成执行，不得因依赖未合流放宽断言。保持原任务范围和主仓/历史保护。

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：无新增跨组调用。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：ADMISSION, BOUNDS, DELIVERY, HISTORY, IDENTITY, LAYOUT, RECOVERY, STATE, VECTOR。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `S.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；全部交付后GPT与Qwen集中Review，GPT统一修复并按真实组合证据验收。
