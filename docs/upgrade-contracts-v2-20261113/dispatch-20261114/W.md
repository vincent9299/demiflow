# W组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

复用封存计划与身份桥，不创建第二账本。一个logical顶点内选择occurrence；扩真实HTTP传输字段，历史源码/状态按同run/attempt绑定。保留Notebook/Case/开发独立生命周期。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [W001](../tasks/W001.md) | `demiflow/workbench/runs/facts.py:read_sealed_plan` | implement |
| [W002](../tasks/W002.md) | `demiflow/workbench/runs/facts.py:read_identity_bridge` | implement |
| [W003](../tasks/W003.md) | `demiflow/workbench/runs/facts.py:join_node_observations` | implement |
| [W004](../tasks/W004.md) | `demiflow/workbench/runs/facts.py:overlay_sealed` | implement |
| [W005](../tasks/W005.md) | `demiflow/workbench/graph/model.py:get` | implement |
| [W006](../tasks/W006.md) | `demiflow/workbench/graph/model.py:bound_source` | implement |
| [W007](../tasks/W007.md) | `demiflow/workbench/runs/worker.py:execute` | verify_preserve |
| [W008](../tasks/W008.md) | `demiflow/workbench/runs/manager.py:prepare` | verify_preserve |
| [W009](../tasks/W009.md) | `demiflow/workbench/runs/manager.py:start` | verify_preserve |
| [W010](../tasks/W010.md) | `demiflow/workbench/runs/manager.py:show` | verify_preserve |
| [W011](../tasks/W011.md) | `demiflow/workbench/runs/manager.py:cancel` | verify_preserve |
| [W012](../tasks/W012.md) | `demiflow/workbench/runs/manager.py:resume` | verify_preserve |
| [W013](../tasks/W013.md) | `demiflow/workbench/runs/manager.py:register_results` | verify_preserve |
| [W014](../tasks/W014.md) | `demiflow/workbench/runs/facts.py:_sealed_entry` | implement |
| [W015](../tasks/W015.md) | `demiflow/workbench/runs/facts.py:_bridge_node_map` | verify_preserve |
| [W016](../tasks/W016.md) | `demiflow/workbench/web/workbench.js:logicalName` | implement |
| [W017](../tasks/W017.md) | `demiflow/workbench/web/workbench.js:logicalEntry` | implement |
| [W018](../tasks/W018.md) | `demiflow/workbench/web/workbench.js:renderLogical` | implement |
| [W019](../tasks/W019.md) | `demiflow/workbench/web/workbench.js:chooseLogicalNode` | implement |
| [W020](../tasks/W020.md) | `demiflow/workbench/web/workbench-graph.js:mountLogical.update` | implement |
| [W021](../tasks/W021.md) | `demiflow/workbench/web/workbench-graph.js:mountLogical.select` | implement |
| [W022](../tasks/W022.md) | `demiflow/workbench/api.py:GET_FIELDS` | implement |
| [W023](../tasks/W023.md) | `demiflow/workbench/graph/source_view.py:get` | verify_preserve |
| [W024](../tasks/W024.md) | `demiflow/workbench/graph/model.py:_resolve_bridge_node` | verify_preserve |
| [W025](../tasks/W025.md) | `demiflow/workbench/web/workbench.js:chooseNode` | implement |
| [W026](../tasks/W026.md) | `demiflow/workbench/web/workbench.js:loadSource` | implement |
| [W027](../tasks/W027.md) | `demiflow/workbench/web/workbench-state.js:create` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：无新增跨组调用。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：DEMO, HISTORY, WORKBENCH, WRITE。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `W.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
