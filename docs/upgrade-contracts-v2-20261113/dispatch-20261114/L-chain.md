# L-chain组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

本组独占chain.py，吸收现有候选而非重写。历史写出恢复复用原封存程序，源callable依赖纳入同一code pin；不改变唯一提交协议。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [L012](../tasks/L012.md) | `demiflow/execution/chain.py:seal_write_topology` | implement |
| [L013](../tasks/L013.md) | `demiflow/execution/chain.py:_WriteSeal.layout` | implement |
| [L014](../tasks/L014.md) | `demiflow/execution/chain.py:verify_loaded_plan` | verify_preserve |
| [L015](../tasks/L015.md) | `demiflow/execution/chain.py:run_local_chain` | verify_preserve |
| [L016](../tasks/L016.md) | `demiflow/execution/chain.py:run_ray_chain` | verify_preserve |
| [L032](../tasks/L032.md) | `demiflow/execution/chain.py:_WriteSeal.__init__` | implement |
| [L033](../tasks/L033.md) | `demiflow/execution/chain.py:_WriteSeal.verify` | implement |
| [L040](../tasks/L040.md) | `demiflow/execution/chain.py:source_callable_roots` | implement |
| [L041](../tasks/L041.md) | `demiflow/execution/chain.py:_business_dependency_modules` | implement |
| [L042](../tasks/L042.md) | `demiflow/execution/chain.py:plan_code_pin` | implement |
| [L043](../tasks/L043.md) | `demiflow/execution/chain.py:snapshot_code_sources` | implement |
| [L044](../tasks/L044.md) | `demiflow/execution/chain.py:seal_write_plan_ref` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：L-contract。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：HISTORY, IDENTITY, LAYOUT, RAY, RECOVERY, RELATIONAL, WRITE。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `L-chain.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
