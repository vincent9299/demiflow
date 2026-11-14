# L-native组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

本组拥有Arrow真实grant与临时写出闭合，不能只透传execution对象。消费L-contract给定placement，不改公共shape；借用既有准入/额度，约束真实任务、输出及临时文件。附加TaskScheduler四方法和必要同文件私有装配归本组，见清单。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [L010](../tasks/L010.md) | `demiflow/execution/dataset_native.py:execute` | implement |
| [L011](../tasks/L011.md) | `demiflow/execution/arrow_batch_sink.py:execute` | implement |
| [L036](../tasks/L036.md) | `demiflow/execution/dataset_native.py:Engine.__init__` | implement |
| [L037](../tasks/L037.md) | `demiflow/execution/dataset_native.py:Engine.session` | implement |
| [L038](../tasks/L038.md) | `demiflow/execution/dataset_native.py:Engine.batch_rows` | implement |
| [L039](../tasks/L039.md) | `demiflow/execution/native_resources.py:dataset_session` | implement |
| [L045](../tasks/L045.md) | `demiflow/execution/dataset_native.py:Engine.python_rows` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：A。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：BOUNDS, LAYOUT, RELATIONAL, WRITE。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `L-native.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。

## 已授权的资源闭合补充单元

除7张既有卡，以下6个单元归本组。rechunk是真实输入缓冲，apply_batch是真实IPC写出；不能只改execute的参数传递。原签名变化、私有预算字段和消费者更新随实现逐项列清，不改组外公共接口。

| 文件与符号 | 基线行 | 基线函数SHA256 |
|---|---:|---|
| `demiflow/execution/local_tasks.py:TaskScheduler.__init__` | 427 | `cfa2053bd62c31c49c0a4c34539f35f4630469903549f708f65472d3cd786e73` |
| `demiflow/execution/local_tasks.py:TaskScheduler.__enter__` | 434 | `f9e616a8adcc35bd16ff7467ba870b969f492bad98998b5df15b4abb75ef69cc` |
| `demiflow/execution/local_tasks.py:TaskScheduler.run` | 443 | `413e6a8a69a6f6bb59cb706e6873c286c0107eae651e403c3fae4c89f3107d71` |
| `demiflow/execution/local_tasks.py:TaskScheduler.__exit__` | 502 | `3f9746a4304106635161b30b46f03add6398f4bd4fff586f3ec063978f843939` |
| `demiflow/execution/arrow_batch_sink.py:rechunk` | 28 | `44cb686f6822404b348da1e51f019d57c7bba0aa35f4682852f844761cba30aa` |
| `demiflow/execution/arrow_batch_sink.py:apply_batch` | 46 | `7433edd8f6d14573f5529cf70db628be0c940c6fd9d306105370437cad60ab0a` |

新增独占文件：`demiflow/execution/local_tasks.py`；arrow_batch_sink.py原本已属本组。
补充单元单独写函数回执，不冒称已包含在198张旧卡中。
