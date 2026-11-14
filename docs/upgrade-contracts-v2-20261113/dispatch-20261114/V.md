# V组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

立即编写21项真实验收，并锁定旧反例。替身只放外部transport或独立单测边界；最终组合必须真解释器、journal、actor、sink、driver。缺其他组实现时报告未验，不能改断言凑绿。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [V001](../tasks/V001.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_supported_stream_terminals` | implement |
| [V002](../tasks/V002.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_actor_counter_crash_resume` | implement |
| [V003](../tasks/V003.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_identity_distinguishes_business_configuration` | implement |
| [V004](../tasks/V004.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_signed_zero_roundtrip` | implement |
| [V005](../tasks/V005.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_reprepare_budget_same_bytes` | implement |
| [V006](../tasks/V006.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_vector_search_real_state` | implement |
| [V007](../tasks/V007.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_embedding_batch_recovery` | implement |
| [V008](../tasks/V008.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_grouped_and_prepend_buffers` | implement |
| [V009](../tasks/V009.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_prompt_policy_and_unknown` | implement |
| [V010](../tasks/V010.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_write_routes_consume_layout` | implement |
| [V011](../tasks/V011.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_write_create_recovery` | implement |
| [V012](../tasks/V012.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_graph_queue_commit_authority` | implement |
| [V013](../tasks/V013.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_workbench_history_end_to_end` | implement |
| [V014](../tasks/V014.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_notebook_sql_lifecycle` | implement |
| [V015](../tasks/V015.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_case_media_review_roundtrip` | implement |
| [V016](../tasks/V016.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_demo_navigation_and_layout` | implement |
| [V017](../tasks/V017.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_development_context_isolation` | implement |
| [V018](../tasks/V018.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_resource_and_performance_envelope` | implement |
| [V019](../tasks/V019.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_ray_backend_actual` | implement |
| [V020](../tasks/V020.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_historical_read_and_code_drift` | implement |
| [V021](../tasks/V021.md) | `tests/test_upgrade_v2_acceptance.py:test_v2_nested_program_code_pin` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：无新增跨组调用。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：ADMISSION, BOUNDS, BUFFER, CASE, CODEC, DEMO, DEVELOPMENT, EMBED, GRAPH, HISTORY, IDENTITY, LAYOUT, NOTEBOOK, PROMPT, RAY, RELATIONAL, STATE, VECTOR, WORKBENCH, WRITE。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `V.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
