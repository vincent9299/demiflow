# M组：直接实施工单

设计基线：`13d19b58881a67ca7cc9cdb66c7feca63736b413`；运行代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。

先读 [开工决定](../EXECUTION-START.md)。真人已授权转入实现；旧卡的整包暂停提示不阻止本组编码。尚未冻结的两个局部契约按开工决定各自闭合，不伪称全局已完成。

原始目标：工作台真实看懂/配置/运行及数据分析评测；平台正确、稳定、高性能、通用，计划/资源/提交权威唯一，历史保护和恢复正确。

## 本组交付

优先复用N1已授权的在途候选；prompt_facts守卫不动。fresh预算与intent同事务，borrowed journal不能自关，unknown不重发。只替换外部transport做验收。

## 精确函数任务

| ID | 文件与符号 | 动作 |
|---|---|---|
| [M001](../tasks/M001.md) | `demiflow/operator_llm/stream_recovery.py:NativePromptRecovery.bind_client` | verify_preserve |
| [M002](../tasks/M002.md) | `demiflow/operator_llm/stream_recovery.py:RunPromptJournal.check_recovery` | verify_preserve |
| [M003](../tasks/M003.md) | `demiflow/embeddings/stream_recovery.py:NativeEmbeddingRecovery.__init__` | implement |
| [M004](../tasks/M004.md) | `demiflow/embeddings/stream_recovery.py:NativeEmbeddingRecovery.bind_actor` | implement |
| [M005](../tasks/M005.md) | `demiflow/embeddings/runtime.py:EmbeddingActor.native_stream_recovery` | implement |
| [M006](../tasks/M006.md) | `demiflow/embeddings/runtime.py:EmbeddingActor.astart` | implement |
| [M007](../tasks/M007.md) | `demiflow/embeddings/runtime.py:EmbeddingActor._exchange` | implement |
| [M008](../tasks/M008.md) | `demiflow/embeddings/stream_recovery.py:NativeEmbeddingRecovery.scope_request` | implement |
| [M009](../tasks/M009.md) | `demiflow/embeddings/runtime.py:EmbeddingActor.__init__` | implement |
| [M010](../tasks/M010.md) | `demiflow/embeddings/runtime.py:EmbeddingActor.cancel_drain_timeout_s` | implement |
| [M011](../tasks/M011.md) | `demiflow/embeddings/runtime.py:EmbeddingActor.aclose` | implement |
| [M012](../tasks/M012.md) | `demiflow/embeddings/runtime.py:EmbeddingActor._fresh_exchange` | verify_preserve |
| [M013](../tasks/M013.md) | `demiflow/operator_llm/stream_recovery.py:native_request_budget` | implement |
| [M014](../tasks/M014.md) | `demiflow/embeddings/stream_recovery.py:NativeEmbeddingRecovery.drain` | implement |
| [M015](../tasks/M015.md) | `demiflow/operator_llm/http_options.py:validate_http_options` | implement |

## 执行与回执

由Qwen绑定真实活跃负责人和独立工作树；同一范围已有会话则优先续作，不新开重复任务。任何代码/测试不得在主仓运行。组内可按函数分发，但同文件imports/字段/slots由本组唯一装配。

签名和外部形状以当前CONTRACTS.md、interfaces.pyi及RESPONSE-3为准；较早报告用于历史取证，不要求重新执行其已撤回建议。只在本组文件实现，本组外发现缺陷写最小反例交总控。

需消费其他组契约：无新增跨组调用。这是接口依赖，不是开始编码的等待顺序。使用测试域样本可以独立验证，生产不得放stub或ImportError fallback。

涉及验收门：BOUNDS, BUFFER, EMBED, HISTORY, IDENTITY, PROMPT, UNKNOWN。按变更跑直接反例和消费者，组合测试在隔离集成树跑；不重复跑无关全量集合。

每函数交付窄补丁、准确提交/命令/结果，在 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json` 写回执；本组汇总为 `M.json`。blocked/needs_decision说明具体接口与最小反例；不要只发往已停止旧总控。

禁止push、合主仓、重启服务或付费模型。保持冻结文件与符号保护，不扩大预算/改业务并发凑绿。测试产物只写/tmp/demiflow-*；最终GPT按真实代码和组合证据验收。
