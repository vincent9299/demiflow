# 文件装配归属

**修订中，新增交接点和函数归属尚未全部装配。** 见 [Review裁决](review-20261114/RESPONSE.md)。

每文件一个装配负责人；叶任务可由不同 agent 在独立工作树同时实现。禁止直接互相 cherry-pick 未验收实现作为隐含接口。

| 文件 | 唯一负责人 | 叶任务 |
|---|---|---|
| `demiflow/data/dataset.py` | A | A003, A004, A009, A010, A011, A012, A013, A026, A027, A028, A029 |
| `demiflow/data/plan.py` | A | A005 |
| `demiflow/embeddings/runtime.py` | M | M005, M006, M007, M009, M010, M011, M012 |
| `demiflow/embeddings/stream_recovery.py` | M | M003, M004, M008, M014 |
| `demiflow/execution/arrow_batch_sink.py` | L | L011 |
| `demiflow/execution/block_sink.py` | Q | Q001, Q002, Q003 |
| `demiflow/execution/capabilities.py` | A | A001, A002 |
| `demiflow/execution/chain.py` | L | L012, L013, L014, L015, L016, L032, L033, L040, L041, L042, L043, L044 |
| `demiflow/execution/contracts/operator.py` | S | S009, S010 |
| `demiflow/execution/dataset_native.py` | L | L010, L036, L037, L038, L045 |
| `demiflow/execution/executors/local.py` | A | A006, A007, A008, A015, A016, A017, A018, A019, A020, A021, A022, A023, A024, A025 |
| `demiflow/execution/graph.py` | L | L017, L018 |
| `demiflow/execution/native_plan.py` | L | L007, L008, L009, L030, L031 |
| `demiflow/execution/native_resources.py` | L | L039 |
| `demiflow/execution/operator_policy.py` | S | S001, S002, S003 |
| `demiflow/execution/operator_precheck.py` | S | S004, S005 |
| `demiflow/execution/operator_state.py` | S | S006, S007, S008, S016 |
| `demiflow/execution/payload_limits.py` | Q | Q004, Q005 |
| `demiflow/execution/plan_facts.py` | F | F001, F002, F003 |
| `demiflow/execution/plan_interpreter.py` | L | L001, L002, L003 |
| `demiflow/execution/queue_chain.py` | L | L019 |
| `demiflow/execution/recovery_identity.py` | S | S013 |
| `demiflow/execution/reservation.py` | Q | Q007, Q008 |
| `demiflow/execution/run_context.py` | R | R013, R014 |
| `demiflow/execution/sealed_plans.py` | L | L004, L005, L006 |
| `demiflow/execution/stage_buffers.py` | R | R001, R002, R003, R004, R005 |
| `demiflow/execution/stream.py` | R | R010, R011, R012, R015, R016, R022 |
| `demiflow/execution/stream_barrier.py` | R | R017, R018, R019, R020, R021 |
| `demiflow/execution/stream_delivery.py` | A | A014 |
| `demiflow/execution/stream_map.py` | S | S012 |
| `demiflow/execution/stream_materialize.py` | R | R006, R007, R008, R009 |
| `demiflow/execution/stream_resources.py` | Q | Q006 |
| `demiflow/execution/worker/execution.py` | L | L021, L022, L023, L034 |
| `demiflow/execution/worker/graph_execution.py` | L | L024, L025, L026, L035 |
| `demiflow/execution/worker/layout.py` | L | L020 |
| `demiflow/execution/worker/queue_transform.py` | L | L027, L028, L029 |
| `demiflow/lance/search.py` | S | S014, S015 |
| `demiflow/operator_llm/http_options.py` | M | M015 |
| `demiflow/operator_llm/stream_recovery.py` | M | M001, M002, M013 |
| `demiflow/planning/dataset_dag.py` | F | F004 |
| `demiflow/planning/graph_dag.py` | F | F005 |
| `demiflow/planning/traits.py` | S | S011 |
| `demiflow/workbench/api.py` | W | W022 |
| `demiflow/workbench/graph/model.py` | W | W005, W006, W024 |
| `demiflow/workbench/graph/source_view.py` | W | W023 |
| `demiflow/workbench/runs/facts.py` | W | W001, W002, W003, W004, W014, W015 |
| `demiflow/workbench/runs/manager.py` | W | W008, W009, W010, W011, W012, W013 |
| `demiflow/workbench/runs/worker.py` | W | W007 |
| `demiflow/workbench/web/workbench-graph.js` | W | W020, W021 |
| `demiflow/workbench/web/workbench-state.js` | W | W027 |
| `demiflow/workbench/web/workbench.js` | W | W016, W017, W018, W019, W025, W026 |
| `tests/test_upgrade_v2_acceptance.py` | V | V001, V002, V003, V004, V005, V006, V007, V008, V009, V010, V011, V012, V013, V014, V015, V016, V017, V018, V019, V020, V021 |

## 非函数代码的唯一装配责任

- A：capabilities.py 的 ActionDecision 声明和 imports；所有字段见 C03。
- S：operator_policy.py 的 StatePolicy 声明；OperatorStates 只增加策略选择所需私有映射，不改变已有 owners/aliases/identities 语义。
- R：stage_buffers.py 的类声明；stream._Stage.__slots__ 加 buffer_adapter，初始化见 R 叶任务；不能让别的函数 agent单独发另一版本 slots。
- F：dataset_dag.py 的 ProgramSlice 声明，精确字段见 interfaces.pyi。
- L：plan_interpreter.py 的 ExecutionPlacement 声明及 ExecutionLayout 末尾 executions 默认值；v1/v2常量/schema键集由 L 一次装配。
- W：前端图/详情/源码与HTTP字段声明已进入任务表；事实生产者 _sealed_entry 与消费者同步装配。
- V：共享验收文件的 imports/fixture；单函数 agent只能新增自己的测试函数。

构造器和 slots 是一个接口事务：字段声明与 __init__ 必须在同一文件装配提交中出现，不能先合一个会 AttributeError 的半成品。
