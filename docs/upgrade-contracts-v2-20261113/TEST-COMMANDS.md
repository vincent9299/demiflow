# 验收命令索引（2026-11-14 修订）

本文件列未来验收命令，**不是已通过报告**。python 使用自己的已验证环境，PYTHONPATH 必须指向隔离集成树。

命令需从隔离树根运行。run_gate.py 记录 commit、收集结果和逐测试结果；缺测试、缺环境、skip、xfail、零测试均不得通过。输出仅 /tmp/demiflow-*，不覆盖旧证据。RAY 使用实际具备 Ray 的环境；其他门使用具备各自依赖的环境。

新验收文件 `tests/test_upgrade_v2_acceptance.py` 尚待 V 实现；以下已选择21个函数，文件缺失会阻塞。没有用 -k 把已有回归一起过滤掉。REGRESSION117 执行11个历史文件，117个原失败ID必须实际通过；单独时间敏感项在 HISTORICAL_TIMING 连续3轮执行。

依赖门须在同一集成源码 revision 分别通过；脚本不擅自重复跑依赖。命令通过仍不代表两大root全部完成。

## ADMISSION

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py ADMISSION --output /tmp/demiflow-v2-admission-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_native_delivery_barrier.py`, `tests/test_stream_admission.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_supported_stream_terminals`。

叶任务：A001, A002, A003, A004, A005, A006, S005, Q005, V001, A009, A010, A011, A012, A013, A014, A015, A016, A017, A018, A019, A020, A021, A022, A023, A024, A025, A026, A027, A028, A029。依赖门：无。

## BOUNDS

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py BOUNDS --output /tmp/demiflow-v2-bounds-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_native_resources.py`, `tests/test_u2_resources_local.py`, `tests/test_forge_analysis_storage_limits.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_resource_and_performance_envelope`。

叶任务：S002, S003, S008, R004, R012, Q001, Q002, Q003, Q004, Q005, Q006, M006, L010, L011, V018, R016, M011, L034, L035, L036, L037, L038, L039, M013, M014, L045, Q007, Q008。依赖门：无。

## BUFFER

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py BUFFER --output /tmp/demiflow-v2-buffer-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_stream_grouping_lookup.py`, `tests/test_native_barrier.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_grouped_and_prepend_buffers`。

叶任务：R001, R002, R003, R004, R005, R006, R007, R008, R009, R010, R011, R012, M005, V008, R015, R016, R017, R018, R019, R020, R021, R022。依赖门：无。

## CASE

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py CASE --output /tmp/demiflow-v2-case-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_forge_analysis_case_browser.py`, `tests/test_forge_analysis_case_groups_ui_browser.py`, `tests/test_forge_analysis_case_query.py`, `tests/test_forge_analysis_case_media_browser.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_case_media_review_roundtrip`。

叶任务：V015。依赖门：无。

## CODEC

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py CODEC --output /tmp/demiflow-v2-codec-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_u3_graph_type_fidelity.py`, `tests/test_execution_recovery_stream.py`, `tests/test_s6_commit_window_quota.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_signed_zero_roundtrip`, `tests/test_upgrade_v2_acceptance.py::test_v2_reprepare_budget_same_bytes`。

叶任务：Q001, Q002, Q003, V004, V005。依赖门：无。

## DELIVERY

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py DELIVERY --output /tmp/demiflow-v2-delivery-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_native_delivery_barrier.py`。

叶任务：A001, A005, A006, A007, S012, A009, A010, A011, A012, A013, A014。依赖门：ADMISSION, BOUNDS。

## DEMO

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py DEMO --output /tmp/demiflow-v2-demo-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_forge_analysis_case_demo_browser.py`, `tests/test_forge_workbench_config_run_browser.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_demo_navigation_and_layout`。

叶任务：V016, W016, W017, W018, W019, W020, W021, W025, W026, W027。依赖门：无。

## DEVELOPMENT

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py DEVELOPMENT --output /tmp/demiflow-v2-development-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_forge_analysis_authority_rpc.py`, `tests/test_forge_analysis_frontend_state.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_development_context_isolation`。

叶任务：V017。依赖门：无。

## EMBED

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py EMBED --output /tmp/demiflow-v2-embed-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_embeddings.py`, `tests/test_document_embeddings.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_embedding_batch_recovery`。

叶任务：F002, R006, M003, M004, M005, M006, M007, V007, R015, M008, M009, M010, M011, M012, M013, M014。依赖门：无。

## FACTS

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py FACTS --output /tmp/demiflow-v2-facts-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_runtime_builtin_recovery.py`, `tests/test_c_write_seam.py`。

叶任务：F001, F002, F003, A002。依赖门：LAYOUT。

## GRAPH

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py GRAPH --output /tmp/demiflow-v2-graph-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_u3_graph_local.py`, `tests/test_u3_graph_restore.py`, `tests/test_u3_graph_code_pin.py`, `tests/test_u3_graph_router.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_graph_queue_commit_authority`。

叶任务：F005, L003, L017, L018, L020, L024, L025, L026, V012, L034, L035, Q007, Q008。依赖门：无。

## HISTORY

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py HISTORY --output /tmp/demiflow-v2-history-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_execution_recovery_identity.py`, `tests/test_workbench_sealed_facts.py`, `tests/test_code_pin_serialization.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_historical_read_and_code_drift`, `tests/test_upgrade_v2_acceptance.py::test_v2_nested_program_code_pin`。

叶任务：F001, F002, F003, F004, F005, A008, S010, S013, R013, R014, L001, L002, L004, L005, L006, L008, L012, L014, L021, L022, L023, L024, L025, L026, L027, L028, L029, W001, W002, W003, W004, W005, W006, W007, W008, W009, W010, W011, W012, W013, V020, L030, L031, L033, W014, V021, W015, W016, W017, W018, W019, W020, W021, A009, A010, A011, A012, A013, M015, L040, L041, L042, L043, L044, W022, W023, W024, W025, W026, W027。依赖门：无。

## IDENTITY

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py IDENTITY --output /tmp/demiflow-v2-identity-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_execution_recovery_identity.py`, `tests/test_code_pin_serialization.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_identity_distinguishes_business_configuration`。

叶任务：S001, S013, V003, M015, L041, L042, L043。依赖门：无。

## LAYOUT

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py LAYOUT --output /tmp/demiflow-v2-layout-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_execution_layout.py`, `tests/test_sealed_execution_bindings.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_write_routes_consume_layout`, `tests/test_upgrade_v2_acceptance.py::test_v2_nested_program_code_pin`。

叶任务：F004, A008, S009, S010, R011, L001, L002, L003, L004, L005, L006, L007, L008, L009, L012, L013, L014, L015, L016, L020, L021, L022, L023, V010, R016, L030, L031, L032, V021, L036, L037, L039, A015, A016, A017, A018, A019, A020, A021, A022, A023, A024, A025, L045, A026, A027, A028, A029。依赖门：无。

## NOTEBOOK

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py NOTEBOOK --output /tmp/demiflow-v2-notebook-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_forge_analysis_browser.py`, `tests/test_forge_analysis_sessions.py`, `tests/test_forge_analysis_kernel_contract.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_notebook_sql_lifecycle`。

叶任务：V014。依赖门：无。

## PREPEND

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py PREPEND --output /tmp/demiflow-v2-prepend-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_native_barrier.py`, `tests/test_stream_grouping_lookup.py`。

叶任务：F002。依赖门：BUFFER。

## PROMPT

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py PROMPT --output /tmp/demiflow-v2-prompt-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_prompt_http_stream.py`, `tests/test_prompt_stream_recovery.py`, `tests/test_inference_execution.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_prompt_policy_and_unknown`。

叶任务：M001, M002, V009, M013, M015。依赖门：无。

## QUEUE

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py QUEUE --output /tmp/demiflow-v2-queue-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_queue_v2_crash.py`, `tests/test_mvp2_queue_unified.py`。

叶任务：A003, L003, L019, L020, L027, L028, L029。依赖门：GRAPH。

## RAY

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py RAY --output /tmp/demiflow-v2-ray-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_u2_chain_ray.py`, `tests/test_u3_graph_ray.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_ray_backend_actual`。

叶任务：L016, L018, V019。依赖门：无。

## RECOVERY

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py RECOVERY --output /tmp/demiflow-v2-recovery-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_u2_chain_faults.py`, `tests/test_u3_graph_faults.py`, `tests/test_native_barrier.py`。

叶任务：S007, S008, S014, S015, R002, R003, R007, R008, R009, R012, R013, Q003, L015, L017, L019, L021, L022, L023, L024, L025, L026, L027, L028, L029, R015, L034, L035, S016, A014, R017, R018, R019, R020, R021, Q007, Q008。依赖门：STATE, BUFFER, WRITE。

## RELATIONAL

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py RELATIONAL --output /tmp/demiflow-v2-relational-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_local_join_chain.py`, `tests/test_c_write_seam.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_nested_program_code_pin`。

叶任务：F004, A008, L007, L009, L010, L011, L030, L031, V021, L036, L037, L038, L039, A016, L040, L041, L042, L043, L044, L045。依赖门：无。

## STATE

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py STATE --output /tmp/demiflow-v2-state-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_u4_state_recovery.py`, `tests/test_runtime_builtin_recovery.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_actor_counter_crash_resume`。

叶任务：S001, S002, S003, S004, S005, S006, S007, S008, S009, S010, S011, S012, S013, S014, S015, R008, V002, S016。依赖门：无。

## STREAM

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py STREAM --output /tmp/demiflow-v2-stream-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_stream.py`, `tests/test_stream_timeout_propagation.py`。

叶任务：A003, A004, A007, Q004, Q006, A017, A018, A019, A020, A021, A022, A023, A024, A025。依赖门：ADMISSION, REGRESSION117。

## UNKNOWN

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py UNKNOWN --output /tmp/demiflow-v2-unknown-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_prompt_stream_recovery.py`, `tests/test_managed_image_execution.py`。

叶任务：R002, R007, R012, M001, M002, M003, M004, M007, M008, M010, M012, M014, R017, R018, R019, R020。依赖门：PROMPT。

## VECTOR

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py VECTOR --output /tmp/demiflow-v2-vector-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_vector_search_stream.py`, `tests/test_search_recovery.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_vector_search_real_state`。

叶任务：F002, S004, S011, S014, S015, R006, V006。依赖门：无。

## WORKBENCH

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py WORKBENCH --output /tmp/demiflow-v2-workbench-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_workbench_sealed_facts.py`, `tests/test_forge_workbench_config_run_browser.py`, `tests/test_forge_workbench_runtime_browser.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_workbench_history_end_to_end`。

叶任务：W001, W002, W003, W004, W005, W006, W007, W008, W009, W010, W011, W012, W013, V013, W014, W015, W016, W017, W018, W019, W020, W021, W022, W023, W024, W025, W026, W027。依赖门：无。

## WRITE

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py WRITE --output /tmp/demiflow-v2-write-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_gpt_write_recovery.py`, `tests/test_execution_recovery_lance.py`, `tests/test_c_write_seam.py`, `tests/test_upgrade_v2_acceptance.py::test_v2_write_create_recovery`。

叶任务：A008, R009, R013, R014, L009, L011, L012, L013, W007, W008, W009, W010, W011, W012, W013, V011, L032, L033, L044, A026, A027, A028, A029。依赖门：无。

## REGRESSION117

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py REGRESSION117 --output /tmp/demiflow-v2-regression117-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_inference_execution.py`, `tests/test_prompt_http_stream.py`, `tests/test_stream.py`, `tests/test_stream_checkpoint_migration.py`, `tests/test_stream_checkpoint_resume.py`, `tests/test_stream_deduplicate.py`, `tests/test_stream_grouping_lookup.py`, `tests/test_stream_lance_references.py`, `tests/test_stream_map.py`, `tests/test_stream_threads.py`, `tests/test_vector_search_stream.py`。

叶任务：全局验收。依赖门：无。

## HISTORICAL_TIMING

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD" python docs/upgrade-contracts-v2-20261113/tools/run_gate.py HISTORICAL_TIMING --output /tmp/demiflow-v2-historical_timing-REPLACE_WITH_UNIQUE_RUN_ID
```

实际选择：`tests/test_stream.py::test_unordered_emission_no_head_of_line_blocking`。

叶任务：全局验收。依赖门：无。

## 跨门聚合

所有结果产生后，使用 tools/aggregate_gates.py 检查同revision、同gate配置及完整依赖闭包；缺门或混revision不通过。该聚合不代替产品、性能和全局意图验收。
