# 验收命令索引

本文件列未来验收命令，**不是已通过报告**。python 使用自己的已验证环境，PYTHONPATH 必须指向隔离集成树。

执行环境变量：`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD"`。pytest 额外加 `-p no:cacheprovider --basetemp=/tmp/demiflow-v2-<gate>`；不得在主仓运行。不同进程使用不同 basetemp，重用路径前保留证据。

新验收文件 `tests/test_upgrade_v2_acceptance.py` 尚待 V 实现，不能因为不存在而跳过验收。

## ADMISSION

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-admission tests/test_native_delivery_barrier.py tests/test_stream_admission.py
```

叶任务：A001, A002, A003, A004, A005, A006, S005, Q005, V001。

## BOUNDS

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-bounds tests/test_native_resources.py tests/test_u2_resources_local.py tests/test_forge_analysis_storage_limits.py
```

叶任务：S002, S003, S008, R004, R012, Q001, Q002, Q003, Q004, Q005, Q006, M006, L010, L011, V018, R016, M011, L034, L035, L036, L037, L038, L039。

## BUFFER

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-buffer tests/test_stream_grouping_lookup.py tests/test_native_barrier.py
```

叶任务：R001, R002, R003, R004, R005, R006, R007, R008, R009, R010, R011, R012, M005, V008, R015, R016。

## CASE

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-case tests/test_forge_analysis_case_browser.py tests/test_forge_analysis_case_groups_ui_browser.py tests/test_forge_analysis_case_query.py tests/test_forge_analysis_case_media_browser.py
```

叶任务：V015。

## CODEC

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-codec tests/test_u3_graph_type_fidelity.py tests/test_execution_recovery_stream.py
```

叶任务：Q001, Q002, Q003, V004, V005。

## DELIVERY

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-delivery tests/test_native_delivery_barrier.py
```

叶任务：A001, A005, A006, A007, S012。

## DEMO

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-demo tests/test_forge_analysis_case_demo_browser.py tests/test_forge_workbench_config_run_browser.py
```

叶任务：V016, W016, W017, W018, W019, W020, W021。

## DEVELOPMENT

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-development tests/test_forge_analysis_authority_rpc.py tests/test_forge_analysis_frontend_state.py
```

叶任务：V017。

## EMBED

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-embed tests/test_embeddings.py tests/test_document_embeddings.py
```

叶任务：F002, R006, M003, M004, M005, M006, M007, V007, R015, M008, M009, M010, M011, M012。

## FACTS

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-facts tests/test_runtime_builtin_recovery.py tests/test_c_write_seam.py
```

叶任务：F001, F002, F003, A002。

## GRAPH

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-graph tests/test_u3_graph_local.py tests/test_u3_graph_restore.py tests/test_u3_graph_code_pin.py tests/test_u3_graph_router.py
```

叶任务：F005, L003, L017, L018, L020, L024, L025, L026, V012, L034, L035。

## HISTORY

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-history tests/test_execution_recovery_identity.py tests/test_workbench_sealed_facts.py tests/test_code_pin_serialization.py
```

叶任务：F001, F002, F003, F004, F005, A008, S010, S013, R013, R014, L001, L002, L004, L005, L006, L008, L012, L014, L021, L022, L023, L024, L025, L026, L027, L028, L029, W001, W002, W003, W004, W005, W006, W007, W008, W009, W010, W011, W012, W013, V020, L030, L031, L033, W014, V021, W015, W016, W017, W018, W019, W020, W021。

## IDENTITY

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-identity tests/test_execution_recovery_identity.py tests/test_code_pin_serialization.py
```

叶任务：S001, S013, V003。

## LAYOUT

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-layout tests/test_execution_layout.py tests/test_sealed_execution_bindings.py
```

叶任务：F004, A008, S009, S010, R011, L001, L002, L003, L004, L005, L006, L007, L008, L009, L012, L013, L014, L015, L016, L020, L021, L022, L023, V010, R016, L030, L031, L032, V021, L036, L037, L039。

## NOTEBOOK

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-notebook tests/test_forge_analysis_browser.py tests/test_forge_analysis_sessions.py tests/test_forge_analysis_kernel_contract.py
```

叶任务：V014。

## PREPEND

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-prepend tests/test_native_barrier.py tests/test_stream_grouping_lookup.py
```

叶任务：F002。

## PROMPT

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-prompt tests/test_prompt_http_stream.py tests/test_prompt_stream_recovery.py tests/test_inference_execution.py
```

叶任务：M001, M002, V009。

## QUEUE

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-queue tests/test_queue_v2_crash.py tests/test_mvp2_queue_unified.py
```

叶任务：A003, L003, L019, L020, L027, L028, L029。

## RAY

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-ray tests/test_u2_chain_ray.py tests/test_u3_graph_ray.py
```

叶任务：L016, L018, V019。

## RECOVERY

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-recovery tests/test_u2_chain_faults.py tests/test_u3_graph_faults.py tests/test_native_barrier.py
```

叶任务：S007, S008, S014, S015, R002, R003, R007, R008, R009, R012, R013, Q003, L015, L017, L019, L021, L022, L023, L024, L025, L026, L027, L028, L029, R015, L034, L035, S016。

## RELATIONAL

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-relational tests/test_local_join_chain.py tests/test_c_write_seam.py
```

叶任务：F004, A008, L007, L009, L010, L011, L030, L031, V021, L036, L037, L038, L039。

## STATE

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-state tests/test_u4_state_recovery.py tests/test_runtime_builtin_recovery.py
```

叶任务：S001, S002, S003, S004, S005, S006, S007, S008, S009, S010, S011, S012, S013, S014, S015, R008, V002, S016。

## STREAM

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-stream tests/test_stream.py tests/test_stream_timeout_propagation.py
```

叶任务：A003, A004, A007, Q004, Q006。

## UNKNOWN

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-unknown tests/test_prompt_stream_recovery.py tests/test_managed_image_execution.py
```

叶任务：R002, R007, R012, M001, M002, M003, M004, M007, M008, M010, M012。

## VECTOR

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-vector tests/test_vector_search_stream.py tests/test_search_recovery.py
```

叶任务：F002, S004, S011, S014, S015, R006, V006。

## WORKBENCH

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-workbench tests/test_workbench_sealed_facts.py tests/test_forge_workbench_config_run_browser.py tests/test_forge_workbench_runtime_browser.py
```

叶任务：W001, W002, W003, W004, W005, W006, W007, W008, W009, W010, W011, W012, W013, V013, W014, W015, W016, W017, W018, W019, W020, W021。

## WRITE

```bash
python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-write tests/test_gpt_write_recovery.py tests/test_execution_recovery_lance.py tests/test_c_write_seam.py
```

叶任务：A008, R009, R013, R014, L009, L011, L012, L013, W007, W008, W009, W010, W011, W012, W013, V011, L032, L033。
