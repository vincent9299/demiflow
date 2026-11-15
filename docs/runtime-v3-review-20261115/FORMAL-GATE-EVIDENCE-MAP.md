# 正式验收门与真实专项证据映射

本报告只读审计源码、已提交报告和现存 JUnit；不运行新测试，不修改正式验收 route，不把门改成返回常量。审计树 `34af611`，后续最新产品证据通过 `git show eb4b50e` 读取，原 106 项证据通过 `git show ed5518c` 读取。两份提交记录的 XML 摘要均独立核对；106 记录的测试 tree 确实等于 root `e6bdc2f`。

**17 个正式 gate 仍直接 `NotRunError`；其中很多已有独立真实专项，但证明范围、执行版本和正式门要求并不自动相等。** 完整 machine map 见 [FORMAL-GATE-EVIDENCE-MAP.json](FORMAL-GATE-EVIDENCE-MAP.json)：逐 gate 保存正式测试原文、每份 XML 的摘要/整组统计、实际通过的精确 nodeid、证明边界及建议同 HEAD 选择。一个混合成败报告中的已通过子集，不被改写成整报告全绿。

本表的“已有证明”均指本轮升级过程中执行过的证据，不声称 38 组选择全部绑定最新集成 HEAD。主控需要按最终改动影响选少量组合；已经完整执行且未受影响的模块门不必全量重复。

## 17 门逐项

| 正式门 | 已有证明 | 仍缺什么 |
| --- | --- | --- |
| `cpu_gpu_isolation` | 真实原allocator授予、CPU排除可见设备、退出后返还，受控inventory/逻辑Ray GPU。 | 无物理CUDA推理/显存峰值；dataflow_devices明确拒绝多GPU host；不能把配置mask当物理隔离。 |
| `fairness_deadlock` | 真实容量PENDING复用原allocation、短TTL续租；慢消费者、跨线程相互释放、下游定时flush不被上游EOI堵塞。 | 没有混合CPU/GPU多租户的饥饿上界证明；不能据普通union/单次成功推导全局公平。 |
| `multiworker_barrier` | 在途native输入/scope进原cut且12条HTTP不重发；真实有限join部分输出cut恢复1000条；native EOI在途崩溃恢复。 | 不是原门joined=9的实际返回；现有模型用例遍历contributions但未独立断言每种物理多worker成员组合。缺跨host与丢worker中途barrier组合。 |
| `route_recovery` | durable park前/后/消费后恢复原结果、实际消费ACK丢失用frontier恢复；memory重复投递执行业务前拒绝；物理块manifest恢复。 | 缺多producer/多consumer hash+broadcast跨host故障矩阵；分区内顺序证明不等于全局排序。 |
| `actor_state_lifecycle` | class/closure/task中途状态恢复；真实process成功/启动失败/业务与close双失败/取消/空类/staleboot观测；旧thread的真实排空与原stop/close协议。 | 尚未在最新同HEAD把所有host/state/failure组合一次验；不能把旧生命周期项通过代替最新eb4观察接线，也不能凭其它数值返回formal final_total=10。 |
| `queue_full_business` | 实际enqueue→read_queue→HTTP fetch→原结果settlement/ACK→固定queue历史；嵌套fetch distinct occurrence、原receipt恢复。 | 外部PostgreSQL未验；不由两条队列链推定任意Search/Agent→enqueue跨host完整工作流。 |
| `memory_peak` | 真实slow consumer在途上界、native process RSS/headroom guard、Arrow输入/输出副本共存超限保留旧Lance版本。 | 没有同预算代表性业务全过程实测驻留峰值；Arrow逻辑/编码字节与RSS不同。不可直接返回formal rows=16当峰值验收。 |
| `disk_gc` | 原journal暂存/临时scratch唯一总额、5个真实os._exit窗口重复恢复不累积；live writer/FD不回收；历史pins保存；未知发布不退款。 | artifact owner确认：没有回收已结算且所有当前/历史/prepared/unknown根均不引用的immutable published artifacts；当前published对象不删除。暂存orphan reconciliation不能冒充完整GC。 |
| `two_hosts` | 同机Ray专项不能提供第二台机器证明。 | formal两host/两node以及共享artifact读取/本地source域授权未实测；JSON host声明不是证据。 |
| `node_loss` | 有真实driver SIGKILL与单机worker/资源退出反例。 | 没有实际producer宿主断机/网络隔离后的恢复与unknown清理证明；driver失效不等价于host失效。 |
| `driver_restart` | 真实driver SIGKILL，原6个commit故障窗恢复；真实旧worker event拒收；原RPC丢回复幂等。 | 不是断远端宿主/网络分区；现有证据版本需同根控制loop性能补丁整合复验。 |
| `history_identity` | 真实原ACK结果重读、原seal/commit不变；新旧seal可选元数据不改plan；live program plan身份排除attempt/fence/token；set身份跨seed、来源变化拒绝。 | formal要求的3个原历史prompt enum identities尚无本表中对应真实fixture/XML；不能拿合成enum哈希替代。跨所有旧版本/旧程序语义不能据局部通过推定。 |
| `workbench_real_plan` | 真实prepare/start/show执行图/原journal dag_id plan_id、F10、bridge及观察；prepare-only历史源码读原封存；cancel/resume和native Save已ACK部分结果。 | 多次不同commit实跑，不是当前全产品root组合；declared图仍不能充当execution图。缺当前全套浏览器cancel/resume与复杂图同HEAD组合。 |
| `product_regression` | 真实A/B结果/比较/固定版本续跑、媒体Case窄屏和历史、私有Jupyter/Notebook草稿历史/SQLite；最新真实6族managed运行。 | 不是最新同HEAD Notebook/Python/SQL/Case/开发对话全浏览器验收；media shell部分外层runtime用double。image/document Embedding工作台授权未开放，文本成功不替代。 |
| `artifact_security` | 实际staging inode/lock绑定、未知owner/篡改publication拒绝；真实live程序code-pin拒漂移；当前D/H严格grant/owner/fence/路径/摘要契约。 | 没有跨真实host数据域/恶意共享挂载/网络分区完整矩阵；协议单元fake不能替代跨机权限实测。 |
| `performance` | 有实际计时与原固定预算，不等于吞吐达标。 | root仍处理40k/120s压力；原31行窄链两执行147.387s。formal五类workload+单独warmup+同预算基线/峰值尚未形成。 |
| `all_regressions` | 当前19契约564/原33/原147全绿；e6逐字树原106全绿；另有98 API Local168案例早期真实报告。 | 不是全部历史仓库；formal44分类处置清单未由本次生成；Ray全API/PG/多机/GPU不能被集合总数覆盖。 |

## 可直接定位的真实证据

下面列代表性精确 nodeid；参数化的全部实际项与其它对应断言均在 JSON，不能把函数前缀当作执行过的证据。每个引用都来自实际读取的 XML，不是仅按源码猜测。

### cpu_gpu_isolation

状态：`partial_controlled_devices_only`。正式 route：`tests/runtime_v3_support.py:1012`，仍未接线。

- `/tmp/demiflow-residency-devices6.xml`，整份结果 {"passed": 24, "skipped": 1}；本门选取 4 个已通过项。性质：`real_local`。版本依据：DEVICE-GRANTS.md。
  - `tests/test_runtime_v3_devices.py::test_real_dataset_gpu_grant_reaches_child_and_releases_on_exit`。
  - `tests/test_runtime_v3_devices.py::test_cpu_worker_does_not_inherit_ungranted_configured_gpu`。
  - `tests/test_runtime_v3_devices.py::test_gpu_without_inventory_rejects_before_business_activation`。
  - `tests/test_runtime_v3_devices.py::test_nested_run_cannot_create_second_device_authority`。

- `/tmp/demiflow-residency-ray-device3.xml`，整份结果 {"passed": 2}；本门选取 1 个已通过项。性质：`real_same_host_ray_logical_gpu`。版本依据：DEVICE-GRANTS.md。
  - `tests/test_runtime_v3_devices.py::test_ray_core_device_grant_uses_ray_assigned_visibility`。

未覆盖：无物理CUDA推理/显存峰值；dataflow_devices明确拒绝多GPU host；不能把配置mask当物理隔离。

### fairness_deadlock

状态：`partial_real_liveness`。正式 route：`tests/runtime_v3_support.py:1018`，仍未接线。

- `/tmp/demiflow-resource-progress-2.xml`，整份结果 {"passed": 28}；本门选取 4 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_resource_progress.py::test_runs_share_one_capacity_domain`。
  - `tests/test_runtime_v3_resource_progress.py::test_pending_admission_retries_same_allocation_then_executes`。
  - `tests/test_runtime_v3_resource_progress.py::test_unavailable_capacity_times_out_without_business_effect`。
  - `tests/test_runtime_v3_resource_progress.py::test_long_call_renews_same_worker_and_commits`。

- `/tmp/demiflow-regression-original106-e6bdc2f.xml`，整份结果 {"passed": 106}；本门选取 2 个已通过项。性质：`real_local`。版本依据：ed5518c ORIGINAL106; tree identical e6bdc2f。
  - `tests/test_stream_map.py::test_thread_map_allows_async_consumer_to_release_other_workers`。
  - `tests/test_stream_map.py::test_slow_consumer_bounds_wide_rows_and_inflight_calls`。

- `/tmp/demiflow-regression-finite-finish-final.xml`，整份结果 {"passed": 12}；本门选取 2 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_eoi_progress.py::test_eoi_keeps_polling_a_downstream_timer_while_upstream_waits[row_calls]`。

未覆盖：没有混合CPU/GPU多租户的饥饿上界证明；不能据普通union/单次成功推导全局公平。

### multiworker_barrier

状态：`real_pending_cut_and_restore_subset`。正式 route：`tests/runtime_v3_support.py:1075`，仍未接线。

- `/tmp/demiflow-inflight-full-r5.xml`，整份结果 {"passed": 75}；本门选取 2 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_managed_image_execution.py::test_known_inflight_model_response_crosses_barrier_and_recovers_once`。
  - `tests/test_runtime_v3_public_execution.py::test_partial_cut_restores_input_kernel_and_terminal_prefix`。

- `/tmp/demiflow-regression-finite-finish-final.xml`，整份结果 {"passed": 12}；本门选取 2 个已通过项。性质：`real_local`。版本依据：4b98d13 FINITE-FINISH-RECOVERY.json。
  - `tests/test_runtime_v3_finite_finish.py::test_finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity`。
  - `tests/test_runtime_v3_eoi_progress.py::test_final_native_batch_eoi_cut_survives_crash_without_second_request`。

未覆盖：不是原门joined=9的实际返回；现有模型用例遍历contributions但未独立断言每种物理多worker成员组合。缺跨host与丢worker中途barrier组合。

### route_recovery

状态：`real_local_route_subset`。正式 route：`tests/runtime_v3_support.py:1081`，仍未接线。

- `/tmp/demiflow-regression-finite-finish-combo.xml`，整份结果 {"passed": 51, "skipped": 1}；本门选取 4 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_durable_residency.py::test_durable_resume_preserves_original_delivery_and_actual_ack[before_commit]`。
  - `tests/test_runtime_v3_durable_residency.py::test_lost_consumer_ack_reconciles_only_actual_input_frontier`。

- `/tmp/demiflow-regression-finite-finish-final.xml`，整份结果 {"passed": 12}；本门选取 1 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_finite_finish.py::test_duplicate_memory_delivery_is_rejected_before_second_business_call`。

- `/tmp/demiflow-blocks5.xml`，整份结果 {"passed": 12, "skipped": 1}；本门选取 1 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_blocks.py::test_real_partition_manifest_survives_native_committed_recovery`。

未覆盖：缺多producer/多consumer hash+broadcast跨host故障矩阵；分区内顺序证明不等于全局排序。

### actor_state_lifecycle

状态：`real_state_and_lifecycle_subsets`。正式 route：`tests/runtime_v3_support.py:1097`，仍未接线。

- `/tmp/demiflow-regression-task-final.xml`，整份结果 {"passed": 94}；本门选取 16 个已通过项。性质：`real_local`。版本依据：78bfcd7 LOGICAL-TASK-STATE.json。
  - `tests/test_runtime_v3_task_scope.py::test_actual_driver_resume_preserves_partial_task_and_committed_prefix[class]`。
  - `tests/test_runtime_v3_actor_lifecycle.py::test_public_plain_actor_owner_is_the_lifecycle_owner[instance]`。
  - `tests/test_runtime_v3_actor_lifecycle.py::test_lifecycle_is_once_and_failed_hook_does_not_skip_close[None]`。
  - `tests/test_runtime_v3_actor_lifecycle.py::test_cancelled_future_finishes_its_finally_before_actor_close`。

- `/tmp/demiflow-product-final.xml`，整份结果 {"passed": 45, "failed": 1}；本门选取 8 个已通过项。性质：`real_local`。版本依据：eb4b50e PRODUCT-LIFECYCLE-SOURCE-CLOSURE.verification.json; selected seven all passed, whole run 45pass/1 historical injection failure。
  - `tests/test_runtime_v3_actor_lifecycle_observations.py::test_real_process_hooks_survive_cleanup_and_preserve_failures[success]`。
  - `tests/test_runtime_v3_actor_lifecycle_observations.py::test_declared_payload_limits_are_not_silently_shrunk`。

- `/tmp/demiflow-regression-original106-e6bdc2f.xml`，整份结果 {"passed": 106}；本门选取 2 个已通过项。性质：`real_local`。版本依据：ed5518c; exact e6bdc2f tree。
  - `tests/test_stream_map.py::test_failure_drains_sibling_thread_before_all_instance_cleanup`。
  - `tests/test_stream_map.py::test_external_cancel_drains_map_calls_before_action_cleanup`。

未覆盖：尚未在最新同HEAD把所有host/state/failure组合一次验；不能把旧生命周期项通过代替最新eb4观察接线，也不能凭其它数值返回formal final_total=10。

### queue_full_business

状态：`real_sqlite_and_same_host_ray_subset`。正式 route：`tests/runtime_v3_support.py:1118`，仍未接线。

- `/tmp/demiflow-inflight-ray-r5.xml`，整份结果 {"passed": 2}；本门选取 1 个已通过项。性质：`real_same_host_ray`。版本依据：2c5b391 INFLIGHT-CHECKPOINT-PROGRESS.md。
  - `tests/test_runtime_v3_ray_native.py::test_ray_native_producer_fetch_settlement_and_history`。

- `/tmp/demiflow-fetch-graph-r3.xml`，整份结果 {"passed": 9, "skipped": 2}；本门选取 5 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_fetch_graph.py::test_nested_fetch_union_keeps_occurrences_and_receipts[False]`。
  - `tests/test_runtime_v3_fetch_graph.py::test_sequential_fetches_keep_distinct_native_namespaces`。
  - `tests/test_runtime_v3_fetch_graph.py::test_fetch_graph_read_does_not_mutate_declarations`。
  - `tests/test_runtime_v3_fetch_graph.py::test_nested_fetch_recovers_original_native_receipt_before_first_cut`。

未覆盖：外部PostgreSQL未验；不由两条队列链推定任意Search/Agent→enqueue跨host完整工作流。

### memory_peak

状态：`partial_bounds_and_copy_rejection`。正式 route：`tests/runtime_v3_support.py:1124`，仍未接线。

- `/tmp/demiflow-regression-original106-e6bdc2f.xml`，整份结果 {"passed": 106}；本门选取 1 个已通过项。性质：`real_local`。版本依据：ed5518c e6bdc2f。
  - `tests/test_stream_map.py::test_slow_consumer_bounds_wide_rows_and_inflight_calls`。

- `/tmp/demiflow-native-limits.xml`，整份结果 {"passed": 8}；本门选取 6 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_native_limits.py::test_policy_and_assignment_preserve_old_wire_and_bind_new_ceilings`。
  - `tests/test_runtime_v3_native_limits.py::test_native_guard_samples_actual_process_and_preserves_headroom_direction`。
  - `tests/test_runtime_v3_native_limits.py::test_public_native_limits_constrain_real_lance_input_join_and_sink`。
  - `tests/test_runtime_v3_native_limits.py::test_public_native_row_cap_rejects_before_lance_publication`。

- `/tmp/demiflow-timed-native-recovery.xml`，整份结果 {"passed": 6}；本门选取 1 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_stream_lance.py::test_native_arrow_conversion_rejects_coexisting_copies_before_overwrite`。

未覆盖：没有同预算代表性业务全过程实测驻留峰值；Arrow逻辑/编码字节与RSS不同。不可直接返回formal rows=16当峰值验收。

### disk_gc

状态：`implementation_gap_published_content_gc`。正式 route：`tests/runtime_v3_support.py:1130`，仍未接线。

- `/tmp/demiflow-growth.xml`，整份结果 {"passed": 27}；本门选取 21 个已通过项。性质：`real_local`。版本依据：e6bdc2f contains staging growth; this XML 27pass, separate pre-integration run。
  - `tests/test_runtime_v3_staging_growth.py::test_growth_is_bounded_and_amortized_before_every_write[artifact]`。
  - `tests/test_runtime_v3_staging_growth.py::test_exact_growth_fits_when_optional_headroom_is_rejected[artifact]`。
  - `tests/test_runtime_v3_staging_growth.py::test_unknown_growth_is_never_retried_as_an_exact_reservation`。
  - `tests/test_runtime_v3_joint_scratch.py::test_native_then_dataflow_admission_checks_sum_and_rolls_back`。

未覆盖：artifact owner确认：没有回收已结算且所有当前/历史/prepared/unknown根均不引用的immutable published artifacts；当前published对象不删除。暂存orphan reconciliation不能冒充完整GC。

### two_hosts

状态：`not_verified`。正式 route：`tests/runtime_v3_support.py:1141`，仍未接线。

没有能替代该门的完整真实证据；formal两host/两node以及共享artifact读取/本地source域授权未实测；JSON host声明不是证据。

### node_loss

状态：`not_verified`。正式 route：`tests/runtime_v3_support.py:1147`，仍未接线。

没有能替代该门的完整真实证据；没有实际producer宿主断机/网络隔离后的恢复与unknown清理证明；driver失效不等价于host失效。

### driver_restart

状态：`real_local_and_same_host_ray`。正式 route：`tests/runtime_v3_support.py:1153`，仍未接线。

- `/tmp/demiflow-v-final-local.xml`，整份结果 {"passed": 14}；本门选取 8 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_recovery_scenarios.py::test_real_dataset_commit_six_kill_windows[before_intent]`。
  - `tests/test_runtime_v3_recovery_scenarios.py::test_real_driver_restart_rejects_actual_old_worker_event`。
  - `tests/test_runtime_v3_recovery_scenarios.py::test_real_worker_remote_journal_guards_and_lost_reply_idempotency`。

- `/tmp/demiflow-v-real-ray2.xml`，整份结果 {"passed": 2}；本门选取 2 个已通过项。性质：`real_same_host_ray`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_recovery_scenarios.py::test_real_driver_restart_rejects_actual_old_worker_event`。
  - `tests/test_runtime_v3_recovery_scenarios.py::test_real_worker_remote_journal_guards_and_lost_reply_idempotency`。

未覆盖：不是断远端宿主/网络分区；现有证据版本需同根控制loop性能补丁整合复验。

### history_identity

状态：`real_identity_subsets`。正式 route：`tests/runtime_v3_support.py:1159`，仍未接线。

- `/tmp/demiflow-v3-contracts-fixes-r2.xml`，整份结果 {"passed": 15}；本门选取 1 个已通过项。性质：`real_local`。版本依据：inputs byte-match 6b2a731。
  - `tests/test_runtime_v3_public_execution.py::test_replay_committed_result_reuses_seal_and_commit`。

- `/tmp/demiflow-v3-contracts-full-r3.xml`，整份结果 {"passed": 564}；本门选取 1 个已通过项。性质：`real_local`。版本依据：clean 6b2a731。
  - `tests/test_runtime_v3_w.py::test_w001`。

- `/tmp/demiflow-product-final.xml`，整份结果 {"passed": 45, "failed": 1}；本门选取 1 个已通过项。性质：`real_local`。版本依据：eb4b50e evidence。
  - `tests/test_runtime_v3_thread_host.py::test_live_manifest_and_plan_identity_exclude_attempt_fence_and_token`。

- `/tmp/demiflow-regression-scalars-r2.xml`，整份结果 {"passed": 55}；本门选取 7 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_execution_recovery_identity.py::test_unordered_collection_identity_is_stable_across_process_seeds`。
  - `tests/test_execution_recovery_identity.py::test_changed_set_source_is_rejected_on_resume`。
  - `tests/test_execution_recovery_identity.py::test_set_identity_has_cardinality_and_total_descriptor_byte_limits[value0]`。
  - `tests/test_execution_recovery_identity.py::test_unadapted_objects_still_fail_closed`。

未覆盖：formal要求的3个原历史prompt enum identities尚无本表中对应真实fixture/XML；不能拿合成enum哈希替代。跨所有旧版本/旧程序语义不能据局部通过推定。

### workbench_real_plan

状态：`real_managed_and_browser_subsets`。正式 route：`tests/runtime_v3_support.py:1165`，仍未接线。

- `/tmp/demiflow-regression-browser-final.xml`，整份结果 {"passed": 2}；本门选取 1 个已通过项。性质：`real_browser_managed_run`。版本依据：0a20645 REGRESSION-PRODUCT-EVIDENCE.json。
  - `tests/test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge`。

- `/tmp/demiflow-product-final-browser.xml`，整份结果 {"passed": 31}；本门选取 31 个已通过项。性质：`real_browser_plus_bridge_contracts`。版本依据：eb4b50e PRODUCT-LIFECYCLE-SOURCE-CLOSURE.verification.json。
  - `tests/test_workbench_logical_browser.py::test_logical_view_projects_sealed_run_report`。
  - `tests/test_workbench_logical_browser.py::test_logical_view_source_absence_then_report_projection`。
  - `tests/test_workbench_execution_binding.py::test_read_identity_bridge_success`。
  - `tests/test_workbench_execution_binding.py::test_read_identity_bridge_missing`。

- `/tmp/demiflow-inflight-full-r5.xml`，整份结果 {"passed": 75}；本门选取 1 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_gpt_write_recovery.py::test_workbench_cancel_resume_golden`。

- `/tmp/demiflow-save-admission-final.xml`，整份结果 {"passed": 2}；本门选取 2 个已通过项。性质：`real_local`。版本依据：2b9f988 SAVE-PRODUCT-CLOSURE.json。
  - `tests/test_runtime_v3_save_product.py::test_managed_published_before_receipt_admits_only_original_native_recovery[False]`。

未覆盖：多次不同commit实跑，不是当前全产品root组合；declared图仍不能充当execution图。缺当前全套浏览器cancel/resume与复杂图同HEAD组合。

### product_regression

状态：`historical_product_subsets`。正式 route：`tests/runtime_v3_support.py:1171`，仍未接线。

- `/tmp/demiflow-regression-runs2.xml`，整份结果 {"passed": 16}；本门选取 16 个已通过项。性质：`real_local`。版本依据：REGRESSION-PRODUCT-EVIDENCE.json。
  - `tests/test_forge_runs.py::test_real_ab_receipts_case_comparison_and_fixed_continuation`。
  - `tests/test_forge_runs.py::test_prepare_does_not_execute_and_rejects_changed_basis`。
  - `tests/test_forge_runs.py::test_run_log_rotation_preserves_tail_and_reports_expired_cursor`。
  - `tests/test_forge_runs.py::test_seal_tamper_and_exact_create_conflict`。

- `/tmp/demiflow-regression-browser-final.xml`，整份结果 {"passed": 2}；本门选取 1 个已通过项。性质：`real_browser_http_sqlite_media_with_outer_service_double`。版本依据：0a20645。
  - `tests/test_forge_analysis_media_shell_browser.py::test_actual_shell_field_capture_preview_annotation_history_and_narrow_layout`。

- `/tmp/demiflow-regression-product-final.xml`，整份结果 {"failed": 2, "passed": 24}；本门选取 2 个已通过项。性质：`real_local`。版本依据：Only selected actual passes; whole report 24pass/2old-label failures。
  - `tests/test_forge_analysis_case_browser.py::test_three_regions_view_definition_changes_and_annotation_history_are_separate`。
  - `tests/test_forge_analysis_case_media_browser.py::test_media_basis_save_receipt_history_clear_revert_preserve_exact_consumer_evidence`。

- `/tmp/demiflow-product-final.xml`，整份结果 {"passed": 45, "failed": 1}；本门选取 6 个已通过项。性质：`real_local`。版本依据：eb4b50e。
  - `tests/test_operator_foundation_runs.py::test_managed_native_families[async]`。

未覆盖：不是最新同HEAD Notebook/Python/SQL/Case/开发对话全浏览器验收；media shell部分外层runtime用double。image/document Embedding工作台授权未开放，文本成功不替代。

### artifact_security

状态：`real_local_authority_and_code_subsets`。正式 route：`tests/runtime_v3_support.py:1196`，仍未接线。

- `/tmp/demiflow-growth.xml`，整份结果 {"passed": 27}；本门选取 3 个已通过项。性质：`real_local`。版本依据：Earlier candidate; not claimed to match final integrated HEAD。
  - `tests/test_runtime_v3_artifact_recovery.py::test_changed_or_missing_lock_cannot_prove_owner_exit`。
  - `tests/test_runtime_v3_artifact_recovery.py::test_unregistered_legacy_reservation_and_corrupt_publication_are_not_refunded`。
  - `tests/test_runtime_v3_artifact_recovery.py::test_begin_requires_locked_original_inode_and_intent_is_immutable`。

- `/tmp/demiflow-product-final.xml`，整份结果 {"passed": 45, "failed": 1}；本门选取 1 个已通过项。性质：`real_local`。版本依据：eb4b50e。
  - `tests/test_runtime_v3_thread_host.py::test_live_program_still_checks_real_code_pin`。

- `/tmp/demiflow-v3-contracts-full-r3.xml`，整份结果 {"passed": 564}；本门选取 80 个已通过项。性质：`real_contract_authorities_with_provider_boundary_doubles`。版本依据：clean 6b2a731。
  - `tests/test_runtime_v3_d.py::test_d016`。
  - `tests/test_runtime_v3_h.py::test_h001`。
  - `tests/test_runtime_v3_h.py::test_h002`。
  - `tests/test_runtime_v3_h.py::test_h003`。

未覆盖：没有跨真实host数据域/恶意共享挂载/网络分区完整矩阵；协议单元fake不能替代跨机权限实测。

### performance

状态：`not_passed`。正式 route：`tests/runtime_v3_support.py:1202`，仍未接线。

没有能替代该门的完整真实证据；root仍处理40k/120s压力；原31行窄链两执行147.387s。formal五类workload+单独warmup+同预算基线/峰值尚未形成。

### all_regressions

状态：`real_named_regression_sets_only`。正式 route：`tests/runtime_v3_support.py:1208`，仍未接线。

- `/tmp/demiflow-v3-contracts-full-r3.xml`，整份结果 {"passed": 564}；本门选取 564 个已通过项。性质：`mixed_real_runtime_and_unit_contracts`。版本依据：clean 6b2a731; CONTRACT-SUITES-CLOSURE.json exact original33/147 maps。
  - `tests/test_runtime_v3_b.py::test_b001`。
  - `tests/test_runtime_v3_b.py::test_b002`。
  - `tests/test_runtime_v3_b.py::test_b003`。
  - `tests/test_runtime_v3_b.py::test_b004`。

- `/tmp/demiflow-regression-original106-e6bdc2f.xml`，整份结果 {"passed": 106}；本门选取 106 个已通过项。性质：`real_local`。版本依据：ed5518c; tree exactly e6bdc2f。
  - `tests/test_stream_map.py::test_thread_map_only_overlaps_preserves_context_and_arguments`。
  - `tests/test_stream_map.py::test_thread_map_allows_async_consumer_to_release_other_workers`。
  - `tests/test_stream_map.py::test_default_map_remains_lazy_and_single_inline_stream_stage`。
  - `tests/test_stream_map.py::test_class_scope_is_lazy_recreated_per_action_and_closed[stage-1]`。

未覆盖：不是全部历史仓库；formal44分类处置清单未由本次生成；Ray全API/PG/多机/GPU不能被集合总数覆盖。

## 不能伪造正式门的返回值

独立专项虽可覆盖更广或更难的情形，仍不能为使原 gate 绿而返回没有执行依据的 `joined=9`、`final_total=10`、`rows=16`、`released=True`、三个历史 enum 身份或五类性能测量。重接时必须真正跑该场景，或先由总控明确修订具体验收契约；本报告没有进行这种修订。

特别是 `disk_gc`：artifact owner 已确认，当前只回收原 journal 登记、且有真实退出锁证明的 staging/scratch orphan；已发布对象核验后结算，但 immutable published artifacts 一律不删除。历史 pin、schema、finite seals、residency/frontier 根通过保留这些对象避免误删。没有“所有有效根均不引用”的持久内容 GC 实现或成功反例，这是能力缺口，不只是正式 harness 未接。

## 建议最终同 HEAD 组合顺序

1. 提交/恢复与控制循环：已在真实 Local/Ray 跑过的 driver SIGKILL + 旧 event 拒绝，至少保留 publish-before-receipt 窗口、native lost-reply 幂等。控制 loop/性能改动后优先重验这几个点。
2. 在途/有限输出/路由：known-inflight 原生调用、finite join 中途输出恢复、native EOI pending 恢复、durable park 前后及丢消费 ACK。明确核实际 contributor membership；不重跑整套 K/C 单元门替代这些接缝。
3. 状态/清理：partial task 的 class/closure 恢复，最新真实 process lifecycle 7 臂，外部 cancel 排空。覆盖原状态解释与新观测运输，物理退出与 actor closed 不混称。
4. 工件/磁盘：publish-before-settle 原进程退出恢复、live writer/FD 反例、native+dataflow scratch 总额。若本轮不实现 published-content GC，继续保留明确缺口，不能让前述专项抵消它。
5. 工作台真实计划与产品：封存执行图 dag_id/plan_id/原 bridge 浏览器、prepare-only 历史源码、managed cancel/resume、A/B 固定结果比较、最新原生六族/Save。Notebook/Case/开发对话按实际受影响领域和已有版本证据选择；不从引擎总数推导全产品。
JSON 的 `recommended_same_head_selectors` 已给出可直接组合的精确入口；这是候选集合，不代表已经在最新 HEAD 执行。

## 环境与能力边界

- **Ray 全 API**：现有 98 API / 168 例完整报告只证明 Local；同机 Ray 已有 native、队列、Save、分期、分块、stream 等专项。`runtime_v3_api_scenarios.api_matrix` 已通过 `_provider(fx.provider)` 支持选择后端，但尚无本报告中的完整 Ray 矩阵证据。
- **GPU**：真实资源协议的受控设备描述/可见性/退出验证，不能替代物理 GPU 模型、显存与 CPU/GPU 实际并行。配置的 Ray inventory 只允许单 GPU host，多 GPU host 是当前明确拒绝的实现边界。
- **多机/节点故障**：需要真实不同机器的 placement、读取域、共享工件、节点断失与恢复；同一台机器多个 Ray actor 不计两机。
- **外部数据库**：原 PostgreSQL fixture 需要任务私有 `DEMIFLOW_TEST_PG_DSN`，旧结果是排除/环境 skip；SQLite 事务与队列通过不能替代 PostgreSQL 的锁、租约、事务隔离反例。
- **性能**：root 原 40k/120 秒压力尚在修；原106中的31行窄链两次执行147.387秒只证明结果正确。正式五类工作负载、独立warmup、同预算基线和峰值尚未完成。

本报告仅新增文档。没有编辑旧 route、源码、主仓或其他工作树；没有开启数据库/集群，没有以硬件缺失项算通过。
