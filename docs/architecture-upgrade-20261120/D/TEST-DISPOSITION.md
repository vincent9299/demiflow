# D · 旧测试处置和契约验收 v1

绑定 `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。这是设计及有限基线，不是删后验收。本轮所有产品/测试文件保持原状。

## 1. 每个批次 3 对象的测试处置

| 对象 | 当前直接测试 | 本批处置及依据 |
|---|---|---|
| NativeCommitStore | 产品/tests 对类或模块无引用；无直接行为测试 | 无旧测试可删。Protocol 删除不代表取消其列举的原生提交/预算/ACK 行为；既有 RunJournal/queue 和历史测试保持 |
| timeout | 对模块/三个公开名字无直接测试 | 全部保留；无消费者不能推翻 `__all__` 公开表面，不为小模块无用性另造行为测试 |
| unified_chain_eligible | 当前 tests 无 token 引用，不同于旧版本的 eligibility 单元 | 无旧测试可删。保留真实 chain 的 materialize、单 action、row ceiling、错误前置、commit、handle 清理及源代码固定断言 |
| dataflow_control | `test_runtime_v3_h.py::test_h045/test_h046/test_h047/test_h048` | 四项全保留，逐断言对应如下；不将整文件其他 H 测试纳入删除 |

不因历史清单中的旧统计或本批通过数量删除测试。批次 0–2 已删除的用例不恢复、不重裁；已知失败仍不是通过。

## 2. H045–H048 到生产路径的逐断言对应

生产链路固定事实：Local `_run_v3_worker` 创建 `ControlMailbox`、一个 socket reader 与有界发送；`HostedDataflowWorker.__init__` 将同一个 mailbox 作为 config 的 transport。`LocalHostEndpoint`/`_EndpointBase` 负责有界读取、allocation/attempt/boot 校验和原生 journal 服务接线；Ray 通过 `DataflowRayConnector` 与 HostedDataflowWorker 使用同种 mailbox。`dataflow_control` 的两类没有产品导入方。

| 原用例/断言 | 生产责任与已有精确对应 | 当前对应程度和本批处置 |
|---|---|---|
| H045 request 返回本 operation_id | `ControlMailbox.request/drain/deliver`；H060、H064；`test_runtime_v3_control_wakeup.py::test_sequential_rpc_wakes_socket_reader_without_manufacturing_receipts` | mailbox 行为有现成覆盖；真实 Local 入口不得只以 fake connection 代替。原 H045 保留 |
| H045 宣告过大帧在读 body 前拒绝 `BUDGET_EXCEEDED` | `worker.bootstrap.ControlFrameReader`，`test_runtime_v3_control_batching.py::test_partial_deadline_and_oversized_header_fail_before_body_allocation`；codec 的 `test_exact_escape_bytes_reject_before_any_encoding_or_decoding` | 不同 owner、可能不同错误映射；不能以 budget 这一个词就称错误码完全相同。原断言保留，将来删前核精确错误映射 |
| H045 无回执到期限为 `UNKNOWN_EFFECT` | `ControlMailbox.request`；H062；`test_runtime_v3_worker_connection_loss.py::test_real_eof_and_reset_leave_the_original_operation_unknown` | mailbox/真实连接 loss 分别覆盖等待与不确定副作用；不自动重试。不以连接错误替代 timeout，保留原 H045 |
| H046 收到其他 operation_id 回执即 `INVALID_CONTRACT` | mailbox `deliver` 对未知 id **忽略**，H064 确认 wrong reply 不唤醒；Local 生产 reader 负责把合法 reply 投递到对应 waiter | **不是一对一等价**：旧同步 transport 是直接拒绝，生产多请求 reader 是正确忽略非本请求并保持关联。删前须裁定旧实现专属断言随实现退役，且用真实交错请求证明不串回执；本批不删 |
| H046 event allocation/boot 不同为 `FENCED` | `_EndpointBase._check_event_identity` (`dataflow_hosts.py:330`)、`DataflowExecution` worker-event 检查 (`dataflow.py:891` 附近) | `ControlMailbox.request` 自身不持 lease，不能把 H046 简改为直接调 mailbox。将来需在真实 endpoint 边界注入错误 allocation/boot，断言业务/journal 未被调用；本批保留 |
| H046 close 后 check_stop 拒绝 | 生产 stop 回调/ControlMailbox.close/cancel_pending、H062/H065、control cleanup/drain 用例 | 取消和 close 的语义不同：pending 副作用仍 UNKNOWN_EFFECT，新请求 CANCELLED。原 H046 保留 |
| H047 wrapper._mailbox 是传入对象、store None、无 journal/connection | HostedDataflowWorker 自有 mailbox，无 driver RunJournal；生产 mailbox.store **有意持有 RunArtifactStore**，bootstrap 也给原生 artifact 端口 | wrapper 私有属性与真实生产装配不等价，`store is None` 不能照抄到生产。将来应断言非 RunJournal/SQLite、实际 artifact owner/预算有效；本批原测试保留 |
| H048 drain→driver deliver→request 得同 op/status；close 后拒绝 | ControlMailbox + H060/H064/H065；`test_runtime_v3_control_drain.py::test_early_stopped_reserve_returns_original_error_reply_and_releases_waiter`、`test_late_reply_drain_preserves_other_worker_boot_and_original_notify` | mailbox/driver authority 已有真实路径断言；H048 本身不启动 Ray。本批保留，任何未来删除另需真实 Ray RPC、预算和取消清理证据 |

H061–H065 的 pending ceiling、deadline、drain credit、wrong/duplicate reply、close 唤醒全部保留。控制帧 max bytes、outbox/pending 个数、同请求不重复付费、cancel/UNKNOWN_EFFECT、worker/driver 单 owner 是要保留的合同；“wrapper 必须存在”与其私有字段不是新的生产要求。

因此 dataflow_control 本阶段的完成条件是**对应已写清、差异可审、原用例保持**。没有宣称四项已经可以删除，也不把修订整个控制系统作为两个可选对象退役的前置。

## 3. 两个条件删除对象的最小验收矩阵

实施前固定 base，实施后固定候选 SHA，分别记录完整 collection/execution 与原始失败；不对历史证据归一化。

| 契约 | 精确文件/用例选择 | 预期与负例 |
|---|---|---|
| 清单真实、依赖无新增、兼容入口仍翻译 | `test_pre_v3_retention.py` 全 6 项、`test_package_dependency_ratchet.py` 全 7 项 | 清单移除和 `removed` 记录准确，混合模块无漏项，五个入口还存在；不改预算/GUARDS/FROZEN 消红 |
| 五个公开入口 | retention 的翻译守卫；`test_runtime_v3_f.py::test_f017`；`test_runtime_v3_chain_compatibility.py` 全 5 项；graph 现成 `test_runtime_v3_graph_adapter.py::test_graph_legacy_location_cannot_override_executor_domain` 与 `test_real_graph_preserves_caller_and_run_bounds` | 保留公开签名；真实 action 只一次；foreign source/plan/provider 在资源 owner 初始化前拒绝；无目标依赖 |
| 预算、结果、释放 | chain compatibility 的 `test_chain_returns_repeatable_materialization_and_closes_its_runtime`、`test_chain_keeps_the_original_row_ceiling` | 输出相同且可重复读、ACK 存在、关闭原 runtime；宽单行拒绝、失败无 commit；不扩大 memory/row/batch 上限 |
| v1/v2 历史只读/原环境恢复 | `test_runtime_v3_history_protection.py` 全 5 collected（含 with_image 参数） | 旧 prompt golden 不变；篡改 golden 不能通过；缺 image/adapter 明确不支持；原 `b4dd417...` native fixture 在原环境读/恢复，旧工件摘要不变 |
| 当前身份拒绝 | `test_execution_recovery_identity.py` 全 7 collected | same input stable；source 变更同 run 恢复拒绝；超限/未知对象 fail closed |
| chain 活代码固定工具 | `test_code_pin_serialization.py` 全文件，尤其 default 未改/修改同 run、worker load gate、cell capture、by-value roundtrip | 未改正常；改默认值或 helper 明确拒绝；保留路径、code_record、闭包和 seal 行为。不能因删的是闲函数就声称此文件不受影响 |
| Ray 重复门槛 4/9 | env-ray 中 private Ray fixture + chain compatibility；`test_runtime_v3_ray_terminal_seams.py::test_final_ack_replay_without_worker_or_new_commit`、`test_iter_rows_close_keeps_real_nonfinal_prefix` | actual provider 为 ray_core，真实 ray 模块/Actor/ObjectRef，无 ray.data，completed replay 无新 worker/commit，早停保留非 final ACK/cursor 并释放资源；同进程 stub 不计 |
| 冻结与历史数据不变 | 七件摘要；fixture 内文件哈希前后比较 | 七件相同，旧源/计划/日志/结果不覆盖；不写业务根 |

Ray 的明确实施装配：解释器采用已发布回执中的 `/yzp/zhaozy/yangzepeng/0905/env-ray/bin/python`；复用 `test_runtime_v3_ray_terminal_seams.py::private_ray_provider`（由其 `_provider('ray_core')` 管理 address=local、num_cpus=6、唯一 /tmp 根）。chain compatibility 本身只看 `DEMIFLOW_V3_PROVIDER`，**不能只设置 env 就声称建立了私有 Ray 域**。实施时用本任务证据目录的薄 pytest 插件激活现有 private fixture，并在该 fixture 生效后再设 provider；不将 `test_runtime_v3_h.py` 的 fake Ray 与 actual Ray 放同一进程。若 fixture 未成功、已经连外部 cluster、Ray 缺失或 skip，则对应门槛不通过，保留对象。

精确 Ray nodeid 清单/插件会在获准实施、协调窗口时固定；本轮未执行，未编写产品/测试实现。既有原始回执不当成当前删后的 Ray 验收。

## 4. 本轮有限基线实况

源码 SHA `122edf4bd0c827338846abd0114f0b36cf8c1b6d`，34 collected/executed/passed，0 failed/error/skipped，分别 13/4/5/12。实际执行器通过 subprocess 使用 `env/bin/python -B -m pytest -q -p no:cacheprovider <上述组> --basetemp <独立根>/pytest --junitxml <共享证据>/<组>.xml`；组间顺序执行、独立 domain，无模型/生产/Ray。

实际完整命令、env、解释器版本、pytest/cloudpickle/pylance/pyarrow 版本、导入来源、时长、用例结果在共享 `test-receipts.json`；原 log/xml 全保留。设计文档提交不影响这 34 项的产品/测试源字节，回执仍绑定原 base，不改填设计 SHA。

未验证：删后前后对照、当前跨进程函数测量、actual Ray、完整四线、全业务/Notebook、GPU/多主机。计划中的其他用例没有算进 34。
