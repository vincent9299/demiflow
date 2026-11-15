# 产品原意图专项复核与历史门补充

本批从两大 root 的实际含义出发：用户看到的必须是原执行计划和原结果；停止/恢复必须沿原运行权威；旧 Notebook、Case、数据版本和业务身份不能被新实现覆盖。没有再跑完整 API 矩阵，也没有因正式门未接线而用常量填绿。

## 实际产品结果

固定 `73304ccecf90ca7a23ff36999e52e312d73c8aff`、1763 个源码输入摘要前后相等，原浏览器/产品七项执行 **6 passed / 1 failed，396.42 秒**。完整原始失败保留在 `/tmp/demiflow-regression-product-intent-audit.xml`，同名 log，绑定证据在 `/tmp/demiflow-regression-product-intent-audit-evidence/verification.json`。本批没有修改六项已通过测试。

已通过的精确节点：

- `tests/test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge`：实际运行、原 sealed DAG/plan、时间、原身份桥；旧“封存时刻缺失”浏览器红点已关闭。
- `tests/test_forge_analysis_media_shell_browser.py::test_actual_shell_field_capture_preview_annotation_history_and_narrow_layout`：实际浏览器的字段、媒体、标注、Case 历史和窄屏；原内部 view ID 显示期待已按真实用户显示语义迁移。此套外层 Runtime/WebService 部分是明确的测试替身，不冒充完整生产 Notebook 后端。
- `tests/test_forge_analysis_notebook_history.py::test_reopened_notebook_discovers_old_generation_without_execution_or_relabeling`：重开读取原 generation，不重新执行或重新标识旧结果。
- `tests/test_forge_analysis_notebook_history.py::test_history_does_not_cross_project_or_revoked_membership`：项目/撤销权限边界保持。
- `tests/test_forge_outputs_jupyter.py::test_real_private_jupyter_mime_output_projects_to_same_typed_renderer_contract`：实际私有 Jupyter kernel 输出，不把返回预制 MIME 的替身算作 kernel 运行；不证明生产 OCI 隔离。
- `tests/test_forge_runs.py::test_real_ab_receipts_case_comparison_and_fixed_continuation`：实际 A/B 收据、结果比较及固定版本续算。

浏览器二进制使用原已安装 Chromium，截图明确写新 `/tmp/demiflow-regression-product-intent-browser`，Jupyter 证据写新 `/tmp/demiflow-regression-product-intent-jupyter`；没有覆盖旧截图、触碰共享服务或安装依赖。

## 唯一旧红：恢复多了一条合法空进度 ACK

`tests/test_gpt_write_recovery.py::test_workbench_cancel_resume_golden` 实际已恢复 succeeded，输出两行正确，原 seal 和 bridge 正常；失败点仅是旧 `len(execution_commits)==1`。

逐条原 journal 取证：第一条为零行、零字节、空 partitions、`final=false` 的驻留进度，`prepared.native=None` 且没有 `native_receipt`；第二条是唯一 final ACK，包含唯一原生 Lance 发布。实际 Lance versions 恰为 `[1]`，固定 v1 两行与原业务期望完全一致。

测试改为直接保证原本要保护的“只发布一次”：所有 cut 已 ACK、恰一个且最后一个 final、final 包含唯一原生 receipt、所有中间 cut 严格零行/零字节/空 partitions/无原生发布；真实 Lance 仍只有 v1，固定 v1 的输入/写出条数和原业务值不变。原 dag、report digest、bridge、停止/恢复控制与原结果断言全部保留。未更改预算、输入、超时或生产提交行为。

最终与历史保护专项一起运行：`/tmp/demiflow-regression-golden-r1.xml` **7 passed，85.57 秒**。1766 个源码输入前后摘要一致，完整命令/实际 import 路径/固定 HEAD 见 `/tmp/demiflow-regression-golden-r1-evidence/verification.json`。不把前一轮整组红改写成当时全绿。

## 正式 17 门的补充与纠错

原 `FORMAL-GATE-EVIDENCE-MAP` 是旧版本证据映射，不是当前完成率。其三个优先域按本批更新理解：

| 门 | 新核实的事实 | 当前边界 |
| --- | --- | --- |
| history_identity | 1024/1025/1500 的真实旧 Prompt 黄金已找到并复现；固定旧 Git 环境产生原生两行 run，当前只读与跨 revision 拒绝不改任何历史字节，同原环境/RunRef 恢复成功。 | 当前 native adapter 不能证明可续算，已修报告精确拒绝。C12 允许原环境恢复；不扩写不可靠转译去取正式门绿。详见 HISTORY-PROTECTION-AUDIT。 |
| workbench_real_plan | 两个原浏览器红点在本候选真通过；恢复 golden 保留唯一外部发布、原 DAG/bridge 与固定结果。 | 非全产品同 HEAD 浏览器控制矩阵；不能由少量正例推断任意复杂图/全部宿主。 |
| product_regression | Notebook 原历史/权限、实际私有 Jupyter、A/B 固定版本与 Case/媒体重新实跑。 | SQL/开发对话全浏览器及全部部署域未在本批重跑；不能把 shell 外层替身当真实生产隔离。 |

另一个真实产品限制仍在：`embeddings.authoring.analyze_embedding` 只允许管理入口 text 模态，image/document 需要显式封存的对象读取资源绑定。底层引擎支持这些模态并不自动授权工作台任意 URI；现有 `workspace.resolve_flow_resources(kind=objects)` 到原 Embedding image/document 读取点的接线尚未完成。本批不放开守卫，也不把文本模型正例算成所有模态交付。

原 map 的 queue 推荐 selector 需要使用本次此前已交付的新参数化节点：

- `tests/test_runtime_v3_fetch_graph.py::test_fetch_recovers_original_native_receipt_across_dataset_cut[before_first_cut]`
- `tests/test_runtime_v3_fetch_graph.py::test_fetch_recovers_original_native_receipt_across_dataset_cut[after_empty_residency_cut]`

旧历史 XML 中的原 nodeid 必须保留，不回写成新名字。其余正式门（真实多 host/GPU、节点失效、全面公平/峰值、完整回归等）沿原明确证据边界；本批不重复计数，也不据旧 map 推断主控最新 GC、Ray 或 40k 压力工作仍未完成。

本批没有主仓改动、没有主仓测试、没有 push。七份承重文件与 `49b666f` 字节相同。配套 `PRODUCT-INTENT-AUDIT.json` 记录报告摘要和所测输入，历史生产修复与本产品测试迁移分提交交付。
