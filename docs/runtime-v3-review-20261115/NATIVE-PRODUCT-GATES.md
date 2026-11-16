# 历史与产品正式验收接线

本轮复用了真实服务、私有 Chromium/Jupyter、原 Dataset、原 RunJournal/ACK/固定 Lance 版本，没有以组件常量代替产品路径。固定版本与每项证据 SHA-256 见 `NATIVE-PRODUCT-GATES.json`。

| 正式门 | 固定版本与结果 | 实际范围 |
|---|---|---|
| history_identity | `5fefe8b`，1 passed / 15.52s | prompt enum 1024/1025/1500 原黄金身份；旧原生运行的完成/中断载体，当前环境诚实拒绝不支持的恢复，原环境续跑原 ACK、固定输出不重复；读取不改旧字节 |
| product_regression | `8ace8c4`，外门 1 passed / 265.81s；内部 10 passed / 254.83s | union+嵌套 materialize 实际执行、两份封存计划/身份桥；真实 Jupyter SQL 编译→BrokerClient→原 native query/DataFusion→Ingress→TableHandle；MIME；Notebook CAS/响应丢失/重启历史；Case 全量/1e-6筛选/分组排序/固定视图重载/移动布局；开发对话导航、权限、diff和生命周期 |
| workbench_real_plan | `8ace8c4`，内部 5 passed / 1 failed，外门 failed / 577.86s | 已实证原封存 E2、真实 bridge、CPU profile/exchange/recovery 显示、多 run 切换/过期响应隔离；单失败在 stop/resume 完成后的历史日志导航，详见下文；完整 GPU 显示仍未覆盖，不能计为正式门通过 |

三个正式运行均锁定 2126 项源码/测试摘要，测试前后及 HEAD 一致；冻结七件等于 `49b666f`。不是把早先测试重新绑定到较新交付 HEAD。

## 封存事实证明

新增 `read_native_execution` 重读 owned run 的原 manifest、attempt、候选平台源码摘要、v3 graph/physical、原 action/ACK、原 write receipt、不可变 DatasetRef/Lance 版本与真实行、report_digest/桥上的节点。父进程再次独立重读验证。空证明、错 run/attempt/plan/report/receipt/源码摘要和真实封存文件篡改均拒绝，两个实际运行反例变体及历史门探索验收为 3 passed / 100.32s。

受管子进程使用封存 `-I` 入口，因此没有伪造 `execute_action_calls`。只有产品两个正式门可消费单列的 `original_native_execution_evidence`；其他门仍要求真实 execute_action 探针。Ray Data 禁用探针只覆盖测试进程，不能据此声称封存子进程实测零导入；子进程以原 v3 事实证明，完整禁 Ray Data 门另行负责。

SQL 路径外层权限及私有路径映射是明确的测试 broker adapter；实际内核、SQL 编译器、查询、传输、Ingress、Lance 和结果展示都执行。Case 使用原 analysis-helper 权限传输 adapter。开发对话只有外部模型 transport 是确定性替身。不把这些范围冒称完整 B1/OCI 或真实模型服务验收。

## 失败、修正与未完成项

正式红证据保留在 `/tmp/demiflow-product-formal4-workbench_real_plan-evidence/scenarios/workbench_real_plan-child.json`，含完整原 traceback。stop/resume 用例已完成：删除当前源码后停止、原部分输出保留、恢复新 attempt、再次停止、选择 attempt 1 历史只读且不能停止。唯一红点是第 150 行：从“输出结果”页直接点只存在于“运行监控”的“运行日志”。`333fd18` 仅补先切“运行监控”，原 nodeid 和全部停止/恢复/输出/历史断言保留；本文件落盘时该单例尚待独占窗口复验，不算绿。

Case 原用例此前点击旧“配置选样条件”入口失败。修改仅使用当前筛选、分组和排序入口；查询实际已得到 9 项且首项 Case 22。当前产品有正向测试锁定“保留被过滤掉的当前草稿”，故显式点击 Case 22 后继续原详情/reload/390px/320px 断言。24/24 全量、1e-6、9 项、22、固定视图和布局断言均保留，单例 1 passed / 51.70s，随后已进入上述完整产品门通过。

探索中的错误没有隐藏：Ray 私有环境缺 Jupyter/Forge拒绝 symlink metadata；proof collector 初版误用 DatasetRef.uri 和把 driver-resources 目录当 run；临时 runner 不使用 `-P` 时误发现 `/tmp/ray` namespace。修正测试工具后重新验收。正式失败日志保留 `/tmp/demiflow-product-*`，没有降低门阈值。

工作台 GPU 当前仍被真实入口拒绝：静态 backend_options、checked 参数绑定、受管进程部署 inventory/共享资源根、thread host 与 CUDA 虚拟地址 guard 都需接通。总控已授权下一窄修，沿原 DevicePool/resource scope，CPU 旧路径和历史 manifest 摘要保持；本轮 CPU 图和资源展示不替代 GPU 正向证明。

## 集成

提交 `73bd331`、`a218d1c`、`5fefe8b`、`8ace8c4` 为本轮 gate/test 实现；`333fd18` 为待复验导航单行修正。本轮这些提交不修改生产引擎文件。主仓、共享安装、在运服务未作为测试或写入目标；没有 push 或合主仓。
