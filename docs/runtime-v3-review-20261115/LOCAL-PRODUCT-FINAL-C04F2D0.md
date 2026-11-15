# c04f2d0：Local API 与真实工作台最终验收

本轮在隔离分支 `engine/gpt-v3-regression-20261115` 完成，**4 passed、0 failed、0 skipped，542.25 秒**。生产候选为 `c04f2d0e2b2e13836ce763f17117d8754e0d01ea`，实际测试 HEAD 为 `0491e98c0cdb0896e241012565caf90ed9e4ace8`；两者的 `demiflow/`、`tests/` 字节完全一致。测试全程 HEAD 不变，2097 个输入摘要前后一致，测试结束时工作树干净，七份冻结基础文件与 `49b666f` 逐字节一致。没有修改断言、输入规模、资源额度或 600 秒 action 时限，也没有在主仓执行或写入。

| 原验收节点 | 结果 | 含夹具耗时 |
|---|---|---:|
| `test_runtime_v3_acceptance.py::test_api_matrix` | 98/98 API tested，168/168 正反例 passed | 361.230 秒 |
| `test_runtime_v3_acceptance.py::test_no_ray_data` | 真实统一执行，零 Ray Data 导入尝试 | 11.270 秒 |
| `test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge` | 实际成功运行、真实封存图及节点观测绑定全部通过 | 91.054 秒 |
| `test_gpt_write_recovery.py::test_workbench_cancel_resume_golden` | 实际停止→同运行恢复→原始固定结果与历史桥全部通过 | 75.762 秒 |

## 两大原始意图的实际证据

**数据流引擎。** 原 98 API 矩阵完整执行 168 个场景，包括支持能力的真实成功臂、非法输入的拒绝臂和四个明确退休入口。去重后共有 **107 次真实 `execute_action`**；outer 的 107 条和逐 case 的 107 条是同一批调用的两层观察，不能再记为 214 次执行。原 no-Ray-Data 节点另有 2 次真实调用。两者 `ray_data_import_attempts=[]`；进程内所有 DemiFlow 模块均来自本工作树，分类为 406 个真实模块、0 个测试替身。外部模型仍为测试契约允许的本地 transport，不冒充真实付费模型服务。

**产品。** 浏览器通过页面选择原流程和配置、执行检查、真正启动流程并取得成功结果，再读取该次执行的 v3 封存计划。页面实际显示同一 `dag_id`、`plan_id`、合法封存时间，以及已绑定报告节点和已有观测，三张原截图均保留。停止恢复 golden 保留原业务结果、原 DAG 与 report identity bridge；原生 Lance 只发布版本 1，最终 ACK 唯一，合法中间空 ACK 不被算成第二次外部发布。

## GC 导致误标失败已在真实产品链路复验

旧候选 `55acec6` 的原始结果仍为 **3 passed / 1 failed，594.09 秒**；其现场未覆盖或重标。旧真实运行已发布结果，却因监督进程 `record_usage` 竞争 GC 对象独占锁抛出 FENCED，被错误标记为 failed。

本轮使用包含 `4be72a1` 共享租约有限等待及 `c04f2d0` 终端收集预算修复的完整候选。只读原 journal 进一步核对：

- 浏览器运行：attempt 1 为 `succeeded`，`usage_complete=true`，两个 ACK（中间零行、最终两行），原生 receipt 为 committed version 1，17 条 GC intent 均 done。
- 停止恢复运行：attempt 1 为 `cancelled`，attempt 2 为 `succeeded`，两次 `usage_complete=true`；两个 ACK、原生唯一版本 1，20 条 GC intent 均 done。

因此，本轮既保持实际数据提交，又恢复了正确产品状态。独立锁竞争正反例及更广 GC 验收另见 `GC-SUPERVISOR-CONTENTION.md`，本报告不重复冒领那些测试。

## 验收纪律与证据位置

仅在 `/tmp/demiflow-final-v3-pressure-r3-evidence/verification.json` 出现且确认 `head=c04f2d0...`、`exit_code=0`、`changed_inputs=[]` 后，才启动本轮四节点；没有与原宽数据压力测试争抢同机资源。压力性能结论由该原始证据独立说明，本次不把其测试数量计入上述四节点。

- 运行器与完整环境：`/tmp/demiflow-regression-final4-runner.py`，`/tmp/demiflow-regression-final4-evidence/verification.json`。
- 原始输出与 XML：`/tmp/demiflow-regression-final4.log`，`/tmp/demiflow-regression-final4-evidence/result.xml`。
- 全部 168 case 与观察身份：`/tmp/demiflow-regression-final4-evidence/test_api_matrix/api_matrix.json`。
- 原产品账本只读提取：`/tmp/demiflow-regression-final4-evidence/product-original-journals.json`；原运行目录留在 `/tmp/demiflow-regression-final4-tests/`。
- 浏览器截图：`/tmp/demiflow-regression-final4-browser/screenshots/`。
- 同名 JSON 报告保存 2097 个输入摘要、原命令、每节点结果、168 个 case 名称、原账本关键字段及原始证据文件摘要。

API 夹具 FD 从 54 到 60，原 no-Ray-Data 夹具保持 60，均通过原 +8 上限；两个夹具拥有的临时 scope 已由原 teardown 清理。这里不声称进程级零残留句柄。

本轮结论限定于列出的完整 Local API 与两条产品链路。Ray、多机、真实 GPU、全部正式门的覆盖需看各自证据；已有 Notebook/Jupyter/Case 专项未无故重跑，也不据这四项宣称整个产品 root 100%。
