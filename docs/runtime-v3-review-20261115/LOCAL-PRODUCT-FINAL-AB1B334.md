# ab1b334：Local API 与真实工作台最终验收

固定根候选 `ab1b33442685d09257b1ebc46e55fb40262317de`，隔离分支测试 HEAD `799f233101e2b6aaf3907b8ec567fdb4309a8cf2`。两者 `demiflow/`、`tests/` 字节完全相同。本轮 **4 passed、0 failed、0 skipped，547.35 秒**；2105 个源码、测试、页面与夹具输入摘要前后全等，HEAD 未变、测试后全树干净，七份冻结基础文件与 `49b666f` 一致。没有改断言、输入规模、预算或原 600 秒 action 时限，没有在主仓测试或写入。

| 原节点 | 实际结果 | 含夹具耗时 |
|---|---|---:|
| `test_runtime_v3_acceptance.py::test_api_matrix` | 98/98 API，168/168 场景，无 failed/not_run | 369.056 秒 |
| `test_runtime_v3_acceptance.py::test_no_ray_data` | 真统一执行，零 Ray Data 导入尝试 | 11.402 秒 |
| `test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge` | 真运行、原封存计划、封存时间、节点观测映射 | 87.199 秒 |
| `test_gpt_write_recovery.py::test_workbench_cancel_resume_golden` | 实际停止→同运行恢复，原结果与历史桥保持 | 76.478 秒 |

## 对两大原始意图的证据

**数据流引擎。** 全部 168 场景真实跑完，包含支持能力正向、非法输入拒绝与四个明确退休入口。去重后是 **107 次真实 `execute_action`**，outer 与 case 两层各 107 条是同一批事件，不能累计成 214 次。no-Ray-Data 节点另有 2 次真实调用。两组 `ray_data_import_attempts=[]`；408 个 DemiFlow 模块来自本隔离树，0 个平台模块测试替身。外部模型 transport 仍为允许的本地夹具，不冒充真实付费服务。

**产品。** 浏览器选择流程和参数，真实检查并启动执行，然后读到该次运行的 v3 封存计划；页面 dag_id、plan_id、封存时间和报告节点观测对应原 journal。三张原截图保留。停止恢复 golden 保留原 DAG、report bridge、固定版本结果与原生发布语义。

只读原账本进一步确认：

- 浏览器运行 `re84760530699424081a94a1d`：attempt 1 succeeded，cleanup complete、usage_complete=true；两次 ACK 分别是零输出非 final 进度和最终两行发布；17 条 GC intent 全 done。
- 停止恢复运行 `r89b14f1acbbb4e7ab42a9028`：attempt 1 cancelled，attempt 2 succeeded；两次 usage_complete=true、cleanup complete；两次 ACK 同样仅一次实际原生发布；20 条 GC intent 全 done。
- 两个真实 Lance 目标都仅有版本 1，固定版本均为预期的两行业务结果，没有把中间零行进度 ACK 算成第二次外部发布。

这轮使用包含批转换预准入、终端实际读取/解码窗口、显式终端停止授权、提前关闭收尾修复的同一个候选；没有借旧候选的绿色代替当前结果。

## 可复核证据

只有在总控明确放行、且 `/tmp/demiflow-final-v3-pressure-r4-evidence/verification.json` 实际满足同候选 head、exit_code=0、changed_inputs=[] 后才启动，未与原 40k 压力抢占同机资源。该压力结果独立统计，本报告不将它的数量计入四节点。

- 运行器：`/tmp/demiflow-regression-final5-runner.py`；启动脚本同前缀 `-launch.sh`。
- 全部绑定与 XML：`/tmp/demiflow-regression-final5-evidence/verification.json`、`result.xml`。
- 168 场景与唯一观察身份：`.../test_api_matrix/api_matrix.json`。
- 原始日志：`/tmp/demiflow-regression-final5.log`。
- 原产品全表只读导出：`.../product-original-journals.json`；原运行目录、代码/工件/日志及固定 Lance 结果已复制至 `.../product-originals/`，另有一致性 SQLite 备份。2844 个原运行文件、71,893,493 字节逐文件核对摘要；索引为 `.../product-original-copy-index.json`，输出 Lance 四文件另有独立摘要。
- 三张截图：`/tmp/demiflow-regression-final5-browser/screenshots/`。
- 同名 JSON 报告含 2105 个输入摘要、完整命令/环境、逐节点结果、168 场景、107 次执行身份、原账本摘要与证据文件摘要。

FD 夹具矩阵 54→60，no-Ray-Data 60→60，均满足原 +8 上限；两个测试拥有的临时 scope 均已清理。这里不声称进程级零残留句柄。旧 final3 的 3 passed / 1 failed 和 final4 的 4 passed 原证据均保留，未覆盖、未重标。

## 结论边界

本轮证明列出的完整 Local API 与产品计划/控制闭环，不据此宣称两大 root 100%。同机 Ray 的结果需引用其独立证据，不能冒充物理多机、真实 GPU 和整机故障恢复；原压力通过也不是所有工作负载性能比较。当前管理入口 image/document embedding 的显式对象读取资源接线仍有限制。Notebook/Jupyter/Case 与历史原环境恢复有独立既有实证，但本轮没有重跑全部 SQL/开发对话浏览器，也不宣称任意旧状态可由当前 v3 跨 revision 继续执行。正式剩余 NotRun 门与已存在专项证据须分开列示，不能互相替代。
