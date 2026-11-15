# 产品真实链路与历史回归收口

135 个不同产品测试 nodeid 的最新结果均通过，无 skip/xfail。它们是分批验证，**不是一次在最终 HEAD 上运行全部 135 项**。逐项最新证据、各批次提交状态、日志/JUnit 摘要及当前相关源码摘要见 `REGRESSION-PRODUCT-EVIDENCE.json`。主控仍负责各路合并后的组合验收。

工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`。分支：`engine/gpt-v3-regression-20261115`。主仓、生产 console、生产项目配置未修改，无 push/合并主仓。承重七件与 `49b666f` 字节一致。

## 实际修复

1. `6198c33`：Queue/Producer 的下一次 pull 已确认上条记录按序交付时，清理内存中的 `_pending_record`。否则后续返回 pending/EOI，snapshot 会重新封存旧记录，恢复时重复回放。native lease/claim 引用仍保留，不授予新 lease、不自行 ACK。四条新反例修前全部失败，修后 S 全组 55 passed；日志 `/tmp/demiflow-regression-pending-{red,green}.log`，同名 XML。
2. `0a20645`：v3 `seal_dataflow` 原 journal facts 缺 `sealed_at`，真实成功 run 的工作台因此显示“未提供”。首次封存现在记录 epoch milliseconds；重封存复用原值；旧 v3 缺字段的历史保持缺失，不回填。图、physical、program、code-pin 等工件身份均不包含时间。新反例修前 1 failed / 1 passed，修后元数据 2 项 + D 全组 17 项 + 工作台封存/桥契约 29 项共 **48 passed**。证据 `/tmp/demiflow-regression-seal-{red,green}.{log,xml}`。
3. 浏览器验收直接将页面 `dag_id`、`plan_id` 与原 journal 引用读取出的封存计划比较，将 ISO 时间与原始 epoch-ms 精确比较；继续要求真实结果为 2 行、报告节点可定位、执行观测存在、源码可读。没有以修改文案代替接线证明。

## 测试接线修正（不改变产品语义）

| 原问题 | 修正与保留断言 |
|---|---|
| `test_forge_runs` 临时项目落进机器已有部署域，因固定工作区白名单被拒 | root 夹具设置自身 `/tmp/demiflow-*` 部署域。未改业务预算、partition、宿主准入或生产白名单。原始默认预算下 16 项真实运行测试全部通过。 |
| Case 的题目编辑已归“配置评测”tab，清除操作已归历史折叠栏，旧测试直接操作隐藏控件 | 按真实用户路径打开控件；保留答案版本、旧题历史、clear/revert、精确媒体依据、草稿、窄屏与无新执行断言。 |
| media shell 的显式 `WebService` double 未实现新增只读 `execution_profile`，导致 context 500 | double 明确返回 None（夹具不提供执行后端）；真实 HTTP/SQLite/媒体读取继续执行。 |
| 保存视图入口显示可读名称，旧测试要求正文显示内部 view id | 校验实际链接解码后的完整 view id/revision，保留 v1 提示、真实采用后的原字段标题与 v2 标题不存在、固定 source digest、不重跑生产者等断言。 |
| 图浏览器旧期待“结构校验通过” | 迁移到当前“封存计划结构”语义，并增加原 journal 身份和时间精确比对；不恢复误导性标签。 |

## 分批证据

| 批次 | 结果与范围 |
|---|---|
| `/tmp/demiflow-regression-product.{log,xml}` | 58 passed / 11 failed。11 项均为临时 workspace 被已有部署白名单拒绝，尚未进入执行。此批保留为诊断，不作完整通过证据。 |
| `/tmp/demiflow-regression-runs2.{log,xml}` | **16 passed，440.22s**。默认业务预算：真实 A/B 结果和比较、固定输出版本续跑、准备不执行、漂移拒绝、封存篡改、materialize/union/独立输出、部分提交、空输出失败、输出行预算、取消/超时、丢失 receipt 重整与原版本读取。 |
| `/tmp/demiflow-regression-media.{log,xml}` | 41 passed / 3 failed；三条旧夹具问题见上表，后续精确复验闭合。包括真实 PNG/WAV 解码、固定范围读取、历史代次、媒体权限/来源、Case 浏览器。 |
| `/tmp/demiflow-regression-product-final.{log,xml}` | 24 passed / 2 failed，531.43s。在引入 fetch/finite-residency/artifact-recovery 后复验：真实 run 源码、Case clear/revert、Jupyter MIME、Notebook 草稿/历史/权限、SQLite 事务、默认 materialize/union。两条余项为图旧标签和媒体旧内部 id 文案，另定位并修复了真实缺失的封存时间。 |
| `/tmp/demiflow-regression-browser-final.{log,xml}` | **2 passed，219.55s**。当前生产提交 `0a20645` 的真实 prepare/check/start/show → 封存计划 → 原 journal 身份/时间 → 节点观测；完整 Chromium + HTTP + SQLite + 原生媒体读取 → 标注历史 → 固定 v1 视图链接。 |

浏览器截图：`/tmp/demiflow-regression-browser-final-evidence/screenshots/`；真实 Jupyter 输出证据：`/tmp/demiflow-regression-product-jupyter-evidence/`。所有进程均断言 `demiflow.__file__` 属于本工作树；测试在运行期间没有修改同树 Python 或提交。

执行统一使用 `PYTHONPATH=$PWD:$PWD/tests /yzp/zhaozy/yangzepeng/0905/env/bin/python -B` 调用 `pytest.main`，`DEMIFLOW_RESOURCE_ROOT`、`--basetemp`、日志/JUnit、独立部署域均为上述 `/tmp/demiflow-regression-*` 前缀。Chromium 显式使用 `/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome`，没有因缺浏览器跳过。最后两项的完整节点是：

```text
tests/test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge
tests/test_forge_analysis_media_shell_browser.py::test_actual_shell_field_capture_preview_annotation_history_and_narrow_layout
```

## 对原始意图的证明边界

- **I12 / 产品真实计划**：浏览器所见的图与实际执行的 v3 dag/plan 身份一致，时间来自封存事实；真实报告节点、参数/源码与已有观测可达。
- **I07/I13 / 恢复与历史**：队列 pending 保留与确认边界双向有反例，恢复不复制已交付记录；既有提交版本、丢 receipt 重整、原历史事实保持；元数据升级不重写历史。
- **R1–R5 产品回归**：真实运行及固定结果、Notebook 草稿/历史、Case 定义与答案独立版本、媒体依据与权限继续通过。
- media shell 明确使用 Runtime/WebService 测试 double；真实的是 Chromium、HTTP、SQLite、PNG worker/固定媒体读取及交互，不宣称部署 OCI/cgroup 已验。私有 Jupyter 是真实内核协议测试，不冒充完整生产 Notebook 部署。未访问真实外部模型、业务数据库或多机 Ray。本报告不把引擎接线验收替代整个产品 root 的 100% 交付。
