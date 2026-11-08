# Forge 第三阶段实施与验收

日期：2026-11-02。实施范围：已批准的本地同步实验、原生执行、固定结果表、运行状态与 Case 可视化。已完成测试与验收；当前版本共 309 项相关测试通过。运行、源码和分发身份见 [验收身份 JSON](forge-stage3-demo-20261102.json)。

## 用户现在能做什么

从 Flows 选择入口、参数和固定输入 → 准备并核对实际配置 → 启动 → 离开页面后重连查看 → 打开真实结果及图片 → 按业务键对照输入输出或 A/B → 把某次结果的准确版本接给下一条 Flow。多输出的部分提交会保留，取消、失败与提交待核对分别显示。默认查看服务不开放执行；必须显式 `--enable-runs`。

演示工作区为 `/tmp/forge-stage3-release-workspace`。A/B 是实际运行，不是预制对照表；`comparison_fixture` 继续只用于第二阶段重复/缺失键预览测试。

| 实验 | 固定输入 | 实际产物 | 验证 |
| --- | --- | --- | --- |
| A，阈值 0.80 | candidates@v3、scores@v2 | c01、c02，共 2 行，新表 v1 | 原生提交回执、固定版本读回、浏览器 Case |
| B，阈值 0.90 | 同一输入 | c01，共 1 行，另一张新表 v1 | 源码与输入相同，配置不同，真实 A/B 对照 |
| A 结果继续加工 | A 的准确输出 v1 | limit(1)，新表 1 行 | 上游引用为 `run:<A>:target`，另有显式别名 continued_case |

结果顺序不作为 join 的承诺。演示中的 c01/c02 是独立集合断言；业务质量判断没有自动评分。

## 架构落点

| 层 | 实施职责 |
| --- | --- |
| forge/runs.py、run_worker.py | 封存身份、准入、启动/取消/核对、独立监督进程、结果入口；不解释图或调度节点 |
| execution/checked.py、data/runtime_contract.py | 核对真实原生计划、callable 与实际参数/返回值；可选上下文，普通 Dataset 不启用 |
| execution/run_journal.py | 同一原生 journal 增加有限 CAS 状态和不可变动作事实；沿用 manifest 与结果引用 |
| lance/write.py、reconcile.py | 可选 writer 预算，create 的固定初始版本与提交标记核对；沿用原生提交算法 |
| workspace_view.py、web | UI/CLI/Python 共用运行事实；历史图、封存源码、结果分组、Case 与 A/B，不保存第二份 DAG |

工程标准更新为 `demiflow_forge_engineering_standard_v2` 内容版本 2.1.0，增加 EX-01—EX-10；FL/OP 含义与旧 v1 生命周期保留。命令采用 `forge workspace run`，避免占用已有 `forge run PROJECT --config ...`；没有兼容转译层。

## 验收矩阵

| 门禁 | 已覆盖的证据 | 通过口径 |
| --- | --- | --- |
| EX-01 身份 | 准备不执行、源码变化拒绝、无关源码不误伤、封存篡改拒绝、历史源码读取；平台/配置/固定表沿用工作区绑定回归 | prepare/start 与实际封存内容一致，不能借旧报告运行新源码 |
| EX-02 能力 | 真实多 map/filter/join、纯计算 actor；资源 hook 在准备时拒绝；纯 Lance copy、union/列变换/limit/materialize；多动作；静态未知/越界调用原有回归 | 真正调用原生 Flow；sync/create 子集之外明确拒绝 |
| EX-03 位置 | 固定输入、原表版本不变、新建独立输出、精确目标冲突；原有路径/重叠/符号链接/只读位置测试 | 不覆盖原表、不追随 head、不能写到未声明位置 |
| EX-04 端口 | 原生计划不匹配在消费源前失败；实际参数和返回类型、必需键/null/bool 区分、嵌套/超宽对象；真实值与静态推断不一致失败 | 静态通过不能豁免实际值检查，错误不伪装成成功 |
| EX-05 提交 | 丢失回执后用 create 标记恢复；即使 head 已到 v2 仍核对 v1；同一 URI/同一请求但不同 operation_id 保持未知；登记缺失补登记；无 schema 空输出失败 | 只凭可靠回执/原生提交证据登记，不重复执行、不猜测成功 |
| EX-06 停止/资源 | join 扩增、12 MB 返回值、4 GB 分配拒绝、输出行数/批父缓冲/晚批超限、scratch 超限、超时、取消、慢算子、监督进程 SIGKILL | 超限失败而非截断提交；停止所属进程、清理本次临时目录，保留已经提交结果 |
| EX-07 多动作 | materialize、共享上游、多输出、第一表成功第二表空结果失败、第二动作慢计算时取消 | 第一张实际提交可查看，整次失败/取消不会标成全部成功 |
| EX-08 结果 | A={c01,c02}、B={c01}，实际 normalized_question；固定结果继续加工；浏览器图片、匹配/缺失键、显式别名与刷新后产物分组 | 表引用、回执、元数据和实际 Case 一致；结果有明确来源 |
| EX-09 幂等 | 重复 start 返回同 run、另一运行准入拒绝、journal CAS/事件不可变、重复核对补登记 | 同一尝试不会重复计算；新尝试新 ID、新目标 |
| EX-10 可见性 | 真实 Chromium、透明 TCP 转发下 Host/Origin/带 token POST、浏览器重连、状态图与实际结果、readonly POST 拒绝、日志轮转游标 | UI 使用同一事实；未单独测量的计算进度不编造百分比 |
| 兼容与分发 | 全 Forge 回归含旧 v1；原生 Lance/普通 Dataset/物化/journal；临时副本构建 wheel/sdist、仓库外隔离安装 | 没有改共享环境、生产数据、业务仓或其他服务；包中包含 worker、标准和网页资源 |

资源验收是在自有小 fixture 与小预算下触发真实边界。不能据此声称生产大表吞吐、任意第三方代码、cgroup 配额或所有故障窗口已获证明。

## 当前边界

- 只支持本地同步计算与 create 新表；资源生命周期 actor、async、模型/Agentic、append/overwrite、远程存储、自动续跑和正式发布未开放。
- 空结果没有显式 schema 时保留原生失败。失败目标可能有未提交碎片，不作为结果表，也不自动删除提交证据。
- 每工作区单活跃运行，无排队。数据结果在 data；运行控制、有限日志与源码在 .forge/runs。结果登记不会自动改 tables，用户明确保留别名才写入。
- 模型/生产执行、环境完整离线制品、业务质量验收与 Review/Freeze 属于后续阶段。没有执行真实模型、生产 canary 或改动生产数据。
- RSS/磁盘为采样阈值，允许检测间隔内超调；虚拟地址分配保护与原生池不等于精确 RSS。输出行数上限按动作计，编码磁盘阈值按本次全部输出合计；不隐含跨表事务。
- 运行时只核对已声明的实际值与计划，不宣称证明输入不可变、线程安全或任意副作用隔离。普通 Python 的可接受范围仍由 FL/OP 检查收紧。
- `reconcile` 是用户显式动作：验证提交和补登记、清理已停止运行的 scratch；不会重算、不改变失败运行的业务结论。

## 测试和分发证据

所有计数对应最终实现源码，合计 309 项，不复用历史“1,865 项”结果。各组不重复计数：

| 范围 | 结果 | 日志 |
| --- | --- | --- |
| Forge 既有契约、v1 生命周期、平台/工作区、CLI 等 | 167 passed，338.27 秒 | `/tmp/forge-stage3-final-static.log` |
| 执行闭环、实际值边界、资源及故障 | 31 passed，544.38 秒 | `/tmp/forge-stage3-final-execution.log` |
| 纯计算 actor 实际执行与资源 hook 早期拒绝 | 1 passed，75.12 秒 | `/tmp/forge-stage3-actor-test.log` |
| 第三阶段真实 Chromium 操作 | 2 passed | `/tmp/forge-stage3-release-browser.log`；同次旧导出遇到文件已存在，单独复验如下 |
| 第二阶段 Chromium 原有功能回归 | 1 passed，66.38 秒 | `/tmp/forge-stage3-legacy-browser.log` |
| 原生 Lance、普通 Dataset、物化、旧 RunJournal | 107 passed，12.62 秒 | `/tmp/forge-stage3-final-native.log` |

主要验收使用项目现有 `../env/bin/python`，设置 `PYTHONDONTWRITEBYTECODE=1`、`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`、`OPENBLAS_NUM_THREADS=1`、`OMP_NUM_THREADS=1`，pytest 指定 `-p no:cacheprovider -p pytest_asyncio.plugin`。新增执行测试在 `tests/test_forge_runs.py`、`test_forge_run_boundaries.py`、`test_forge_run_faults.py`、`test_forge_run_cli.py`、`test_forge_run_actor.py`、`test_forge_run_browser.py`。

首次浏览器整套运行出现 Chromium 截图协议错误，单独复验通过；再次联测中旧导出的 create 防覆盖拒绝已经存在的 `flow-a.html`，改用独立产物目录复验通过。没有为通过验收放宽契约或绕过导出防覆盖。最终没有遗留未处理的测试失败。

真实浏览器经透明 TCP 转发使用不同的客户端/服务端端口，验证了实际同源 POST、关闭浏览器重连、图片、A/B 结果缺失匹配、固定结果续接、显式别名、准备页观察另一客户端启动，以及第二动作执行中取消后第一张表仍可查看。运行图不显示虚构的逐节点百分比。

分发在临时源码副本中构建 wheel/sdist；新建独立 venv，依赖路径只读复用现有环境。仓库外实际导入新 wheel，创建工作区并准备/启动 Flow，确认原生输出 2 行、回执和固定引用一致，再通过已安装 CLI 列出运行。wheel/sdist 包含运行入口、worker、原生校验、标准与网页资源；日志在 `/tmp/forge-stage3-package.log`，完整构建身份在 `/tmp/forge-stage3-package-evidence.json`。共享环境没有安装或覆盖包。

`git diff --check` 与 JavaScript 语法检查通过。开始时保留了 dirty 文件哈希与备份；最终差异仅在本阶段的 Forge、原生调用/写入/journal 接点、测试及文档，业务仓、其他已有 dirty 文件和 Notebook 输出未改动。

## 实际使用入口

本轮演示服务已启用运行，服务器监听 `127.0.0.1:59865`，沿用既有 SSH 转发至本机 `localhost:55971` 和原会话 token。打开原页面或刷新后，点“运行记录”，选择 `flows/select_a.json` 与 `flows/select_b.json`，点击“比较两次实验”；随后“查看这次结果”进入真实 Case。无需 Notebook 或 agent 解释接线。

核心演示截图：`01-prepared-run.png`、`02-result-and-input.png`、`03-actual-ab-results.png`、`04-continued-run.png`、`05-cancelled-partial-result.png`，位于 `/yzp/zhaozy/yangzepeng/0905/.codex-home/visualizations/2026/11/02/forge-stage3-delivery/`。最后一张来自独立取消故障 fixture；A/B/续接三次成功运行保留在上述演示目录，便于复查；/tmp 不承担生产持久化保证。
