# Forge 通用算子基础能力：实施与验收

日期：2026-11-02。对应已批准的 [中粒度方案](forge-operator-foundation-design-20261102.md)、[源码级方案](forge-operator-foundation-source-plan-20261102.md) 与 [UC 验收矩阵](forge-operator-foundation-review-20261102.md)。这是实施自审与实际测试记录，不是独立第三方审计，也不是生产放行。

## 1 用户现在可以做什么

1. 在 `flows/` 用原生 Dataset API 接线，在同目录配置不同参数；`operators/` 仍是有明确类型的普通函数或 actor，不需要平台基类或注册装饰器。
2. 对同步、异步、展开、批处理、actor、关系操作、固定 Lance lookup 和有界 HTTP JSON prompt 使用同一检查、准备、执行、结果入口。未知 API 不会因出现在目录中就获得执行许可。
3. 在运行图上点击节点，按动作及执行次数查看实际输入/输出/调用计数、处理耗时与生命周期。融合组耗时和无法测量的数据分别标明，不伪造节点进度。
4. 从新写出的固定版本结果表逐条看 Case、与输入或另一实验按业务键对照、继续作为下一次实验输入。模型 Case 保留原生引用时可只读打开准确请求/响应/错误。
5. 从浏览器、CLI 或 Python 获取相同事实。浏览器关闭、SSH 重连不依赖 agent 的对话状态；Notebook 可以使用，但不承担接线标准或必需的可视化职责。

可重跑的 [完整示例](../examples/forge_operator_foundation/README.md) 在新目录创建 3 条输入，经 `flat_map` 生成 6 条，由普通异步 actor 标注，再物化、写出并独立核对结果。实例数据与测量原始数值保存在 [验收数据](forge-operator-foundation-demo-20261102.json)。

## 2 模块、依赖和实际数据流

| 层 | 最终职责 | 边界 |
| --- | --- | --- |
| `data/callable_contract.py`、`data/flow_contract.py` | 类型化默认值、真实签名绑定、参数与字段语义、可声明 callable 端口 | 不解析工作区，不执行候选代码，不注册业务算子 |
| `lance/flow_contract.py`、`operator_llm/flow_contract.py` | 各自原生配置/资源/字段语义，以及从实际原生 actor 读取事实 | 不依赖 Forge；不知道 FL/OP 编号或用户项目 |
| `execution/capabilities.py`、`plan_facts.py`、`checked.py` | 后端组合检查；实际 source/plan/callable 与封存事实核对；值校验和观察身份 | 不解释第二份 DAG，不调度另一套执行器 |
| 原有 local/stream/stream_map/stream_resources | 真正执行、所有权、初始化/排空/清理、背压和有界计量 | 继续使用普通 Dataset、现有 actor 生命周期及原生资源实现 |
| `execution/run_journal.py` | 动作与节点观察事实；观察身份/空间预留、终态和乱序保护 | 不复制模型请求账本或 Lance 提交账本 |
| Forge checker / workspace / runs | 候选源码约束、表/文件位置授权、封存、准入、监督、结果登记 | 不靠 API 名字分派业务，不把静态 checked 当作运行证据 |
| CLI / workspace HTTP / web | 读取上述事实；显式准备、开始、取消、核对；结果和证据查看 | GET 不启动服务、不执行算子、不重发模型请求 |

数据流：真实公开 API → 类型化签名与原生契约 → 静态 Flow/Operator 检查 → 有界资源解析与固定数据绑定 → 准备运行及观察空间预留 → 原生实际计划核对 → 现有执行器 → 原生表回执/请求账本 + RunJournal 观察 → 页面/CLI/Python。

静态与运行时独立：实际事实从真实原生对象读取，不能用报告中预期 metadata 自证。候选源码、构造器和条件函数在静态阶段均不导入执行。原生层导入审查没有发现对 Forge 的反向依赖。

## 3 分阶段 Review 与修正

| Review 点 | 发现并修正 | 最终判断 |
| --- | --- | --- |
| 签名与字段 | 展示 repr 不能作机器默认值；未提供与显式 None 分开；union、缺失/null、iterator 不能复用一入一出假设 | 默认值有类型与大小界限；await 后、展开时校验真实值 |
| 声明与降低 | 高级原生 API 降为 AsyncMapOp 后会失去领域事实；bound method 的真实拥有者不是方法对象本身 | 所属原生模块读取真实 actor/config；换模型、选项、predicate 或裸 async 冒充均在请求前拒绝 |
| 执行依赖 | 将 lookup 纯契约放在 execution 包会造成导入环 | 纯契约归属 Lance；执行层只实现执行，未增加兼容包装层 |
| 生命周期 | 普通实例/绑定方法即使没有策略属性也可能拥有资源；部分启动失败仍须关闭 | 使用既有作用域与去重所有权；分别记录创建/start/stop/close 的实际完成情况 |
| 组合和动作 | 异步物化不应追加假业务算子或第二个 action；单输入 union 仍是关系边界；run_stream 不能接受纯同步输入 | 内部消费回调复用同一 driver；静态与原生组合门禁一致 |
| 资源 | 已出队的在途结果仍驻留；展开列表、序列化副本、物化缓存及 ledger rollback 都占空间 | 队列与 worker 分开预留；spill 写前准入；SQLite 页数在写前限额；RSS/目录采样仍是最后保护 |
| 观察 | 共享上游、同 label、多 action、融合、旧观察序号、终态快照失败容易伪造成功 | action/node/occurrence 身份；终态优先；融合组耗时不分摊；观察保存失败不抹掉已提交原生回执 |
| 展示与兼容 | 原生调用记录占满 Case；重复引用按钮；轮询重置节点；初始 context 加载覆盖用户导航；旧工程标准 coverage 文本过时 | 结构化字段折叠、引用去重、保留选择、加载完成再启用导航、更新当前标准；旧格式只读，不提升权限 |

没有增加调度器、通用规则 DSL、业务 OperatorAPI、注册基类或 fake PipelineProgram。公共原生 API 不因 Forge 启用而自动接受全部受管规则。一个明确的原生修正是 batch 并发非正数立即失败，避免建立零 worker 队列后停滞。

## 4 UC 验收映射

下表记录实际对应测试；“通过”以第 5 节具体运行结果为准。覆盖不等于任意 Python、任意后端或任意外部资源都已支持。

| 编号 | 关键证据与反例 | 实现口径 |
| --- | --- | --- |
| UC-01 | `test_forge_platform_snapshot`、`test_forge_flow_checks`：伪声明、平台漂移、闭包来源 | 仅绑定当前可信原生 handler，不导入候选 handler |
| UC-02 | platform discovery 与 workspace context：公开成员、继承成员、别名、缺契约 | declared/unknown 与实际后端准入分开，unknown 有原因 |
| UC-03 | `test_operator_foundation`、`test_operator_foundation_models`：opaque 默认、显式 None、实际参数与配置变异 | 不解析 repr；必要未知阻断；运行前比对真实规范参数 |
| UC-04 | flow checks 的四类诊断与 stream 原生组合反例 | unknown / unsupported / denied / invalid；同一组合规则两端使用 |
| UC-05 | execution、managed runs、既有 run tests：别名、union、物化句柄、lookup、模型变异 | 实际 native facts；保留重复执行身份，不把 AsyncMapOp 当领域授权 |
| UC-06 | foundation stream/iterator/union + native semantics | await 后、每个 yield、批输入/返回与缺失/null 校验；真实结果独立断言 |
| UC-07 | async catch Exception、模型 when/error_output、模型与 SQLite 故障 | 硬契约/停止/磁盘错误不被普通 catch 或 error_output 吞掉 |
| UC-08 | foundation stream/runs + `test_stream_map` | 类/实例/bound method、共享实例、部分构造/start 失败、stop/close 失败 |
| UC-09 | materialize consumer 测试、实际计划参数变异测试 | 检查后的 source/plan 确实进入执行；预检失败零读取/请求/输出 |
| UC-10 | resources、models、run boundaries/faults、stream map | spill 预留保留其他句柄；小 ledger 磁盘额度；父 Arrow 缓冲、宽行、展开、慢消费者与失败不提交 |
| UC-11 | run tests、observations、`test_explicit_sqlite_lifetime_budget_survives_new_node` | action/occurrence 分开；共享 SQLite 持久请求额度不随新节点或 action 重置 |
| UC-12 | run faults、stream cancellation/threads/map、model cancellation | 停新调度、有限排空、部分初始化清理、监督进程消失、线程关闭及请求取消 |
| UC-13 | models、managed model、run recovery + SQLite uncertain | 本次声明账本精确引用、有界只读、跨 run 路径拒绝；uncertain 不自动重发 |
| UC-14 | observations + execution 中 terminal snapshot failure + 性能对照 | 预留总量、旧序号与终态、融合计数、复用、持久化失败与原生回执共存 |
| UC-15 | `test_real_native_alias_executes_and_parameter_drift_fails` | 真正新增 Dataset 原生 alias 后检查并写出真实表；Forge/runner/UI 无该名字特判 |
| UC-16 | managed native families、lookup/model、native join 语义 | 普通函数、async/actor、flat/batch/iterator、join、模型、非模型资源均真实运行 |
| UC-17 | foundation browser + 既有 workspace/run browser/CLI | 节点 → 结果 Case → 原生证据；读取前后请求数不变；历史与 SSH 转发链路 |
| UC-18 | 历史 observation/catalog/report 测试、原生回归、独立安装包示例 | 历史只读；打包网页/规范/worker；源码仓外完整创建、执行及结果核验 |

## 5 实际测试记录

测试均使用自有临时目录、固定 Lance fixture 和 loopback 模拟 HTTP；没有真实外部模型请求、生产数据写入或共享环境安装。

| 批次 | 结果 | 证据 |
| --- | --- | --- |
| 初期契约/原生集成 | 205 passed | `/tmp/forge-foundation-phase1-ax6qlzql/pytest.log` |
| async/flat/batch/actor/iterator 集成 | 129 passed | `/tmp/forge-foundation-phase3-p63pxcwh/pytest.log` |
| 模型原生回归 | 114 passed | `/tmp/forge-native-model-regression.log` |
| 规范化与原生兼容复审 | 192 passed | `/tmp/forge-native-normalization-review.log` |
| 最新完整 foundation + workspace/boundaries | 62 passed，475.54 秒 | `/tmp/forge-foundation-final-yfkhdlfm/pytest.log` |
| 流程诊断与资源补测 | 70 passed | `/tmp/forge-foundation-diagnostic-regression.log` |
| 浏览器安装路径及遗漏 fixture 修正后回归 | 36 passed | `/tmp/forge-foundation-env-regression.log` |
| 最新模型节点/Case/证据真实 Chromium | 1 passed，63.13 秒 | `/tmp/forge-foundation-release-o6t54b37/model-browser-final.log` |

另完成最后执行/诊断/资源自审 **76 passed**（`/tmp/forge-foundation-final-review.log`）；修复浏览器初始加载导航竞态后，重连、A/B、继续加工、工作区及模型证据浏览器组 **4 passed，299.11 秒**（`browser-ready.log`）；受管模型实际请求取消 **1 passed**（`cancellation.log`），1 次请求后约 **2.18 秒**停止，未发第二次请求、无结果提交，保留请求与错误记录。

全仓首次选中 2,198 项（排除 network/gpu/ray），其中 2,097 passed、17 failed、1 skipped 已输出后，测试进程退出码为 3。定位到原有 `test_watchdog_feed_throttles_within_gap` 启动了 60 秒看门狗却未退出上下文；修复该测试的资源清理，生产 Watchdog 实现未改，relay 组 **22 passed**。剩余 83 项已单独完成 **83 passed**。17 项失败包括 4 项旧测试事实/标准版本未迁移、12 项未传 Chromium 安装路径、1 项隔离副本漏复制 benchmark；对应修正已纳入复跑，初次失败不计为通过。最后这 17 项及原跳过的 1 项已一起复跑，**18 passed，184.66 秒**（`retry.log`）。按当前用例 ID 去重并剔除被替换的 2 个参数用例，再纳入已通过的 15 个新增用例，**验收过程覆盖当前集合中的 2,211 项，均有通过记录**。这些记录来自分阶段源码快照；最后变更后完成受影响用例与浏览器回归，未重新对最终 revision 做一次整库运行，不能将合并数宣称为最终版本单次全量通过；逐项证据索引为 `/tmp/forge-foundation-release-o6t54b37/aggregate-tests.json`。

这些批次存在重叠，不能直接相加成唯一测试数量。

安装包：wheel SHA-256 为 `9dbb6975b172b8809da0f61dd9f7ad14333311b5882d2cd2dfc24800099ee7ba`。临时 `--system-site-packages` venv 内安装 wheel，仅复用既有依赖，未写入共享环境。实际导入路径位于该 venv 的 site-packages，网页、engineering-v3、run_worker 资源齐全。最终 wheel 中的所有可对照包文件与当前源码逐字节一致。源码仓外示例 `r03902866a291418f8ffcf00a` 成功，独立核对 6 条结果及 6 条观察。

首次安装验证使用 `pip --target` + PYTHONPATH，隔离 worker 正确拒绝与父进程不同的依赖 metadata；改为标准临时 venv 后完成整条链路，未放宽快照检查。示例最初的 TypedDict 继承也被静态规则拒绝，已改成当前支持的显式字段声明，没有为演示放开门禁。

## 6 性能与资源保证范围

同一 100,000 行整数递增、真实 Lance 输出、本地单 worker、batch=256、原生池 128 MiB；每种模式在独立进程跑两次。输出按独立预期检查，输入命中 OS 缓存（read_bytes=0）。

| 模式 | 两次耗时 | 吞吐 | 采样峰值 RSS | 采样峰值 scratch | 写 I/O |
| --- | --- | --- | --- | --- | --- |
| 普通原生 | 0.684 / 0.686 秒 | 约 14.6 万行/秒 | 约 275–279 MB | 约 2.8–3.0 MB | 3,416,064 bytes |
| 受管检查 | 1.508 / 1.610 秒 | 约 6.2–6.6 万行/秒 | 约 275–277 MB | 约 2.9–3.0 MB | 3,416,064 bytes |
| 检查 + 节点观察 | 1.633 / 1.704 秒 | 约 5.9–6.1 万行/秒 | 约 279–281 MB | 约 2.9–3.0 MB | 3,489,792 bytes |

该对照直接测量 Dataset 执行与 CheckedExecution，不包含工作区准备、进程启动及 UI 加载时间。观察额外约 6–8% 耗时；完整检查对这个极轻量回调约为原生 2.2–2.4 倍。不可将此数字推广到模型或复杂 join 吞吐。保存的观察 JSON 为 803 bytes，未留存业务 payload；没有每行提交 SQLite。测量源数据见验收 JSON。native 池预算不是进程 RSS 硬上限，采样峰值也不是分配前证明。

资源实现：流式队列、worker 在途输入/展开结果、序列化副本、物化缓存分开预留；物化 spill 文件在写前核算本执行器拥有的量；SQLite 通过 max_page_count 预留 rollback 余量。候选函数内部、第三方解码及 writer 分配仍需进程保护，不能把分配后的检查宣称为预防此次分配。其他 scratch/结果/总证据目录仍有采样超调窗口。

## 7 明确的当前限制

- 通用的是契约与执行核对机制；所有原生 API 没有因此全部获得受管运行能力。新语义必须由所属原生模块提供可验证契约与后端支持；目录 unknown 明确阻断。
- 当前受管 prompt 只准入有界 HTTP(S) text/JSON 模板与 JSON 响应、固定配置和显式凭据名；agent、图片模型、动态路由、受管服务构造等没有本轮准入。模拟服务只能验证机制，不能验证真实模型质量。
- 实验仍以工作区 create 新表收束；纯 run_stream 无结果表不是当前工作区运行 profile。append/overwrite、远程对象存储、跨表事务、生产调度与自动重试未开放。
- actor 实例只允许静态可确定的构造状态；TypedDict 继承等未覆盖源码形态仍 incomplete。现有普通原生 Dataset 的更广泛表达不自动等于 Forge 可分析。
- 节点 DAG 不是逐行血缘。DataFusion 内部及某些被折叠的关系边界没有独立计数/耗时；界面保留 incomplete/未测量，不均摊、不填零。模型 when 跳过或 error_output 行不能仅按节点完成推断“模型成功”。
- 没有远端撤回保证。取消、超时或进程中断后的原生账本 uncertain 仍保留，需显式核对；生命周期 closed 仅表示本地钩子完成。
- 历史平台目录 v1/v2、旧检查/运行仍只读；新运行要求当前目录 v3、工程标准 2.2.0、检查 v2、manifest v2。必须显式重新绑定平台和检查，不能无声升级权限。
- 正式验证、Review/Freeze、受管交付及生产放行仍按用户要求暂缓。

## 8 变更保护

实施开始已保存 202 个原有 dirty/untracked 文件的清单与副本，后续差异相对该基线及原本干净文件的 HEAD 检查。仅修改本次原生契约、执行核对、资源/观察、Forge 接入、相关测试/文档/示例；不还原原有 dirty、不清理 Notebook 输出、不修改业务仓或既有运行服务。

最终深度自审检查：真实事实与预期分离、静态/实际边界、依赖方向、actor 所有权、异常不被吞、资源预算、持久化失败与部分提交、历史版本、网页只读、安装包资源与真实结果。证据不覆盖的能力均在上一节显式列出，不以测试数量代替支持范围。
