# 工作区变化核验与后台巡检：A1–A4 实施记录

本轮只实施 demiflow 平台能力。依据用户批准的[设计](demi-workspace-audit-design-20261103.md)、[源码方案](demi-workspace-audit-source-plan-20261103.md)和[验收矩阵](demi-workspace-audit-review-20261103.md)。业务原仓、旧 pipeline、整个 training/、历史表与运行记录保持原状。

## 1. 已实现的闭环

| 阶段 | 交付 | Review 结论 |
| --- | --- | --- |
| A1 | workspace v3；明确绑定报告；不可变作者内容基线；file/tree 允许范围；开始、导入、修订、结束的 CAS 控制 | 报告依据与任务范围独立。重新 inspect、刷新或巡检不会重拍原基线 |
| A2 | prepare/start/resume 现场核验；manifest v4 封存 scope 和基线；start 最终预检置于执行锁、控制锁内 | 缓存不参与准入；报告 pin 切换不改变 scope；旧历史读取和取消不被基线损坏阻断 |
| A3 | 单对象核验；有限状态缓存；独立 console watch；每工作区观察/核验租约；轮转与到期完整核验 | 无 HTTP/UI 依赖，不执行 Flow、不生成通过报告、不自动扩范围 |
| A4 | 原 Web 同步链展示范围、报告依据、核验时间和巡检鲜度；HTTP/CLI 同源；插件交接补充 | 没有新增聊天入口、宿主专用协议、执行引擎或通用中间层 |

新实现模块只有 `forge/audit.py`（有界清单、契约与比较）和 `forge/watch.py`（有界调度）。公共用例仍归 workspace，执行准入仍归 runs，CLI/HTTP 只做输入输出适配。

观察报告最多 64 个，写范围最多 256 条；作者根最多 64 个。默认作者清单 4,096 项、深度 16、单文件 32 MiB、总字节 128 MiB；可调整但受实现中的硬上限限制。基线最多 8 MiB，状态缓存最多 1 MiB，保留基线最多 1,024 份/256 MiB。枚举、分块读取、特殊文件、空间与稳定性检查均在核验路径中执行；不能核验时不返回范围通过。

## 2. 实施 Review 与修订

- 保留报告状态的旧原因码 `table_unavailable:<alias>`，同时增加结构化 `verification`，避免改变原客户端语义。
- 准备状态的 cancel 继续遵循原行为：不把“尚未执行”偷偷变成取消；新增测试最初假设不对，修正了测试而不是运行状态机。
- 对新增/删除父目录的范围判断按祖先聚合，避免清单规模增长时进行平方级后代搜索；违规子项不能被父目录放行掩盖。
- 派生缓存可损坏后重新生成；它与不可变基线、报告等权威文件不共用“损坏即禁止覆盖”的持久语义。临时文件失败后清理。
- 缓存错误字段类型、非有限时间和非法变化项返回未知，不能让状态 GET 报内部错误。普通 JSON 重复键拒绝解析。
- 心跳、轻量观察和实际核验时间相互独立。前端使用服务器 observed_at 加本地单调时钟判断到期，缓存 revision 不变也会显示超期。
- 手动核验和自动读取的迟到响应有请求序号保护；既有导航 epoch、草稿和固定历史选择保护保留。
- 核验、scope 生命周期、报告 pin 和 HTTP 字段都只接收通用契约；实现中没有 identity、concept_images、inventory、training 或客户端名称分支。

## 3. 实现取舍与保证边界

- 元数据提示变化后，对有限的 pin 集合保守排队；没有另建依赖索引或增量事件系统。完整核验仍按报告实际依赖判断，所以无关 Flow 改动不会自动让另一报告失效。
- 一次手动检查一个 scope/report。方案中的 CLI `--all` 是可选项，本轮没有增加它。
- 内容捕获的稳定性检查发生在同一个有界 worker 内；发布前对工作区控制做 CAS。没有把发布瞬间描述成整个文件树事务，也没有用第二次无界扫描来补这个承诺。
- 演示和默认 watcher 使用 30 秒轻量观察、300 秒完整核验目标；这是调度间隔，不是所有规模下的完成 SLA。一个进程同时一个 worker，各项独立显示过期。
- 平台环境沿用既有快照和 metadata assurance，不声称完整哈希所有已安装依赖。
- 此机制约束 Forge 管理的 prepare/start/resume。直接执行任意 Python、调用 Dataset 或拥有相同 OS 权限后手改权威 scope/policy，不是这套协议能隔离的行为；它不是权限沙箱或防篡改审计系统。
- 旧项目不静默升级；未启用范围约束、未绑定报告必须如实显示。启用平台能力不等于所有生产项目已经自动获得任务范围保护。
- 中性原基线导入已实现；DemiForge 私有 task 格式的导出适配、具体 Harness 插件、系统开机自启配置不属于本轮交付。巡检由独立进程运行，页面关闭不会停止它。

## 4. 测试记录

全部日志、原 dirty 文件副本、保护摘要及独立验收工作区保存在：

`/tmp/demi-audit-a1-a4-9bk4kk3b`

| 日志 | 实际结果 | 覆盖 |
| --- | --- | --- |
| a1-tests.log | 初轮 26 通过、1 失败；旧报告原因码兼容问题随后修复 | A1/原工作区与控制接口 |
| a2-tests.log、a2-corrections.log | 初轮 15 通过、2 个新增测试断言不符原 cancel 语义；修正后 2 通过 | 准备、启动、恢复与原参数绑定 |
| a3-tests.log | 5 通过 | 独立 watcher、HTTP/CLI、无 Web 服务能力 |
| a4-regression.log | 40 通过 | audit、准入、watch、浏览器、原生库存 Flow、恢复、公共 API、同步 |
| final-boundaries.log | 10 通过 | 报告篡改、基准独立、锁内预检、显式升级、浏览器和 watch |
| final-hardening.log | 23 通过 | 增加稳定读取竞态、非法类型、数据根重叠、磁盘不足、缓存异常后复测 |
| business-final.log | 2 通过 | identity/concept_images 的真实源码 fixture，在生效 scope 下原生执行并核验输出 |
| inventory-final.log | 1 通过 | 第三种原生 Flow，两配置、四个独立结果，生效 scope 下运行 |
| final-client-regression.log | 6 通过 | track 部分失败、草稿/选择保留、认证恢复、两个浏览器与 CLI 共享结果、丢响应不重放 |

以上批次有重复用例，不能将表格数字相加作为唯一测试数。去重后，最终涉及的 51 个平台用例、5 个专项浏览器用例、2 个业务用例，共 58 项已通过；最后代码修正后的针对性复测为 23 + 6 项与三个 Flow 验收。本轮没有执行全仓测试，也没有把历史全仓数量当作新结果。另有真实服务联调，不计入 pytest 数字。

## 5. 验收矩阵证据

| 验收项 | 实际证据及保证程度 |
| --- | --- |
| AU-01/02/04/05/17/27 | audit 与 audit_binding 测试覆盖既有 dirty、完整 Notebook、允许修改、新增删除/类型替换/父目录、同尺寸改写、重新检查不洗掉范围、原基线导入 |
| AU-03/10/11/12/24 | audit_runs 测试实际执行 prepare/start/resume；伪绿缓存不能绕过；scope 变化/基线缺失拒绝；历史仍可读 |
| AU-06/07/23 | 打开前并发修改实际文件后拒绝；symlink/FIFO/数据根重叠/条数/字节/深度/磁盘不足使用小预算触发；不是所有 OS 故障排列的穷尽证明 |
| AU-08/18 | 复测原工作区报告依赖与不可用表语义；报告检查逐项输出 match/mismatch/unavailable；实际平台变化通过快照核对 |
| AU-09/14/15 | scope 下的真实运行成功且新增结果不越界；运行沿用封存代码；缺失基线时 show/cancel 可用；原恢复测试复测，未重写 journal 或清理接口 |
| AU-13/28 | 测试在实际 start preflight 检查 execution/control 锁均被持有；切换 pin 后同一准备仍成功；scope 更改要求重新准备 |
| AU-16/25/26 | pin 文件篡改而 index 不变仍发现；v2 显式升级/内联报告拒绝绑定；track 的 CAS 失败保留报告并明确返回绑定失败 |
| AU-19/20/21 | 真实独立 watch 子进程在无浏览器时发现文件修改，退出释放租约；过期/心跳/非法时钟测试；两次争用同一工作区锁被拒绝 |
| AU-22 | 轮转测试验证持续 pending 对象不饿死其他目标；worker 复用既有有界进程。没有声称完成多台机器或极端存储停顿的长期压力验收 |
| AU-29/30 | HTTP 与真实 CLI 读取同一观察记录；多浏览器/丢响应/认证失败/草稿和迟到响应使用专项浏览器回归；无自动重放写请求 |

## 6. 原入口与隔离验收项目

以下工作区由各真实 pipeline 自己的测试 fixture 复制源码并创建本任务数据，不读取或改写生产输出。模型请求均为 loopback 确定性 fixture；它们证明平台的契约、准入、原生执行与可视化链路，不证明真实模型质量或生产容量。

| 控制台名称 | 工作区 ID | 原生运行 |
| --- | --- | --- |
| audit-identity | w18dd51b554134d0cb77a38481c591dcc | r2151919ec9fd41108cf1a213，成功 |
| audit-concept-images | wcb6bbc68fc0e4d3583a69f8f1c3fea13 | r859c771b2cab43dba9fb2a2f，成功 |
| audit-inventory | wf9064d9ed6ed47ebacddddc79c082e63 | r8d3296b8f07a425f8889f5a1 / r5ff7677247e745c4b0aa4bcc，成功 |

验收结束后，仅对这些本任务副本命名，明确结束旧 scope 并建立用于面板演示的新 scope；原测试基线、manifest 和输出保留。模型 fixture 服务随测试退出，因此两个业务副本在控制台登记为只读；库存副本是可执行的无模型示例。

查看服务仍使用原 registry/token 文件，服务器 127.0.0.1:59865；本机 SSH 仍为 55971 → 59865。没有新增访问端口或要求新转发。独立 watcher 只观察本任务新建的三个验收副本，其 registry 位于产物目录，避免给用户正在开发的原项目擅自设置任务范围。

## 7. 原服务现场验证

- 查看服务 PID 1438，监听仍是 59865；巡检进程在测试中停止后重新启动，最终 PID 2735。实际部署回执为 `deployment.json`，不保存到业务项目。
- 在没有浏览器的情况下，watcher 为三个副本生成范围/报告核验结果。HTTP 读取 3 + 4 + 4 张原生结果表及其固定版本样本成功。
- 仅在 `audit-inventory` 的 operators/ 下新增 `unexpected_audit_demo.py`，watcher 检出越界。这个文件用于展示范围拒绝，保留在隔离副本；不在真实业务原仓。
- 停止本任务 watcher 后，真正调用 CLI prepare，退出码 1、`operation_rejected`；HTTP 同一 prepare 返回 422。两者都指向该越界文件，未创建运行。
- CLI 和 HTTP 读取同一缓存（仅服务器读取时间不同）；真实浏览器显示越界路径，刷新成功，两个业务 Flow 的接线/运行页面可打开，浏览器脚本错误为 0。
- 巡检已恢复。默认每项完整核验到期后继续检查；没有让停止/重启刷新旧 verified_at。

现场回执：[live-acceptance.json](/tmp/demi-audit-a1-a4-9bk4kk3b/live-acceptance.json)。截图：[越界与拒绝准备](/tmp/demi-audit-a1-a4-9bk4kk3b/panel-violation.png)、[identity 运行](/tmp/demi-audit-a1-a4-9bk4kk3b/audit-identity-run.png)、[concept_images 运行](/tmp/demi-audit-a1-a4-9bk4kk3b/audit-concept-images-run.png)。

## 8. 变更保护与交接

实施前保存了现有 dirty 文件副本、Git 状态和 103 个原工作区控制/来源文件摘要，最终 diff 相对这个起点，而不是相对干净 HEAD。已有 6 个 console 登记项逐项相同，仅追加 3 个验收副本。历史工作区、表引用与运行不被重绑。

保护核对发现业务仓的 workspace.json、generation_evaluation.py、其 score.yaml 与 records.py 有并行开发变动；本任务没有写这些文件，也没有覆盖或回退它们。不能把其他任务的改动算作本轮改造，也不能声称整个共享仓库期间完全没有变化。整个 training/、旧 pipeline 和 Notebook 输出，本任务无写入。

平台已有的其他 dirty 修改保持原样；本轮实现只改变上述 Forge 模块、对应测试与接口/设计文档。`task-diff.patch`、`changed-existing.json`、`protection-report.json` 可核对具体起点与范围。无新运行引擎、客户端适配实体、业务名称分支或生产数据迁移。

插件开发继续参照[公共交接接口](demi-harness-plugin-handoff-20261103.md)；独立 Web 和插件消费相同 HTTP 状态。实际 Harness 插件仍由接手方实现，不能将本轮 HTTP/浏览器验收称为插件已交付。
