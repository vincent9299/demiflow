# 开发 Harness 试用交付记录

2026-11-19，Asia/Singapore。本记录对应真人“达到最终交付、多验证测试、明早试用验收”的授权。原意图仍是：通用 CLI 拥有 coding loop，Forge 在进程外落实唯一规范、固定验证与独立审阅，用户在工作台自然交互。完整 [DH.1—DH.9](../../../forge_coordination_20261104/original_task_intents.md#dh-20261118开发入口-harness-与-forge-硬规范原始意图)及 R/15 叶不变。

## 交付位置与试用

- 唯一候选：`_demiflow/worktrees/gpt_prompt_replay_20261118`，分支 `fix/prompt-replay-delivery-20261118`，基于平台总控 (2) 的 `dab45bb`。主仓不是本版开发或试用来源。
- 页面入口沿用用户当前本地转发 `http://localhost:59866/`。选择“Forge 开发试用”→“开发”，直接输入需求。试用根目录为 `_demiflow/harness-trial-release-20261119`，workspace `w5da6c62c6cb24b0c913de4a1e4636e7d`。
- 建议试用：“把 normalize_name 的输入上限从 128 改为 256 个字符，保留 None 和空白处理，补边界测试并更新说明。”
- 项目包含两个独立 flow、共享算子及 3 条合成数据。开发仅写 `lab/`，数据和控制文件只读；未授权本试用项目提交管道运行。现有项目的运行权限保持原值。
- 此次交付针对已登记并明确授权的项目；不会自动给存量项目补登记、迁移历史或签发接受。Codex 适配与边界测试已覆盖，但真实账号推理仅验收已配置的 DeepSeek。

页面切换和重启后的最终核验状态见本记录末尾；最终用户试用验收仍由真人完成。

## 本版实现与 Review 结论

模块和接口以[实施契约](forge-development-harness-contract-20261118.md)为准，内核机制和限制以[执行边界](forge-development-execution-boundary-20261118.md)为准。Forge 标准/基线/收据与工作台控制器、执行边界、固定模型通道分别拥有唯一职责，工作台单向调用 Forge；计算底座、SDK、算子与执行引擎未修改。

针对 v1/v2 Review 的关键缺口：

| 问题 | 本版落实 |
| --- | --- |
| 本机不能挂载，执行后端未定 | 实际 user/PID/net namespace、清零 capability、Landlock 与 seccomp；不用只读挂载、ACL 租约或新宿主用户 |
| 门禁价值依赖沙箱阶段 | Forge 领域门禁独立；restricted 需真实后端证明，trusted_local 如实标未防篡改 |
| Review 没有默认形态 | 同 provider/model 独立无工具请求；每 turn 最多两次，失败可有限自动修复，未知响应不重试、不变绿 |
| 权威记录和源码混放 | 权威 TaskStore、验证日志、签名密钥与控制库在受保护 state root，CLI 不可读写 |
| 工作副本/应用事务过重 | 授权范围内原位修改，固定验证只读，前后重验内容；明确为 live_tree_rechecked |
| 强制能力依赖特定 CLI | B1 从进程外约束整个 CLI 与后代，两种适配共用启动口 |
| 非对话写入口遗漏 | 配置、Notebook、外部编辑的变化使相关当前证据失效；严格准入复核登记的精确 root/entry |
| 存量项目运行兼容 | 默认 observe，显式 require 才对新准备/首次启动执行准入；历史读取/停止/恢复保留原语义 |

资源上界有具体口径：模型请求、输入输出、连接、FD、单文件大小有固定预算；RSS、进程数和总写盘采用有界采样保护，不能称为 cgroup 或文件系统硬配额。当前 UID 映射保留宿主身份，不声称独立非 root 账号或抵抗其他宿主 root。

## 验证证据

所有证据位于工作区 `_demiflow/shared-review/forge-harness-final-delivery-20261118/`；不在报告中复制凭据或模型推理正文。计数按各批分别列出，不把重叠批次相加为独立覆盖数。

| 验证批次 | 结果与范围 |
| --- | --- |
| 31 模块综合回归 | 462 passed、78 subtests passed；两个失败分别为漏配浏览器环境与下述原基线引擎问题 |
| 冻结后受影响回归 | 53 passed；补配实际 Chromium 路径后组合开发/运行页面通过，含最终独立 Review 及目录清理竞态修复 |
| 产品浏览器/视图批次 | 25 passed；三项旧 UI 用例按当前产品契约更新后联合补跑 3 passed。产品图页面实现相对基线未改 |
| 外部执行边界 | 28 passed，实际内核正反例、继承/逃逸、网络/FD/路径、父进程死亡与磁盘采样；另有真实 DSH ACP 初始化且不调用模型 |
| 严格运行准入 | 独立最终组 24 passed，覆盖精确工作区/入口、伪造/陈旧/失败证据、并发停止与历史恢复 |
| 独立审阅 | 最终组 29 passed；完整正常结论才有效，工具调用/缺失块/截断/异常响应不签收据 |
| 发布物 | 离线 wheel 全部 1,430 个包文件逐字节匹配源码，安装后标准、开发功能与冷导入验证通过 |

最终 wheel SHA-256：`8ef0d6cb66bb5d04d6eda23e529d270508d19b679711e6c372c10c409d470a5f`。它包含新增 `.py`、DeepSeek `.cjs`、原标准与 skill；构建在专属副本，未污染主仓或候选目录。

### 真实 CLI、自动门禁与模型审阅

最终独立合成项目从浏览器只发送一次真实需求，要求为 `normalize_name` 增加原始输入 128 字符上限并补测试/说明。DeepSeek CLI 使用原安装、原 `deepseek-account/deepseek-v4-flash`，编码 effort 为 high；Forge 负责外部限制、验证和独立 Review。

192.345 秒后，原 turn `t56c8ebdb10631c019fc2cb385f16a7bf` 达到 `accepted/enforced`，check、verify、review 均 passed，开发和门禁双 cleanup 均 complete。系统在同一原需求内自动续修一次上下文检查反馈，未要求用户执行规范命令。当前指纹与接受指纹一致：`32a3fbf383d14bb026d9a4b94b4bf8fbe795c3d85c6ee5772a16d9fbea7e6dc7`。页面无 JS 错误，390px 无横向溢出；原始结果和两幅截图在 `live-release/`。

独立 Review 的真实诊断曾复现 high 在 8,192 和 16,384 输出 token 内只产生推理、没有结论。最终仅为审阅设置独立 low 等级，保持同一 provider/model、完整材料、16,384 token/180 秒/64 KiB 限额；真实完整请求正常 stop，七条规则结论通过校验。确定截断现在显示不可用，未知网络/流错误仍保留 unknown。此前三个未通过的试验保留原失败/未知历史，未重建基线或补签接受。

### 已知原基线问题

`test_native_materialize_union_columns_and_independent_outputs` 在原 `dab45bb` 独立源码副本和本候选上使用相同预算，都在第二个 union checkpoint 报 `RecoveryContractError: Execution metadata exceeds byte budget`；均清理完成且先前提交的输出保留。证据为 `native-baseline-comparison.json` 及 `native-baseline-logs/`。本轮未修改引擎或放大测试预算，因此不宣称全仓测试全绿，也不把该既有引擎问题归为开发 Harness 引入的回归。

## 历史保护、部署与回退

切换前已逐字节核对主仓原 18 个 dirty/untracked 文件、Git staging/status/HEAD 保持；旧开发 4 个会话、5 个轮次、82 条事件按原列值完全一致；Notebook/分析数据库 21 张表的逻辑哈希保持（含 3 文档、7 修订、60 对象）。原始 DH/R 意图保留。后续同样脚本核对切换后结果。

预览使用原 59866、原 token/registry、原分析数据库和工作区。新版启动脚本为 `/tmp/demiflow-combined-preview-20261117-q0st03t2/resume-harness.py`；旧 `resume-project-operators.py`、原 ready/registry 和私有 token 均保留。切换只终止核对身份后的旧预览进程，先确认没有活动 Notebook/执行，再启动新源代码与同一历史库。未改正式业务服务，未执行生产管道。

回退须先停止当前开发并确认双 cleanup 与进程域已排空，再终止本预览，恢复备份 registry，并使用旧 worktree/PYTHONPATH 启动原脚本。state root、Notebook 数据库、任务与对话历史留存；unknown 不因回退改为 complete。不能在旧版本上继续宣称新版受管接受或撤销 require。

当前待完成：预览入口切换、重启后同会话续开发与最终历史核对。验收结果完成后更新本段。
