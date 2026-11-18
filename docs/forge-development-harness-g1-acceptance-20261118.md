# 开发 Harness G1：深入 Review 与本批验收

**2026-11-18 指定候选集成完成**：按真人纠偏，本批已从主仓迁入 `gpt_prompt_replay_20261118` / `fix/prompt-replay-delivery-20261118`，基线 `dab45bb`，保留 candidate.10/v3、多流程及原恢复修复。主协调目标组合 184 项测试、44 项子测试通过；开发页 7 项浏览器回归通过；算子/多流程页面首次因 Chromium 路径未配置跳过，指定现有 Chromium 后单项通过。独立 Review 另补一条 v3 双流程受管门禁测试并通过（相关 58 项组合含重叠，不累计）。主仓只撤回本批 17 个跟踪文件增量和 13 个新增文件，既有 18 份 dirty/untracked 内容和暂存内容保持。改动留在目标 worktree，未提交。59866 仍是旧 `74d2f99` 预览，未重启服务、改部署或调用真实模型。当前为 G1 / 未防篡改，完整 DH 尚未完成。 详见[迁移与组合证据](../../../shared-review/forge-harness-relocation-20261118/README.md)。

**2026-11-18 工作位置更新**：当前实现位于 `fix/prompt-replay-delivery-20261118` / `gpt_prompt_replay_20261118`，基线 `dab45bb`；后续不直接主仓开发。59866 仍运行 `74d2f99`，迁移代码不等于已更新试用服务。v1 原件按原字节保留，其相对路径以编写时主仓位置解释；当前入口以本页和实施契约为准。

日期：2026-11-18。以下初始 G1 证据对应迁移前的主仓基线 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`。依据：[DH.1—DH.9 原始意图](../../../forge_coordination_20261104/original_task_intents.md#dh-20261118开发入口-harness-与-forge-硬规范原始意图)、[完整方案 v2](forge-development-harness-design-20261118-v2.md)、[冻结实施契约](forge-development-harness-contract-20261118.md)。用户已明确授权深入 Review 后按模块交由 subagent Review 和实现。

## 验收结论

**本批 G1 自动检查/验证门禁、准确的产品状态及统一 CLI 启动入口通过验收，这是迁移前验收结论；整体 DH 尚未完成。** 产品未配置安全的独立只读 reviewer，涉及必要审阅的开发只能到 `review_unavailable`，不能显示开发接受。L2 完整执行边界继续关闭；没有拿 trusted_local 或 syscall 小探针冒充防篡改。

主协调冻结契约并负责集成与最终 Review；三个 subagent 分别负责 Forge 门禁、工作台控制器、外部启动边界。执行边界负责人还对控制器恢复、取消、历史读取做了交叉 Review。主协调独立审查实际差异并从浏览器走通完整临时项目链路。

## 已实现的用户结果与模块边界

| 模块 | 实际行为 |
|---|---|
| Forge 受管任务与门禁 | 项目外唯一 TaskStore；复用原 check/verify/review/指纹；原始 dirty/readonly/staging 基线、原始目标和后续真人要求绑定；旧 local 记录不被迁移或改写 |
| 工作台控制器 | 新 registered 授权自动建立会话任务；provider 排空后后台自行检查/验证，不依赖 agent 主动调用或浏览器轮询；原 provider 状态与开发验收分开 |
| 上下文与版本 | 真实分页送达；压缩后再次注入原始目标与真人决定；额外全文规范加入同一合同/指纹，禁止通过增删来源或重绑掩盖变化 |
| 取消、恢复与预算 | gate/verifier 使用有归属的监督进程；共享用户 turn 的总时限及原资源预算；正常停止排空；不确定已执行操作不自动重放，不证明清理就保留租约 |
| 公共启动入口 | Codex/DeepSeek 共用 `execution.py`；未实现的 restricted 在 adapter 准备 HOME、读取认证及 Popen 之前拒绝，无静默降级 |
| UI | 显示检查、验证、待独立审阅、stale；不会把 provider 完成当接受；gate 占用时保留停止操作；时间线只保留简洁检查结果，详情注明未防篡改 |

`workbench → forge` 单向依赖保持；没有复制规则解释器、agent loop、运行账本或引擎。初始验收时主仓 candidate.9 与 candidate.10 候选分开；最新真人已要求迁入指定会话的最新候选，现基于 `dab45bb` 合入 candidate.10/v3 多流程组合，保留其新能力。新组合验证单独记录。当前运行策略固定 `observe`，原运行权限和检查仍适用，没有伪造未实现的 `require` 配置开关。

## 深入 Review 抓到并修复的问题

1. **外置记录的日志与权限**：原 verifier 日志使用项目相对路径，移到项目外会报错。新增明确的 task-relative 日志引用与完整性核验；控制目录要求 controller 所有、0700，旧 local 日志格式保持。
2. **自报 reviewer 与旧证据**：现有 reviewer 字符串不能成为独立受管证据。受管 API 只接收控制器独立请求的候选凭证；harness 工具不提供 verify/review/accept 权限；无适配器时 unavailable。
3. **物理根、合同与 staging**：同路径换根、重复合同副本不一致、源码/规则/verifier/日志/staging 变化都使接受失效。split index 直接有界读取；不用会更新其时间戳的 Git 查询。原索引的字节、mtime、ctime 保持。
4. **原始意图只留磁盘**：最初 prompt 只含当前一轮要求。现已将原始 goal 和控制器绑定的真人决定加入实际发送内容，Codex 和 DeepSeek 的第二轮及压缩后都实测重新收到。
5. **额外规范只留 manifest**：祖先 AGENTS 等未进入原 Project 快照时，其变化曾不改变候选指纹。新增 `bind_context`；未绑定不能通过，字节或来源集合改变持久 stale，重新提交原清单也不能洗掉拒绝。
6. **启动失败留下永久 busy**：provider 尚未启动的失败现在同事务结束开发忙碌状态，避免页面无法继续且停止无效。
7. **重启误用新授权**：旧 queued gate 在 spawn 前核对保存的 policy 与物理根；改变即 stale，不使用新预算/权限偷偷续跑。未改变时使用原策略快照。
8. **根变更破坏历史读取**：只将当前开发状态标 stale，保留已经保存的对话历史。
9. **磁盘统计漏算共同占用**：gate 和 Service 现在共享同一有界私有目录统计，合计 provider 历史与 Forge 证据，不各自占满同一个总额。
10. **旧运行服务未迁移的表**：新版 worker 的完成回执兼容旧表没有 development 列的情况，不在 worker 中擅自迁移运行数据库，不因缺列丢失 cleanup 回执。

## 验证证据与范围

全部测试使用临时项目、固定本地验证命令和 CLI 协议夹具。真实进程、SQLite 生命周期和真实浏览器参与验证；没有调用工作台真实模型、生产 pipeline 或业务数据。

| 证据 | 结果与覆盖 |
|---|---|
| `tests/test_forge_development_managed.py` | 最终领域实现 23 项通过；其中来源集合增/减后再次提交原清单的两支再单独通过。覆盖日志/规则/合同/根/staging/审阅身份/上下文及接受失效 |
| `tests/test_forge_development_chat_managed.py` | 12 个场景经相关小组及针对性复验通过，不将重复运行相加。覆盖两个 provider、无轮询自动 gate、原意图/压缩、取消/总时限、未知恢复、外部规范、磁盘合计超限及旧表完成回执 |
| `tests/test_forge_development_chat_policy_managed.py` | 9 项通过：自动 managed、旧 policy 快照保持、非法混合授权拒绝、restricted 不被开启 |
| `tests/test_workbench_development_execution.py` 与公共 process 测试 | 9 项通过：副作用前拒绝、显式环境和 FD、真实临时子进程 stdio/会话、失败无重试/降级 |
| `tests/test_forge_development_chat_managed_browser.py` | **最终代码 2 项通过（19.83 秒）**：错误的 accepted/指纹组合不能变绿；正常页面输入→临时源码修改→自动检查验证→待审→外部编辑后 stale；停止/发送、移动端无横向溢出 |
| 既有兼容回归 | Forge development/migration/project-local/source-coverage、聊天/lifecycle/compaction/providers、HTTP/remote/context-budget、既有浏览器回归通过；这些阶段组有重叠，不汇总成虚增总数 |
| 静态与历史保护 | `git diff --check`、JS 语法检查、本批文档链接检查通过；v1 和外部 Review 原件 SHA 保持；R 完整树和 DH 原始条款逐字保持；Git staging 与基线一致，HEAD 未改 |

第一次新增浏览器用例在并行编辑期间触发了平台源快照变化保护，发生在 setup，未绕过保护；停止代码写入后通过。测试夹具的触发词曾误命中原有 hang 分支，已更正夹具并复验；没有据此放宽产品门槛。

## 仍未交付的边界

- **独立模型 Review 与有限自动修复**：仍属于 G2；当前需要 Review 就保持未完成，不改成自评。领域测试的受信模拟 receipt 不代表已有产品模型 reviewer。
- **只读验证快照**：当前明确为 `live_tree_rechecked` / `sandbox=False`，复核活源码前后版本。
- **完整 L2**：精确编辑授权与文件替换、ACL mask/default/new inode/并发恢复、专用身份、控制 IPC、可信后代回收、固定模型出口与凭证保护尚未完整实现/实测。详见[执行边界独立 Review](../../../shared-review/forge-harness-execution-boundary-review-20261118.md)。
- **监督进程自身消失**：普通取消、deadline 和 Service 退出可由存活 supervisor 排空；若 supervisor 被 SIGKILL 等不可捕获终止，当前只能保留 unknown 与租约，不能证明任意遗留 verifier 已停，也不能承诺继续受到 deadline 监督。
- **严格运行准入与真实模型端到端**：`require`、真实 provider/model 使用及最终组合验收留在 I1；本批不改变现有运行许可。

这些是显式待交付项，不能在后续交接中把 L1 当最终原意图完成。正常流程的 task ID、来源装配和机械检查已由控制器承担；完整“用户无感且硬规范”仍需继续完成 G2/B1/I1。

本批未提交或暂存代码，未修改服务配置、重启运行服务、创建系统用户或修改既有 ACL；未清理业务记录、Notebook 输出或其他 dirty 改动。
