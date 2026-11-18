# 开发 Harness 实施契约

2026-11-19。原始目标保持[DH.1—DH.9](../../../forge_coordination_20261104/original_task_intents.md#dh-20261118开发入口-harness-与-forge-硬规范原始意图)：通用 coding CLI 拥有模型与工具循环，Forge 从进程外落实规范，用户在工作台自然提出需求。

本轮按真人“达到最终交付的状态，多验证、测试，明早试用验收”继续 G2/B1/I1。唯一开发位置为 `fix/prompt-replay-delivery-20261118` / `_demiflow/worktrees/gpt_prompt_replay_20261118`，基线 `dab45bb`；主仓不开发。代码、运行预览与用户最终验收分别记录。G1 的原结论留在[G1 验收记录](forge-development-harness-g1-acceptance-20261118.md)，不能用于描述本版能力。

## 1. 模块契约与依赖

| 模块 | 职责 | 入口与边界 |
| --- | --- | --- |
| Forge 领域 | 唯一标准、任务基线、内容指纹、check/verify、独立 Review 收据与接受 | `forge/development/managed.py`、`taskstore.py`，复用原 tasks/runner；不导入工作台 |
| 工作台控制器 | 真实需求与上下文送达、持久状态、自动门禁、有限续修、取消和排空 | `conversations/service.py`、`store.py`、`development.py`、窄 MCP IPC |
| 外部执行边界 | 整个 CLI、验证器与其后代的文件/进程/网络约束 | `execution.py`、`sandbox*.py`；实际证明缺失即拒绝 restricted |
| 固定模型通道 | 已配置同一 provider/model 的有界文本传输；上游凭证不进入 CLI | `model_broker.py`、`model_http.py`、DeepSeek model plugins；不做 coding loop |
| 独立审阅 | 无工具、无开发会话状态的独立模型请求；验证结构化规则结论 | `review.py`、`deepseek_review.cjs`；不采信代理自报 Review |
| 运行准入 | 在已有准备/首次启动处消费当前接受身份 | `admission.py`、原 `runs/manager.py`；不新增运行引擎、队列或接受账本 |
| 产品投影 | 显示当前阶段、通过范围、停止与续修情况 | 原开发页；状态来自结构化证据，不解析聊天“完成” |

工作台单向使用 Forge。计算底座、算子、SDK 和 `demiflow/execution/` 不在本轮修改范围。

## 2. 部署与权威状态

- `kind=demiforge_registered`、`phase=implementation`、明确 `pipeline`，不传 `task_id` 即受管开发；也可显式 `managed: true`。普通用户不填写任务 ID 或执行规范命令。
- 受信部署明确 writes/watch/sources/non_goals。只有 `allow_tests: true` 才运行登记的固定验证；代理无法改写部署授权或更换 verifier。
- 每个项目主会话对应一个受管任务。首次真实消息是 original goal，后续真实消息作为 decision 去重追加；自动续修反馈不是新授权。dirty/staging、readonly、规范与补充上下文基线保持，不在续修或重启时重建。
- 权威任务、日志、审阅材料与接受记录位于 `state_root/forge-development/<workspace>/<session>/`。模型 profile 在独立私有目录；控制库、租约和模型凭证不在它的可读写范围。
- restricted 受管部署必须有明确且与项目、state 分离的 `runtime_domain`。共享运行预算仍由现有 host partition 管理。
- 旧明确 `task_id` 的 local 记录保持原解释；restricted 拒绝执行旧项目 launcher。旧 trusted_local 行为与历史保留。授权、平台实现或规范变化不静默升级既有会话，需明确重新接入，不能改旧基线来消除 stale。

## 3. 实际执行后端 B1

当前后端为 `linux-user-pid-net-landlock-seccomp-v1`，详见[边界与实际证明](forge-development-execution-boundary-20261118.md)。它不依赖只读挂载、ACL 租约或新宿主账号：

1. 受信启动器建立 user/PID/network namespace，调用者的宿主 UID 映射保持；清空 capability 与 bounding set，设置 no_new_privs，再进入候选进程。
2. Landlock ABI 1 配合 seccomp allowlist限制文件、截断/权限/链接、进程注入、namespace 与网络相关系统调用。后代继承，CLI 自有权限只作附加限制。
3. 开发只写明确源码范围和私有 HOME；固定 verifier 与图预览只写 scratch。平台代码、Python/CLI 运行时、规范和控制域与写范围重叠时拒绝。
4. 候选没有宿主网络和管理 socket，只能访问任务绑定的局部模型/控制转接；不能提供任意 URL。上游认证仅由受信传输组件持有。
5. PID namespace 的可信 PID 1 退出会回收整个后代域；父进程退出、停止、超时与资源中止均走受信清理。不能凭 provider 自报或裸 PID 消失签发接受。
6. 每文件大小、FD/请求/输入输出预算有确定上界；总 RSS、进程数和源码/私有目录磁盘增长采用有界采样保护，存在采样间隙。没有 cgroup RSS 或文件系统总量硬配额的承诺。

这是宿主上的实际组合，不宣称独立非 root 宿主账号或抵抗另一个不受管宿主 root。无边界、无安全模型路由或路径不可证明时拒绝启动，不降级。

## 4. 门禁、独立 Review 与接受

机械检查、固定验证、独立审阅三者都绑定同一内容指纹。规范、原始需求、scope、readonly、补充祖先 AGENTS/Skill、平台实现、验证定义、日志与 staging 身份都参与证据检查。分页上下文记录实际送达，不能伪造已读。

验证为 `live_tree_rechecked`：受限进程只读当前候选，前后复核指纹。没有复制不可变源码视图，不称不可变快照验证。任务字段和候选变化后旧通过事实保留为历史，当前状态变 stale。

独立审阅默认使用已配置同 provider/model 的独立无工具请求，不共享开发会话的可写记忆。材料包含规则全文、original goal/真实 decisions、授权范围、原始 dirty 基线、当前候选源码与 diff、验证定义和有界日志证据。只读非源码二进制资料以完整哈希、字节数及未审阅内容声明绑定；不把数据哈希当作语义审阅。可写候选、源码、规范与配置无法完整表示时拒绝审阅，不截断成通过。

审阅必须逐条提交 DF-01—DF-07 的有效结论；适用性和证据由控制器校验。缺少审阅者、返回不完整、来源改变或不确定响应均不能变绿。受保护的 HMAC 权威将规则结论、审阅材料摘要和候选指纹绑定；代理没有签发或接受 API。

DeepSeek 独立审阅使用自身固定的 `low` 推理等级，开发 CLI 保留部署的 `high`，provider/model 不变。单次审阅最多 16,384 输出 token、180 秒、64 KiB 结论，并受原 turn 总预算约束。只有完整正常结束的响应才能校验和签发；已确认耗尽输出预算显示审阅不可用，不伪造结论、不自动扩大额度。真实诊断已验证该路由能完成七条规则审阅，诊断本身不会修改旧任务或签收据。

`enforced` 接受还需要真实 developer 与 verifier 的外部后端完成证据、全部进程排空，以及控制器事务中的停止/时限复核。trusted_local 只能标“可信本地、未防篡改”。受管接受不是对任意业务语义的数学证明，也不是对整个存量项目的全面认证。

## 5. 有界自动续修与取消

原用户请求最多原始执行 + 3 次续修，最多 2 次独立审阅，全部共用原 turn 的总时限（本部署 900 秒）。模型传输另有每原 turn 的持久请求/连接/字节预算，跨续修 epoch 不重置。

检查或确定的 Review 失败可产生结构化反馈，原 harness 在原授权内继续修复。两次相同候选与同类失败停止；禁止绕过规范、改 verifier 或重建基线。未知发送、未知清理、验证超时/资源中止、取消不自动重放。

阶段包括 `preparing/developing/queued/checking/verifying/reviewing/repair_pending/repairing/stopping`。期间发送禁用、停止可用；停止阻止后续派发、自动批准及工具请求。`accepted/failed/stale/review_unavailable/not_applicable/cancelled/unknown` 的文字终态不能单独释放租约，必须有实际 cleanup。

Service 复用 Store 的持久 gate/epoch/repair 状态，不新增调度账本。压缩保留原始消息与契约，不创造新用户目标。服务重启只恢复可证明的状态，不猜测不确定操作是否完成。

## 6. 所有写入口与运行门禁 I1

| 入口 | 本版约束 |
| --- | --- |
| 对话开发 CLI 与工具 | B1 写范围、受保护权威记录、自动门禁与 Review |
| 管道配置编辑 | 原权限/原子保存；不获得开发接受，变化使相关当前证据 stale |
| Notebook 保存/执行 | 原 Notebook 权限、执行与历史生命周期；不会因此获签开发证书。纳入登记源码/readonly 的内容改变会使证据失效 |
| 外部编辑、CLI 本地改文件 | 不伪装受管开发；再次运行严格准入时复核当前指纹 |
| 候选算子图预览 | 受管 restricted 范围从受保护域解析，在 B1 只读进程构图；不把候选代码在宿主直接执行 |
| 新运行准备/首次启动 | `observe` 保持旧行为；显式 `require` 校验当前接受和已登记入口 |
| 历史 run/attempt 读取、停止、续跑 | 保留原历史封存与生命周期；新开发失败不改写旧事实或阻止停止 |

默认 `run_policy=observe`，避免阻断存量项目。显式 `require` 只对受管 restricted 接入开放；受保护域只记录配置引用，领域 TaskStore 与对话 Store 仍是唯一事实来源。最新用户请求须 completed、双 cleanup 完成、无停止，且当前 task 为同指纹 accepted/enforced。准备把接受身份写入原 manifest，首次启动与最后提交再次核对；省略字段不能撤销既有 require。

运行入口必须属于本次受管 pipeline 的登记流程。严格准入不从父项目隐式授权一个独立的子工作区运行根；需要精确受信接入，不能用 nested workspace 绕过。切换 require 会改变授权绑定，旧 observe 证据不能自动升级。

## 7. 交付验收口径

验收同时包括真实内核负向测试、provider 协议/独立审阅测试、取消/未知/重启/排空、严格运行准入与旧历史、真实 Chromium、已配置 DeepSeek CLI 的实际修改→门禁→独立模型审阅，以及预览运行源码核对。Codex 的协议适配与边界测试不等于已完成 Codex 真实账号推理验收。

最终结果、运行提交、试用地址和限制统一写入交付记录。生产任务和新付费接入不属于这次验收。原始 DH 与 R/15 叶不因某个测试计数或阶段完成而改写；用户明早试用验收保留为最终产品判断。
